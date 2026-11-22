import asyncio
import logging
import os
import time
from collections import deque
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Literal
from uuid import uuid4

import aiomqtt
import networkx as nx
import osmnx as ox
from pydantic import BaseModel, ValidationError
from pyproj import Transformer
from shapely import LineString, Point
from shapely.ops import substring

from src.messages import (
    Assignment,
    AssignmentCompleted,
    AssignmentConfirmed,
    CenterMessage,
    Command,
    Connected,
    Disconnected,
    Envelope,
    EventType,
    Offered,
    OfferResponse,
    OfferWithdrawn,
    Phase,
    Ready,
    Registration,
    RegistrationRequired,
    Revoked,
    RouteFailed,
    State,
    Telemetry,
)

log = logging.getLogger(__name__)

SPEED_MPS = 40 / 3.6
PICKUP_SECONDS = 20
HANDOVER_SECONDS = 15
SIMULATION_TICK_SECONDS = 0.1
MAX_SIMULATION_STEP_SECONDS = 0.5
REGISTRATION_RETRY_SECONDS = 2
RECONNECT_DELAY_SECONDS = 1
MQTT_KEEPALIVE_SECONDS = 5
MQTT_TIMEOUT_SECONDS = 5


class LocalEventType(StrEnum):
    LOCATION_UPDATED = "location_updated"
    TICK = "tick"
    REPORT_TELEMETRY = "report_telemetry"


ALLOWED_STATES = {
    EventType.CONNECTED: {State.CREATED, State.DISCONNECTED},
    EventType.READY: {State.CONNECTED},
    EventType.OFFERED: {State.READY},
    EventType.ASSIGNMENT_CONFIRMED: {State.ACCEPTING},
    EventType.ASSIGNMENT_COMPLETED: {State.ASSIGNED},
    EventType.OFFER_WITHDRAWN: {State.ACCEPTING},
    EventType.REVOKED: {State.ASSIGNED},
    EventType.DISCONNECTED: set(State) - {State.DISCONNECTED},
    LocalEventType.LOCATION_UPDATED: set(State),
    LocalEventType.TICK: {State.ASSIGNED},
    LocalEventType.REPORT_TELEMETRY: {
        State.READY,
        State.ACCEPTING,
        State.ASSIGNED,
    },
}


class LocationUpdated(BaseModel, frozen=True):
    type: Literal[LocalEventType.LOCATION_UPDATED] = LocalEventType.LOCATION_UPDATED
    position: tuple[float, float]


class Tick(BaseModel, frozen=True):
    type: Literal[LocalEventType.TICK] = LocalEventType.TICK
    seconds: float


class ReportTelemetry(BaseModel, frozen=True):
    type: Literal[LocalEventType.REPORT_TELEMETRY] = LocalEventType.REPORT_TELEMETRY


type Input = (
    Connected
    | Ready
    | Offered
    | AssignmentConfirmed
    | AssignmentCompleted
    | OfferWithdrawn
    | Revoked
    | Disconnected
    | LocationUpdated
    | Tick
    | ReportTelemetry
)


class Ambulance:
    def __init__(
        self,
        vehicle_id: str,
        graph: nx.MultiDiGraph,
        position: tuple[float, float],
        *,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        time_scale: float = 12,
        telemetry_interval: float = 1,
    ):
        self.id = vehicle_id
        self.state = State.CREATED
        self.phase = Phase.IDLE
        self.position = position
        self.offer: Offered | None = None
        self.assignment: Assignment | None = None
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._client: aiomqtt.Client | None = None
        self._session_id = ""
        self._registered = False
        self._graph = ox.project_graph(graph)
        self._to_xy = Transformer.from_crs(
            "EPSG:4326", self._graph.graph["crs"], always_xy=True
        )
        self._to_latlon = Transformer.from_crs(
            self._graph.graph["crs"], "EPSG:4326", always_xy=True
        )
        self._time_scale = time_scale
        self._telemetry_interval = telemetry_interval
        self._events: asyncio.Queue[Input] = asyncio.Queue()
        self._commands: asyncio.Queue[Command] = asyncio.Queue()
        self._sending: asyncio.Task[None] | None = None
        self._route: deque[tuple[int, LineString]] = deque()
        self._edge: tuple[int, LineString] | None = None
        self._travelled = 0.0
        self._remaining_stop = 0.0
        self._sequence = 0

    def _transition(self, state: State):
        previous = self.state
        self.state = state
        log.info("%s: %s -> %s", self.id, previous, state)

    def _set_phase(self, phase: Phase):
        previous = self.phase
        self.phase = phase
        log.info("%s: %s -> %s", self.id, previous, phase)

    def _set_route(self, destination: tuple[float, float]):
        if self._edge is not None:
            origin, _ = self._edge
        else:
            lat, lon = self.position
            x, y = self._to_xy.transform(lon, lat)
            origin = ox.distance.nearest_nodes(self._graph, X=x, Y=y)
        lat, lon = destination
        x, y = self._to_xy.transform(lon, lat)
        target = ox.distance.nearest_nodes(self._graph, X=x, Y=y)
        path = nx.shortest_path(self._graph, origin, target, weight="length")
        route: deque[tuple[int, LineString]] = deque()
        if len(path) > 1:
            edges = ox.routing.route_to_gdf(self._graph, path, weight="length")
            for (start, end), line in zip(pairwise(path), edges.geometry, strict=True):
                node = self._graph.nodes[start]
                point = Point(node["x"], node["y"])
                if point.distance(Point(line.coords[-1])) < point.distance(
                    Point(line.coords[0])
                ):
                    line = line.reverse()
                route.append((end, line))
        self._route = route
        if self._edge is None:
            self._edge = self._route.popleft() if self._route else None
            self._travelled = 0.0
        length = sum(line.length for _, line in self._route)
        if self._edge is not None:
            length += self._edge[1].length - self._travelled
        log.info("%s: route to %s, %.0f m", self.id, destination, length)

    def _start_assignment(self, assignment: Assignment, phase: Phase):
        self.offer = None
        if self.assignment == assignment and self.phase == phase:
            log.info(
                "%s: resuming %s in %s, stop remaining %.1fs",
                self.id,
                assignment.id,
                phase,
                self._remaining_stop,
            )
            self._transition(State.ASSIGNED)
            return

        self.assignment = assignment
        self._remaining_stop = 0.0
        self._set_phase(phase)
        try:
            if phase == Phase.RESPONDING:
                self._set_route(assignment.location)
            elif phase == Phase.TRANSPORTING:
                self._set_route(assignment.hospital)
            else:
                self._route.clear()
                self._edge = None
                self._travelled = 0.0
                if phase == Phase.ON_SCENE:
                    self._remaining_stop = PICKUP_SECONDS
                elif phase == Phase.AT_HOSPITAL:
                    self._remaining_stop = HANDOVER_SECONDS
        except (nx.NetworkXNoPath, nx.NodeNotFound) as error:
            self._report_route_failure(assignment, error)
        self._transition(State.ASSIGNED)

    def _report_route_failure(self, assignment: Assignment, error: Exception):
        self._route.clear()
        self._set_phase(Phase.FAILED)
        self._commands.put_nowait(
            RouteFailed(
                assignment_id=assignment.id,
                reason=str(error),
            )
        )
        log.error("%s: route failed for %s: %s", self.id, assignment.id, error)

    def _release_assignment(self):
        self.assignment = None
        self._route.clear()
        self._remaining_stop = 0.0
        self._set_phase(Phase.IDLE)
        self._transition(State.READY)

    def _move(self, seconds: float) -> bool:
        self._travelled += SPEED_MPS * seconds
        while self._edge is not None:
            _, line = self._edge
            point = line.interpolate(self._travelled)
            lon, lat = self._to_latlon.transform(point.x, point.y)
            self.position = (lat, lon)
            if self._travelled < line.length:
                return False
            self._travelled -= line.length
            self._edge = self._route.popleft() if self._route else None
        self._travelled = 0.0
        return True

    def _advance(self, seconds: float):
        while seconds > 1e-8:
            step = min(seconds, MAX_SIMULATION_STEP_SECONDS)
            seconds -= step
            if self.phase == Phase.RESPONDING:
                if self._move(step):
                    self._set_phase(Phase.ON_SCENE)
                    self._remaining_stop = PICKUP_SECONDS
            elif self.phase == Phase.ON_SCENE:
                self._remaining_stop -= step
                if self._remaining_stop <= 0 and self.assignment is not None:
                    try:
                        self._set_route(self.assignment.hospital)
                    except (nx.NetworkXNoPath, nx.NodeNotFound) as error:
                        self._report_route_failure(self.assignment, error)
                    else:
                        self._set_phase(Phase.TRANSPORTING)
            elif self.phase == Phase.TRANSPORTING:
                if self._move(step):
                    self._set_phase(Phase.AT_HOSPITAL)
                    self._remaining_stop = HANDOVER_SECONDS
            elif self.phase == Phase.AT_HOSPITAL:
                self._remaining_stop -= step
                if self._remaining_stop <= 0:
                    self._set_phase(Phase.COMPLETED)

    def snapshot(self) -> Telemetry:
        route = [self.position]
        lines = [line for _, line in self._route]
        if self._edge is not None:
            remaining = substring(self._edge[1], self._travelled, self._edge[1].length)
            if isinstance(remaining, LineString):
                lines.insert(0, remaining)
        for line in lines:
            for x, y in line.coords:
                lon, lat = self._to_latlon.transform(x, y)
                route.append((lat, lon))
        return Telemetry(
            route=route if len(route) > 1 else [],
            vehicle_id=self.id,
            sequence=self._sequence,
            state=self.state,
            phase=self.phase,
            position=self.position,
            assignment_id=self.assignment.id if self.assignment else None,
            incident_id=self.assignment.incident_id if self.assignment else None,
            routing_node=self._edge[0] if self._edge else None,
            remaining_edge_metres=(
                self._edge[1].length - self._travelled if self._edge else 0
            ),
        )

    def _report_telemetry(self):
        self._sequence += 1
        self._commands.put_nowait(self.snapshot())

    def _update_position(self, position: tuple[float, float]):
        self.position = position
        if self._edge is not None:
            lat, lon = position
            point = Point(*self._to_xy.transform(lon, lat))
            edges = [self._edge, *self._route]
            index, (end, line) = min(
                enumerate(edges),
                key=lambda item: item[1][1].distance(point),
            )
            progress = line.project(point)
            if index:
                self._edge = (end, line)
                self._route = deque(edges[index + 1 :])
                self._travelled = progress
            else:
                self._travelled = max(self._travelled, progress)
            point = line.interpolate(self._travelled)
            lon, lat = self._to_latlon.transform(point.x, point.y)
            self.position = (lat, lon)

    def _handle(self, event: Input):
        if self.state not in ALLOWED_STATES[event.type]:
            return

        match event.type:
            case EventType.CONNECTED:
                self._transition(State.CONNECTED)

            case EventType.READY:
                if event.assignment is None:
                    self.offer = None
                    self._release_assignment()
                else:
                    self._start_assignment(event.assignment, event.phase)

            case EventType.OFFERED:
                self.offer = event
                self._commands.put_nowait(
                    OfferResponse(
                        offer_id=event.offer_id,
                        accepted=True,
                    )
                )
                self._transition(State.ACCEPTING)

            case EventType.ASSIGNMENT_CONFIRMED:
                if self.offer is None or self.offer.offer_id != event.offer_id:
                    return
                self._start_assignment(event.assignment, Phase.RESPONDING)

            case EventType.ASSIGNMENT_COMPLETED:
                if (
                    self.assignment is None
                    or self.assignment.id != event.assignment_id
                    or self.phase != Phase.COMPLETED
                ):
                    return
                self._release_assignment()

            case EventType.OFFER_WITHDRAWN:
                if self.offer is None or self.offer.offer_id != event.offer_id:
                    return
                self.offer = None
                self._transition(State.READY)

            case EventType.REVOKED:
                if self.assignment is None or self.assignment.id != event.assignment_id:
                    return
                self._release_assignment()

            case EventType.DISCONNECTED:
                while not self._commands.empty():
                    self._commands.get_nowait()
                if self._sending is not None:
                    self._sending.cancel()
                self.offer = None
                self._transition(State.DISCONNECTED)
                log.warning("%s disconnected: %s", self.id, event.reason)

            case LocalEventType.LOCATION_UPDATED:
                self._update_position(event.position)

            case LocalEventType.TICK:
                self._advance(event.seconds)

            case LocalEventType.REPORT_TELEMETRY:
                self._report_telemetry()

    async def update_location(self, latitude: float, longitude: float):
        await self._events.put(LocationUpdated(position=(latitude, longitude)))

    async def _register(self, client: aiomqtt.Client):
        await client.publish(
            f"cajuru/central/inbox/{self.id}",
            Envelope(
                session_id=self._session_id,
                payload=Registration(snapshot=self.snapshot()),
            ).model_dump_json(),
            qos=1,
            retain=False,
        )

    async def _receive(self):
        while True:
            reason = "Central requested registration"
            self._session_id = uuid4().hex
            will = aiomqtt.Will(
                f"cajuru/central/inbox/{self.id}",
                Envelope(
                    session_id=self._session_id,
                    payload=Disconnected(reason="MQTT connection lost"),
                ).model_dump_json(),
                qos=1,
                retain=False,
            )
            try:
                async with aiomqtt.Client(
                    self._broker_host,
                    self._broker_port,
                    username=os.getenv("BROKER_USERNAME", self.id),
                    password=os.getenv("BROKER_PASSWORD", "cajuru123"),
                    identifier=f"cajuru-{self.id}",
                    will=will,
                    keepalive=MQTT_KEEPALIVE_SECONDS,
                    timeout=MQTT_TIMEOUT_SECONDS,
                ) as client:
                    self._client = client
                    await client.subscribe(f"cajuru/vehicles/{self.id}/inbox", qos=1)
                    log.info("%s connected; session %s", self.id, self._session_id)
                    self._events.put_nowait(Connected())
                    await self._register(client)
                    while True:
                        try:
                            async with asyncio.timeout(
                                None if self._registered else REGISTRATION_RETRY_SECONDS
                            ):
                                message = await anext(client.messages)
                        except TimeoutError:
                            await self._register(client)
                            continue
                        if message.retain or not isinstance(
                            message.payload, (str, bytes, bytearray)
                        ):
                            continue
                        try:
                            envelope = Envelope[CenterMessage].model_validate_json(
                                message.payload
                            )
                        except ValidationError as error:
                            log.warning("Invalid central message: %s", error)
                            continue
                        if envelope.session_id != self._session_id:
                            continue
                        if isinstance(envelope.payload, RegistrationRequired):
                            break
                        event = envelope.payload
                        if event.type == EventType.READY:
                            self._registered = True
                        self._events.put_nowait(event)
            except aiomqtt.MqttError as error:
                reason = str(error)
                log.warning("%s MQTT connection failed: %s", self.id, error)
            self._client = None
            self._registered = False
            self._events.put_nowait(Disconnected(reason=reason))
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def _publish(self, command: Command):
        client = self._client
        if client is None or not self._registered:
            return
        try:
            await client.publish(
                f"cajuru/central/inbox/{self.id}",
                Envelope(
                    session_id=self._session_id, payload=command
                ).model_dump_json(),
                qos=1,
                retain=False,
            )
        except aiomqtt.MqttError as error:
            log.warning("%s publish failed: %s", self.id, error)

    async def _send(self):
        while True:
            command = await self._commands.get()
            self._sending = asyncio.create_task(self._publish(command))
            try:
                await self._sending
            except asyncio.CancelledError:
                sender = asyncio.current_task()
                if sender is not None and sender.cancelling():
                    raise
            self._sending = None

    async def _process_events(self):
        while True:
            self._handle(await self._events.get())

    async def _simulate(self):
        previous = time.monotonic()
        while True:
            await asyncio.sleep(SIMULATION_TICK_SECONDS)
            current = time.monotonic()
            await self._events.put(
                Tick(seconds=(current - previous) * self._time_scale)
            )
            previous = current

    async def _telemetry(self):
        while True:
            await asyncio.sleep(self._telemetry_interval)
            await self._events.put(ReportTelemetry())

    async def run(self):
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._receive())
            tasks.create_task(self._process_events())
            tasks.create_task(self._send())
            tasks.create_task(self._simulate())
            tasks.create_task(self._telemetry())


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    vehicle_id = os.getenv("VEHICLE_ID", "AMB-01")
    graph = ox.load_graphml(
        Path(__file__).resolve().parent.parent / "data/streets.graphml"
    )
    ambulance = Ambulance(
        vehicle_id,
        graph,
        position=(
            float(os.getenv("VEHICLE_LAT", "-25.4386732")),
            float(os.getenv("VEHICLE_LON", "-49.23786")),
        ),
        broker_host=os.getenv("BROKER_HOST", "127.0.0.1"),
        broker_port=int(os.getenv("BROKER_PORT", "1883")),
        time_scale=float(os.getenv("TIME_SCALE", "12")),
        telemetry_interval=float(os.getenv("TELEMETRY_INTERVAL", "1")),
    )
    asyncio.run(ambulance.run())


if __name__ == "__main__":
    main()
