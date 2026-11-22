import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import count
from pathlib import Path
from uuid import uuid4

import aiomqtt
import networkx as nx
import osmnx as ox
from pydantic import ValidationError

from src.messages import (
    Assignment,
    AssignmentCompleted,
    AssignmentConfirmed,
    Command,
    Disconnected,
    Envelope,
    Event,
    EventType,
    Offered,
    OfferResponse,
    OfferWithdrawn,
    Phase,
    Ready,
    Registration,
    RegistrationRequired,
    Revoked,
    State,
    Telemetry,
    VehicleMessage,
)

log = logging.getLogger(__name__)

DEADLINE_CHECK_INTERVAL_SECONDS = 0.1
RECONNECT_DELAY_SECONDS = 1
MQTT_KEEPALIVE_SECONDS = 5
MQTT_TIMEOUT_SECONDS = 5


class IncidentState(StrEnum):
    WAITING = "WAITING"
    OFFERED = "OFFERED"
    ASSIGNED = "ASSIGNED"
    COMPLETED = "COMPLETED"


@dataclass
class Incident:
    id: str
    location: tuple[float, float]
    hospital: tuple[float, float]
    state: IncidentState = IncidentState.WAITING
    vehicle_id: str | None = None
    rejected: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class PendingOffer:
    id: str
    incident_id: str
    expires_at: float


@dataclass
class RegisteredVehicle:
    id: str
    position: tuple[float, float]
    events: asyncio.Queue[Event]
    last_seen: float
    online: bool = True
    connected: bool = True
    ready: bool = False
    phase: Phase = Phase.IDLE
    sequence: int = -1
    routing_node: int | None = None
    remaining_edge_metres: float = 0
    route: list[tuple[float, float]] = field(default_factory=list)
    offer: PendingOffer | None = None
    assignment: Assignment | None = None
    confirmed_offer_id: str | None = None
    lease_expires_at: float = 0


class ControlCenter:
    def __init__(
        self,
        graph: nx.MultiDiGraph,
        *,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        offer_timeout: float = 5,
        assignment_ttl: float = 10,
        vehicle_timeout: float = 10,
    ):
        self.broker_connected = False
        self.vehicles: dict[str, RegisteredVehicle] = {}
        self.incidents: dict[str, Incident] = {}
        self._graph = graph
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._sessions: dict[str, str] = {}
        self._retired: dict[str, set[str]] = {}
        self.offer_timeout = offer_timeout
        self.assignment_ttl = assignment_ttl
        self.vehicle_timeout = vehicle_timeout
        self._offer_ids = count(1)
        self._assignment_ids = count(1)
        self._identity = uuid4().hex[:8]

    def _node(self, position: tuple[float, float]) -> int:
        lat, lon = position
        return int(ox.distance.nearest_nodes(self._graph, X=lon, Y=lat))

    def _send(self, vehicle: RegisteredVehicle, event: Event):
        if vehicle.connected:
            log.info("%s <- %s", vehicle.id, event.model_dump_json())
            vehicle.events.put_nowait(event)

    def register(
        self,
        vehicle_id: str,
        report: Telemetry,
        events: asyncio.Queue[Event],
    ):
        if report.routing_node is not None and report.routing_node not in self._graph:
            log.warning("Invalid routing node from %s", vehicle_id)
            return
        self.check_timeouts()
        now = time.monotonic()
        vehicle = self.vehicles.get(vehicle_id)
        if vehicle is None:
            vehicle = RegisteredVehicle(vehicle_id, report.position, events, now)
            self.vehicles[vehicle_id] = vehicle
        else:
            if vehicle.offer is not None:
                self._withdraw_offer(vehicle, vehicle.offer)
            vehicle.events = events
        vehicle.position = report.position
        vehicle.last_seen = now
        vehicle.online = True
        vehicle.connected = True
        vehicle.ready = False
        vehicle.sequence = -1
        vehicle.routing_node = report.routing_node
        vehicle.route = report.route
        vehicle.remaining_edge_metres = report.remaining_edge_metres
        if (
            vehicle.assignment is not None
            and report.assignment_id == vehicle.assignment.id
        ):
            vehicle.phase = report.phase
            vehicle.lease_expires_at = now + self.assignment_ttl
        for incident in self.incidents.values():
            incident.rejected.discard(vehicle_id)
        log.info("%s registered at %s", vehicle_id, report.position)
        self._send(vehicle, Ready(assignment=vehicle.assignment, phase=vehicle.phase))
        self._dispatch(now)

    def disconnect(self, vehicle_id: str):
        vehicle = self.vehicles[vehicle_id]
        vehicle.online = False
        vehicle.connected = False
        vehicle.ready = False
        log.info("%s offline; active assignment keeps its existing TTL", vehicle_id)

    def add_incident(
        self,
        location: tuple[float, float],
        hospital: tuple[float, float],
    ) -> Incident:
        if not nx.has_path(self._graph, self._node(location), self._node(hospital)):
            raise ValueError("Incident location has no road route to the hospital")
        incident = Incident(f"INC-{len(self.incidents) + 1:03}", location, hospital)
        self.incidents[incident.id] = incident
        log.info("%s created at %s", incident.id, location)
        self.check_timeouts()
        return incident

    def _dispatch(self, now: float):
        for incident in self.incidents.values():
            if incident.state != IncidentState.WAITING:
                continue
            eligible = [
                vehicle
                for vehicle in self.vehicles.values()
                if vehicle.online
                and vehicle.connected
                and vehicle.ready
                and vehicle.offer is None
                and vehicle.assignment is None
                and vehicle.id not in incident.rejected
            ]
            if not eligible:
                continue
            distances = nx.single_source_dijkstra_path_length(
                self._graph.reverse(copy=False),
                self._node(incident.location),
                weight="length",
            )
            candidates = []
            for vehicle in eligible:
                origin = (
                    vehicle.routing_node
                    if vehicle.routing_node is not None
                    else self._node(vehicle.position)
                )
                distance = (
                    distances.get(origin, math.inf) + vehicle.remaining_edge_metres
                )
                if math.isfinite(distance):
                    candidates.append((distance, vehicle.id))
            if not candidates:
                continue
            distance, vehicle_id = min(candidates)
            vehicle = self.vehicles[vehicle_id]
            offer = PendingOffer(
                f"offer-{self._identity}-{next(self._offer_ids)}",
                incident.id,
                now + self.offer_timeout,
            )
            vehicle.offer = offer
            vehicle.ready = False
            incident.state = IncidentState.OFFERED
            incident.vehicle_id = vehicle_id
            log.info(
                "%s selected for %s: %.0f m by road", vehicle_id, incident.id, distance
            )
            self._send(
                vehicle,
                Offered(
                    offer_id=offer.id,
                    incident_id=incident.id,
                    location=incident.location,
                ),
            )

    def _withdraw_offer(self, vehicle: RegisteredVehicle, offer: PendingOffer):
        incident = self.incidents[offer.incident_id]
        incident.state = IncidentState.WAITING
        incident.vehicle_id = None
        vehicle.offer = None
        vehicle.ready = False
        self._send(vehicle, OfferWithdrawn(offer_id=offer.id))

    def _clear_assignment(self, vehicle: RegisteredVehicle):
        vehicle.assignment = None
        vehicle.confirmed_offer_id = None
        vehicle.lease_expires_at = 0
        vehicle.phase = Phase.IDLE
        vehicle.ready = False

    def _requeue(self, vehicle: RegisteredVehicle, assignment: Assignment, reason: str):
        incident = self.incidents[assignment.incident_id]
        incident.state = IncidentState.WAITING
        incident.vehicle_id = None
        self._clear_assignment(vehicle)
        log.warning("%s released from %s: %s", vehicle.id, incident.id, reason)
        self._send(vehicle, Revoked(assignment_id=assignment.id))

    def _accept_offer(
        self, vehicle: RegisteredVehicle, response: OfferResponse, now: float
    ):
        if (
            vehicle.assignment is not None
            and vehicle.confirmed_offer_id == response.offer_id
        ):
            self._send(
                vehicle,
                AssignmentConfirmed(
                    offer_id=response.offer_id,
                    assignment=vehicle.assignment,
                ),
            )
            return
        offer = vehicle.offer
        if offer is None or offer.id != response.offer_id:
            self._send(vehicle, OfferWithdrawn(offer_id=response.offer_id))
            return
        incident = self.incidents[offer.incident_id]
        if not response.accepted:
            incident.rejected.add(vehicle.id)
            self._withdraw_offer(vehicle, offer)
            return
        assignment = Assignment(
            id=f"assignment-{self._identity}-{next(self._assignment_ids)}",
            incident_id=incident.id,
            location=incident.location,
            hospital=incident.hospital,
        )
        vehicle.offer = None
        vehicle.assignment = assignment
        vehicle.confirmed_offer_id = offer.id
        vehicle.lease_expires_at = now + self.assignment_ttl
        vehicle.ready = False
        vehicle.phase = Phase.RESPONDING
        incident.state = IncidentState.ASSIGNED
        log.info(
            "%s confirmed for %s; TTL %.1fs",
            assignment.id,
            vehicle.id,
            self.assignment_ttl,
        )
        self._send(
            vehicle, AssignmentConfirmed(offer_id=offer.id, assignment=assignment)
        )

    def _telemetry(self, vehicle: RegisteredVehicle, report: Telemetry, now: float):
        if report.vehicle_id != vehicle.id or report.sequence <= vehicle.sequence:
            return
        if report.routing_node is not None and report.routing_node not in self._graph:
            log.warning("Invalid routing node from %s", vehicle.id)
            return
        vehicle.sequence = report.sequence
        vehicle.online = True
        vehicle.last_seen = now
        vehicle.position = report.position
        vehicle.routing_node = report.routing_node
        vehicle.route = report.route
        vehicle.remaining_edge_metres = report.remaining_edge_metres
        assignment = vehicle.assignment
        if assignment is not None:
            vehicle.ready = False
            if report.assignment_id != assignment.id:
                if (
                    report.state == State.ACCEPTING
                    and vehicle.confirmed_offer_id is not None
                ):
                    self._send(
                        vehicle,
                        AssignmentConfirmed(
                            offer_id=vehicle.confirmed_offer_id,
                            assignment=assignment,
                        ),
                    )
                return
            if report.phase == Phase.FAILED:
                self.incidents[assignment.incident_id].rejected.add(vehicle.id)
                self._requeue(vehicle, assignment, "Vehicle reported a failed route")
                return
            vehicle.phase = report.phase
            vehicle.lease_expires_at = now + self.assignment_ttl
            if report.phase == Phase.COMPLETED:
                self.incidents[assignment.incident_id].state = IncidentState.COMPLETED
                self._clear_assignment(vehicle)
                log.info("%s completed by %s", assignment.incident_id, vehicle.id)
                self._send(vehicle, AssignmentCompleted(assignment_id=assignment.id))
        elif report.assignment_id is not None:
            vehicle.ready = False
            incident = self.incidents.get(report.incident_id or "")
            if incident is not None and incident.state == IncidentState.COMPLETED:
                self._send(
                    vehicle, AssignmentCompleted(assignment_id=report.assignment_id)
                )
            else:
                self._send(vehicle, Revoked(assignment_id=report.assignment_id))
        else:
            vehicle.ready = report.state == State.READY and vehicle.offer is None
            vehicle.phase = Phase.IDLE

    def handle(self, vehicle_id: str, command: Command):
        now = time.monotonic()
        self._expire(now)
        vehicle = self.vehicles[vehicle_id]
        if not vehicle.connected:
            return
        log.info("%s -> %s", vehicle_id, command.model_dump_json())
        match command.type:
            case EventType.OFFER_RESPONSE:
                self._accept_offer(vehicle, command, now)
            case EventType.TELEMETRY:
                self._telemetry(vehicle, command, now)
            case EventType.ROUTE_FAILED:
                assignment = vehicle.assignment
                if assignment is not None and assignment.id == command.assignment_id:
                    self.incidents[assignment.incident_id].rejected.add(vehicle_id)
                    self._requeue(vehicle, assignment, command.reason)
        self._dispatch(now)

    def _expire(self, now: float):
        for vehicle in self.vehicles.values():
            offer = vehicle.offer
            if offer is not None and now >= offer.expires_at:
                log.warning("%s expired for %s", offer.id, vehicle.id)
                self._withdraw_offer(vehicle, offer)
            assignment = vehicle.assignment
            if assignment is not None and now >= vehicle.lease_expires_at:
                self._requeue(vehicle, assignment, "Assignment TTL expired")
                vehicle.online = False
            if vehicle.online and now - vehicle.last_seen >= self.vehicle_timeout:
                vehicle.online = False
                vehicle.ready = False
                log.warning("%s stopped reporting", vehicle.id)

    def check_timeouts(self):
        now = time.monotonic()
        self._expire(now)
        self._dispatch(now)

    async def _check_deadlines(self):
        while True:
            await asyncio.sleep(DEADLINE_CHECK_INTERVAL_SECONDS)
            self.check_timeouts()

    async def _forward(
        self,
        client: aiomqtt.Client,
        vehicle_id: str,
        session_id: str,
        events: asyncio.Queue[Event],
    ):
        while True:
            event = await events.get()
            await client.publish(
                f"cajuru/vehicles/{vehicle_id}/inbox",
                Envelope(session_id=session_id, payload=event).model_dump_json(),
                qos=1,
                retain=False,
            )

    def _register_session(
        self,
        client: aiomqtt.Client,
        tasks: asyncio.TaskGroup,
        senders: dict[str, asyncio.Task[None]],
        vehicle_id: str,
        session_id: str,
        report: Telemetry,
    ):
        current = self._sessions.get(vehicle_id)
        if report.vehicle_id != vehicle_id:
            return
        if report.routing_node is not None and report.routing_node not in self._graph:
            log.warning("Invalid registration routing node from %s", vehicle_id)
            return
        if session_id in self._retired.get(vehicle_id, set()):
            return
        if current == session_id:
            vehicle = self.vehicles[vehicle_id]
            vehicle.events.put_nowait(
                Ready(assignment=vehicle.assignment, phase=vehicle.phase)
            )
            return
        if current is not None:
            self._retired.setdefault(vehicle_id, set()).add(current)
            senders[vehicle_id].cancel()
        events: asyncio.Queue[Event] = asyncio.Queue()
        self._sessions[vehicle_id] = session_id
        self.register(vehicle_id, report, events)
        senders[vehicle_id] = tasks.create_task(
            self._forward(client, vehicle_id, session_id, events)
        )

    async def _receive(self, client: aiomqtt.Client):
        senders: dict[str, asyncio.Task[None]] = {}
        async with asyncio.TaskGroup() as tasks:
            async for message in client.messages:
                if message.retain or not isinstance(
                    message.payload, (str, bytes, bytearray)
                ):
                    continue
                vehicle_id = str(message.topic).rsplit("/", 1)[1]
                try:
                    envelope = Envelope[VehicleMessage].model_validate_json(
                        message.payload
                    )
                except ValidationError as error:
                    log.warning("Invalid vehicle message: %s", error)
                    continue
                session_id = envelope.session_id
                payload = envelope.payload
                current = self._sessions.get(vehicle_id)
                if isinstance(payload, Registration):
                    self._register_session(
                        client,
                        tasks,
                        senders,
                        vehicle_id,
                        session_id,
                        payload.snapshot,
                    )
                elif current != session_id:
                    if session_id not in self._retired.get(vehicle_id, set()):
                        await client.publish(
                            f"cajuru/vehicles/{vehicle_id}/inbox",
                            Envelope(
                                session_id=session_id, payload=RegistrationRequired()
                            ).model_dump_json(),
                            qos=1,
                            retain=False,
                        )
                elif isinstance(payload, Disconnected):
                    self.disconnect(vehicle_id)
                else:
                    self.handle(vehicle_id, payload)

    async def _connect(self):
        while True:
            try:
                async with aiomqtt.Client(
                    self._broker_host,
                    self._broker_port,
                    username=os.getenv("BROKER_USERNAME", "central"),
                    password=os.getenv("BROKER_PASSWORD", "cajuru123"),
                    identifier="cajuru-central",
                    keepalive=MQTT_KEEPALIVE_SECONDS,
                    timeout=MQTT_TIMEOUT_SECONDS,
                ) as client:
                    await client.subscribe("cajuru/central/inbox/+", qos=1)
                    self.broker_connected = True
                    log.info("Central subscribed to cajuru/central/inbox/+")
                    await self._receive(client)
            except* aiomqtt.MqttError as errors:
                log.warning("Central MQTT connection failed: %s", errors)
            self.broker_connected = False
            for vehicle_id in self._sessions:
                self.disconnect(vehicle_id)
            self._sessions.clear()
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def run(self):
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._check_deadlines())
            tasks.create_task(self._connect())


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    graph = ox.load_graphml(
        Path(__file__).resolve().parent.parent / "data/streets.graphml"
    )
    center = ControlCenter(
        graph,
        broker_host=os.getenv("BROKER_HOST", "127.0.0.1"),
        broker_port=int(os.getenv("BROKER_PORT", "1883")),
        offer_timeout=float(os.getenv("OFFER_TIMEOUT", "5")),
        assignment_ttl=float(os.getenv("ASSIGNMENT_TTL", "10")),
        vehicle_timeout=float(os.getenv("VEHICLE_TIMEOUT", "10")),
    )
    center.add_incident(
        location=(-25.436937, -49.2439694),
        hospital=(-25.4359282, -49.2454083),
    )
    asyncio.run(center.run())


if __name__ == "__main__":
    main()
