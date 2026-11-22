import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Annotated

import osmnx as ox
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from src.control_center import ControlCenter, IncidentState
from src.messages import Phase

ROOT = Path(__file__).resolve().parent

DATA = ROOT / "data"
GRAPH_FILE = DATA / "streets.graphml"
LANDMARKS_FILE = DATA / "landmarks.json"

BBOX = (-49.269, -25.466, -49.225, -25.414)


class IncidentRequest(BaseModel):
    location: (
        tuple[
            Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False)],
            Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False)],
        ]
        | None
    ) = None


class Dashboard:
    def __init__(self, graph, landmarks):
        self.graph = graph
        self.landmarks = {"hospital": landmarks["hospital"]}
        self.started_at = time.monotonic()
        self.events = deque(maxlen=60)
        self.center = ControlCenter(
            graph,
            broker_host=os.getenv("BROKER_HOST", "127.0.0.1"),
            broker_port=int(os.getenv("BROKER_PORT", "1883")),
            offer_timeout=float(os.getenv("OFFER_TIMEOUT", "5")),
            assignment_ttl=float(os.getenv("ASSIGNMENT_TTL", "10")),
            vehicle_timeout=float(os.getenv("VEHICLE_TIMEOUT", "10")),
        )
        self.nodes = list(graph.nodes)

    def add_incident(self, location=None):
        if (
            sum(
                i.state != IncidentState.COMPLETED
                for i in self.center.incidents.values()
            )
            >= 100
        ):
            raise ValueError("Demo limit: 100 active incidents")
        if location is None:
            node = self.graph.nodes[random.choice(self.nodes)]
            location = (float(node["y"]), float(node["x"]))
        incident = self.center.add_incident(
            location, tuple(self.landmarks["hospital"]["position"])
        )
        return incident

    def snapshot(self):
        vehicles = []
        for vehicle in self.center.vehicles.values():
            state = vehicle.phase.value.lower()
            if not vehicle.connected or not vehicle.online:
                state = "offline"
            elif vehicle.offer is not None:
                state = "offered"
            elif vehicle.assignment is None:
                state = "available" if vehicle.ready else "connecting"
            vehicles.append(
                {
                    "id": vehicle.id,
                    "position": vehicle.position,
                    "state": state,
                    "connected": vehicle.connected,
                    "online": vehicle.online,
                    "incident_id": vehicle.assignment.incident_id
                    if vehicle.assignment
                    else None,
                    "assignment_id": vehicle.assignment.id
                    if vehicle.assignment
                    else None,
                    "route": vehicle.route if vehicle.assignment else [],
                    "destination": vehicle.assignment.hospital
                    if vehicle.assignment
                    else None,
                    "sequence": vehicle.sequence,
                    "last_seen_seconds": round(time.monotonic() - vehicle.last_seen, 2),
                    "lease_remaining_seconds": max(
                        0, vehicle.lease_expires_at - time.monotonic()
                    )
                    if vehicle.assignment
                    else None,
                }
            )
        incidents = []
        for incident in reversed(list(self.center.incidents.values())):
            state = incident.state.value.lower()
            vehicle = self.center.vehicles.get(incident.vehicle_id)
            if state == "offered":
                state = "waiting"
            elif (
                state == "assigned"
                and vehicle is not None
                and vehicle.phase
                in {Phase.ON_SCENE, Phase.TRANSPORTING, Phase.AT_HOSPITAL}
            ):
                state = vehicle.phase.value.lower()
            incidents.append(
                {
                    "id": incident.id,
                    "position": incident.location,
                    "state": state,
                    "vehicle_id": incident.vehicle_id,
                    "eta_seconds": None,
                }
            )
        return {
            "source": "mqtt",
            "broker_connected": self.center.broker_connected,
            "assignment_ttl_seconds": self.center.assignment_ttl,
            "time": round(time.monotonic() - self.started_at, 1),
            "time_scale": float(os.getenv("TIME_SCALE", "12")),
            "landmarks": self.landmarks,
            "bounds": [[BBOX[1], BBOX[0]], [BBOX[3], BBOX[2]]],
            "vehicles": vehicles,
            "incidents": incidents,
            "events": list(self.events),
            "stats": {
                "available": sum(v["state"] == "available" for v in vehicles),
                "active": sum(
                    i.state == IncidentState.ASSIGNED
                    for i in self.center.incidents.values()
                ),
                "waiting": sum(
                    i.state in {IncidentState.WAITING, IncidentState.OFFERED}
                    for i in self.center.incidents.values()
                ),
                "completed": sum(
                    i.state == IncidentState.COMPLETED
                    for i in self.center.incidents.values()
                ),
            },
        }


class ActivityHandler(logging.Handler):
    def __init__(self, dashboard):
        super().__init__()
        self.dashboard = dashboard

    def emit(self, record):
        message = record.getMessage()
        if " -> {" in message or " <- {" in message:
            return
        self.dashboard.events.appendleft(
            {
                "time": round(time.monotonic() - self.dashboard.started_at, 1),
                "message": message,
            }
        )


@asynccontextmanager
async def lifespan(app):
    dashboard = Dashboard(
        ox.load_graphml(GRAPH_FILE), json.loads(LANDMARKS_FILE.read_text())
    )
    app.state.dashboard = dashboard
    handler = ActivityHandler(dashboard)
    logger = logging.getLogger("src.control_center")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    task = asyncio.create_task(dashboard.center.run())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        logger.removeHandler(handler)


app = FastAPI(title="Cajuru Dispatch", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv(
        "PRESENTATION_ORIGINS", "http://127.0.0.1:4173,http://localhost:4173"
    ).split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(ROOT / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/api/state")
async def state():
    return JSONResponse(
        app.state.dashboard.snapshot(), headers={"Cache-Control": "no-store"}
    )


@app.delete("/api/events")
async def clear_events():
    events = app.state.dashboard.events
    cleared = len(events)
    events.clear()
    return {"cleared": cleared}


@app.post("/api/incidents", status_code=201)
async def create_incident(request: IncidentRequest | None = None):
    try:
        incident = app.state.dashboard.add_incident(
            request.location if request else None
        )
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return {"id": incident.id}


@app.get("/api/health")
async def health():
    connected = app.state.dashboard.center.broker_connected
    return JSONResponse(
        {"broker_connected": connected, "source": "mqtt"},
        status_code=200 if connected else 503,
    )
