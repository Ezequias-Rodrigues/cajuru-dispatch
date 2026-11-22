from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class State(StrEnum):
    CREATED = "CREATED"
    CONNECTED = "CONNECTED"
    READY = "READY"
    ACCEPTING = "ACCEPTING"
    ASSIGNED = "ASSIGNED"
    DISCONNECTED = "DISCONNECTED"


class Phase(StrEnum):
    IDLE = "IDLE"
    RESPONDING = "RESPONDING"
    ON_SCENE = "ON_SCENE"
    TRANSPORTING = "TRANSPORTING"
    AT_HOSPITAL = "AT_HOSPITAL"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class EventType(StrEnum):
    CONNECTED = "connected"
    READY = "ready"
    OFFERED = "offered"
    ASSIGNMENT_CONFIRMED = "assignment_confirmed"
    ASSIGNMENT_COMPLETED = "assignment_completed"
    OFFER_WITHDRAWN = "offer_withdrawn"
    REVOKED = "revoked"
    DISCONNECTED = "disconnected"
    OFFER_RESPONSE = "offer_response"
    TELEMETRY = "telemetry"
    ROUTE_FAILED = "route_failed"


class Assignment(BaseModel, frozen=True):
    id: str
    incident_id: str
    location: tuple[float, float]
    hospital: tuple[float, float]


class Connected(BaseModel, frozen=True):
    type: Literal[EventType.CONNECTED] = EventType.CONNECTED


class Ready(BaseModel, frozen=True):
    type: Literal[EventType.READY] = EventType.READY
    assignment: Assignment | None = None
    phase: Phase = Phase.RESPONDING


class Offered(BaseModel, frozen=True):
    type: Literal[EventType.OFFERED] = EventType.OFFERED
    offer_id: str
    incident_id: str
    location: tuple[float, float]


class AssignmentConfirmed(BaseModel, frozen=True):
    type: Literal[EventType.ASSIGNMENT_CONFIRMED] = EventType.ASSIGNMENT_CONFIRMED
    offer_id: str
    assignment: Assignment


class AssignmentCompleted(BaseModel, frozen=True):
    type: Literal[EventType.ASSIGNMENT_COMPLETED] = EventType.ASSIGNMENT_COMPLETED
    assignment_id: str


class OfferWithdrawn(BaseModel, frozen=True):
    type: Literal[EventType.OFFER_WITHDRAWN] = EventType.OFFER_WITHDRAWN
    offer_id: str


class Revoked(BaseModel, frozen=True):
    type: Literal[EventType.REVOKED] = EventType.REVOKED
    assignment_id: str


class Disconnected(BaseModel, frozen=True):
    type: Literal[EventType.DISCONNECTED] = EventType.DISCONNECTED
    reason: str


type Event = Annotated[
    Connected
    | Ready
    | Offered
    | AssignmentConfirmed
    | AssignmentCompleted
    | OfferWithdrawn
    | Revoked
    | Disconnected,
    Field(discriminator="type"),
]


class OfferResponse(BaseModel, frozen=True):
    type: Literal[EventType.OFFER_RESPONSE] = EventType.OFFER_RESPONSE
    offer_id: str
    accepted: bool


class Telemetry(BaseModel, frozen=True):
    type: Literal[EventType.TELEMETRY] = EventType.TELEMETRY
    vehicle_id: str
    sequence: int
    state: State
    phase: Phase
    position: tuple[
        Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False)],
        Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False)],
    ]
    assignment_id: str | None
    incident_id: str | None
    routing_node: int | None = None
    remaining_edge_metres: float = Field(default=0, ge=0, allow_inf_nan=False)
    route: list[tuple[float, float]] = Field(default_factory=list, max_length=10000)


class RouteFailed(BaseModel, frozen=True):
    type: Literal[EventType.ROUTE_FAILED] = EventType.ROUTE_FAILED
    assignment_id: str
    reason: str


type Command = OfferResponse | Telemetry | RouteFailed


class Registration(BaseModel, frozen=True):
    type: Literal["register"] = "register"
    snapshot: Telemetry


class RegistrationRequired(BaseModel, frozen=True):
    type: Literal["registration_required"] = "registration_required"


class Envelope[T](BaseModel, frozen=True):
    session_id: str
    payload: T


type VehicleMessage = Annotated[
    Registration | OfferResponse | Telemetry | RouteFailed | Disconnected,
    Field(discriminator="type"),
]


type CenterMessage = Event | RegistrationRequired
