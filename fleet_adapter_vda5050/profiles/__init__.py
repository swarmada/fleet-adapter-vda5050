# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Pluggable comms profiles — the robot-side wire dialect behind one binding.

A :class:`CommsProfile` encapsulates everything protocol-specific about talking to
a robot over its native fleet-interface messages: the topic layout, and the
parse/encode of connection/state/order/instantActions. The adapter core and
``Vda5050Binding`` are written against this Protocol, so a second profile (e.g. ISO
21423 once it publishes) slots in behind :func:`get_profile` **without touching the
core adapter** — that is the one place a new protocol registers.

VDA5050 is the first profile (:mod:`fleet_adapter_vda5050.profiles.vda5050`). No
per-vendor dialects live here; this is the VDA5050 CLASS, and vendor quirks belong
in vendor repositories.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

# connectionState values shared across profiles (VDA5050 names; ISO 21423 maps onto
# the same tri-state). OFFLINE and CONNECTIONBROKEN are both "not usable"; only
# ONLINE is a healthy link.
CONN_ONLINE = "ONLINE"
CONN_OFFLINE = "OFFLINE"
CONN_BROKEN = "CONNECTIONBROKEN"

# instantAction types (public VDA5050 names). NB: `stopPause` RESUMES motion — it is
# NOT a halt; the confirmed-stop safe-hold uses `cancelOrder` (abort + hold).
ACTION_CANCEL_ORDER = "cancelOrder"
ACTION_START_PAUSE = "startPause"
# stopPause RESUMES a paused order — used only for an explicit operator resume,
# never as a stop (the confirmed-stop safe-hold is cancelOrder).
ACTION_STOP_PAUSE = "stopPause"
# factsheetRequest asks the AGV to publish its `factsheet` (capabilities / type).
ACTION_FACTSHEET_REQUEST = "factsheetRequest"

# Bounded hardware-status names (bound to the proto HardwareStatus enum by the adapter).
STATUS_HEALTHY, STATUS_DEGRADED, STATUS_FAILED = "Healthy", "Degraded", "Failed"

# Bounded robot-phase hints (bound to the proto RobotPhase enum by the adapter). A
# profile derives this from its own operating-mode / error / charging signals.
PHASE_IDLE = "IDLE"
PHASE_ACTIVE = "ACTIVE"
PHASE_CHARGING = "CHARGING"
PHASE_ERROR = "ERROR"
PHASE_MAINTENANCE = "MAINTENANCE"


@dataclass
class Position:
    x: float
    y: float
    yaw: float
    floor: int = 0


@dataclass
class Battery:
    percent: int | None = None
    charging: bool | None = None    # None = unknown (never defaulted to False)
    voltage: float | None = None


@dataclass
class Hardware:
    component_name: str
    status: str                     # STATUS_HEALTHY / _DEGRADED / _FAILED
    reason: str = ""                # bounded human-readable reason (not opaque data)


@dataclass
class OrderTarget:
    """A navigation target the adapter hands a profile to encode into a wire order.
    Profile-agnostic: the protocol-side FleetTask parse (task.py) produces it; the
    profile turns it into VDA5050 nodes/edges (or, later, ISO 21423)."""

    x: float
    y: float
    theta: float = 0.0
    map_id: str = ""        # string map identifier (e.g. a floor/level map), not a number
    label: str = ""         # named destination when coordinates are resolved by the robot's map;
                            # on a lane node of a route (encode_order's via), its node id


@dataclass
class StateSnapshot:
    """The profile-neutral telemetry model every profile parses its wire ``state``
    into. Numeric + bounded-enum only — no opaque key/value blobs cross this seam."""

    position: Position | None = None
    battery: Battery = field(default_factory=Battery)
    hardware: list[Hardware] = field(default_factory=list)
    phase: str = PHASE_IDLE
    driving: bool = False
    velocity: tuple[float, float] = (0.0, 0.0)  # (linear, angular) magnitudes, ground truth
    operating_mode: str = ""                     # bounded autonomy mode (profile-native name)
    # Order progress (for task-status streaming). map_id is used only to build the
    # return order — it is NOT telemetry and never crosses to the control plane.
    order_id: str = ""
    order_update_id: int = 0
    active_nodes: int = 0                         # nodeStates still to be reached (0 = arrived)
    # actionStates not yet ended (WAITING, INITIALIZING, RUNNING or PAUSED): work the vehicle
    # can still do or resume, which a cancelOrder would cancel.
    active_actions: int = 0
    # actionIds the vehicle reports ended (FINISHED or FAILED), or named in an error's
    # references: an instantAction it refused, such as a cancelOrder with no order to cancel.
    # Read inside the adapter only; never sent to the control plane.
    ended_actions: frozenset[str] = frozenset()
    # actionIds listed as not ended (the ids behind active_actions). Inside the adapter only.
    active_action_ids: frozenset[str] = frozenset()
    # Each orderId an error's references name, with that error's (errorType, errorDescription):
    # how a vehicle reports an order it rejected (VDA5050 2.0.0 6.6.4, 8.1). The text is the
    # vehicle's, stripped of control characters and cut to length. Inside the adapter only,
    # except as the reason on the action it ends.
    order_errors: dict[str, tuple[str, str]] = field(default_factory=dict)
    # The vehicle reports it is in pause mode (VDA5050 state.paused): by a startPause, or by a
    # button on the vehicle. VDA5050 does not say which.
    paused: bool = False
    map_id: str = ""


@dataclass(frozen=True)
class ProfileTopics:
    """The MQTT topics the adapter (fleet master) uses for one robot.

    Subscribe (robot→master): ``connection``, ``state``, ``factsheet``. Publish
    (master→robot): ``order``, ``instant_actions``.
    """

    connection: str
    state: str
    order: str
    instant_actions: str
    factsheet: str = ""


@dataclass
class FactsheetInfo:
    """The subset of a VDA5050 ``factsheet`` the adapter surfaces as capabilities:
    the AGV's type identity and the action types it supports (``protocolFeatures``).
    Numeric/bounded only — no opaque blobs cross the seam."""

    manufacturer: str = ""
    serial: str = ""
    series: str = ""
    agv_class: str = ""
    actions: list[str] = field(default_factory=list)  # supported protocolFeatures.agvActions
    # protocolLimits.maxStringLens.idLen: the longest orderId, actionId, nodeId, edgeId or mapId
    # the vehicle takes. None when the factsheet does not say.
    id_len: int | None = None


@runtime_checkable
class CommsProfile(Protocol):
    """The wire dialect seam. A profile is stateless w.r.t. any one robot — the
    binding owns robot state and calls the profile to translate messages."""

    name: str

    def topics(self, manufacturer: str, serial: str) -> ProfileTopics:
        """Build the per-robot topic set from the robot's identity."""

    def parse_connection(self, payload: bytes | str) -> str:
        """Robot connection message → one of ``CONN_ONLINE``/``OFFLINE``/``BROKEN``."""

    def parse_state(self, payload: bytes | str) -> StateSnapshot:
        """Robot ``state`` message → the profile-neutral :class:`StateSnapshot`
        (numeric + bounded-enum only)."""

    def encode_order(self, header_id: int, order_id: str, order_update_id: int,
                     manufacturer: str, serial: str, start: OrderTarget | None,
                     goal: OrderTarget, timestamp: str,
                     via: Sequence[OrderTarget] = ()) -> str:
        """A navigation :class:`OrderTarget` → the wire order payload (a JSON string
        for VDA5050), passing ``via`` in order on the way (a route along the lanes).
        ``header_id``/``order_update_id`` are monotonic — the robot-side at-least-once
        dedup keys."""

    def encode_instant_action(self, header_id: int, action_type: str, action_id: str,
                              manufacturer: str, serial: str, timestamp: str,
                              blocking_type: str = "HARD") -> str:
        """A safety/control instantAction (e.g. ``ACTION_CANCEL_ORDER``) → the wire
        payload. ``header_id`` is monotonic (the at-least-once dedup key)."""


def get_profile(name: str) -> CommsProfile:
    """Return the comms profile by name. New protocol profiles register here — the
    single seam that changes when one is added; the adapter core does not.

    ``iso21423`` is a planned follow-on profile (added once the standard publishes).
    """
    if name == "vda5050":
        from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
        return Vda5050Profile()
    raise ValueError(f"unknown comms profile: {name!r} (known: vda5050)")
