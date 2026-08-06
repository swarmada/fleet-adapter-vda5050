# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""FleetTask ↔ task-status translation (protocol-side, profile-agnostic).

This is the part of task execution that does NOT depend on the wire protocol: it
parses a protocol ``AssignAction`` into a neutral :class:`~...profiles.OrderTarget`
(which any comms profile then encodes into its own order), and derives a
:class:`TaskUpdate` from a parsed :class:`~...profiles.StateSnapshot`. VDA5050 and a
future ISO 21423 profile share this unchanged.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from fleet_adapter_vda5050.profiles import PHASE_ERROR, OrderTarget, StateSnapshot

# protocol ActionState enum names (bound to the proto ActionState by the adapter).
TASK_RUNNING = "ACTION_STATE_RUNNING"
TASK_SUCCEEDED = "ACTION_STATE_SUCCEEDED"
TASK_FAILED = "ACTION_STATE_FAILED"
TASK_CANCELLED = "ACTION_STATE_CANCELLED"
# A clean commanded stop — NOT a failure. Reported when the assignment lease
# lapses (C4.2) or an estop safe-holds the action, so the control plane can tell
# "stopped on purpose" from "crashed" and from silence.
TASK_STOPPED = "ACTION_STATE_STOPPED"


@dataclass
class TaskUpdate:
    """What the adapter reports as a ``ActionStatusUpdate`` (numeric + bounded only)."""

    action_id: str
    state: str                        # one of the TASK_* names
    fencing_token: int | None = None  # echoed for C6.4 (superseded-assignment detection)
    progress_pct: int = 0             # 0–100; 0 if unknown
    message: str = ""


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _coords_from_string(text: str) -> tuple[float, float, float] | None:
    parts = [p.strip() for p in text.split(",")]
    if len(parts) not in (2, 3):
        return None
    try:
        nums = [float(p) for p in parts]
    except ValueError:
        return None
    return nums[0], nums[1], (nums[2] if len(nums) == 3 else 0.0)


def goal_from_assign(destination: str, payload_json: bytes | str = b"") -> OrderTarget:
    """``AssignAction`` → :class:`OrderTarget`. Resolution order (profile-agnostic):

    1. ``payload_json`` ``{x, y, theta?, mapId?}`` — explicit coordinates.
    2. ``destination`` parsed as ``"x,y"`` / ``"x,y,theta"``.
    3. otherwise ``destination`` is a **named** location (``label``); the robot's map
       resolves it. Coordinates default to 0,0.
    """
    payload: dict = {}
    if payload_json:
        try:
            raw = payload_json.decode() if isinstance(payload_json, bytes) else payload_json
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                payload = loaded
        except (ValueError, UnicodeDecodeError):
            payload = {}

    map_id = payload["mapId"] if isinstance(payload.get("mapId"), str) else ""

    if _is_number(payload.get("x")) and _is_number(payload.get("y")):
        theta = float(payload["theta"]) if _is_number(payload.get("theta")) else 0.0
        return OrderTarget(x=float(payload["x"]), y=float(payload["y"]), theta=theta,
                           map_id=map_id, label=str(destination or ""))

    if destination:
        coords = _coords_from_string(destination)
        if coords is not None:
            x, y, theta = coords
            return OrderTarget(x=x, y=y, theta=theta, map_id=map_id)
        return OrderTarget(x=0.0, y=0.0, map_id=map_id, label=destination)

    return OrderTarget(x=0.0, y=0.0, map_id=map_id)


def task_state_from_snapshot(snapshot: StateSnapshot, current_order_id: str) -> str:
    """Derive the protocol ``TaskState`` from a parsed robot ``state``.

    * a FATAL error (phase ERROR) → FAILED;
    * the robot's active order matches ours and it has reached the last node
      (no active nodeStates, not driving) → SUCCEEDED;
    * otherwise the order is still in flight → RUNNING.
    """
    if snapshot.phase == PHASE_ERROR:
        return TASK_FAILED
    arrived = (current_order_id
               and snapshot.order_id == current_order_id
               and snapshot.active_nodes == 0
               and not snapshot.driving)
    return TASK_SUCCEEDED if arrived else TASK_RUNNING
