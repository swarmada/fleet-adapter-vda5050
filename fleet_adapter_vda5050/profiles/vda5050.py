# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 comms profile — the public VDA5050 dialect over MQTT/JSON.

Grounded in the public VDA5050 message spec (``order``, ``state``, ``connection``,
``instantActions``). This is the VDA5050 CLASS — no single vendor's dialect is
hard-coded.

Implements the full ``CommsProfile`` seam: the topic layout, the ``connection``
parse, the ``state`` → :class:`StateSnapshot` telemetry parse (numeric-only),
``order`` encoding (FleetTask → VDA5050 nodes/edges), and ``instantActions``
encoding (the confirmed-stop ``cancelOrder`` safe-hold).
"""

from __future__ import annotations

import json
import math
import unicodedata

from fleet_adapter_vda5050.profiles import (
    CONN_BROKEN,
    CONN_OFFLINE,
    CONN_ONLINE,
    PHASE_ACTIVE,
    PHASE_CHARGING,
    PHASE_ERROR,
    PHASE_IDLE,
    PHASE_MAINTENANCE,
    STATUS_DEGRADED,
    STATUS_FAILED,
    Battery,
    FactsheetInfo,
    Hardware,
    OrderTarget,
    Position,
    ProfileTopics,
    StateSnapshot,
)

# VDA5050 topic layout: {interfaceName}/v{majorVersion}/{manufacturer}/{serialNumber}/{topic}
_INTERFACE = "uagv"
_MAJOR_VERSION = 2  # VDA5050 2.x
_VERSION = "2.0.0"  # VDA5050 protocol version string carried in each message

_VALID_CONN = {CONN_ONLINE, CONN_OFFLINE, CONN_BROKEN}

# VDA5050 error.errorLevel and operatingMode values (public spec, vendor-neutral).
_LEVEL_FATAL = "FATAL"
# operatingMode values that mean the AGV is NOT under autonomous fleet control:
_MODE_MANUAL = {"MANUAL", "SERVICE", "TEACHIN"}


class Vda5050Profile:
    """VDA5050 implementation of ``CommsProfile`` (see profiles/__init__.py)."""

    name = "vda5050"

    def __init__(self, interface: str = _INTERFACE, major_version: int = _MAJOR_VERSION) -> None:
        self._interface = interface
        self._major = major_version

    def _base(self, manufacturer: str, serial: str) -> str:
        return f"{self._interface}/v{self._major}/{manufacturer}/{serial}"

    def topics(self, manufacturer: str, serial: str) -> ProfileTopics:
        base = self._base(manufacturer, serial)
        return ProfileTopics(
            connection=f"{base}/connection",
            state=f"{base}/state",
            order=f"{base}/order",
            instant_actions=f"{base}/instantActions",
            factsheet=f"{base}/factsheet",
        )

    def parse_connection(self, payload: bytes | str) -> str:
        """VDA5050 ``connection`` → ``CONN_ONLINE``/``OFFLINE``/``BROKEN``.

        FAIL SAFE: an unparseable, missing, or unknown ``connectionState`` is
        treated as ``CONNECTIONBROKEN`` — the adapter assumes the link is NOT
        healthy rather than asserting ONLINE from a malformed message. (VDA5050
        publishes CONNECTIONBROKEN via the MQTT last-will when a robot drops.)
        """
        try:
            raw = payload.decode() if isinstance(payload, bytes) else payload
            obj = json.loads(raw)
            state = obj.get("connectionState") if isinstance(obj, dict) else None
        except (ValueError, UnicodeDecodeError, AttributeError):
            return CONN_BROKEN
        return state if state in _VALID_CONN else CONN_BROKEN

    def parse_state(self, payload: bytes | str) -> StateSnapshot:
        """VDA5050 ``state`` → :class:`StateSnapshot`, numeric-only. A malformed
        message degrades to an empty snapshot (unknown), never a false-healthy
        reading. ``errorReferences`` key/values are dropped (opaque-data rule)."""
        obj = _load(payload)
        ap = obj.get("agvPosition")
        node_states = obj.get("nodeStates")
        return StateSnapshot(
            position=_position(ap),
            battery=_battery(obj.get("batteryState")),
            hardware=_hardware(obj.get("errors")),
            phase=_phase(obj),
            driving=bool(obj.get("driving", False)),
            velocity=_velocity(obj.get("velocity")),
            operating_mode=str(obj.get("operatingMode", "")),
            order_id=str(obj.get("orderId", "")),
            order_update_id=int(_num(obj.get("orderUpdateId")) or 0),
            active_nodes=len(node_states) if isinstance(node_states, list) else 0,
            active_actions=_active_actions(obj.get("actionStates")),
            ended_actions=_ended_actions(obj.get("actionStates"), obj.get("errors")),
            active_action_ids=_active_action_ids(obj.get("actionStates")),
            order_errors=_order_errors(obj.get("errors")),
            paused=obj.get("paused") is True,
            map_id=str(ap.get("mapId", "")) if isinstance(ap, dict) else "",
        )

    def parse_factsheet(self, payload: bytes | str) -> FactsheetInfo:
        """VDA5050 ``factsheet`` → :class:`FactsheetInfo`: the AGV's type identity
        (``typeSpecification``) and the action types it supports
        (``protocolFeatures.agvActions``). A malformed message degrades to an empty
        info (unknown), never a false-capable reading."""
        obj = _load(payload)
        ts = obj.get("typeSpecification")
        ts = ts if isinstance(ts, dict) else {}
        pf = obj.get("protocolFeatures")
        raw_actions = pf.get("agvActions") if isinstance(pf, dict) else None
        actions: list[str] = []
        if isinstance(raw_actions, list):
            for a in raw_actions:
                at = a.get("actionType") if isinstance(a, dict) else None
                if isinstance(at, str) and at and at not in actions:
                    actions.append(at)
        return FactsheetInfo(
            manufacturer=str(obj.get("manufacturer", "")),
            serial=str(obj.get("serialNumber", "")),
            series=str(ts.get("seriesName", "")),
            agv_class=str(ts.get("agvClass", "")),
            actions=actions,
        )

    def encode_order(self, header_id: int, order_id: str, order_update_id: int,
                     manufacturer: str, serial: str, start: OrderTarget | None,
                     goal: OrderTarget, timestamp: str) -> str:
        """:class:`OrderTarget` → a canonical VDA5050 base order (JSON string).

        A two-node released order: node seq 0 (start = the robot's current pose, so
        the AGV is already on the first node as VDA5050 requires) → edge seq 1 →
        node seq 2 (goal). ``headerId`` and ``orderUpdateId`` are the robot-side
        at-least-once dedup keys — a re-delivered identical order is a no-op.
        """
        start = start or OrderTarget(x=goal.x, y=goal.y, map_id=goal.map_id)
        nodes = [
            _node("start", 0, start),
            _node("goal", 2, goal),
        ]
        edges = [{
            "edgeId": "edge-0", "sequenceId": 1, "released": True,
            "startNodeId": "start", "endNodeId": "goal", "actions": [],
        }]
        order = {
            "headerId": header_id,
            "timestamp": timestamp,
            "version": _VERSION,
            "manufacturer": manufacturer,
            "serialNumber": serial,
            "orderId": order_id,
            "orderUpdateId": order_update_id,
            "nodes": nodes,
            "edges": edges,
        }
        return json.dumps(order)

    def encode_instant_action(self, header_id: int, action_type: str, action_id: str,
                              manufacturer: str, serial: str, timestamp: str,
                              blocking_type: str = "HARD") -> str:
        """Build a VDA5050 ``instantActions`` message carrying one action. Used for
        the confirmed-stop safe-hold (``cancelOrder``, blocking HARD) — a decisive
        halt, never the resume action ``stopPause``."""
        msg = {
            "headerId": header_id,
            "timestamp": timestamp,
            "version": _VERSION,
            "manufacturer": manufacturer,
            "serialNumber": serial,
            "actions": [{
                "actionType": action_type,
                "actionId": action_id,
                "blockingType": blocking_type,
                "actionParameters": [],
            }],
        }
        return json.dumps(msg)


def _load(payload: bytes | str) -> dict:
    try:
        raw = payload.decode() if isinstance(payload, bytes) else payload
        obj = json.loads(raw)
    except (ValueError, UnicodeDecodeError, AttributeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _node(node_id: str, sequence_id: int, t: OrderTarget) -> dict:
    """A released VDA5050 order node at an OrderTarget's pose."""
    return {
        "nodeId": node_id,
        "sequenceId": sequence_id,
        "released": True,
        "nodePosition": {"x": t.x, "y": t.y, "theta": t.theta, "mapId": t.map_id},
        "actions": [],
    }


def _position(ap) -> Position | None:
    """agvPosition {x, y, theta} → Position. Honors positionInitialized: an
    uninitialized pose is reported as absent, never as a bogus (0,0)."""
    if not isinstance(ap, dict) or ap.get("positionInitialized") is False:
        return None
    x, y = _num(ap.get("x")), _num(ap.get("y"))
    if x is None or y is None:
        return None
    theta = _num(ap.get("theta")) or 0.0
    # mapId is a string map identifier (not a numeric floor) — not smuggled in.
    return Position(x=x, y=y, yaw=theta, floor=0)


def _battery(bs) -> Battery:
    """batteryState {batteryCharge (already 0–100 %), charging, batteryVoltage}."""
    if not isinstance(bs, dict):
        return Battery()
    charge = _num(bs.get("batteryCharge"))
    percent = None if charge is None else max(0, min(100, round(charge)))
    charging = bs.get("charging")
    charging = bool(charging) if isinstance(charging, bool) else None  # unknown, not False
    return Battery(percent=percent, charging=charging, voltage=_num(bs.get("batteryVoltage")))


def _hardware(errors) -> list[Hardware]:
    """errors[] {errorType, errorLevel, errorDescription} → Hardware. WARNING →
    Degraded, FATAL → Failed. errorReferences are intentionally dropped."""
    out: list[Hardware] = []
    if not isinstance(errors, list):
        return out
    for e in errors:
        if not isinstance(e, dict):
            continue
        status = STATUS_FAILED if e.get("errorLevel") == _LEVEL_FATAL else STATUS_DEGRADED
        out.append(Hardware(
            component_name=str(e.get("errorType", "error")),
            status=status,
            reason=str(e.get("errorDescription", "")),
        ))
    return out


def _velocity(vel) -> tuple[float, float]:
    """velocity {vx, vy, omega} → (linear, angular) magnitudes (ground truth for
    the confirmed stop in sub-item 4)."""
    if not isinstance(vel, dict):
        return (0.0, 0.0)
    vx, vy = _num(vel.get("vx")) or 0.0, _num(vel.get("vy")) or 0.0
    omega = _num(vel.get("omega")) or 0.0
    return (math.hypot(vx, vy), abs(omega))


# actionStatus values of an action that has not ended (VDA5050 2.0.0, actionStates).
_ACTION_NOT_ENDED = frozenset({"WAITING", "INITIALIZING", "RUNNING", "PAUSED"})


def _active_actions(states) -> int:
    """How many ``actionStates`` have not ended. A malformed list counts as one, so a state
    the profile cannot read never looks like a vehicle with nothing to cancel."""
    if states is None:
        return 0
    if not isinstance(states, list):
        return 1
    return sum(1 for st in states
               if not isinstance(st, dict) or st.get("actionStatus") in _ACTION_NOT_ENDED)


# Lengths the vehicle's own words are cut to, after control characters are stripped.
_ERROR_TYPE_MAX = 64
_ERROR_TEXT_MAX = 200


def clean_text(value, limit: int) -> str:
    """Vehicle-supplied text made safe to pass on: not a string gives ""; every control,
    format, surrogate, private-use or unassigned character (Unicode category C) becomes a space;
    runs of whitespace, line and paragraph separators among them (``str.split``), become one
    space; then the text is cut to ``limit`` characters. A newline or a terminal escape in an
    errorDescription can then not forge a line in a log, and a bidirectional override cannot
    reorder what an operator reads."""
    if not isinstance(value, str):
        return ""
    text = "".join(" " if unicodedata.category(ch)[0] == "C" else ch for ch in value)
    return " ".join(text.split())[:limit]


def _order_errors(errors) -> dict[str, tuple[str, str]]:
    """Each orderId named by an error's ``orderId`` reference, with that error's type and
    description, cleaned (:func:`clean_text`). The first error naming an order wins.

    Skipped as vda-5050-lib's own master controller skips them (``_dispatchState``): an error
    whose ``topic`` reference is not ``order``, and one with an ``actionId`` reference unless
    its type is ``orderError`` (6.6.4.2: an order with actions the vehicle cannot perform).
    Those are about an action, not about taking the order."""
    out: dict[str, tuple[str, str]] = {}
    if not isinstance(errors, list):
        return out
    for e in errors:
        refs = e.get("errorReferences") if isinstance(e, dict) else None
        if not isinstance(refs, list):
            continue
        refs = [r for r in refs if isinstance(r, dict)]
        keys = {r.get("referenceKey"): r.get("referenceValue") for r in refs}
        if "topic" in keys and keys["topic"] != "order":
            continue
        if "actionId" in keys and e.get("errorType") != "orderError":
            continue
        for r in refs:
            if (r.get("referenceKey") == "orderId"
                    and isinstance(r.get("referenceValue"), str) and r["referenceValue"]):
                out.setdefault(r["referenceValue"], (
                    clean_text(e.get("errorType"), _ERROR_TYPE_MAX),
                    clean_text(e.get("errorDescription"), _ERROR_TEXT_MAX)))
    return out


def _active_action_ids(states) -> frozenset[str]:
    """The actionIds ``actionStates`` lists as not ended."""
    if not isinstance(states, list):
        return frozenset()
    return frozenset(st["actionId"] for st in states
                     if isinstance(st, dict) and st.get("actionStatus") in _ACTION_NOT_ENDED
                     and isinstance(st.get("actionId"), str))


# actionStatus values of an action that has ended.
_ACTION_ENDED = frozenset({"FINISHED", "FAILED"})


def _ended_actions(states, errors) -> frozenset[str]:
    """The actionIds that have ended: listed in ``actionStates`` as FINISHED or FAILED, or named
    by an ``actionId`` error reference. VDA5050 2.0.0 (6.6.3.2): a cancelOrder with no order to
    cancel "must report as failed", with a noOrderToCancel error whose references carry the
    actionId, and a vehicle may report only the error. The ids are read here and go no further
    than the binding; the references stay off the control plane (see :func:`_hardware`)."""
    out: set[str] = set()
    if isinstance(states, list):
        for st in states:
            if (isinstance(st, dict) and st.get("actionStatus") in _ACTION_ENDED
                    and isinstance(st.get("actionId"), str)):
                out.add(st["actionId"])
    if isinstance(errors, list):
        for e in errors:
            refs = e.get("errorReferences") if isinstance(e, dict) else None
            if not isinstance(refs, list):
                continue
            for r in refs:
                if (isinstance(r, dict) and r.get("referenceKey") == "actionId"
                        and isinstance(r.get("referenceValue"), str)):
                    out.add(r["referenceValue"])
    return frozenset(out)


def _phase(obj: dict) -> str:
    """Derive a bounded RobotPhase hint from operatingMode / errors / charging /
    driving. FATAL error and manual mode take precedence over normal activity."""
    errors = obj.get("errors")
    if isinstance(errors, list) and any(
            isinstance(e, dict) and e.get("errorLevel") == _LEVEL_FATAL for e in errors):
        return PHASE_ERROR
    if str(obj.get("operatingMode", "")).upper() in _MODE_MANUAL:
        return PHASE_MAINTENANCE
    bs = obj.get("batteryState")
    if isinstance(bs, dict) and bs.get("charging") is True:
        return PHASE_CHARGING
    return PHASE_ACTIVE if obj.get("driving") else PHASE_IDLE
