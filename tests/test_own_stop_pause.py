# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A pause this binding is lifting is not a pause at the vehicle.

The binding sends a stopPause before the first order after a stop, and on a Swarmada resume. The
vehicle's states keep saying paused until it has taken that stopPause, and reports it FINISHED in
the first state after. Until then its paused: true is the pause being lifted, not a pause at the
vehicle, so work is taken. Once the stopPause is reported ended (FINISHED or FAILED), or after
_STOP_PAUSE_REPORT_S without a report, a paused: true is the vehicle's own, and work is refused as
a pause at the vehicle."""

from __future__ import annotations

import itertools
import json

from conftest import mark_registered, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import vda5050_binding as binding_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
_N = itertools.count(1)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _state(paused: bool, actions=(), order_id: str = "") -> str:
    """A counted state (headerId and timestamp): at rest, with ``actions`` as (actionId,
    actionType, actionStatus)."""
    n = next(_N)
    return json.dumps({
        "headerId": n, "timestamp": f"t-{n}", "driving": False,
        "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0}, "paused": paused,
        "operatingMode": "AUTOMATIC", "orderId": order_id, "nodeStates": [],
        "actionStates": [{"actionId": i, "actionType": t, "actionStatus": s}
                         for i, t, s in actions]})


def _sent(b: Vda5050Binding, action_type: str) -> str:
    """The actionId of the last instantAction of ``action_type`` the binding published."""
    for topic, payload in reversed(list(b.published)):
        if topic.endswith("/instantActions"):
            a = json.loads(payload)["actions"][0]
            if a["actionType"] == action_type:
                return a["actionId"]
    raise AssertionError(f"no {action_type} was published")


def _wire(b: Vda5050Binding) -> list[str]:
    return ["order" if t.endswith("/order") else json.loads(p)["actions"][0]["actionType"]
            for t, p in b.published]


def _assign(a: Adapter, action_id: str, token: int):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}'), res)
    return res.assign_action


def _after_estop(clock: _Clock | None = None) -> tuple[Vda5050Binding, Adapter]:
    """A vehicle this binding stopped with an estop, halted and paused: startPause FINISHED."""
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, clock=clock or _Clock()))
    b.on_state(_state(paused=False))
    b.command_estop(ROBOT)
    b.on_state(_state(paused=True, actions=[(_sent(b, "startPause"), "startPause", "FINISHED")]))
    return b, mark_registered(Adapter(pb, None, b, ROBOT))


def test_work_right_after_the_first_order_after_a_stop_is_taken() -> None:
    """The first order after a stop goes with a stopPause; a state the vehicle sent before
    taking it still says paused. Work then is taken, not refused as a pause at the vehicle."""
    b, a = _after_estop()
    assert _assign(a, "act-1", 5).accepted
    assert _wire(b)[-2:] == ["stopPause", "order"]
    b.on_state(_state(paused=True, order_id=b._last_order_id))  # stopPause not yet taken
    assert not b.paused_at_vehicle(ROBOT)
    r = _assign(a, "act-2", 6)
    assert r.accepted, r.message


def test_work_right_after_a_resume_is_taken() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, clock=_Clock()))
    b.on_state(_state(paused=False))
    b.command_pause(ROBOT)
    b.on_state(_state(paused=True, actions=[(_sent(b, "startPause"), "startPause", "FINISHED")]))
    b.command_resume(ROBOT)
    b.on_state(_state(paused=True))  # the resume's stopPause not yet taken
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert not b.paused_at_vehicle(ROBOT)
    r = _assign(a, "act-1", 5)
    assert r.accepted, r.message


def test_a_stop_pause_the_vehicle_reports_failed_leaves_a_pause_at_the_vehicle() -> None:
    """The vehicle did not lift the pause: it stays paused, and work is refused."""
    b, a = _after_estop()
    assert _assign(a, "act-1", 5).accepted
    stop_pause = _sent(b, "stopPause")
    b.on_state(_state(paused=True, actions=[(stop_pause, "stopPause", "FAILED")]))
    assert b.paused_at_vehicle(ROBOT)
    r = _assign(a, "act-2", 6)
    assert not r.accepted and r.message == "paused at the vehicle"


def test_a_stop_pause_never_reported_ended_counts_for_two_seconds_only() -> None:
    """A vehicle that never reports the stopPause: its paused: true is taken as the pause being
    lifted for _STOP_PAUSE_REPORT_S, then as a pause at the vehicle, as before the fix."""
    clock = _Clock()
    b, a = _after_estop(clock)
    assert binding_mod._STOP_PAUSE_REPORT_S == 2.0
    assert _assign(a, "act-1", 5).accepted
    clock.now += 1.9
    b.on_state(_state(paused=True, order_id=b._last_order_id))
    assert not b.paused_at_vehicle(ROBOT)
    clock.now += 0.2  # 2.1 s after the stopPause
    b.on_state(_state(paused=True, order_id=b._last_order_id))
    assert b.paused_at_vehicle(ROBOT)
    r = _assign(a, "act-2", 6)
    assert not r.accepted and r.message == "paused at the vehicle"


def test_a_pause_pressed_at_the_vehicle_in_the_window_is_honoured_once_reported() -> None:
    """A button pressed after the vehicle took our stopPause: the state that reports the
    stopPause FINISHED, or any after it, says paused. That pause is the vehicle's: work is
    refused, and no stopPause is sent to release it."""
    b, a = _after_estop()
    assert _assign(a, "act-1", 5).accepted
    stop_pause = _sent(b, "stopPause")
    b.on_state(_state(paused=True, actions=[(stop_pause, "stopPause", "FINISHED")],
                      order_id=b._last_order_id))
    assert b.paused_at_vehicle(ROBOT)
    before = _wire(b)
    r = _assign(a, "act-2", 6)
    assert not r.accepted and r.message == "paused at the vehicle"
    assert _wire(b) == before


def test_documented_a_pause_pressed_before_the_stop_pause_is_reported_looks_like_ours() -> None:
    """The remaining case, pinned so that a change to it is deliberate. VDA5050 does not say who
    paused a vehicle. A button pressed before the vehicle reports our stopPause looks like the
    pause being lifted: work is taken. Its order goes with no stopPause, so the vehicle, paused,
    does not move; the next state that reports the stopPause ended shows the pause as the
    vehicle's, and work after it is refused."""
    b, a = _after_estop()
    assert _assign(a, "act-1", 5).accepted
    stop_pause = _sent(b, "stopPause")
    b.on_state(_state(paused=True, order_id=b._last_order_id))  # the button, or our pause
    published = len(b.published)
    assert _assign(a, "act-2", 6).accepted
    assert "stopPause" not in _wire(b)[published:]  # nothing lifts a pause the vehicle holds
    b.on_state(_state(paused=True, actions=[(stop_pause, "stopPause", "FINISHED")],
                      order_id=b._last_order_id))
    assert b.paused_at_vehicle(ROBOT)
    assert not _assign(a, "act-3", 7).accepted
