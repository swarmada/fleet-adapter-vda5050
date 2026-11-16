# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Work for a vehicle the adapter cannot observe is refused.

The control plane keeps a robot Idle and Ready while its adapter streams OFFLINE frames for it
(RFC-0001: only adapter or robot silence takes a robot Offline), so it can assign that robot
work. Accepted, the order would go to a topic the vehicle does not read, and the work would sit
on the robot. Refused ROBOT_UNAVAILABLE, the plane releases it to be scheduled again."""

from __future__ import annotations

import json

import pytest
from conftest import mark_registered, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
ONLINE = '{"connectionState": "ONLINE"}'
POSE = {"agvPosition": {"x": 1.0, "y": 1.0, "theta": 0.0, "mapId": "L1",
                        "positionInitialized": True},
        "driving": False, "nodeStates": [], "actionStates": []}


def _adapter() -> tuple[Adapter, Vda5050Binding]:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))
    b.on_connection(ONLINE)
    b.on_state(json.dumps(POSE))
    return mark_registered(Adapter(pb, None, b, ROBOT)), b


def _assign(a: Adapter, token: int, action_id: str = "act-1"):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}'), res)
    return res.assign_action


def _orders(b: Vda5050Binding) -> int:
    return sum(1 for topic, _ in b.published if topic.endswith("/order"))


@pytest.mark.parametrize("state", ["OFFLINE", "CONNECTIONBROKEN"])
def test_work_for_a_vehicle_that_is_not_connected_is_refused(state) -> None:
    a, b = _adapter()
    b.on_connection(json.dumps({"connectionState": state}))
    r = _assign(a, 7)
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert r.message.startswith("the vehicle is not connected") and state in r.message
    assert _orders(b) == 0 and a._current_action == ""
    # Work the robot never took does not raise the fencing mark (as for any refusal).
    assert a._fence.highest(ROBOT) < 7


def test_work_is_refused_while_the_broker_session_is_down() -> None:
    a, b = _adapter()
    b._on_broker_session(False)
    b._mqtt._up = False
    r = _assign(a, 7)
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert "broker session is down" in r.message
    assert _orders(b) == 0


def test_the_same_work_is_taken_once_the_vehicle_is_online_again() -> None:
    a, b = _adapter()
    b.on_connection('{"connectionState": "OFFLINE"}')
    assert not _assign(a, 7).accepted
    b.on_connection(ONLINE)
    b.on_state(json.dumps(POSE))
    r = _assign(a, 7)
    assert r.accepted and r.accepted_fencing_token == 7
    assert _orders(b) == 1


def test_an_identical_redelivery_is_reacked_while_the_vehicle_is_not_connected() -> None:
    # C3.4. Refused, it would tell the plane this robot is not doing work it may still be doing,
    # and the plane would give it to another robot.
    a, b = _adapter()
    assert _assign(a, 5).accepted
    b.on_connection('{"connectionState": "CONNECTIONBROKEN"}')
    r = _assign(a, 5)
    assert r.accepted and r.accepted_fencing_token == 5
    assert a._current_action == "act-1" and _orders(b) == 1


def test_an_identical_redelivery_starts_nothing_again() -> None:
    a, b = _adapter()
    assert _assign(a, 5).accepted
    a._outbox.queue.clear()
    assert _assign(a, 5).accepted
    assert _orders(b) == 1
    assert not [m for m in a._outbox.queue if m.HasField("action_status")]


def test_an_identical_redelivery_renews_the_lease_of_the_running_action() -> None:
    a, _ = _adapter()
    res = pb.CommandResult()
    task = pb.AssignAction(action_id="act-1", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=5,
                           assignment_id="as-5", payload_json=b'{"x": 5, "y": 0}',
                           lease_duration_ms=10_000, lease_generation=5)
    a._on_assign(task, res)
    before = a._lease._deadline[ROBOT]
    a._lease._clock = lambda: 1e12        # later: a re-grant moves the deadline
    a._on_assign(task, pb.CommandResult())
    assert a._lease._deadline[ROBOT] > before


def test_stale_and_missing_tokens_keep_their_answers_while_not_connected() -> None:
    a, b = _adapter()
    assert _assign(a, 5).accepted
    b.on_connection('{"connectionState": "OFFLINE"}')
    assert _assign(a, 4, action_id="act-2").rejection == pb.ASSIGN_ACTION_REJECTION_STALE_FENCING_TOKEN
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(action_id="act-3", action_type=adapter_mod.ACTION_NAVIGATE), res)
    assert res.assign_action.rejection == pb.ASSIGN_ACTION_REJECTION_MISSING_FENCING_TOKEN


def test_work_is_refused_once_states_stop_for_the_lapse() -> None:
    now = [1000.0]
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", clock=lambda: now[0],
                                           state_lapse_s=60.0))
    b.on_connection('{"connectionState": "CONNECTIONBROKEN"}')
    b.on_state(json.dumps({**POSE, "headerId": 1, "timestamp": "t1"}))  # taken as connected
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    now[0] += 61.0
    r = _assign(a, 7)
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert "none has come for 60 s" in r.message
