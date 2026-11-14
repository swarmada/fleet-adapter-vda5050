# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The simulated vehicle drives to its target and the order ends there.

Without this a Navigate on `--comms simulated` never finished: the robot jumped to the target
and reported moving for ever, so no terminal ActionStatusUpdate was ever sent and the action
ran until its lease lapsed. It moves at 0.5 m/s, the speed the conformance fixture assumes."""

from __future__ import annotations

import pytest
from conftest import mark_registered

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"


def _robot() -> SimulatedRobot:
    r = SimulatedRobot()
    r.spawn(ROBOT)
    return r


def test_the_robot_drives_to_its_target_at_half_a_metre_a_second() -> None:
    r = _robot()
    r.command_move(ROBOT, 3.0, 4.0)
    assert r.pose(ROBOT)[:2] == (0.0, 0.0) and not r.is_stopped(ROBOT)
    r.tick(2.0)
    assert r.pose(ROBOT)[:2] == pytest.approx((0.6, 0.8))
    assert r.task_state(ROBOT) == task_map.TASK_RUNNING
    r.tick(8.0)
    assert r.pose(ROBOT)[:2] == pytest.approx((3.0, 4.0))
    assert r.is_stopped(ROBOT) and r.task_state(ROBOT) == task_map.TASK_SUCCEEDED


def test_a_stop_ends_the_order_where_the_robot_is() -> None:
    r = _robot()
    r.command_move(ROBOT, 10.0, 0.0)
    r.tick(2.0)
    r.command_stop(ROBOT)
    r.tick(5.0)
    assert r.pose(ROBOT)[:2] == pytest.approx((1.0, 0.0))
    assert r.is_stopped(ROBOT) and r.task_state(ROBOT) == ""


def test_a_pause_holds_and_a_resume_carries_on() -> None:
    r = _robot()
    r.command_move(ROBOT, 10.0, 0.0)
    r.tick(2.0)
    r.command_pause(ROBOT)
    r.tick(5.0)
    assert r.pose(ROBOT)[:2] == pytest.approx((1.0, 0.0)) and r.is_stopped(ROBOT)
    r.command_resume(ROBOT)
    r.tick(2.0)
    assert r.pose(ROBOT)[:2] == pytest.approx((2.0, 0.0))


def test_the_adapter_reports_the_order_succeeded_once_and_clears_it() -> None:
    r = _robot()
    a = mark_registered(Adapter(pb, None, r, ROBOT))
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(action_id="act-1", action_type=adapter_mod.ACTION_NAVIGATE,
                                 fencing_token=5, assignment_id="as-5",
                                 payload_json=b'{"x": 1, "y": 0}'), res)
    assert res.assign_action.accepted
    r.tick(3.0)
    a._maybe_emit_task_progress()
    a._maybe_emit_task_progress()
    states = []
    while not a._outbox.empty():
        m = a._outbox.get_nowait()
        if m.WhichOneof("payload") == "action_status":
            states.append((m.action_status.state, m.action_status.fencing_token))
    assert states == [(pb.ACTION_STATE_RUNNING, 5), (pb.ACTION_STATE_SUCCEEDED, 5)]
    assert a._current_action == ""


def test_the_robot_is_ticked_by_the_time_that_passed_not_by_the_cadence() -> None:
    # The telemetry loop sleeps for the cadence the RegisterAck sets (1 s or more), so a fixed
    # 0.2 s tick ran the simulated vehicle five times slower than the clock.
    r = _robot()
    a = Adapter(pb, None, r, ROBOT)
    now = [100.0]
    a._clock = lambda: now[0]
    a._tick_robot()  # the first tick only starts the clock
    r.command_move(ROBOT, 10.0, 0.0)
    now[0] += 2.0
    a._tick_robot()
    assert r.pose(ROBOT)[:2] == pytest.approx((1.0, 0.0))
    now[0] += 60.0  # a long gap (a suspended host) is not driven through in one step
    a._tick_robot()
    assert r.pose(ROBOT)[:2] == pytest.approx((1.0 + 0.5 * adapter_mod._MAX_TICK_S, 0.0))
