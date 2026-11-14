# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Two obligations a 1.4.0 control plane grades once it sends this adapter work:

* a lease that lapses ends the action STOPPED, then FAILED with LEASE_EXPIRED, so the control
  plane can tell the action ended (STOPPED alone is the commanded stop, not terminal);
* a cancel naming an action this adapter no longer runs (it was preempted in place)
  leaves the action that replaced it running."""

from __future__ import annotations

from conftest import mark_registered

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"


def _adapter() -> Adapter:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    a = Adapter(pb, None, robot, ROBOT)
    a._telemetry_started = True
    a._handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _drain(a)
    return mark_registered(a)


def _drain(a: Adapter) -> list:
    out = []
    while not a._outbox.empty():
        out.append(a._outbox.get_nowait())
    return out


def _statuses(msgs) -> list:
    return [(m.action_status.action_id, m.action_status.state, m.action_status.end_cause)
            for m in msgs if m.WhichOneof("payload") == "action_status"]


def _assign(a: Adapter, action_id: str, token: int, lease_ms: int = 0):
    r = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 1, "y": 2}',
        lease_duration_ms=lease_ms, lease_generation=token), r)
    assert r.assign_action.accepted
    return r


def test_a_lapsed_lease_ends_stopped_then_failed_lease_expired() -> None:
    a = _adapter()
    _assign(a, "act-1", 5, lease_ms=1000)
    _drain(a)
    a._self_stop(ROBOT)
    assert _statuses(_drain(a)) == [
        ("act-1", pb.ACTION_STATE_STOPPED, pb.ACTION_END_CAUSE_LEASE_EXPIRED),
        ("act-1", pb.ACTION_STATE_FAILED, pb.ACTION_END_CAUSE_LEASE_EXPIRED)]
    assert a._current_action == ""


def test_a_cancel_naming_a_preempted_action_leaves_the_preemptor_running() -> None:
    a = _adapter()
    _assign(a, "t-preempted", 15)
    _assign(a, "t-preemptor", 16)  # in place
    _drain(a)
    a._handle_command(pb.Command(command_id=9, robot_id=ROBOT, cancel_action=pb.CancelAction(
        action_id="t-preempted", fencing_token=17)))
    msgs = _drain(a)
    (res,) = [m.command_result.cancel_action for m in msgs
              if m.WhichOneof("payload") == "command_result"]
    assert res.acknowledged and res.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    assert _statuses(msgs) == []
    assert a._current_action == "t-preemptor"


def test_a_cancel_naming_the_running_action_cancels_it() -> None:
    a = _adapter()
    _assign(a, "act-1", 5)
    _drain(a)
    a._handle_command(pb.Command(command_id=9, robot_id=ROBOT, cancel_action=pb.CancelAction(
        action_id="act-1", fencing_token=6)))
    assert _statuses(_drain(a)) == [("act-1", pb.ACTION_STATE_CANCELLED, 0)]
    assert a._current_action == ""
