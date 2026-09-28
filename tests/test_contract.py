# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The contract-1.3.0 obligations this adapter carries beyond the safety primitives: the
action catalog it declares and holds assignments to (C19), operating constraints it cannot
honour (C17), and the authoritative action state a RegisterAck hands it (C2.2, C3.8)."""

from __future__ import annotations

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"


def _adapter() -> Adapter:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    return Adapter(pb, None, robot, ROBOT)


def _drain(q) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _assign(a: Adapter, token: int, action_type: str = adapter_mod.ACTION_NAVIGATE,
            constraints=(), action_id: str = "act-1"):
    result = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=action_type, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 1, "y": 2}',
        operating_constraints=list(constraints)), result)
    return result.assign_action


def test_declares_the_current_contract_version() -> None:
    assert adapter_mod.CONTRACT_VERSION == "1.3.0"


def test_scan_declares_the_navigate_catalog() -> None:
    result = pb.CommandResult()
    _adapter()._on_scan(result)
    assert [s.action_type for s in result.scan.supported_actions] == ["Navigate"]
    assert not result.scan.honoured_operating_constraints


def test_declared_action_is_accepted() -> None:
    r = _assign(_adapter(), 1)
    assert r.accepted and r.accepted_fencing_token == 1


def test_undeclared_action_type_is_declined_invalid_action() -> None:
    r = _assign(_adapter(), 1, action_type="Clean")
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_INVALID_ACTION


def test_empty_action_type_is_declined_invalid_action() -> None:
    r = _assign(_adapter(), 1, action_type="")
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_INVALID_ACTION


def test_operating_constraint_is_declined_and_named() -> None:
    a = _adapter()
    r = _assign(a, 1, constraints=[pb.EffectiveOperatingConstraint(subject="camera"),
                                   pb.EffectiveOperatingConstraint(subject="example.com/x")])
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_OPERATING_CONSTRAINT_UNMET
    assert list(r.unhonourable_subjects) == ["camera", "example.com/x"]
    assert a._current_action == ""


def test_register_ack_token_is_adopted_as_the_high_water_mark() -> None:
    a = _adapter()
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            phase=pb.ROBOT_ACTION_PHASE_IDLE, fencing_token=20)))
    r = _assign(a, 18)
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_STALE_FENCING_TOKEN
    assert _assign(a, 21).accepted


def test_register_ack_cancel_stops_and_reports_the_action() -> None:
    a = _adapter()
    assert _assign(a, 1).accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-1", phase=pb.ROBOT_ACTION_PHASE_CANCELLED, fencing_token=1)))
    msgs = [m for m in _drain(a._outbox) if m.HasField("action_status")]
    assert [(m.action_status.action_id, m.action_status.state) for m in msgs] == [
        ("act-1", pb.ACTION_STATE_CANCELLED)]
    assert a._current_action == ""


def test_register_ack_in_progress_for_the_local_action_keeps_it() -> None:
    a = _adapter()
    assert _assign(a, 1).accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-1", phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS, fencing_token=1)))
    assert a._current_action == "act-1"
    assert not [m for m in _drain(a._outbox) if m.HasField("action_status")]
