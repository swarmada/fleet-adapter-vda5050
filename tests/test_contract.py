# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The contract-1.3.0 obligations this adapter carries beyond the safety primitives: the
action catalog it declares and holds assignments to (C19), operating constraints it cannot
honour (C17), and the authoritative action state a RegisterAck hands it (C2.2, C3.8)."""

from __future__ import annotations

import pathlib
import tomllib

from conftest import mark_registered

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"


def _adapter() -> Adapter:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    return mark_registered(Adapter(pb, None, robot, ROBOT))


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
    assert adapter_mod.CONTRACT_VERSION == "1.4.0"


def test_the_adapter_version_is_the_contract_plus_its_own_releases() -> None:
    """X.Y of the adapter's version is the contract its hello declares; the package metadata,
    __version__ and the hello's adapter_version are one value."""
    pyproject = pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject.open("rb") as f:
        packaged = tomllib.load(f)["project"]["version"]
    version = adapter_mod.__version__
    assert packaged == version
    assert version.split(".")[:2] == adapter_mod.CONTRACT_VERSION.split(".")[:2]
    hello = next(iter(Adapter(pb, None, SimulatedRobot(), ROBOT)._control_requests())).hello
    assert hello.adapter_version == version
    assert hello.contract_version == adapter_mod.CONTRACT_VERSION


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
    assert [(m.action_status.action_id, m.action_status.state, m.action_status.fencing_token)
            for m in msgs] == [("act-1", pb.ACTION_STATE_CANCELLED, 1)]
    assert a._current_action == ""


def _settle(a: Adapter) -> None:
    """Wait for the reports of superseded actions, which follow the vehicle's confirmation."""
    for t in a._superseded_threads:
        t.join(10)


def _reported(a: Adapter) -> list[tuple]:
    """(action, state, token or None) for each ActionStatusUpdate in the outbox."""
    return [(s.action_id, s.state, s.fencing_token if s.HasField("fencing_token") else None)
            for s in (m.action_status for m in _drain(a._outbox) if m.HasField("action_status"))]


def test_register_ack_in_progress_for_an_action_never_received_is_cancelled_under_its_token() -> None:
    # The plane assigned wp1 before this register was processed, so the assignment never
    # arrived. The reply names wp1 IN_PROGRESS; the adapter does not hold it and reports it
    # cancelled. The report must carry wp1's token from the reply: without one the plane
    # refuses it and records the adapter as omitting the token (C6.4).
    a = _adapter()
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="wp1", phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS, fencing_token=41)))
    assert _reported(a) == [("wp1", pb.ACTION_STATE_CANCELLED, 41)]


def test_a_cancel_for_the_local_action_is_retained_under_the_replys_token() -> None:
    # The plane acknowledges a retained outcome only under the token it was reported with.
    a = _adapter()
    assert _assign(a, 5, action_id="act-a").accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-a", phase=pb.ROBOT_ACTION_PHASE_CANCELLED, fencing_token=9)))
    assert _reported(a) == [("act-a", pb.ACTION_STATE_CANCELLED, 9)]
    kept = a._retained.get(ROBOT)
    assert (kept.action_id, kept.fencing_token) == ("act-a", 9)


def test_a_report_for_the_replys_action_never_carries_the_local_actions_token() -> None:
    a = _adapter()
    assert _assign(a, 5, action_id="act-a").accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-b", phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS, fencing_token=9)))
    _settle(a)
    # act-b under the reply's token; act-a, which the reply does not name, under its own.
    assert sorted(_reported(a)) == [("act-a", pb.ACTION_STATE_STOPPED, 5),
                                    ("act-b", pb.ACTION_STATE_CANCELLED, 9)]
    assert a._current_action == ""


def test_a_reply_without_a_token_puts_no_token_on_a_report_for_an_action_not_held() -> None:
    # UNKNOWN names an action the plane could not read and carries no token. The adapter
    # holds a different action at token 5: that token is not put on the report, so the
    # report can never match the named action's generation by guesswork.
    a = _adapter()
    assert _assign(a, 5, action_id="act-a").accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-b", phase=pb.ROBOT_ACTION_PHASE_UNKNOWN)))
    _settle(a)
    assert sorted(_reported(a)) == [("act-a", pb.ACTION_STATE_STOPPED, 5),
                                    ("act-b", pb.ACTION_STATE_STOPPED, None)]


def test_an_idle_reply_reports_the_local_action_under_its_own_token() -> None:
    # IDLE names no action; its token is the plane's mark, adopted, but not put on the
    # report for the local action, which keeps the token it was assigned under.
    a = _adapter()
    assert _assign(a, 3, action_id="act-a").accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            phase=pb.ROBOT_ACTION_PHASE_IDLE, fencing_token=20)))
    _settle(a)
    assert _reported(a) == [("act-a", pb.ACTION_STATE_STOPPED, 3)]


def test_register_ack_in_progress_for_the_local_action_keeps_it() -> None:
    a = _adapter()
    assert _assign(a, 1).accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-1", phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS, fencing_token=1)))
    assert a._current_action == "act-1"
    assert not [m for m in _drain(a._outbox) if m.HasField("action_status")]


def test_register_ack_in_progress_for_the_local_action_under_a_newer_token_stops_it() -> None:
    # The plane requeued the action and assigned it again, at a newer generation this adapter
    # never accepted. The run here is not that assignment: stopped, never re-keyed.
    a = _adapter()
    assert _assign(a, 5).accepted
    assert not a._robot.is_stopped(ROBOT)
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-1", phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS, fencing_token=9)))
    assert a._current_action == ""
    assert a._robot.is_stopped(ROBOT)
    assert _reported(a) == [("act-1", pb.ACTION_STATE_CANCELLED, 9)]
    # Not kept for a later connection: the plane never reads a kept CANCELLED for a live action.
    assert a._retained.get(ROBOT) is None


def test_register_ack_in_progress_for_the_local_action_without_a_token_stops_it() -> None:
    a = _adapter()
    assert _assign(a, 5).accepted
    _drain(a._outbox)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id="act-1", phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS)))
    assert a._current_action == ""
    assert a._robot.is_stopped(ROBOT)


def test_live_binding_with_no_manifest_still_pushes_the_catalog() -> None:
    # The live --comms vda5050 path has no hardware manifest. The snapshot pushed at
    # registration must still carry the catalog, the same one scan answers (C12.2): the
    # control plane admits assignments against it.
    a = Adapter(pb, None, Vda5050Binding(robot_id=ROBOT), ROBOT)
    assert a._capability_components() == []
    a._send_capabilities_snapshot()
    msgs = _drain(a._outbox)
    assert len(msgs) == 1 and msgs[0].HasField("capabilities")
    pushed = msgs[0].capabilities
    assert [s.action_type for s in pushed.supported_actions] == ["Navigate"]
    assert not pushed.hardware
    scan = pb.CommandResult()
    a._on_scan(scan)
    assert pushed.supported_actions == scan.scan.supported_actions
