# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Registration: the provisioning loop (C2.1) and a refused register (C13.2).

A register refused NOT_ADMITTED -- no Robot exists for this robot yet -- is answered by
announcing the robot (discover), and register is sent again every provisioning interval,
min(ttl/2, 300) s from the DiscoverAck or 60 s without one, until it is accepted. Any other
refusal is not announced. On any refusal the work held for the robot is stopped as interrupted
work, and no work is accepted until a register is accepted on the current session."""

from __future__ import annotations

import queue
import time

import pytest
from conftest import mark_registered

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"


def _adapter() -> Adapter:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    a = Adapter(pb, None, robot, ROBOT)
    a._telemetry_started = True  # no telemetry thread under test
    a._new_session()
    a._handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    return a


def _drain(q: queue.Queue) -> list:
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def _kinds(msgs) -> list[str]:
    return [m.WhichOneof("payload") for m in msgs if m is not None]


def _ack(accepted: bool, rejection=0) -> pb.ControlPlaneMessage:
    return pb.ControlPlaneMessage(register_ack=pb.RegisterAck(
        accepted=accepted, rejection=rejection, message="test"))


def _assign(a: Adapter, action_id: str, token: int) -> pb.AssignActionResult:
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE,
                                 fencing_token=token, assignment_id=f"as-{token}",
                                 payload_json=b'{"x": 5, "y": 0}'), res)
    return res.assign_action


@pytest.fixture
def fast_loop(monkeypatch):
    """The loop's waits shortened, so an iteration runs in milliseconds."""
    monkeypatch.setattr(adapter_mod, "_DISCOVER_ACK_WAIT_S", 0.05)
    monkeypatch.setattr(adapter_mod, "_provisioning_interval", lambda ttl: 0.05)


def _wait_for(q: queue.Queue, kind: str, timeout: float = 2.0):
    """The next message of `kind` on `q`, or None within `timeout`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            m = q.get(timeout=0.02)
        except queue.Empty:
            continue
        if m is not None and m.WhichOneof("payload") == kind:
            return m
    return None


# ── the interval ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("ttl", "interval"), [(0, 60.0), (2, 1.0), (120, 60.0), (1000, 300.0)])
def test_the_interval_is_half_the_ttl_capped_at_300_or_60_without_one(ttl, interval) -> None:
    assert adapter_mod._provisioning_interval(ttl) == interval


# ── NOT_ADMITTED: announce, then register again ──────────────────────────────

def test_hello_registers_first_and_never_discovers() -> None:
    a = _adapter()
    assert _kinds(_drain(a._outbox)) == ["register"]


def test_not_admitted_is_answered_by_a_discover_and_no_immediate_register() -> None:
    a = _adapter()
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_NOT_ADMITTED), None)
    msgs = _drain(a._outbox)
    assert _kinds(msgs) == ["discover"]
    assert msgs[0].discover.robot_id == ROBOT
    a._shutdown.set()  # end the loop's wait


def test_the_loop_registers_again_after_the_discover_ack(fast_loop) -> None:
    a = _adapter()
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_NOT_ADMITTED), None)
    assert _kinds(_drain(a._outbox)) == ["discover"]
    a._handle_control_plane(pb.ControlPlaneMessage(discover_ack=pb.DiscoverAck(
        accepted=True, ttl_seconds=2)), None)
    assert a._discover_ttl == 2
    again = _wait_for(a._outbox, "register")
    assert again is not None and again.register.robot_id == ROBOT


def test_an_iteration_pending_when_the_robot_is_admitted_sends_no_register(monkeypatch) -> None:
    """The iteration is started by the refusal and fires after the robot was admitted on the
    same session (another register, accepted, got there first): it sends nothing."""
    monkeypatch.setattr(adapter_mod, "_DISCOVER_ACK_WAIT_S", 0.3)  # no DiscoverAck comes
    monkeypatch.setattr(adapter_mod, "_provisioning_interval", lambda ttl: 0.0)
    a = _adapter()
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_NOT_ADMITTED), None)
    assert _kinds(_drain(a._outbox)) == ["discover"]
    a._handle_control_plane(_ack(True), None)  # admitted while the iteration waits
    assert a._registered
    assert _wait_for(a._outbox, "register", timeout=0.8) is None


def test_a_replaced_session_ends_the_loop_without_a_register() -> None:
    a = _adapter()
    _drain(a._outbox)
    old_session, old_outbox = a._session, a._outbox
    a._discover_acked.set()
    a._outbox = queue.Queue()
    a._new_session()  # the stream dropped and a new one opened
    a._shutdown.wait = lambda timeout: False  # the interval passes at once
    a._provisioning_iteration(old_session, old_outbox)
    assert "register" not in _kinds(_drain(old_outbox))
    assert "register" not in _kinds(_drain(a._outbox))


def test_an_admitted_robot_ends_the_loop_without_a_register() -> None:
    a = _adapter()
    _drain(a._outbox)
    a._discover_acked.set()
    a._registered = True
    a._shutdown.wait = lambda timeout: False
    a._provisioning_iteration(a._session, a._outbox)
    assert _drain(a._outbox) == []


def test_shutdown_during_the_interval_ends_the_loop_without_a_register() -> None:
    a = _adapter()
    _drain(a._outbox)
    a._discover_acked.set()
    a._shutdown.set()
    a._provisioning_iteration(a._session, a._outbox)
    assert _drain(a._outbox) == []


def test_a_session_that_opens_during_the_interval_gets_no_register_from_the_old_loop() -> None:
    a = _adapter()
    _drain(a._outbox)
    refused_session, refused_outbox = a._session, a._outbox
    new_outbox: queue.Queue = queue.Queue()
    a._discover_acked.set()

    def the_stream_drops_during_the_interval(timeout) -> bool:
        a._outbox = new_outbox
        a._new_session()
        return False

    a._shutdown.wait = the_stream_drops_during_the_interval
    a._provisioning_iteration(refused_session, refused_outbox)
    assert _drain(refused_outbox) == []
    assert _drain(new_outbox) == []


def test_the_register_is_put_on_the_refused_sessions_outbox() -> None:
    a = _adapter()
    _drain(a._outbox)
    refused = a._outbox
    a._outbox = queue.Queue()  # not the refused session's (same session number, for the put)
    a._discover_acked.set()
    a._shutdown.wait = lambda timeout: False
    a._provisioning_iteration(a._session, refused)
    assert _kinds(_drain(refused)) == ["register"]
    assert _drain(a._outbox) == []


# ── other refusals: not announced ────────────────────────────────────────────

@pytest.mark.parametrize("rejection", [
    pb.REGISTRATION_REJECTION_UNSPECIFIED,
    pb.REGISTRATION_REJECTION_ALREADY_EXISTS,
    pb.REGISTRATION_REJECTION_VERSION_MISMATCH,
    pb.REGISTRATION_REJECTION_NAMESPACE_MISMATCH,
    pb.REGISTRATION_REJECTION_INVALID_ROBOT_ID,
])
def test_any_other_refusal_is_not_announced_and_not_retried(fast_loop, rejection) -> None:
    a = _adapter()
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, rejection), None)
    assert "discover" not in _kinds(_drain(a._outbox))
    assert _wait_for(a._outbox, "register", timeout=0.3) is None
    assert not a._registered


def test_a_rejection_code_this_build_does_not_know_is_a_refusal_not_a_crash(fast_loop) -> None:
    a = _adapter()
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, 42), None)  # proto3 enums are open
    assert "discover" not in _kinds(_drain(a._outbox))
    assert not a._registered
    assert _wait_for(a._outbox, "register", timeout=0.3) is None


# ── admitted: the catalog and a full snapshot ────────────────────────────────

def test_admission_through_the_loop_sends_the_catalog_and_a_full_snapshot(fast_loop) -> None:
    """What was sent at the hello, while no Robot existed, was not kept by the control plane:
    the accepted register is what sends the catalog (C12.2) and a full snapshot (C6.1)."""
    a = _adapter()
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_NOT_ADMITTED), None)
    assert _wait_for(a._outbox, "register") is not None
    a._snapshot_pending = False  # the hello's full snapshot went out while not admitted
    a._handle_control_plane(_ack(True), None)
    caps = [m.capabilities for m in _drain(a._outbox) if m.WhichOneof("payload") == "capabilities"]
    assert [[s.action_type for s in c.supported_actions] for c in caps] == [["Navigate"]]
    assert a._snapshot_pending


# ── C13.2: no work for a robot not under management on this session ──────────

def test_work_before_any_register_is_answered_is_refused_robot_unavailable() -> None:
    a = _adapter()
    r = _assign(a, "act-1", 5)
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


def test_work_after_a_version_mismatch_is_refused() -> None:
    a = _adapter()
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_VERSION_MISMATCH), None)
    r = _assign(a, "act-1", 5)
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


def test_work_while_not_admitted_is_refused(fast_loop) -> None:
    a = _adapter()
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_NOT_ADMITTED), None)
    assert _assign(a, "act-1", 5).rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    a._shutdown.set()


def test_a_stale_token_is_still_refused_stale_when_unregistered() -> None:
    """The registration gate comes after the fencing branches, so a superseded command still
    learns it lost on ordering (C3.1 runs on the refused session in the conformance suite)."""
    a = mark_registered(_adapter())
    assert _assign(a, "act-1", 10).accepted
    a._handle_control_plane(_ack(False, pb.REGISTRATION_REJECTION_VERSION_MISMATCH), None)
    assert _assign(a, "act-2", 6).rejection == pb.ASSIGN_ACTION_REJECTION_STALE_FENCING_TOKEN


def test_work_is_accepted_once_a_register_is_accepted() -> None:
    a = _adapter()
    a._handle_control_plane(_ack(True), None)
    assert _assign(a, "act-1", 5).accepted


def test_a_new_session_is_unregistered_until_its_register_is_accepted() -> None:
    a = _adapter()
    a._handle_control_plane(_ack(True), None)
    a._new_session()
    assert _assign(a, "act-1", 5).rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


# ── a refused register stops the work held for the robot ─────────────────────

@pytest.mark.parametrize("rejection", [
    pb.REGISTRATION_REJECTION_NOT_ADMITTED,
    pb.REGISTRATION_REJECTION_VERSION_MISMATCH,
    pb.REGISTRATION_REJECTION_UNSPECIFIED,
])
def test_a_refused_register_ends_the_running_action_as_interrupted_work(rejection) -> None:
    a = _adapter()
    a._handle_control_plane(_ack(True), None)
    assert _assign(a, "act-1", 5).accepted
    _drain(a._outbox)
    a._handle_control_plane(_ack(False, rejection), None)
    a._shutdown.set()
    statuses = [m.action_status for m in _drain(a._outbox)
                if m.WhichOneof("payload") == "action_status"]
    assert [(s.action_id, s.state) for s in statuses] == [
        ("act-1", getattr(pb, task_map.TASK_STOPPED)),
        ("act-1", getattr(pb, task_map.TASK_FAILED))]
    assert statuses[-1].end_cause == pb.ACTION_END_CAUSE_ESTOP
    assert a._current_action == ""
    assert a._robot.is_stopped(ROBOT)
    # Its re-delivery at the same token starts nothing (interrupted work is never continued).
    a._handle_control_plane(_ack(True), None)
    r = _assign(a, "act-1", 5)
    assert r.accepted and "not continued" in r.message
    assert a._current_action == ""
