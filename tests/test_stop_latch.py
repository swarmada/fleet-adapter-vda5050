# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Contract 1.4.0's stop path (RFC-0001 #fleet-adapter-protocol-the-stop-latch, and the
retained outcome of #fleet-adapter-protocol-reconnect-action-state-reconciliation):

* a stop latches its ID, on the SafetyStream or as its fence on the ControlStream, and while
  any ID is latched every AssignAction is refused ROBOT_UNAVAILABLE and the latch reported;
* only an EstopClear naming the ID, once its fence has arrived, lifts it, and is answered with
  an EstopClearAck; a clear that arrives first is kept;
* a command or a clear from a leader term below the highest seen is not acted on;
* work a stop interrupts ends STOPPED then FAILED (cause ESTOP), and its re-delivery is
  accepted with nothing started;
* the last action's terminal outcome is kept until an ActionOutcomeAck names it and its
  token, and is reported again (retained = true) after each RegisterAck until then."""

from __future__ import annotations

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"


class _Robot(SimulatedRobot):
    """The simulated vehicle, counting the moves it is commanded."""

    def __init__(self) -> None:
        super().__init__()
        self.moves = 0

    def command_move(self, robot_id, x, y, yaw=0.0):
        self.moves += 1
        super().command_move(robot_id, x, y, yaw)


def _adapter(contract: str = "1.4.0", state_dir=None) -> Adapter:
    robot = _Robot()
    robot.spawn(ROBOT)
    a = Adapter(pb, None, robot, ROBOT, state_dir=state_dir)
    a._telemetry_started = True  # no telemetry thread under test
    a.statuses = []  # every ActionStatusUpdate the ControlStream carried, in order
    a._handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version=contract)), None)
    _drain(a._outbox)
    return a


def _drain(q) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _control(a: Adapter) -> list:
    msgs = _drain(a._outbox)
    a.statuses += [m.action_status for m in msgs if m.WhichOneof("payload") == "action_status"]
    return msgs


def _cmd(a: Adapter, term: int = 0, **arm):
    a._handle_command(pb.Command(command_id=1, robot_id=ROBOT, leader_term=term, **arm))
    results = [m.command_result for m in _control(a) if m.WhichOneof("payload") == "command_result"]
    return results[-1] if results else None


def _assign(a: Adapter, token: int, action_id: str = "act-1", term: int = 0):
    r = _cmd(a, term, assign_action=pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 1, "y": 2}'))
    return r.assign_action


def _safety(a: Adapter, **arm) -> list:
    a._on_safety_message(pb.ControlPlaneSafetyMessage(robot_id=ROBOT, **arm))
    return _drain(a._safety_outbox)


def _kinds(msgs) -> list:
    return [m.WhichOneof("payload") for m in msgs]


def _stop(a: Adapter, estop_id: str) -> None:
    """A stop as the control plane sends it: the Estop, and its fence on the ControlStream."""
    _safety(a, estop=pb.Estop(estop_id=estop_id, reason="test"))
    _cmd(a, estop=pb.Estop(estop_id=estop_id, reason="test"))


def test_declares_contract_140() -> None:
    assert adapter_mod.CONTRACT_VERSION == "1.4.0"
    hello = next(iter(Adapter(pb, None, _Robot(), ROBOT)._control_requests()))
    assert hello.hello.contract_version == "1.4.0"


def test_a_stop_latches_and_work_is_refused_robot_unavailable() -> None:
    a = _adapter()
    _safety(a, estop=pb.Estop(estop_id="s-1", reason="test"))
    r = _assign(a, 5)
    assert not r.accepted and r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert "s-1" in r.message
    sent = _drain(a._safety_outbox)
    assert any(m.WhichOneof("payload") == "estop_ack" and m.estop_ack.estop_id == "s-1"
               for m in sent)
    assert "local_stop" in _kinds(sent)
    assert a._robot.moves == 0


def test_the_fence_alone_latches_too() -> None:
    a = _adapter()
    assert _cmd(a, estop=pb.Estop(estop_id="s-2")) is None  # answered on the SafetyStream only
    acks = [m.estop_ack for m in _drain(a._safety_outbox) if m.WhichOneof("payload") == "estop_ack"]
    assert acks and acks[-1].estop_id == "s-2" and acks[-1].state == pb.ESTOP_STATE_STOPPED
    assert _assign(a, 5).rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


def test_the_clear_after_the_fence_lifts_and_is_acknowledged() -> None:
    a = _adapter()
    _stop(a, "s-3")
    sent = _safety(a, estop_clear=pb.EstopClear(estop_id="s-3"))
    assert [m.estop_clear_ack.estop_id for m in sent
            if m.WhichOneof("payload") == "estop_clear_ack"] == ["s-3"]
    r = _assign(a, 6)
    assert r.accepted and a._robot.moves == 1


def test_a_clear_before_its_fence_is_kept_and_takes_effect_with_it() -> None:
    a = _adapter()
    _safety(a, estop=pb.Estop(estop_id="s-4"))
    assert "estop_clear_ack" not in _kinds(_safety(a, estop_clear=pb.EstopClear(estop_id="s-4")))
    assert _assign(a, 5).rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    _drain(a._safety_outbox)
    _cmd(a, estop=pb.Estop(estop_id="s-4"))
    assert "estop_clear_ack" in _kinds(_drain(a._safety_outbox))
    assert _assign(a, 6).accepted


def test_a_clear_lifts_only_the_id_it_names() -> None:
    a = _adapter()
    _stop(a, "s-5")
    _stop(a, "s-6")
    _safety(a, estop_clear=pb.EstopClear(estop_id="s-5"))
    r = _assign(a, 5)
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE and "s-6" in r.message


def test_a_clear_sent_again_is_acknowledged_again_and_a_cleared_fence_is_ignored() -> None:
    a = _adapter()
    _stop(a, "s-7")
    _safety(a, estop_clear=pb.EstopClear(estop_id="s-7"))
    assert "estop_clear_ack" in _kinds(_safety(a, estop_clear=pb.EstopClear(estop_id="s-7")))
    _cmd(a, estop=pb.Estop(estop_id="s-7"))  # a late copy of the fence
    _drain(a._safety_outbox)
    assert _assign(a, 5).accepted


def test_a_stale_leader_term_is_not_acted_on() -> None:
    a = _adapter()
    assert _assign(a, 5, term=3).accepted
    r = _cmd(a, 2, cancel_action=pb.CancelAction(action_id="act-1", fencing_token=6))
    assert r.stale_leader_term and not r.HasField("cancel_action")
    assert a._current_action == "act-1"
    _stop(a, "s-8")
    assert "estop_clear_ack" not in _kinds(_safety(a, estop_clear=pb.EstopClear(
        estop_id="s-8", leader_term=2)))
    assert _assign(a, 7, action_id="act-2", term=3).rejection == \
        pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


def test_a_stop_is_acted_on_whatever_its_term() -> None:
    a = _adapter()
    _assign(a, 5, term=4)
    _cmd(a, 1, estop=pb.Estop(estop_id="s-9"))
    assert _assign(a, 6, action_id="act-2", term=4).rejection == \
        pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


def test_interrupted_work_ends_failed_by_the_stop_and_is_not_continued() -> None:
    a = _adapter()
    assert _assign(a, 5).accepted
    a.statuses.clear()
    _stop(a, "s-10")
    _control(a)
    states = [(s.state, s.end_cause) for s in a.statuses]
    assert states == [(pb.ACTION_STATE_STOPPED, pb.ACTION_END_CAUSE_ESTOP),
                      (pb.ACTION_STATE_FAILED, pb.ACTION_END_CAUSE_ESTOP)]
    assert a._current_action == ""
    # Re-delivered at the same token, under the latch or after it: accepted, nothing started.
    assert _assign(a, 5).accepted
    _safety(a, estop_clear=pb.EstopClear(estop_id="s-10"))
    assert _assign(a, 5).accepted
    assert a._robot.moves == 1 and a._current_action == ""


def test_each_safety_stream_that_opens_reports_the_latch() -> None:
    a = _adapter()
    _stop(a, "s-11")
    _drain(a._safety_outbox)
    a._report_latch()
    sent = _drain(a._safety_outbox)
    assert [m.estop_ack.estop_id for m in sent if m.WhichOneof("payload") == "estop_ack"] == ["s-11"]
    assert "local_stop" in _kinds(sent)


def test_an_edge_stop_sets_the_local_latch_and_reports_it() -> None:
    a = _adapter()
    a._edge_stop("e-1", lambda msg: None)
    reports = [m.local_stop for m in _drain(a._safety_outbox)
               if m.WhichOneof("payload") == "local_stop"]
    assert reports and reports[-1].latched and len(reports[-1].set_id) == 32
    assert _assign(a, 5).rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE


def test_below_140_an_estop_still_stops_but_latches_nothing() -> None:
    a = _adapter("1.3.0")
    _safety(a, estop=pb.Estop(estop_id="s-12"))
    assert _assign(a, 5).accepted


# ── the retained outcome ─────────────────────────────────────────────────────


def _register(a: Adapter) -> list:
    a.statuses.clear()
    a._handle_control_plane(pb.ControlPlaneMessage(register_ack=pb.RegisterAck(accepted=True)), None)
    _control(a)
    return list(a.statuses)


def _cancelled(a: Adapter, token: int = 5) -> None:
    assert _assign(a, token).accepted
    _cmd(a, cancel_action=pb.CancelAction(action_id="act-1", fencing_token=token + 1))


def test_the_terminal_outcome_is_reported_retained_until_acknowledged() -> None:
    a = _adapter()
    _cancelled(a)
    (out,) = _register(a)
    assert (out.action_id, out.state, out.fencing_token, out.retained) == \
        ("act-1", pb.ACTION_STATE_CANCELLED, 5, True)
    assert [s.retained for s in _register(a)] == [True]  # again on the next RegisterAck
    a._handle_control_plane(pb.ControlPlaneMessage(outcome_ack=pb.ActionOutcomeAck(
        action_id="act-1", fencing_token=4)), None)  # not its token: kept
    assert [s.retained for s in _register(a)] == [True]
    a._handle_control_plane(pb.ControlPlaneMessage(outcome_ack=pb.ActionOutcomeAck(
        action_id="act-1", fencing_token=5)), None)
    assert _register(a) == []


def test_only_a_retained_report_carries_retained() -> None:
    a = _adapter()
    assert _assign(a, 5).accepted
    _cmd(a, cancel_action=pb.CancelAction(action_id="act-1", fencing_token=6))
    assert [s.state for s in a.statuses] == [pb.ACTION_STATE_RUNNING, pb.ACTION_STATE_CANCELLED]
    assert not any(s.retained for s in a.statuses)
    assert all(s.fencing_token == 5 for s in a.statuses)  # each report carries its token


def test_the_retained_outcome_survives_a_restart_with_a_state_dir(tmp_path) -> None:
    a = _adapter(state_dir=tmp_path)
    _cancelled(a)
    b = _adapter(state_dir=tmp_path)
    (out,) = _register(b)
    assert (out.action_id, out.fencing_token, out.retained) == ("act-1", 5, True)


def test_below_140_no_outcome_is_retained() -> None:
    a = _adapter("1.3.0")
    _cancelled(a)
    assert _register(a) == []
