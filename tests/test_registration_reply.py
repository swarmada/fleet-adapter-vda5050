# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A local action the registration reply does not name is stopped, and reported once the vehicle
confirms the stop.

The reply names the action the control plane holds for the robot, or none. An action this adapter
runs that the reply does not name is the plane's past: the vehicle is stopped (a cancelOrder), and the
action is reported STOPPED under its own token only once a state the vehicle sent after the stop shows
it at rest, the estop's rule. If none does within the stop budget, or new work is taken meanwhile, it is
reported FAILED with the reason. End cause SUPERSEDED, naming the displacing action, when the reply names
one; no cause when it names none. The report is never kept for a later connection, and never touches the
token record of a newer run under the same action id."""

from __future__ import annotations

import itertools
import json
import queue

from conftest import mark_registered, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
AT_REST = {"agvPosition": {"x": 1.0, "y": 1.0, "theta": 0.0, "mapId": "L1",
                           "positionInitialized": True},
           "driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
           "operatingMode": "AUTOMATIC", "nodeStates": [], "actionStates": []}
_IDS = itertools.count(100)


def _state(b: Vda5050Binding, **fields) -> None:
    """The vehicle sends a state (a new headerId and timestamp, so it is counted)."""
    n = next(_IDS)
    b.on_state(json.dumps({**AT_REST, **fields, "headerId": n, "timestamp": f"s{n}"}))


def _adapter() -> tuple[Adapter, Vda5050Binding]:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))
    b.on_connection('{"connectionState": "ONLINE"}')
    _state(b)
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    a._stop_confirm_sleep = lambda _s: None   # the budget is counted in polls; no real waiting
    return a, b


def _vehicle_sends(a: Adapter, **fields) -> None:
    """While the adapter waits, the vehicle sends one state per poll."""
    b = a._robot
    a._stop_confirm_sleep = lambda _s: _state(b, **fields)


def _settled(a: Adapter) -> None:
    """After the stop, the vehicle reports at rest with nothing left of the order it was sent."""
    _vehicle_sends(a, orderId=a._robot._last_order_id)


def _assign(a: Adapter, action_id: str, token: int):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}'), res)
    return res.assign_action


def _reply(a: Adapter, phase, action_id: str = "", token: int = 0) -> None:
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            action_id=action_id, phase=phase, fencing_token=token)))
    for t in a._superseded_threads:
        t.join(10)


def _reports(a: Adapter) -> list[dict]:
    out = []
    while not a._outbox.empty():
        m = a._outbox.get_nowait()
        if m.HasField("action_status"):
            s = m.action_status
            out.append({"action": s.action_id, "state": pb.ActionState.Name(s.state),
                        "token": s.fencing_token if s.HasField("fencing_token") else None,
                        "cause": pb.ActionEndCause.Name(s.end_cause),
                        "by": s.superseding_action_id, "message": s.message})
    return out


def _cancels(b: Vda5050Binding) -> int:
    return sum(1 for topic, payload in b.published if topic.endswith("/instantActions")
               and json.loads(payload)["actions"][0]["actionType"] == "cancelOrder")


def _running(action: str, token: int) -> tuple[Adapter, Vda5050Binding]:
    a, b = _adapter()
    assert _assign(a, action, token).accepted
    a._outbox.queue.clear()
    return a, b


def test_revoking_for_another_action_stops_the_local_one_and_reports_it_once_confirmed() -> None:
    a, b = _running("act-a", 3)
    _settled(a)
    _reply(a, pb.ROBOT_ACTION_PHASE_REVOKING, "act-b", 9)
    assert a._current_action == "" and _cancels(b) == 1
    [r] = _reports(a)
    assert (r["action"], r["state"], r["token"], r["cause"], r["by"]) == (
        "act-a", "ACTION_STATE_STOPPED", 3, "ACTION_END_CAUSE_SUPERSEDED", "act-b")
    assert "names act-b (ROBOT_ACTION_PHASE_REVOKING)" in r["message"]
    assert "act-a" not in a._action_tokens and a._retained.get(ROBOT) is None


def test_in_progress_for_another_action_reports_both_each_under_its_own_token() -> None:
    a, _ = _running("act-a", 3)
    _settled(a)
    _reply(a, pb.ROBOT_ACTION_PHASE_IN_PROGRESS, "act-b", 9)
    got = sorted((r["action"], r["state"], r["token"], r["cause"], r["by"]) for r in _reports(a))
    assert got == [("act-a", "ACTION_STATE_STOPPED", 3, "ACTION_END_CAUSE_SUPERSEDED", "act-b"),
                   ("act-b", "ACTION_STATE_CANCELLED", 9, "ACTION_END_CAUSE_UNSPECIFIED", "")]


def test_an_idle_reply_ends_the_local_action_once_confirmed_with_no_cause_stated() -> None:
    # No action displaced it, so none is named and no cause is stated (the proto: leave it
    # unset rather than guess; SUPERSEDED without the displacing action is malformed).
    a, _ = _running("act-a", 3)
    _settled(a)
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    [r] = _reports(a)
    assert (r["action"], r["state"], r["token"], r["cause"], r["by"]) == (
        "act-a", "ACTION_STATE_STOPPED", 3, "ACTION_END_CAUSE_UNSPECIFIED", "")
    assert "names no action" in r["message"]


def test_a_state_from_before_the_stop_does_not_confirm_it() -> None:
    # The vehicle's last state, received before the stop, already reads at rest with nothing
    # left. No state comes after the stop: not confirmed, FAILED with the reason.
    a, b = _running("act-a", 3)
    _state(b, orderId=b._last_order_id)
    assert b.stop_confirmed(ROBOT)
    _reply(a, pb.ROBOT_ACTION_PHASE_REVOKING, "act-b", 9)
    [r] = _reports(a)
    assert (r["action"], r["state"], r["token"], r["cause"], r["by"]) == (
        "act-a", "ACTION_STATE_FAILED", 3, "ACTION_END_CAUSE_SUPERSEDED", "act-b")
    assert "the stop was not confirmed: no state after the stop showed the vehicle at rest" in r["message"]
    assert "outcome is unknown" not in r["message"]   # superseded: the reply named who has the robot
    assert a._retained.get(ROBOT) is None   # not kept: the plane has moved on


def test_a_vehicle_still_driving_after_the_stop_is_not_confirmed() -> None:
    a, _ = _running("act-a", 3)
    _vehicle_sends(a, driving=True)
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    [r] = _reports(a)
    assert r["state"] == "ACTION_STATE_FAILED" and "not confirmed" in r["message"]


def test_a_vehicle_that_is_not_connected_is_not_confirmed_and_the_reason_says_so() -> None:
    a, b = _running("act-a", 3)
    b.on_connection('{"connectionState": "CONNECTIONBROKEN"}')
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    [r] = _reports(a)
    assert r["state"] == "ACTION_STATE_FAILED"
    assert "the vehicle is not connected" in r["message"] and "CONNECTIONBROKEN" in r["message"]


def test_the_same_action_assigned_again_during_the_wait_keeps_its_own_record() -> None:
    # The plane requeued act-a, the reply is IDLE, and act-a comes back to this robot under a
    # newer token while the old stop is still being confirmed. The old run is reported FAILED
    # under its own token; the new run's token and record are untouched, and nothing is kept
    # for a later connection.
    a, b = _running("act-a", 3)
    reassigned = []

    def during_wait(_s: float) -> None:
        if not reassigned:
            reassigned.append(_assign(a, "act-a", 7))
        _state(b, orderId=b._last_order_id)       # at rest on the new order's id: must not count

    a._stop_confirm_sleep = during_wait
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    assert reassigned[0].accepted
    reports = [r for r in _reports(a) if r["state"] != "ACTION_STATE_RUNNING"]
    assert [(r["action"], r["state"], r["token"]) for r in reports] == [
        ("act-a", "ACTION_STATE_FAILED", 3)]
    assert "new work (act-a) was taken before the stop was confirmed" in reports[0]["message"]
    assert a._current_action == "act-a" and a._action_tokens["act-a"] == 7
    assert a._retained.get(ROBOT) is None


def test_an_error_while_waiting_is_reported_not_swallowed() -> None:
    a, b = _running("act-a", 3)

    def broken(_robot_id: str) -> bool:
        raise ValueError("binding fault")

    b.stop_confirmed = broken
    _settled(a)
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    [r] = _reports(a)
    assert r["state"] == "ACTION_STATE_FAILED" and "binding fault" in r["message"]


def test_a_report_is_not_sent_on_a_session_whose_registration_is_not_accepted(capsys) -> None:
    # The ControlStream reconnects during the wait: the new session's outbox is installed before
    # its RegisterRobot is answered. Nothing about the robot goes on it until then (C2.1): the
    # report is dropped, on the old session as on the new one, and the log says so.
    a, b = _running("act-a", 3)
    old = a._outbox
    new: queue.Queue = queue.Queue()

    def reconnect_during_wait(_s: float) -> None:
        a._outbox = new
        _state(b, orderId=b._last_order_id)

    a._stop_confirm_sleep = reconnect_during_wait
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    assert not [m for m in list(old.queue) + list(new.queue) if m.HasField("action_status")]
    assert "is dropped: this session's registration has not been accepted (C2.1)" in capsys.readouterr().err


def test_a_report_is_dropped_once_the_registration_is_refused() -> None:
    a, b = _running("act-a", 3)

    def refused_during_wait(_s: float) -> None:
        a._registered = False
        _state(b, orderId=b._last_order_id)

    a._stop_confirm_sleep = refused_during_wait
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    assert not _reports(a)


def test_a_report_goes_on_the_new_session_once_its_registration_is_accepted() -> None:
    a, b = _running("act-a", 3)
    new: queue.Queue = queue.Queue()

    def reconnect_and_register(_s: float) -> None:
        a._outbox = new
        a._registered_outbox = new      # the new session's RegisterAck was accepted
        _state(b, orderId=b._last_order_id)

    a._stop_confirm_sleep = reconnect_and_register
    _reply(a, pb.ROBOT_ACTION_PHASE_IDLE)
    [r] = _reports(a)
    assert (r["action"], r["state"]) == ("act-a", "ACTION_STATE_STOPPED")


def test_an_accepted_registration_records_its_session_outbox() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    a = Adapter(pb, None, b, ROBOT)
    assert a._registered_outbox is None
    a._handle_control_plane(pb.ControlPlaneMessage(register_ack=pb.RegisterAck(
        accepted=True, telemetry_interval_seconds=1)), None)
    assert a._registered and a._registered_outbox is a._outbox


def test_revoking_for_the_local_action_under_a_newer_token_stops_it(capsys) -> None:
    # The plane is revoking a newer generation of act-a, which this adapter never accepted; the
    # run here is not that one. Stopped, reported under the reply's token, nothing kept.
    a, b = _running("act-a", 3)
    _reply(a, pb.ROBOT_ACTION_PHASE_REVOKING, "act-a", 9)
    assert a._current_action == "" and _cancels(b) == 1
    [r] = _reports(a)
    assert (r["action"], r["state"], r["token"]) == ("act-a", "ACTION_STATE_CANCELLED", 9)
    assert a._retained.get(ROBOT) is None and "act-a" not in a._action_tokens
    assert "names act-a REVOKING under token 9, and it runs here under token 3" in capsys.readouterr().err


def test_unknown_for_the_local_action_is_reported_once_confirmed() -> None:
    # The plane could not read act-a and sends no token. The run is stopped, and reported under
    # its own token once the vehicle confirms rest, with no cause (nothing displaced it); its
    # token entry goes, and nothing is kept for a later connection.
    a, b = _running("act-a", 3)
    _settled(a)
    _reply(a, pb.ROBOT_ACTION_PHASE_UNKNOWN, "act-a")
    assert a._current_action == "" and _cancels(b) == 1
    [r] = _reports(a)
    assert (r["action"], r["state"], r["token"], r["cause"], r["by"]) == (
        "act-a", "ACTION_STATE_STOPPED", 3, "ACTION_END_CAUSE_UNSPECIFIED", "")
    assert "could not read this action" in r["message"]
    assert "act-a" not in a._action_tokens and a._retained.get(ROBOT) is None


def test_unknown_for_the_local_action_not_confirmed_is_failed_saying_the_outcome_is_unknown() -> None:
    # Nobody can establish what the run achieved. The v0.4 control plane drops
    # ACTION_STATE_UNKNOWN, so FAILED (fail-closed, as RFC-0001 has a v0.4 adapter do), under its
    # own token, with no cause, and a reason that says the outcome is unknown.
    a, b = _running("act-a", 3)
    _reply(a, pb.ROBOT_ACTION_PHASE_UNKNOWN, "act-a")
    assert a._current_action == "" and _cancels(b) == 1
    [r] = _reports(a)
    assert (r["action"], r["state"], r["token"], r["cause"], r["by"]) == (
        "act-a", "ACTION_STATE_FAILED", 3, "ACTION_END_CAUSE_UNSPECIFIED", "")
    assert "could not read this action" in r["message"] and "not confirmed" in r["message"]
    assert "the outcome is unknown" in r["message"]
    assert "act-a" not in a._action_tokens and a._retained.get(ROBOT) is None


def test_revoking_for_the_local_action_keeps_it() -> None:
    a, b = _running("act-a", 3)
    _reply(a, pb.ROBOT_ACTION_PHASE_REVOKING, "act-a", 3)
    assert a._current_action == "act-a" and _cancels(b) == 0 and not _reports(a)


def test_the_cancel_stop_test_needs_a_state_at_rest_with_nothing_left() -> None:
    _, b = _running("act-a", 3)
    last = b._last_order_id
    assert last
    for fields, want in [
        ({"driving": True}, False),
        ({"velocity": {"vx": 0.5, "vy": 0.0, "omega": 0.0}}, False),
        ({"nodeStates": [{"nodeId": "n1"}]}, False),
        ({"actionStates": [{"actionId": "x", "actionStatus": "RUNNING"}]}, False),
        ({"operatingMode": "MANUAL"}, False),
        ({"orderId": "another-order"}, False),
        ({"paused": True, "operatingMode": "MANUAL"}, True),
        ({}, True),
    ]:
        _state(b, **{"orderId": last, **fields})
        assert b.stop_confirmed(ROBOT) is want, fields
