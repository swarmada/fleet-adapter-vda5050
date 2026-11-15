# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""An order the vehicle rejects ends its action FAILED, with the reason.

VDA5050 2.0.0 6.6.4: a vehicle does not take a rejected order into its buffer, and reports a
warning until it accepts a new order; 8.1: the error's references can name the orderId. The
vehicle goes on reporting its previous orderId, so before this the action stayed RUNNING until
its lease lapsed or the control plane's own timeout ended it.

* Rejected: the order sent last, named by an error's ``orderId`` reference, and not reported
  by the vehicle as its own. The error type is not matched.
* The action ends FAILED with the vehicle's reason and EXECUTION_FAILURE, as every FAILED from
  the task state now does. The reason is the vehicle's text, stripped of control characters
  and cut to length.
* A rejected order is not waited on: the next order goes straight out. But "rejected" is read
  from an error naming the order, and VDA5050 lets any error name one, so the stop that
  follows the FAILED still sends its cancelOrder.
* A vehicle that takes the order after all, once it was stopped, gets one cancelOrder more.
* Errors are read as vda-5050-lib's own master controller reads them: not one about another
  topic, and not one about an action unless it is an orderError.
* orderIds carry a token unique to the adapter process, so a rejection the vehicle still shows
  from an earlier process cannot be taken for one of this process's orders.
* The adapter never sends an order update: every order has a new orderId and orderUpdateId 0."""

from __future__ import annotations

import json

from conftest import mark_registered, mark_transport_live
from swarmada_sdk.safety import LeaseMonitor

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.profiles.vda5050 import clean_text
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
ORDER = f"{ROBOT}-b0-order"  # the orderIds of a binding with boot token "b0"


# ── helpers ──────────────────────────────────────────────────────────────────

def _rejection(order_id: str, etype="validationError", desc="invalid order: bad node") -> dict:
    """A rejection as vda-5050-lib's _createOrderError builds it."""
    return {"errorType": etype, "errorLevel": "WARNING", "errorDescription": desc,
            "errorReferences": [{"referenceKey": "topic", "referenceValue": "order"},
                                {"referenceKey": "headerId", "referenceValue": "3"},
                                {"referenceKey": "orderId", "referenceValue": order_id},
                                {"referenceKey": "orderUpdateId", "referenceValue": "0"}]}


def _state(order_id="old", nodes=0, driving=False, vx=0.0, errors=(), actions=()) -> str:
    return json.dumps({
        "driving": driving, "velocity": {"vx": vx, "vy": 0.0, "omega": 0.0},
        "orderId": order_id, "nodeStates": [{"nodeId": f"n{i}"} for i in range(nodes)],
        "actionStates": [{"actionId": a, "actionStatus": st} for a, st in actions],
        "errors": list(errors)})


def _binding(boot: str = "b0") -> Vda5050Binding:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot=boot))
    b.on_state(_state())  # idle, on an order from before this process
    return b


def _wire(b: Vda5050Binding) -> list[str]:
    out = []
    for topic, payload in b.published:
        msg = json.loads(payload)
        out.append(msg["orderId"] if topic.endswith("/order") else msg["actions"][0]["actionType"])
    return out


def _assign(a: Adapter, action_id: str, token: int):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}',
        lease_duration_ms=30_000, lease_generation=1), res)
    return res.assign_action


def _updates(a: Adapter) -> list:
    out = []
    while not a._outbox.empty():
        m = a._outbox.get_nowait()
        if m.WhichOneof("payload") == "action_status":
            out.append(m.action_status)
    return out


# ── the rule ─────────────────────────────────────────────────────────────────

def test_a_rejected_order_fails_its_action_with_the_reason_and_execution_failure() -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    now = [0.0]
    a._lease = LeaseMonitor(on_expiry=a._self_stop, clock=lambda: now[0])
    assert _assign(a, "act-1", 5).accepted
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    a._maybe_emit_task_progress()
    *_, last = _updates(a)
    assert (last.action_id, last.state) == ("act-1", pb.ACTION_STATE_FAILED)
    assert last.message == (f"the vehicle rejected order {ORDER}-1: validationError: "
                            "invalid order: bad node")
    assert last.end_cause == pb.ACTION_END_CAUSE_EXECUTION_FAILURE
    # The stop on FAILED still cancels: "rejected" is read from an error, not known.
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]
    now[0] = 120.0
    a._lease.tick()  # released with the action
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"] and _updates(a) == []


def test_any_error_type_naming_the_order_counts() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1", etype="vendorSpecificRefusal")]))
    assert b.task_state(ROBOT) == task_map.TASK_FAILED


def test_a_rejection_without_type_or_description_still_says_which_order() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1", etype=None, desc=None)]))
    assert b.task_detail(ROBOT) == f"the vehicle rejected order {ORDER}-1"


def test_an_error_naming_the_order_the_vehicle_has_taken_is_not_a_rejection() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5,
                      errors=[_rejection(f"{ORDER}-1", etype="orderError", desc="lift slow")]))
    assert b.task_state(ROBOT) == task_map.TASK_RUNNING
    assert b.task_detail(ROBOT) == ""


def test_a_rejection_of_an_earlier_order_does_not_fail_the_current_one() -> None:
    """The vehicle reports a rejection until it accepts a new order: it is still there when the
    next order has been sent and not yet reported."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    b.command_move(ROBOT, 9.0, 0.0)
    assert b.task_state(ROBOT) == task_map.TASK_RUNNING


def test_a_stale_rejection_from_an_earlier_adapter_process_does_not_fail_the_first_order() -> None:
    """Two processes with the default token: the first one's first order rejected and still in
    the vehicle's state when the second sends its first order."""
    first = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    first.on_state(_state())
    first.command_move(ROBOT, 5.0, 0.0)
    old = json.loads(first.pending_order(ROBOT))["orderId"]
    second = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    second.on_state(_state(errors=[_rejection(old)]))
    second.command_move(ROBOT, 5.0, 0.0)
    assert json.loads(second.pending_order(ROBOT))["orderId"] != old
    assert second.task_state(ROBOT) == task_map.TASK_RUNNING


def test_a_held_order_is_never_rejected() -> None:
    """It has not been sent. Only the order sent last can be."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    b.command_move(ROBOT, 9.0, 0.0)  # held behind a cancel
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5,
                      errors=[_rejection(f"{ORDER}-2")]))
    assert b.task_state(ROBOT) == task_map.TASK_RUNNING


def test_a_preempting_order_rejected_after_its_release_fails() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    b.command_move(ROBOT, 9.0, 0.0)
    cancel = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert _wire(b)[-1] == f"{ORDER}-2"
    b.on_state(_state(order_id=f"{ORDER}-1", errors=[_rejection(f"{ORDER}-2")]))
    assert b.task_state(ROBOT) == task_map.TASK_FAILED
    assert b.task_detail(ROBOT).startswith(f"the vehicle rejected order {ORDER}-2")


# ── a rejected order is not outstanding ──────────────────────────────────────

def test_after_a_rejection_the_next_order_goes_straight_out_with_no_cancel() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    b.command_move(ROBOT, 9.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", f"{ORDER}-2"]


def test_a_stop_after_a_rejection_still_sends_its_cancel_order() -> None:
    """VDA5050 8.1 lets any error name an order, so a rejection is a reading, not a fact. A
    vehicle that warned and then took the order would otherwise drive it unsupervised."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    b.command_stop(ROBOT)
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]
    assert b.suppressed_stops(ROBOT) == 0


def test_a_rejection_still_in_the_state_does_not_make_a_later_order_look_done() -> None:
    """The vehicle reports the rejection of the first order until it accepts one. The second
    order, sent and not yet reported, is still outstanding: the next one waits behind a cancel."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    b.command_move(ROBOT, 9.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    b.command_move(ROBOT, 7.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", f"{ORDER}-2", "cancelOrder"]


# ── an order the vehicle takes after it was stopped ──────────────────────────

def _stopped_after_rejection() -> Vda5050Binding:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1", desc="order is not executable while charging")]))
    b.command_stop(ROBOT)
    stop = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1"),  # the stop's cancel refused
                              {"errorType": "noOrderToCancel", "errorLevel": "WARNING",
                               "errorReferences": [{"referenceKey": "actionId",
                                                    "referenceValue": stop}]}]))
    return b


def test_a_stopped_order_the_vehicle_takes_after_all_gets_one_more_cancel() -> None:
    """Rejected while charging, delivered again (QoS 1, after a reconnect) and taken: the action
    is ended and its lease released, so nothing else would stop it."""
    b = _stopped_after_rejection()
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "cancelOrder"]
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "cancelOrder"]  # once only


def test_also_when_the_stops_cancel_overtook_the_order() -> None:
    """MQTT orders one client's messages per topic only: the cancel can arrive first, find
    nothing to cancel, and the order be taken after it."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.command_stop(ROBOT)  # not yet reported: the order may be on its way, so it is cancelled
    stop = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5,
                      actions=[(stop, "FAILED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "cancelOrder"]


def test_not_while_the_stops_cancel_is_in_progress() -> None:
    """The usual stop: the vehicle drives the order, the cancel runs. No second cancel."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    b.command_stop(ROBOT)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=1, driving=True, vx=0.2))  # no state since
    stop = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=1, driving=True, vx=0.1,
                      actions=[(stop, "RUNNING")]))
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(stop, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_not_for_an_order_this_adapter_did_not_send() -> None:
    """After the stop's cancel ended the vehicle runs some other order: not this binding's to
    cancel (another master controller's, or an earlier process's)."""
    b = _stopped_after_rejection()
    b.on_state(_state(order_id="someone-elses", nodes=2, driving=True, vx=0.5))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_not_once_a_newer_order_was_sent() -> None:
    """The newer order is the one the vehicle now has to take; the stopped one is no longer
    "the last order sent"."""
    b = _stopped_after_rejection()
    b.command_move(ROBOT, 9.0, 0.0)  # the vehicle is idle: straight out
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-2"]


def test_not_while_a_preempting_order_is_held() -> None:
    """The hold's own cancel and timeout deal with the order before it."""
    b = _stopped_after_rejection()
    b.on_state(_state(order_id="old", nodes=1))  # something on the vehicle: a move is held
    b.command_move(ROBOT, 9.0, 0.0)
    assert b.held_order(ROBOT) == f"{ORDER}-2"
    n = len(_wire(b))
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    assert len(_wire(b)) == n


def test_each_stop_gets_its_own_late_cancel() -> None:
    b = _stopped_after_rejection()
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    b.command_stop(ROBOT)  # another stop: the vehicle still drives it
    late = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5,
                      actions=[(late, "FAILED")]))
    assert _wire(b).count("cancelOrder") == 4


def test_a_rejected_order_while_an_older_one_still_runs_is_still_preempted() -> None:
    """The rejection makes the sent order not outstanding; the order the vehicle is still
    driving is, and the next order waits behind a cancel for it."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id="old", nodes=2, driving=True, vx=0.5,
                      errors=[_rejection(f"{ORDER}-1", desc="active order")]))
    b.command_move(ROBOT, 9.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


# ── every FAILED from the task state ─────────────────────────────────────────

def test_a_fatal_error_ends_failed_with_execution_failure() -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, "act-1", 5).accepted
    fatal = json.loads(_state(order_id=f"{ORDER}-1", nodes=2))
    fatal["errors"] = [{"errorType": "motorFault", "errorLevel": "FATAL"}]
    b.on_state(json.dumps(fatal))
    a._maybe_emit_task_progress()
    *_, last = _updates(a)
    assert last.state == pb.ACTION_STATE_FAILED
    assert last.end_cause == pb.ACTION_END_CAUSE_EXECUTION_FAILURE


def test_the_adapter_reads_the_state_and_its_reason_together() -> None:
    """Read apart, a state arriving between the two reads could give FAILED with no reason."""
    class _Split:
        def task_outcome(self, robot_id):
            return task_map.TASK_FAILED, "why"

        def task_state(self, robot_id):  # what a second, later read would say
            return task_map.TASK_RUNNING

        def task_detail(self, robot_id):
            return ""

        def command_stop(self, robot_id):
            pass

    a = Adapter(pb, None, _Split(), ROBOT)
    a._current_action, a._last_task_state = "act-1", task_map.TASK_RUNNING
    a._maybe_emit_task_progress()
    (upd,) = _updates(a)
    assert (upd.state, upd.message) == (pb.ACTION_STATE_FAILED, "why")


def test_a_rejection_after_the_action_was_ended_reports_nothing() -> None:
    """The action ended another way first (here a stop that interrupts it); a rejection naming
    its order afterwards is not reported for anything."""
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, "act-1", 5).accepted
    a._interrupt_current("stopped")
    _updates(a)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1")]))
    a._maybe_emit_task_progress()
    assert _updates(a) == []


def test_a_success_states_no_end_cause() -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, "act-1", 5).accepted
    b.on_state(_state(order_id=f"{ORDER}-1"))
    a._maybe_emit_task_progress()
    *_, last = _updates(a)
    assert last.state == pb.ACTION_STATE_SUCCEEDED
    assert last.end_cause == pb.ACTION_END_CAUSE_UNSPECIFIED


# ── no order updates ─────────────────────────────────────────────────────────

def test_every_order_sent_has_a_new_order_id_and_order_update_id_zero() -> None:
    """The rejection rule matches orderIds only. Were the adapter to send order updates (the
    same orderId, a higher orderUpdateId), a rejected update would need its own rule: the
    vehicle would still report the orderId, with a lower orderUpdateId. This pins that none
    are sent: direct, held and released, and after an estop."""
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    b.command_move(ROBOT, 9.0, 0.0)
    cancel = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    b.on_state(_state(order_id=f"{ORDER}-2"))
    b.command_estop(ROBOT)
    b.on_state(_state(order_id=f"{ORDER}-2"))
    b.command_move(ROBOT, 1.0, 0.0)
    orders = [json.loads(p) for t, p in b.published if t.endswith("/order")]
    assert [o["orderId"] for o in orders] == [f"{ORDER}-1", f"{ORDER}-2", f"{ORDER}-3"]
    assert {o["orderUpdateId"] for o in orders} == {0}


# ── the vehicle's text ───────────────────────────────────────────────────────

def test_control_characters_are_stripped_from_the_reason() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(
        f"{ORDER}-1", etype="validation\x00Error\x1b[2J",
        desc="bad\nINFO forged log line\r\x1b[31mred\x1b[0m\u202eevil\u2028next\u200b\x7f!")]))
    detail = b.task_detail(ROBOT)
    assert detail == (f"the vehicle rejected order {ORDER}-1: validation Error [2J: "
                      "bad INFO forged log line [31mred [0m evil next !")
    assert all(ch.isprintable() for ch in detail)


def test_the_reason_is_cut_to_length() -> None:
    b = _binding()
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(errors=[_rejection(f"{ORDER}-1", etype="t" * 500, desc="d" * 5000)]))
    detail = b.task_detail(ROBOT)
    assert ": " + "t" * 64 + ": " + "d" * 200 in detail
    assert len(detail) == len(f"the vehicle rejected order {ORDER}-1: ") + 64 + 2 + 200


def test_clean_text() -> None:
    assert clean_text(None, 10) == "" and clean_text(42, 10) == ""
    assert clean_text("  a\t\tb\n\n c  ", 100) == "a b c"
    assert clean_text("x\u2029y\ue000z\ufeff", 100) == "x y z"  # separator, private use, BOM
    assert clean_text("abcdef", 3) == "abc"


# ── the parse ────────────────────────────────────────────────────────────────

def test_order_errors_map_each_named_order_id_to_the_first_error_naming_it() -> None:
    snap = profiles.get_profile("vda5050").parse_state(json.dumps({"errors": [
        _rejection("o1", etype="first"), _rejection("o1", etype="second"),
        {"errorType": "x", "errorReferences": [{"referenceKey": "orderId", "referenceValue": ""}]},
        {"errorType": "y", "errorReferences": [{"referenceKey": "orderId", "referenceValue": 7}]},
        {"errorType": "z", "errorReferences": [{"referenceKey": "headerId", "referenceValue": "9"}]},
        {"errorType": "w", "errorReferences": "junk"}, "junk"]}))
    assert snap.order_errors == {"o1": ("first", "invalid order: bad node")}


def test_errors_about_another_topic_or_an_action_are_not_rejections() -> None:
    """vda-5050-lib's master controller skips them the same way (_dispatchState)."""
    def err(etype, **refs):
        return {"errorType": etype, "errorReferences": [
            {"referenceKey": k, "referenceValue": v} for k, v in refs.items()]}
    snap = profiles.get_profile("vda5050").parse_state(json.dumps({"errors": [
        err("noOrderToCancel", topic="instantActions", orderId="o1"),
        err("actionFailed", actionId="a1", orderId="o2"),
        err("orderError", actionId="a2", orderId="o3"),
        err("validationError", topic="order", orderId="o4")]}))
    assert set(snap.order_errors) == {"o3", "o4"}


def test_a_state_without_errors_has_no_order_errors() -> None:
    assert profiles.get_profile("vda5050").parse_state("{}").order_errors == {}
    assert profiles.get_profile("vda5050").parse_state("not json").order_errors == {}
