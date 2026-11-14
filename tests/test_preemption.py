# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Preemption in place: a new order while the vehicle still has one.

VDA5050 has a vehicle reject an order with a new orderId while an order is active, unless it
stitches onto that order's base. Sent straight away, the new order was rejected, the vehicle
finished the old one, and the new action ran to its timeout. Now the binding sends a
``cancelOrder``, holds the new order, and sends it on the first ``state`` that shows the old
order gone.

* Gone: the vehicle reports the preemption's cancelOrder ended (FINISHED, or refused with an
  error naming it), with no node left, no action that has not ended, and not driving.
* A second order while one is held replaces it; a stop or an estop drops it; a stop's cancel
  still in progress is waited on, not sent again, and one that has ended is not.
* A Swarmada pause stays in force across the preemption and an estop's stopPause.
* A hold that waits past its timeout drops the order, its action ends FAILED, and the adapter
  stops whatever is left of the order before."""

from __future__ import annotations

import json
import threading

from conftest import mark_registered, mark_transport_live
from swarmada_sdk.safety import LeaseMonitor

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
ORDER = f"{ROBOT}-order"


# ── helpers ──────────────────────────────────────────────────────────────────

class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _state(order_id="", nodes=0, actions=(), driving=False, vx=0.0, x=0.0, errors=()) -> str:
    """A VDA5050 state. ``actions`` is ``(actionId, actionStatus)`` pairs; ``errors`` is the
    actionIds named by noOrderToCancel errors."""
    return json.dumps({
        "driving": driving, "velocity": {"vx": vx, "vy": 0.0, "omega": 0.0},
        "agvPosition": {"x": x, "y": 0.0, "theta": 0.0, "mapId": "m", "positionInitialized": True},
        "orderId": order_id, "nodeStates": [{"nodeId": f"n{i}"} for i in range(nodes)],
        "actionStates": [{"actionId": a, "actionStatus": st} for a, st in actions],
        "errors": [{"errorType": "noOrderToCancel", "errorLevel": "WARNING",
                    "errorReferences": [{"referenceKey": "actionId", "referenceValue": e}]}
                   for e in errors]})


def _binding(timeout: float = 60.0) -> tuple[Vda5050Binding, _Clock]:
    clock = _Clock()
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, preempt_timeout_s=timeout,
                                           clock=clock))
    return b, clock


def _wire(b: Vda5050Binding) -> list[str]:
    """What the binding published, in order: the instantAction type, or the orderId."""
    out = []
    for topic, payload in b.published:
        msg = json.loads(payload)
        out.append(msg["orderId"] if topic.endswith("/order") else msg["actions"][0]["actionType"])
    return out


def _cancel_ids(b: Vda5050Binding) -> list[str]:
    return [json.loads(p)["actions"][0]["actionId"] for t, p in b.published
            if not t.endswith("/order")
            and json.loads(p)["actions"][0]["actionType"] == profiles.ACTION_CANCEL_ORDER]


def _driving_first_order() -> tuple[Vda5050Binding, _Clock]:
    """A vehicle driving the binding's first order, two nodes left."""
    b, clock = _binding()
    b.on_state(_state())
    b.command_move(ROBOT, 5.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.8, x=1.0))
    return b, clock


# ── the hold ─────────────────────────────────────────────────────────────────

def test_an_idle_vehicle_gets_the_order_at_once_and_no_cancel() -> None:
    b, _ = _binding()
    b.on_state(_state(order_id="old"))  # an order from before this process, ended
    b.command_move(ROBOT, 5.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1"]


def test_an_order_while_one_is_active_is_held_behind_a_cancel_order() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]
    assert b.held_order(ROBOT) == f"{ORDER}-2"


def test_the_held_order_goes_out_when_the_vehicle_reports_the_cancel_finished() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", x=2.0, actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-2"]
    assert b.held_order(ROBOT) == ""


def test_it_also_goes_out_when_the_vehicle_refused_the_cancel_with_no_order_to_cancel() -> None:
    """VDA5050 2.0.0 6.6.3.2: the actionId is passed as an errorReference. vda-5050-lib reports
    only the error, with no actionState for the refused cancel."""
    b, _ = _binding()
    b.on_state(_state(order_id="old"))
    b.command_move(ROBOT, 5.0, 0.0)      # not reported yet: it may be on its way
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id="old", errors=[cancel]))  # the first order was rejected
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-2"]


def test_the_last_order_reported_ended_does_not_release_it_without_the_cancels_end() -> None:
    """MQTT orders one client's messages per topic only. Released on this state, the held order
    could overtake its cancel on the broker; the cancel would then end it, and the vehicle would
    report the new orderId with no node left: a success for an order that never drove."""
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    b.on_state(_state(order_id=f"{ORDER}-1"))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_not_while_still_driving() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", driving=True, vx=0.3, actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_not_while_a_node_is_left() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=1, actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_not_while_the_cancel_or_another_action_is_still_running() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "RUNNING")]))
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED"), ("lift", "RUNNING")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_not_on_a_state_from_before_the_order_it_waits_on() -> None:
    """The first order was sent and not yet reported; an idle state that crossed it says
    nothing about it."""
    b, _ = _binding()
    b.on_state(_state(order_id="old"))
    b.command_move(ROBOT, 5.0, 0.0)
    b.command_move(ROBOT, 9.0, 0.0)
    b.on_state(_state(order_id="old"))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]


def test_the_held_order_starts_where_the_vehicle_stopped() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", x=2.5, actions=[(cancel, "FINISHED")]))
    order = json.loads(b.published[-1][1])
    assert order["nodes"][0]["nodePosition"]["x"] == 2.5


def test_a_second_order_while_one_is_held_replaces_it_and_sends_no_second_cancel() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    b.command_move(ROBOT, 7.0, 0.0)
    assert b.held_order(ROBOT) == f"{ORDER}-3"
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-3"]


def test_a_stops_cancel_still_in_progress_is_waited_on_not_sent_again() -> None:
    b, _ = _driving_first_order()
    b.command_stop(ROBOT)
    b.command_move(ROBOT, 9.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder"]
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-2"]


def test_a_cancel_the_vehicle_lists_as_running_is_waited_on() -> None:
    b, _ = _driving_first_order()
    b.command_stop(ROBOT)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=1, actions=[(cancel, "RUNNING")]))
    b.command_move(ROBOT, 9.0, 0.0)
    assert _cancel_ids(b) == [cancel]


def test_a_cancel_that_ended_and_aged_out_of_the_state_is_not_waited_on() -> None:
    """A vehicle drops an ended instantAction from its state after a few publishes
    (vda-5050-lib: 5). A refused cancel, gone from the state, is not pending."""
    b, _ = _binding()
    b.on_state(_state(order_id="old"))
    b.command_move(ROBOT, 5.0, 0.0)  # rejected by the vehicle: it still reports "old"
    b.command_stop(ROBOT)
    (stop,) = _cancel_ids(b)
    b.on_state(_state(order_id="old", errors=[stop]))  # noOrderToCancel
    for _ in range(5):
        b.on_state(_state(order_id="old"))
    b.command_move(ROBOT, 9.0, 0.0)
    assert len(_cancel_ids(b)) == 2
    b.on_state(_state(order_id="old", errors=[_cancel_ids(b)[-1]]))
    assert _wire(b)[-1] == f"{ORDER}-2"


def test_an_order_sent_since_a_cancel_ends_waiting_on_it() -> None:
    """An estop on an idle vehicle, the clear, an order straight out, and a second order, all
    before the next state: the estop's cancel (nothing to cancel) is not the one to wait on."""
    b, _ = _binding()
    b.on_state(_state())
    b.command_estop(ROBOT)
    b.command_move(ROBOT, 5.0, 0.0)
    b.command_move(ROBOT, 9.0, 0.0)
    assert _wire(b) == ["startPause", "cancelOrder", "stopPause", f"{ORDER}-1", "cancelOrder"]


def test_a_cancel_from_before_the_last_order_is_not_waited_on() -> None:
    b, _ = _driving_first_order()
    b.command_stop(ROBOT)
    b.on_state(_state(order_id=f"{ORDER}-1"))      # cancelled
    b.command_move(ROBOT, 9.0, 0.0)                 # straight out
    b.on_state(_state(order_id=f"{ORDER}-2", nodes=2, driving=True, vx=0.8))
    b.command_move(ROBOT, 7.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-2", "cancelOrder"]
    assert len(set(_cancel_ids(b))) == 2


def test_a_vehicle_that_cannot_be_observed_gets_the_order_as_before() -> None:
    b, _ = _driving_first_order()
    b.on_connection(json.dumps({"connectionState": "CONNECTIONBROKEN"}))
    b.command_move(ROBOT, 9.0, 0.0)
    assert _wire(b) == [f"{ORDER}-1", f"{ORDER}-2"]


def test_task_state_stays_running_while_held_and_after_release_until_the_new_order_ends() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))  # not ours
    assert _wire(b)[-1] == f"{ORDER}-2"
    assert b.task_state(ROBOT) == task_map.TASK_RUNNING
    b.on_state(_state(order_id=f"{ORDER}-2"))
    assert b.task_state(ROBOT) == task_map.TASK_SUCCEEDED


# ── stops during the hold ────────────────────────────────────────────────────

def test_an_estop_during_the_hold_drops_the_held_order() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    b.command_estop(ROBOT)
    for cancel in _cancel_ids(b):
        b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "startPause", "cancelOrder"]
    assert b.held_order(ROBOT) == ""


def test_a_stop_during_the_hold_drops_the_held_order() -> None:
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    b.command_stop(ROBOT)
    for cancel in _cancel_ids(b):
        b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert f"{ORDER}-2" not in _wire(b)
    assert b.held_order(ROBOT) == ""


def test_after_an_estop_the_stop_pause_goes_out_with_the_held_order_not_before() -> None:
    """The vehicle stays paused until its old order is gone and the new one is sent."""
    b, _ = _driving_first_order()
    b.command_estop(ROBOT)
    b.published.clear()
    b.command_move(ROBOT, 9.0, 0.0)  # the estop's cancel is still in progress
    assert _wire(b) == []
    cancel = json.loads(b.last_action(ROBOT))["actions"][0]["actionId"]  # the estop's cancel
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert _wire(b) == ["stopPause", f"{ORDER}-2"]


def test_a_swarmada_pause_after_the_assign_is_not_lifted_by_the_estops_stop_pause() -> None:
    """Estop; an order held behind the estop's cancel; then a Swarmada pause, acknowledged. The
    order goes out with a startPause before it, not the stopPause that would lift that pause."""
    b, _ = _driving_first_order()
    b.command_estop(ROBOT)
    estop_cancel = _cancel_ids(b)[-1]
    b.command_move(ROBOT, 9.0, 0.0)
    b.command_pause(ROBOT)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(estop_cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "startPause", "cancelOrder", "startPause", "startPause",
                        f"{ORDER}-2"]


def test_a_preemption_under_a_swarmada_pause_sends_the_order_paused() -> None:
    """vda-5050-lib's virtual AGV leaves pause mode on a cancelOrder. The order goes out with a
    startPause before it, so the vehicle stays paused until a Swarmada resume."""
    b, _ = _driving_first_order()
    b.command_pause(ROBOT)
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    assert _wire(b) == [f"{ORDER}-1", "startPause", "cancelOrder", "startPause", f"{ORDER}-2"]
    assert "stopPause" not in _wire(b)


def test_an_estop_waits_for_a_held_order_already_going_out() -> None:
    """The release and the estop are serialised: the order is either out before the pause and
    the cancel (which then end it), or never sent."""
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    sending, go = threading.Event(), threading.Event()
    record = b._mqtt._client.publish

    def slow_publish(topic, payload, qos=1):
        if topic.endswith("/order"):
            sending.set()
            go.wait(2)
        record(topic, payload, qos)

    b._mqtt._client.publish = slow_publish
    release = threading.Thread(target=b.on_state, args=(
        _state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]),))
    release.start()
    assert sending.wait(2)
    estop = threading.Thread(target=b.command_estop, args=(ROBOT,))
    estop.start()
    estop.join(0.2)
    assert estop.is_alive()  # the estop waits for the order going out
    go.set()
    release.join(2)
    estop.join(2)
    assert _wire(b)[-3:] == [f"{ORDER}-2", "startPause", "cancelOrder"]


def test_no_order_goes_out_between_the_estops_pause_and_its_cancel() -> None:
    """A release there would send the stopPause that lifts the estop's pause, and then the
    order: the vehicle would drive on until the cancel stopped it, perhaps on the next node.
    The release is started at exactly that point, after the pause and before the cancel."""
    b, _ = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    (cancel,) = _cancel_ids(b)
    release = threading.Thread(target=b.on_state, args=(
        _state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]),))
    cancel_order = b._cancel_order

    def after_the_pause(robot_id, *, always):
        release.start()
        release.join(0.3)  # the release waits for the estop, so this times out
        cancel_order(robot_id, always=always)

    b._cancel_order = after_the_pause
    b.command_estop(ROBOT)
    release.join(2)
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "startPause", "cancelOrder"]


# ── the timeout ──────────────────────────────────────────────────────────────

def test_a_hold_past_its_timeout_drops_the_order_and_the_task_fails() -> None:
    b, clock = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    clock.now += 59.0
    b.tick(1.0)
    assert b.held_order(ROBOT) == f"{ORDER}-2"
    clock.now += 1.0
    b.tick(1.0)
    assert b.held_order(ROBOT) == ""
    assert b.task_state(ROBOT) == task_map.TASK_FAILED
    assert "did not report its previous order ended within 60 s" in b.task_detail(ROBOT)
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))  # too late
    assert f"{ORDER}-2" not in _wire(b)


def test_a_tick_while_the_hold_is_being_set_up_does_not_time_it_out() -> None:
    """command_move sets the held order, sends the cancel, then sets the deadline. A tick in
    between, unserialised, would see the hold with the previous deadline (none yet) and fail
    the action at once. The tick is started at exactly that point."""
    b, _ = _driving_first_order()
    ticker = threading.Thread(target=b.tick, args=(1.0,))
    send_cancel = b._send_cancel

    def mid_setup(robot_id, tag):
        ticker.start()
        ticker.join(0.3)  # the tick waits for command_move, so this times out
        return send_cancel(robot_id, tag)

    b._send_cancel = mid_setup
    b.command_move(ROBOT, 9.0, 0.0)
    ticker.join(2)
    assert b.held_order(ROBOT) == f"{ORDER}-2"
    assert b.task_state(ROBOT) == task_map.TASK_RUNNING


def test_a_replacement_keeps_the_first_deadline() -> None:
    """The deadline bounds the vehicle ending the order before; a new order held in place of
    another does not change how long that has taken."""
    b, clock = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    clock.now += 40.0
    b.command_move(ROBOT, 7.0, 0.0)
    clock.now += 20.0
    b.tick(1.0)
    assert b.held_order(ROBOT) == ""
    assert b.task_state(ROBOT) == task_map.TASK_FAILED


def test_the_next_order_after_a_timeout_is_not_failed() -> None:
    b, clock = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    clock.now += 61.0
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=1))  # a state times it out too
    assert b.task_state(ROBOT) == task_map.TASK_FAILED
    b.on_state(_state(order_id=f"{ORDER}-1"))
    b.command_move(ROBOT, 7.0, 0.0)
    assert b.task_state(ROBOT) == task_map.TASK_RUNNING
    assert b.task_detail(ROBOT) == ""
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-3"]


def test_after_a_timeout_the_next_preemption_sends_its_own_cancel() -> None:
    """The cancel the hold waited on may have been lost: it is not waited on again."""
    b, clock = _driving_first_order()
    b.command_move(ROBOT, 9.0, 0.0)
    clock.now += 61.0
    b.tick(1.0)
    b.command_move(ROBOT, 7.0, 0.0)  # no state since: the old cancel would look pending
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "cancelOrder"]
    assert len(set(_cancel_ids(b))) == 2


# ── through the adapter ──────────────────────────────────────────────────────

def _assign(a: Adapter, action_id: str, token: int, lease_ms: int = 0):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}',
        lease_duration_ms=lease_ms, lease_generation=1), res)
    return res.assign_action


def _statuses(a: Adapter) -> list[tuple[str, str, str]]:
    names = {v: k for k, v in pb.ActionState.items()}
    out = []
    while not a._outbox.empty():
        m = a._outbox.get_nowait()
        if m.WhichOneof("payload") == "action_status":
            s = m.action_status
            out.append((s.action_id, names[s.state], s.message))
    return out


def test_the_adapter_preempts_in_place_and_the_new_action_succeeds() -> None:
    b, _ = _binding()
    b.on_state(_state())
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, "low", 5, lease_ms=30_000).accepted
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.8))
    assert _assign(a, "critical", 6, lease_ms=30_000).accepted
    (cancel,) = _cancel_ids(b)
    b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    a._maybe_emit_task_progress()  # the old order ended: not reported for the new action
    b.on_state(_state(order_id=f"{ORDER}-2"))
    a._maybe_emit_task_progress()
    assert [s[:2] for s in _statuses(a)] == [
        ("low", "ACTION_STATE_RUNNING"), ("critical", "ACTION_STATE_RUNNING"),
        ("critical", "ACTION_STATE_SUCCEEDED")]
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", f"{ORDER}-2"]


def test_the_adapter_fails_a_preemption_that_timed_out_with_the_reason() -> None:
    b, clock = _binding()
    b.on_state(_state())
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    a._lease = LeaseMonitor(on_expiry=a._self_stop, clock=clock)
    assert _assign(a, "low", 5, lease_ms=30_000).accepted
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.8))
    assert _assign(a, "critical", 6, lease_ms=30_000).accepted
    clock.now += 61.0
    b.tick(1.0)
    a._maybe_emit_task_progress()
    last = _statuses(a)[-1]
    assert last[:2] == ("critical", "ACTION_STATE_FAILED")
    assert "previous order" in last[2]
    # The old order may still be running: it is stopped, not left without action or lease.
    assert _wire(b) == [f"{ORDER}-1", "cancelOrder", "cancelOrder"]
    sent = len(b.published)
    a._lease.tick()  # released with the action: no lapse, no self-stop
    assert len(b.published) == sent
    assert _statuses(a) == []


def test_an_action_failed_during_the_hold_never_gets_its_order() -> None:
    """A FATAL error while the order is held ends the action FAILED. The hold goes with it:
    sent later, the order would drive with no action and no lease."""
    b, _ = _binding()
    b.on_state(_state())
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, "low", 5, lease_ms=30_000).accepted
    b.on_state(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.8))
    assert _assign(a, "critical", 6, lease_ms=30_000).accepted
    (cancel,) = _cancel_ids(b)
    fatal = json.loads(_state(order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.8))
    fatal["errors"] = [{"errorType": "x", "errorLevel": "FATAL"}]
    b.on_state(json.dumps(fatal))
    a._maybe_emit_task_progress()
    assert _statuses(a)[-1][:2] == ("critical", "ACTION_STATE_FAILED")
    assert b.held_order(ROBOT) == ""
    for c in _cancel_ids(b):
        b.on_state(_state(order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED"), (c, "FINISHED")]))
    assert f"{ORDER}-2" not in _wire(b)


# ── the parse ────────────────────────────────────────────────────────────────

def test_ended_actions_are_finished_or_failed_states_and_action_ids_named_by_errors() -> None:
    snap = profiles.get_profile("vda5050").parse_state(json.dumps({
        "actionStates": [{"actionId": "done", "actionStatus": "FINISHED"},
                         {"actionId": "bad", "actionStatus": "FAILED"},
                         {"actionId": "busy", "actionStatus": "RUNNING"},
                         {"actionId": 7, "actionStatus": "FINISHED"}, "junk"],
        "errors": [{"errorType": "noOrderToCancel", "errorReferences": [
                       {"referenceKey": "actionId", "referenceValue": "refused"},
                       {"referenceKey": "orderId", "referenceValue": "not-an-action"}]},
                   {"errorType": "x", "errorReferences": "junk"}, "junk"]}))
    assert snap.ended_actions == frozenset({"done", "bad", "refused"})


def test_active_action_ids_are_the_ones_not_ended() -> None:
    snap = profiles.get_profile("vda5050").parse_state(json.dumps({"actionStates": [
        {"actionId": "w", "actionStatus": "WAITING"}, {"actionId": "r", "actionStatus": "RUNNING"},
        {"actionId": "f", "actionStatus": "FINISHED"}, {"actionId": 3, "actionStatus": "RUNNING"}]}))
    assert snap.active_action_ids == frozenset({"w", "r"})


def test_a_state_without_them_has_none() -> None:
    assert profiles.get_profile("vda5050").parse_state("{}").ended_actions == frozenset()
    assert profiles.get_profile("vda5050").parse_state("not json").ended_actions == frozenset()
