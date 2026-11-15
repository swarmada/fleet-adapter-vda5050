# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""A state that leaves out ``nodeStates`` or ``actionStates`` says nothing about what the vehicle
has left to do: unknown, never empty. VDA5050 2.0.0 requires both lists.

* The cancel suppression sends the cancelOrder (a stop never skips it on such a state).
* Arrival needs a nodeStates list: the task stays RUNNING without one.
* The late cancel takes unknown for work left, and sends its one cancelOrder.
* Preemption takes unknown for an order that may be active: cancel first, hold the new order;
  the held order goes out only on a state with both lists, nothing left.
* The first such state is logged once, as an error naming the list."""

from __future__ import annotations

import json

import pytest
from conftest import mark_transport_live

from fleet_adapter_vda5050 import profiles, task
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
MISSING = [("nodeStates",), ("actionStates",), ("nodeStates", "actionStates")]


def _state(order_id: str = "", *, nodes: int = 0, actions=(), driving: bool = False,
           drop=(), bad=(), ended_by_error: str = "") -> str:
    """A VDA5050 state at rest unless ``driving``. ``drop`` lists the lists left out; ``bad``
    the lists sent as something other than a list; ``ended_by_error`` an actionId a
    noOrderToCancel warning names (ended, whatever the lists say)."""
    body = {"orderId": order_id, "driving": driving, "paused": False,
            "velocity": {"vx": 0.4 if driving else 0.0, "vy": 0.0, "omega": 0.0},
            "nodeStates": [{"nodeId": f"n{i}", "sequenceId": 2 * i, "released": True}
                           for i in range(nodes)],
            "actionStates": [{"actionId": a, "actionType": "x", "actionStatus": s}
                             for a, s in actions]}
    if ended_by_error:
        body["errors"] = [{"errorType": "noOrderToCancel", "errorLevel": "WARNING",
                           "errorReferences": [{"referenceKey": "actionId",
                                                "referenceValue": ended_by_error}]}]
    for k in drop:
        del body[k]
    for k in bad:
        body[k] = {}
    return json.dumps(body)


def _binding() -> Vda5050Binding:
    return mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))


def _wire(b: Vda5050Binding) -> list[str]:
    out = []
    for topic, payload in b.published:
        if topic.endswith("/order"):
            out.append("order")
        else:
            out.extend(a["actionType"] for a in json.loads(payload)["actions"])
    return out


def _orders(b: Vda5050Binding) -> list[str]:
    return [json.loads(p)["orderId"] for t, p in b.published if t.endswith("/order")]


# ── the helper ───────────────────────────────────────────────────────────────

def test_nothing_left_needs_both_lists_reported_and_empty() -> None:
    parse = Vda5050Profile().parse_state
    assert parse(_state()).nothing_left
    assert not parse(_state(nodes=1)).nothing_left
    assert not parse(_state(actions=[("a", "RUNNING")])).nothing_left
    assert parse(_state(actions=[("a", "FINISHED")])).nothing_left
    for drop in MISSING:
        st = parse(_state(drop=drop))
        assert not st.lists_reported and not st.nothing_left, drop
    for bad in MISSING:
        assert not parse(_state(bad=bad)).nothing_left, bad
    assert not profiles.StateSnapshot().nothing_left  # no state, or one that did not parse
    assert not parse("not json").nothing_left


# ── 1. the cancel suppression (the stop path) ────────────────────────────────

@pytest.mark.parametrize("drop", MISSING)
def test_a_stop_sends_its_cancel_on_a_state_without_the_lists(drop) -> None:
    b = _binding()
    b.on_state(_state(drop=drop))
    b.command_stop(ROBOT)
    assert _wire(b) == ["cancelOrder"]
    assert b.suppressed_stops(ROBOT) == 0


def test_a_stop_is_still_skipped_when_both_lists_say_nothing_is_left() -> None:
    b = _binding()
    b.on_state(_state())
    b.command_stop(ROBOT)
    assert _wire(b) == []
    assert b.suppressed_stops(ROBOT) == 1


# ── 2. arrival ───────────────────────────────────────────────────────────────

def test_arrival_needs_a_node_states_list() -> None:
    parse = Vda5050Profile().parse_state
    assert task.task_state_from_snapshot(parse(_state("o1")), "o1") == task.TASK_SUCCEEDED
    assert task.task_state_from_snapshot(
        parse(_state("o1", drop=("nodeStates",))), "o1") == task.TASK_RUNNING
    assert task.task_state_from_snapshot(
        parse(_state("o1", bad=("nodeStates",))), "o1") == task.TASK_RUNNING


def test_the_binding_does_not_mark_a_task_done_without_node_states() -> None:
    b = _binding()
    b.command_move(ROBOT, 1.0, 0.0)
    (order,) = _orders(b)
    b.on_state(_state(order, drop=("nodeStates",)))
    assert b.task_state(ROBOT) == task.TASK_RUNNING
    b.on_state(_state(order))
    assert b.task_state(ROBOT) == task.TASK_SUCCEEDED


# ── 3. the late cancel ───────────────────────────────────────────────────────

def _stopped_order(b: Vda5050Binding) -> str:
    """An order sent and stopped, and the stop's cancel reported ended before the vehicle took
    the order (the cancel overtook it): the case the late cancel is for. Returns the order id."""
    b.on_state(_state())
    b.command_move(ROBOT, 1.0, 0.0)
    (order,) = _orders(b)
    b.command_stop(ROBOT)
    b.on_state(_state(ended_by_error=b._stop_cancel_id))  # nothing to cancel yet
    return order


@pytest.mark.parametrize("drop", MISSING)
def test_the_late_cancel_takes_a_missing_list_for_work_left(drop) -> None:
    b = _binding()
    order = _stopped_order(b)
    b.published.clear()
    b.on_state(_state(order, drop=drop))
    b.on_state(_state(order, drop=drop))
    assert _wire(b) == ["cancelOrder"]  # once


def test_no_late_cancel_when_both_lists_say_nothing_is_left() -> None:
    b = _binding()
    b.on_state(_state())
    b.command_move(ROBOT, 1.0, 0.0)
    (order,) = _orders(b)
    b.on_state(_state(order, nodes=2, driving=True))
    b.command_stop(ROBOT)
    cancel = b._stop_cancel_id
    b.published.clear()
    b.on_state(_state(order, actions=[(cancel, "FINISHED")]))
    assert _wire(b) == []


# ── 4. and 5. preemption ─────────────────────────────────────────────────────

@pytest.mark.parametrize("drop", MISSING)
def test_an_order_waits_behind_a_cancel_on_a_state_without_the_lists(drop) -> None:
    b = _binding()
    b.on_state(_state())
    b.command_move(ROBOT, 1.0, 0.0)
    (first,) = _orders(b)
    b.on_state(_state(first, drop=drop))  # it may still have the order
    b.published.clear()
    b.command_move(ROBOT, 2.0, 0.0)
    assert _wire(b) == ["cancelOrder"]
    assert b._held is not None


@pytest.mark.parametrize("drop", MISSING)
def test_the_held_order_waits_for_a_state_with_both_lists(drop) -> None:
    b = _binding()
    b.on_state(_state())
    b.command_move(ROBOT, 1.0, 0.0)
    (first,) = _orders(b)
    b.on_state(_state(first, nodes=2, driving=True))
    b.command_move(ROBOT, 2.0, 0.0)  # cancel first, hold
    cancel = b._held_cancel_id
    b.published.clear()
    b.on_state(_state(first, drop=drop, ended_by_error=cancel))  # the cancel ended; a list missing
    assert b._held_cancel_id in b._state.ended_actions  # held back by the list, not the cancel
    assert _wire(b) == [] and b._held is not None
    b.on_state(_state(first, ended_by_error=cancel))  # both lists, nothing left
    assert _wire(b) == ["order"] and b._held is None


def test_the_first_order_waits_on_a_vehicle_that_never_sends_the_lists() -> None:
    # The strict reading: with no list ever reported, even the first order waits behind a
    # cancel, and fails when its hold times out.
    b = _binding()
    b.on_state(_state(drop=("nodeStates", "actionStates")))
    b.command_move(ROBOT, 1.0, 0.0)
    assert _wire(b) == ["cancelOrder"]
    assert _orders(b) == []


# ── the log ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("drop", "named"), [
    (("nodeStates",), "no nodeStates list"),
    (("actionStates",), "no actionStates list"),
    (("nodeStates", "actionStates"), "no nodeStates or actionStates list"),
])
def test_a_state_without_the_lists_is_logged_once(capsys, drop, named) -> None:
    b = _binding()
    b.on_state(_state(drop=drop))
    b.on_state(_state(drop=drop))
    b.on_state(_state(drop=("nodeStates", "actionStates")))
    err = capsys.readouterr().err
    assert err.count("so what it has left to do is unknown") == 1
    assert named in err
    assert "every stop sends its cancelOrder" in err
    assert "no task is marked done" in err


def test_a_state_with_both_lists_is_not_logged(capsys) -> None:
    b = _binding()
    b.on_state(_state())
    assert "left to do is unknown" not in capsys.readouterr().err


# ── an action entry whose status is unknown ──────────────────────────────────

@pytest.mark.parametrize("entry", [
    {"actionId": "a1", "actionType": "pick", "actionStatus": "running"},  # not a VDA value
    {"actionId": "a1", "actionType": "pick"},                             # no status
    {"actionId": "a1", "actionType": "pick", "actionStatus": None},
])
def test_an_action_whose_status_is_unknown_is_not_taken_as_ended(entry) -> None:
    body = json.loads(_state())
    body["actionStates"] = [entry]
    st = Vda5050Profile().parse_state(json.dumps(body))
    assert st.active_actions == 1 and not st.nothing_left
    b = _binding()
    b.on_state(json.dumps(body))
    b.command_stop(ROBOT)
    assert _wire(b) == ["cancelOrder"]   # the stop is not skipped
    assert b.suppressed_stops(ROBOT) == 0


def test_finished_and_failed_actions_are_ended() -> None:
    st = Vda5050Profile().parse_state(_state(actions=[("a", "FINISHED"), ("b", "FAILED")]))
    assert st.active_actions == 0 and st.nothing_left


def test_an_unreadable_payload_is_logged_as_possibly_not_a_state(capsys) -> None:
    b = _binding()
    b.on_state("not json")
    assert "or is not a VDA 5050 state at all" in capsys.readouterr().err
