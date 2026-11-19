# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A cancel is answered STOPPED_SAFELY only for what was witnessed.

The control plane reads STOPPED_SAFELY as proof the robot is off the action, and may give the
action to another robot at once. So the adapter answers it only once a state sent after the stop
shows the vehicle at rest, and the broker has accepted the cancelOrder, within the estop's budget.
Otherwise the stop was commanded and not witnessed: CANCEL_DISPOSITION_STOP_UNCONFIRMED (contract
1.4.0), or acknowledged=false to a control plane below 1.4.0. A cancel naming an action the vehicle
no longer runs (it was replaced in place) stops nothing, and is STOPPED_SAFELY only once a state
received after the cancel shows the vehicle on the replacing order.

The wait is off the ControlStream reader, so a command behind a cancel, a fence above all, is
handled at once."""

from __future__ import annotations

import itertools
import json
import threading
import time

import pytest
from conftest import PublishInfo, mark_registered, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
_N = itertools.count(1)


def _state(order_id: str = "", driving: bool = False, nodes: int = 0, actions=()) -> str:
    n = next(_N)
    return json.dumps({
        "headerId": n, "timestamp": f"t-{n}", "driving": driving,
        "velocity": {"vx": 0.8 if driving else 0.0, "vy": 0.0, "omega": 0.0}, "paused": False,
        "operatingMode": "AUTOMATIC", "orderId": order_id,
        "nodeStates": [{"nodeId": f"n{i}", "sequenceId": 2 * i + 2, "released": True}
                       for i in range(nodes)],
        "actionStates": [{"actionId": a, "actionType": "cancelOrder", "actionStatus": "FINISHED"}
                         for a in actions]})


def _adapter(negotiated: str = "1.4.0") -> tuple[Adapter, Vda5050Binding]:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.on_state(_state())
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    a._negotiated = negotiated
    return a, b


def _assign(a: Adapter, action_id: str, token: int) -> None:
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}'), res)
    assert res.assign_action.accepted


def _drain(a: Adapter) -> list:
    out = []
    while not a._outbox.empty():
        out.append(a._outbox.get_nowait())
    return out


def _cancel_cmd(action_id: str, token: int) -> pb.Command:
    return pb.Command(command_id=9, robot_id=ROBOT, leader_term=1,
                      cancel_action=pb.CancelAction(action_id=action_id, fencing_token=token))


def _cancel(a: Adapter, action_id: str, token: int) -> tuple[pb.CancelActionResult, list]:
    """Send the cancel, and wait for its answer, which may come from the wait's own thread."""
    _drain(a)
    a._handle_command(_cancel_cmd(action_id, token))
    msgs: list = []
    deadline = time.monotonic() + 5
    while not any(m.WhichOneof("payload") == "command_result" for m in msgs):
        assert time.monotonic() < deadline, "no answer to the cancel"
        msgs += _drain(a)
        time.sleep(0.01)
    (res,) = [m.command_result.cancel_action for m in msgs
              if m.WhichOneof("payload") == "command_result"]
    return res, msgs


def _wire(b) -> list[str]:
    return ["order" if t.endswith("/order") else json.loads(p)["actions"][0]["actionType"]
            for t, p in b.published]


def _vehicle(a: Adapter, b: Vda5050Binding, **state) -> None:
    """The vehicle sends a state with ``state``'s fields during each look of the wait."""
    a._stop_confirm_sleep = lambda _s: b.on_state(_state(**state))


@pytest.mark.parametrize("negotiated", ["1.4.0", "1.3.0"])
def test_a_cancel_is_stopped_safely_once_rest_is_witnessed(negotiated) -> None:
    a, b = _adapter(negotiated)
    _assign(a, "act-1", 7)
    _vehicle(a, b, order_id=b._last_order_id)  # the order ended, at rest
    res, msgs = _cancel(a, "act-1", 8)
    assert res.acknowledged and res.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    assert _wire(b) == ["order", "cancelOrder"]
    kinds = [m.WhichOneof("payload") for m in msgs]
    assert kinds == ["action_status", "command_result"]  # CANCELLED, then the answer


def test_a_vehicle_still_moving_at_the_bound_is_stop_unconfirmed() -> None:
    a, b = _adapter()
    _assign(a, "act-1", 7)
    _vehicle(a, b, order_id=b._last_order_id, driving=True, nodes=1)
    res, _ = _cancel(a, "act-1", 8)
    assert res.acknowledged
    assert res.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert "not confirmed" in res.message


def test_a_broker_drop_during_the_cancel_is_stop_unconfirmed() -> None:
    """The vehicle shows rest, but the broker never accepted the cancelOrder (paho answers
    MQTT_ERR_NO_CONN during a drop): nothing shows the vehicle will not take the order up."""
    a, b = _adapter()
    _assign(a, "act-1", 7)

    def dropped(topic, payload, qos=1):
        b.published.append((topic, payload))
        return PublishInfo(failed=True)

    b._mqtt._client.publish = dropped
    _vehicle(a, b, order_id=b._last_order_id)
    res, _ = _cancel(a, "act-1", 8)
    assert res.acknowledged
    assert res.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert "broker did not acknowledge" in res.message


def test_below_1_4_0_a_cancel_not_witnessed_is_not_acknowledged() -> None:
    a, b = _adapter("1.3.0")
    _assign(a, "act-1", 7)
    _vehicle(a, b, order_id=b._last_order_id, driving=True, nodes=1)
    res, _ = _cancel(a, "act-1", 8)
    assert not res.acknowledged
    assert res.disposition == pb.CANCEL_DISPOSITION_UNSPECIFIED
    assert "not confirmed" in res.message


def _replaced(negotiated: str, out: bool) -> tuple[Adapter, Vda5050Binding, str, str]:
    """act-1 driving its order (o1); act-2 replaces it in place, its order (o2) held behind the
    cancelOrder. With ``out``, the vehicle reports that cancel ended, and o2 goes out."""
    a, b = _adapter(negotiated)
    _assign(a, "act-1", 7)
    o1 = b._last_order_id
    b.on_state(_state(order_id=o1, driving=True, nodes=2))
    _assign(a, "act-2", 8)  # in place
    (cancel_id,) = [json.loads(p)["actions"][0]["actionId"] for t, p in b.published
                    if not t.endswith("/order")]
    if out:
        b.on_state(_state(order_id=o1, actions=[cancel_id]))
        assert b._last_order_id != o1 and b.held_order(ROBOT) == ""
    return a, b, o1, b._last_order_id


@pytest.mark.parametrize("negotiated", ["1.4.0", "1.3.0"])
def test_a_cancel_naming_a_replaced_action_is_stopped_safely_once_the_new_order_is_seen(
        negotiated) -> None:
    a, b, _o1, o2 = _replaced(negotiated, out=True)
    sent = list(b.published)
    _vehicle(a, b, order_id=o2, driving=True, nodes=2)  # on the replacing order
    res, _ = _cancel(a, "act-1", 9)
    assert res.acknowledged and res.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    assert a._current_action == "act-2"
    assert b.published == sent  # nothing stopped: act-2 runs on


@pytest.mark.parametrize("negotiated", ["1.4.0", "1.3.0"])
def test_a_cancel_naming_a_replaced_action_is_not_confirmed_while_the_old_order_shows(
        negotiated) -> None:
    a, b, o1, _ = _replaced(negotiated, out=False)
    sent = list(b.published)
    _vehicle(a, b, order_id=o1, driving=True, nodes=2)  # still the replaced order
    res, _ = _cancel(a, "act-1", 9)
    assert a._current_action == "act-2"
    assert b.published == sent
    if negotiated == "1.4.0":
        assert res.acknowledged
        assert res.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    else:
        assert not res.acknowledged
    assert "act-2" in res.message


def test_a_fence_during_a_cancels_wait_is_handled_at_once() -> None:
    """The vehicle sends nothing after the cancel, so its wait runs the whole 2 s. The fence the
    reader takes next is handled at once: its stop is commanded, not held behind the wait."""
    a, b = _adapter()
    _assign(a, "act-1", 7)
    fence = pb.Command(command_id=10, robot_id=ROBOT, leader_term=1,
                       estop=pb.Estop(estop_id="e1"))
    seen: dict = {}

    def reader() -> None:  # one ControlStream reader: the cancel, then the fence
        a._handle_command(_cancel_cmd("act-1", 8))
        seen["fence"] = time.monotonic()
        a._handle_command(fence)

    t0 = time.monotonic()
    threading.Thread(target=reader, daemon=True).start()
    deadline = t0 + 3
    while "startPause" not in _wire(b):
        assert time.monotonic() < deadline, "the fence's stop was never commanded"
        time.sleep(0.01)
    assert seen["fence"] - t0 < 0.5  # the reader took the fence at once
    assert time.monotonic() - t0 < 0.5  # and its stop was commanded at once


def _published_later(b: Vda5050Binding) -> list:
    """The broker leaves each publish unacknowledged until the test acknowledges it."""
    handles: list = []

    def publish(topic, payload, qos=1):
        b.published.append((topic, payload))
        handles.append(PublishInfo())  # written, not yet acknowledged
        return handles[-1]

    b._mqtt._client.publish = publish
    return handles


def test_a_cancelorder_the_broker_never_acknowledges_is_stop_unconfirmed() -> None:
    """Written to the socket (rc 0) is not accepted: only the broker's PUBACK is."""
    a, b = _adapter()
    _assign(a, "act-1", 7)
    _published_later(b)
    _vehicle(a, b, order_id=b._last_order_id)
    res, _ = _cancel(a, "act-1", 8)
    assert res.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert "broker did not acknowledge" in res.message


def test_a_puback_that_comes_during_the_wait_confirms_the_cancel() -> None:
    a, b = _adapter()
    _assign(a, "act-1", 7)
    handles = _published_later(b)

    def look(_s) -> None:  # the vehicle at rest; the broker acknowledges on the third look
        b.on_state(_state(order_id=b._last_order_id))
        look.n += 1
        if look.n == 3:
            handles[-1].set_published()

    look.n = 0
    a._stop_confirm_sleep = look
    res, _ = _cancel(a, "act-1", 8)
    assert res.acknowledged and res.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY


def test_a_cancel_with_nothing_to_cancel_sends_none_and_is_confirmed_by_rest() -> None:
    """The vehicle's own state shows the last order ended, at rest: no cancelOrder is sent,
    and a state after the stop showing it still at rest confirms it."""
    a, b = _adapter()
    _assign(a, "act-1", 7)
    b.on_state(_state(order_id=b._last_order_id))  # arrived: at rest, nothing left
    _vehicle(a, b, order_id=b._last_order_id)
    res, _ = _cancel(a, "act-1", 8)
    assert "cancelOrder" not in _wire(b)
    assert res.acknowledged and res.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY


def test_no_state_after_the_stop_is_stop_unconfirmed() -> None:
    a, _b = _adapter()
    _assign(a, "act-1", 7)
    a._stop_confirm_sleep = lambda _s: None  # the vehicle sends nothing
    res, _ = _cancel(a, "act-1", 8)
    assert res.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert "no state after the stop" in res.message
