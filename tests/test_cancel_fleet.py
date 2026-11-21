# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The cancel rule for every vehicle of a process (test_cancel_confirmed.py has it for one).

A cancel is answered STOPPED_SAFELY only on that vehicle's own witnessed rest, counted after its
own stop, and the broker's acknowledgement of that vehicle's own cancelOrder, within the estop's
budget; a cancel naming an action replaced in place on that vehicle, only once a state from that
vehicle shows its own replacing order. Nothing another vehicle reports answers for it. The one
ControlStream reader serves every vehicle, so a cancel's wait runs on a thread of its own: not
on the reader, so no command for any vehicle waits behind it, and not on the vehicle's lane, so
a stop for the same vehicle is answered without queuing behind it.

The vehicles here have an MQTT client each, except where a test puts them all on one, as a
process serving several vehicles does (fleet_on_one_client)."""

from __future__ import annotations

import itertools
import json
import threading
import time

from conftest import fleet_on_one_client, mark_registered, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import _Streams
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.process import FleetProcess
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

FLEET = ("amr-1", "amr-2", "amr-3")
_N = itertools.count(1)
_ID = itertools.count(100)


def _state(order_id: str = "", driving: bool = False, nodes: int = 0, actions=(),
           paused: bool = False) -> str:
    n = next(_N)
    return json.dumps({
        "headerId": n, "timestamp": f"t-{n}", "driving": driving,
        "velocity": {"vx": 0.8 if driving else 0.0, "vy": 0.0, "omega": 0.0}, "paused": paused,
        "operatingMode": "AUTOMATIC", "orderId": order_id,
        "nodeStates": [{"nodeId": f"n{i}", "sequenceId": 2 * i + 2, "released": True}
                       for i in range(nodes)],
        "actionStates": [{"actionId": a, "actionType": "cancelOrder", "actionStatus": "FINISHED"}
                         for a in actions]})


class _Unacknowledged:
    """paho's MQTTMessageInfo for a publish the broker hasn't acknowledged."""

    @staticmethod
    def is_published() -> bool:
        return False


def _fleet(monkeypatch, bindings=None) -> FleetProcess:
    """FLEET, each vehicle driving an action of its own; on ``bindings`` if given, else on a
    binding of its own with a client of its own."""
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    if bindings is None:
        bindings = [mark_transport_live(Vda5050Binding(robot_id=rid, serial=rid))
                    for rid in FLEET]
    streams, cores = _Streams(), []
    for b in bindings:
        rid = b._robot_id
        b.on_state(_state())
        c = mark_registered(Adapter(pb, None, b, rid, streams=streams))
        c._stop_confirm_sleep = lambda _s: None
        cores.append(c)
    p = FleetProcess(cores)
    streams.negotiated = "1.4.0"
    p.start_lanes()
    for i, c in enumerate(cores, start=1):
        res = pb.CommandResult()
        c._on_assign(pb.AssignAction(
            action_id=f"act-{i}", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=i,
            assignment_id=f"as-{i}", payload_json=b'{"x": 5, "y": 0}'), res)
        assert res.assign_action.accepted
        c._robot.on_state(_state(order_id=c._robot._last_order_id, driving=True, nodes=2))
    return p


def _core(p: FleetProcess, rid: str) -> Adapter:
    return p.core_for(rid)


def _looks(c: Adapter, real: bool = False, **state) -> None:
    """During each look of a wait, the vehicle ``c`` sends a state with ``state``'s fields (none
    when ``state`` is empty); with ``real``, the looks take their real 50 ms."""
    def look(s) -> None:
        if state:
            c._robot.on_state(_state(**state))
        if real:
            time.sleep(s)
    c._stop_confirm_sleep = look


def _cancel_msg(rid: str, action_id: str, command_id: int) -> pb.ControlPlaneMessage:
    return pb.ControlPlaneMessage(command=pb.Command(
        command_id=command_id, robot_id=rid, leader_term=1,
        cancel_action=pb.CancelAction(action_id=action_id, fencing_token=next(_ID))))


def _answers(p: FleetProcess, seen: dict) -> dict:
    """Every command result on the shared outbox so far, by command id, added to ``seen``."""
    q = p._streams.outbox
    while not q.empty():
        m = q.get_nowait()
        if m.WhichOneof("payload") == "command_result":
            seen[m.command_result.command_id] = (time.monotonic(), m.command_result)
    return seen


def _await(p: FleetProcess, seen: dict, command_id: int, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while command_id not in _answers(p, seen):
        assert time.monotonic() < deadline, f"no answer to command {command_id}"
        time.sleep(0.01)
    return seen[command_id]


def _wire(b) -> list[str]:
    return ["order" if t.endswith("/order") else json.loads(pl)["actions"][0]["actionType"]
            for t, pl in b.published]


def test_a_cancel_is_answered_on_its_own_vehicles_rest_while_another_drives(monkeypatch) -> None:
    p = _fleet(monkeypatch)
    a1, a2 = _core(p, "amr-1"), _core(p, "amr-2")
    _looks(a1, order_id=a1._robot._last_order_id)  # amr-1 at rest after its cancel
    seen: dict = {}
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    _, res = _await(p, seen, 1)
    assert res.robot_id == "amr-1"
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    assert _wire(a2._robot) == ["order"] and a2._current_action == "act-2"  # amr-2 drives on


def _unacknowledged(c: Adapter) -> None:
    """The broker never acknowledges a publish of ``c``'s."""
    real = c._robot._mqtt._client.publish

    def publish(topic, payload, qos=1):
        real(topic, payload, qos)
        return _Unacknowledged()

    c._robot._mqtt._client.publish = publish


def test_another_vehicles_rest_never_confirms_this_cancel(monkeypatch) -> None:
    """amr-1 drives on after its cancel; amr-2 is at rest. amr-1's cancel is not confirmed.
    amr-2 has reported far more states than amr-1, as vehicles of a fleet that has run a while
    have: each of its states counts past amr-1's baseline, so only reading amr-1's own state
    keeps them from confirming amr-1's stop."""
    p = _fleet(monkeypatch)
    a1, a2 = _core(p, "amr-1"), _core(p, "amr-2")
    for _ in range(50):
        a2._robot.on_state(_state(order_id=a2._robot._last_order_id, driving=True, nodes=2))
    assert a2._robot.state_seq("amr-2") > a1._robot.state_seq("amr-1") + 40

    def look(_s) -> None:
        a2._robot.on_state(_state(order_id=a2._robot._last_order_id))
        a1._robot.on_state(_state(order_id=a1._robot._last_order_id, driving=True, nodes=2))
    a1._stop_confirm_sleep = look
    seen: dict = {}
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    _, res = _await(p, seen, 1)
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED


def test_another_vehicles_acknowledged_cancel_order_never_confirms_this_cancel(
        monkeypatch) -> None:
    """amr-2's cancel is confirmed: at rest, its cancelOrder acknowledged. Then amr-1 is at rest,
    but the broker never acknowledges amr-1's cancelOrder: amr-1's cancel is not confirmed."""
    p = _fleet(monkeypatch)
    a1, a2 = _core(p, "amr-1"), _core(p, "amr-2")
    _looks(a2, order_id=a2._robot._last_order_id)
    seen: dict = {}
    p.handle_control_plane(_cancel_msg("amr-2", "act-2", 2), None)
    _, res = _await(p, seen, 2)
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    _unacknowledged(a1)
    _looks(a1, order_id=a1._robot._last_order_id)
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    _, res = _await(p, seen, 1)
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert "broker did not acknowledge" in res.cancel_action.message


def test_on_one_mqtt_client_another_vehicles_acknowledgement_never_confirms_this_cancel(
        monkeypatch) -> None:
    """The rule above with every vehicle on one MQTT client. amr-2's cancel is confirmed, its
    cancelOrder acknowledged. The broker never acknowledges amr-1's cancelOrder, and during
    amr-1's wait amr-2 goes on publishing on the same client, each publish acknowledged, while
    amr-1 is at rest: amr-1's cancel is not confirmed."""
    _shared, broker, bindings = fleet_on_one_client(FLEET)
    p = _fleet(monkeypatch, bindings)
    a1, a2 = _core(p, "amr-1"), _core(p, "amr-2")
    _looks(a2, order_id=a2._robot._last_order_id)
    seen: dict = {}
    p.handle_control_plane(_cancel_msg("amr-2", "act-2", 2), None)
    _, res = _await(p, seen, 2)
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    broker.hold = lambda topic: topic == a1._robot._topics.instant_actions

    def look(_s) -> None:
        a1._robot.on_state(_state(order_id=a1._robot._last_order_id))  # at rest
        a2._robot.request_factsheet("amr-2")  # on the same client, acknowledged
    a1._stop_confirm_sleep = look
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    _, res = _await(p, seen, 1)
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert "broker did not acknowledge" in res.cancel_action.message
    topics = [t for t, _p in broker.published]
    last_cancel = max(i for i, t in enumerate(topics) if t == a1._robot._topics.instant_actions)
    assert a2._robot._topics.instant_actions in topics[last_cancel + 1:]  # amr-2's came after


def test_a_fence_and_a_cancel_for_other_vehicles_during_a_cancels_wait_go_on_at_once(
        monkeypatch) -> None:
    """amr-1 sends nothing after its cancel, so its wait runs the whole 2 s. A fence for amr-3
    and a cancel for amr-2 behind it on the one reader are handled at once."""
    p = _fleet(monkeypatch)
    a1, a2, a3 = (_core(p, r) for r in FLEET)
    _looks(a1, real=True)
    _looks(a2, order_id=a2._robot._last_order_id)
    _looks(a3, order_id=a3._robot._last_order_id)
    seen: dict = {}
    fence = pb.ControlPlaneMessage(command=pb.Command(
        command_id=2, robot_id="amr-3", leader_term=1, estop=pb.Estop(estop_id="e3")))

    def reader() -> None:
        p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
        p.handle_control_plane(fence, None)
        p.handle_control_plane(_cancel_msg("amr-2", "act-2", 3), None)

    t0 = time.monotonic()
    threading.Thread(target=reader, daemon=True).start()
    deadline = t0 + 3
    while "startPause" not in _wire(a3._robot):
        assert time.monotonic() < deadline, "amr-3's fence was never commanded"
        time.sleep(0.01)
    assert time.monotonic() - t0 < 0.5  # the fence, not after amr-1's 2 s
    at, res = _await(p, seen, 3)
    assert at - t0 < 0.5 and res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOPPED_SAFELY
    at, res = _await(p, seen, 1)
    assert at - t0 > 1.5  # amr-1's own wait ran its budget
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED


def test_two_cancels_for_two_vehicles_wait_at_the_same_time(monkeypatch) -> None:
    p = _fleet(monkeypatch)
    a1, a2 = _core(p, "amr-1"), _core(p, "amr-2")
    _looks(a1, real=True)
    _looks(a2, real=True)
    seen: dict = {}
    t0 = time.monotonic()
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    p.handle_control_plane(_cancel_msg("amr-2", "act-2", 2), None)
    at1, r1 = _await(p, seen, 1)
    at2, r2 = _await(p, seen, 2)
    assert max(at1, at2) - t0 < 3.0  # one 2 s budget each, at the same time, not 4 s
    for r in (r1, r2):
        assert r.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED


def test_a_stop_for_the_same_vehicle_during_its_cancels_wait_is_answered_at_once(
        monkeypatch) -> None:
    """amr-1 is at rest, but the broker never acknowledges its cancelOrder, so the cancel waits
    its whole 2 s. An estop for amr-1 meanwhile needs only the vehicle's rest, paused, and is
    answered STOPPED at once: its wait doesn't queue behind the cancel's."""
    p = _fleet(monkeypatch)
    a1 = _core(p, "amr-1")
    _unacknowledged(a1)
    _looks(a1, real=True, order_id=a1._robot._last_order_id)
    seen: dict = {}
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    time.sleep(0.2)
    _looks(a1, real=True, order_id=a1._robot._last_order_id, paused=True)  # as it obeys startPause
    t0 = time.monotonic()
    p.handle_safety(pb.ControlPlaneSafetyMessage(robot_id="amr-1", estop=pb.Estop(estop_id="e1")))
    acks: list = []
    deadline = t0 + 3
    while not any(k.estop_id == "e1" and k.message != "latched" for k in acks):
        assert time.monotonic() < deadline, "no answer to the estop"
        q = p._streams.safety_outbox
        while not q.empty():
            m = q.get_nowait()
            if m.HasField("estop_ack"):
                acks.append(m.estop_ack)
        time.sleep(0.01)
    assert time.monotonic() - t0 < 0.8
    assert [k.state for k in acks if k.estop_id == "e1" and k.message != "latched"] == [
        pb.ESTOP_STATE_STOPPED]
    _, res = _await(p, seen, 1)
    assert res.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED


def test_a_replaced_action_is_witnessed_by_its_own_vehicles_new_order_only(monkeypatch) -> None:
    """act-1 on amr-1 is replaced in place by act-9; amr-1 still shows the old order. amr-2
    reports an order whose id is amr-1's new one: that witnesses nothing for amr-1."""
    p = _fleet(monkeypatch)
    a1, a2 = _core(p, "amr-1"), _core(p, "amr-2")
    o1 = a1._robot._last_order_id
    res = pb.CommandResult()
    a1._on_assign(pb.AssignAction(
        action_id="act-9", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=9,
        assignment_id="as-9", payload_json=b'{"x": 7, "y": 0}'), res)
    assert res.assign_action.accepted and a1._current_action == "act-9"
    (cancel_id,) = [json.loads(pl)["actions"][0]["actionId"] for t, pl in a1._robot.published
                    if not t.endswith("/order")]
    a1._robot.on_state(_state(order_id=o1, actions=[cancel_id]))  # the held order goes out
    o2 = a1._robot._last_order_id
    assert o2 != o1

    def look(_s) -> None:
        a2._robot.on_state(_state(order_id=o2, driving=True, nodes=2))
        a1._robot.on_state(_state(order_id=o1, driving=True, nodes=2))
    a1._stop_confirm_sleep = look
    seen: dict = {}
    p.handle_control_plane(_cancel_msg("amr-1", "act-1", 1), None)
    _, r = _await(p, seen, 1)
    assert r.cancel_action.disposition == pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED
    assert a1._current_action == "act-9"
