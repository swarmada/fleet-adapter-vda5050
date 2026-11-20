# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""One MQTT client for every vehicle of a process, against a stand-in for paho-mqtt 2.1.

* Each publish returns paho's own MQTTMessageInfo, so one vehicle's acknowledgement never
  answers for another's cancelOrder.
* Each vehicle's messages reach that vehicle's binding only, and each binding hears the
  session go down and come back.
* A vehicle whose binding starts after the session is up has its topics subscribed at once.
* The session ends with the last binding's stop, not the first.
* The in-flight limit follows the fleet's size (inflight_for). paho sends at most that many QoS 1
  messages awaiting their PUBACK and queues the rest unsent, so with the limit at paho's 20, a
  fleet's other traffic can keep a vehicle's stop from leaving the client within its 2 s."""

from __future__ import annotations

import json
import sys
import time
import types
from typing import ClassVar

import pytest

from fleet_adapter_vda5050.mqtt_client import PAHO_MAX_INFLIGHT, MqttClient, inflight_for
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding


class _RC:
    is_failure = False

    def __str__(self) -> str:
        return "Success"


class _Info:
    """paho's MQTTMessageInfo: published once the broker's PUBACK came."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.rc = 0
        self._published = False

    def is_published(self) -> bool:
        return self._published


class _Paho:
    """paho.mqtt.client.Client with its QoS 1 flow: at most ``max_inflight`` messages sent and
    awaiting their PUBACK, the rest queued unsent until one is acknowledged. The broker
    acknowledges at once every message except those ``hold`` names, which it leaves
    unacknowledged (a slow broker, or messages ahead in its queue)."""

    instances: ClassVar[list[_Paho]] = []

    def __init__(self, callback_api_version, client_id="") -> None:
        self.client_id = client_id
        self.max_inflight = 20                       # paho 2.1's default
        self.subscribed: list[str] = []
        self.sent: list[str] = []                    # topics that left the client, in order
        self.inflight: list[_Info] = []
        self.queued: list[_Info] = []
        self.hold = lambda topic: False
        self.on_connect = self.on_disconnect = self.on_message = None
        self.disconnected = False
        _Paho.instances.append(self)

    def reconnect_delay_set(self, min_delay, max_delay) -> None: ...

    def max_inflight_messages_set(self, inflight: int) -> None:
        self.max_inflight = inflight

    def connect(self, host, port) -> None: ...

    def loop_start(self) -> None:
        self.on_connect(self, None, {}, _RC(), None)

    def loop_stop(self) -> None: ...

    def disconnect(self) -> None:
        self.disconnected = True

    def subscribe(self, topic, qos=0) -> None:
        self.subscribed.append(topic)

    def publish(self, topic, payload, qos=0) -> _Info:
        info = _Info(topic)
        if len(self.inflight) < self.max_inflight:
            self._send(info)
        else:
            self.queued.append(info)
        return info

    def _send(self, info: _Info) -> None:
        self.sent.append(info.topic)
        self.inflight.append(info)
        if not self.hold(info.topic):
            self.puback(info)

    def puback(self, info: _Info) -> None:
        self.inflight.remove(info)
        info._published = True
        if self.queued:
            self._send(self.queued.pop(0))

    def release(self) -> None:
        """The broker catches up: it acknowledges every message in flight, in order, and from
        now on each one at once."""
        self.hold = lambda topic: False
        while self.inflight:
            self.puback(self.inflight[0])

    def drop(self) -> None:
        self.on_disconnect(self, None, {}, _RC(), None)

    def deliver(self, topic: str, payload: str) -> None:
        self.on_message(self, None, types.SimpleNamespace(topic=topic, payload=payload.encode()))


@pytest.fixture
def paho(monkeypatch):
    _Paho.instances = []
    client_mod = types.ModuleType("paho.mqtt.client")
    client_mod.Client = _Paho
    client_mod.CallbackAPIVersion = types.SimpleNamespace(VERSION1="v1", VERSION2="v2")
    mqtt_mod = types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod
    paho_mod = types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod
    monkeypatch.setitem(sys.modules, "paho", paho_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", client_mod)
    return _Paho


def _state(header: int, driving: bool = False, order_id: str = "") -> str:
    return json.dumps({"headerId": header, "timestamp": f"t-{header}", "driving": driving,
                       "velocity": {"vx": 0.5 if driving else 0.0, "vy": 0.0, "omega": 0.0},
                       "paused": False, "operatingMode": "AUTOMATIC", "orderId": order_id,
                       "nodeStates": [{"nodeId": "n1", "sequenceId": 2, "released": True}]
                       if driving else [], "actionStates": []})


def _online(fc: _Paho, b: Vda5050Binding, driving: bool = False) -> None:
    fc.deliver(b._topics.connection, json.dumps({"headerId": 0, "connectionState": "ONLINE"}))
    fc.deliver(b._topics.state, _state(1, driving=driving))


def _fleet(n: int) -> tuple[MqttClient, list[Vda5050Binding], _Paho]:
    """``n`` vehicles on one client, the client's limit set for ``n``, every one started."""
    shared = MqttClient("broker", 1883, client_id="swarmada-amr-1", vehicles=n)
    bs = [Vda5050Binding(robot_id=f"amr-{i}", serial=f"amr-{i}", mqtt=shared)
          for i in range(1, n + 1)]
    for b in bs:
        b.start()
    (fc,) = _Paho.instances
    for b in bs:
        _online(fc, b)
    return shared, bs, fc


# ── one client, several vehicles ─────────────────────────────────────────────

def test_one_client_serves_every_vehicle_and_connects_once(paho) -> None:
    shared, bs, fc = _fleet(3)
    assert len(paho.instances) == 1 and fc.client_id == "swarmada-amr-1"
    assert shared.users == 3
    for b in bs:
        assert {b._topics.state, b._topics.connection} <= set(fc.subscribed)
        assert b.transport_alive()


def test_each_publish_has_its_own_acknowledgement(paho) -> None:
    """amr-1's cancelOrder is acknowledged and amr-2's is not: only amr-1's cancel is
    accepted."""
    _shared, (b1, b2), fc = _fleet(2)
    fc.hold = lambda topic: topic == b2._topics.instant_actions
    for b in (b1, b2):
        b.command_move(b._robot_id, 5.0, 0.0)
        fc.deliver(b._topics.state, _state(2, driving=True, order_id=b._last_order_id))
        b.command_stop(b._robot_id)
    t1, t2 = b1.stop_cancel("amr-1"), b2.stop_cancel("amr-2")
    assert t1[1] is not t2[1]
    assert Vda5050Binding.cancel_accepted(t1)
    assert not Vda5050Binding.cancel_accepted(t2)


def test_each_vehicles_messages_reach_that_vehicle_only(paho) -> None:
    _shared, (b1, b2), fc = _fleet(2)
    before = b1.state_seq("amr-1"), b2.state_seq("amr-2")
    fc.deliver(b2._topics.state, _state(7))
    assert (b1.state_seq("amr-1"), b2.state_seq("amr-2")) == (before[0], before[1] + 1)


def test_every_vehicle_hears_the_session_drop(paho) -> None:
    """Each binding's own handler hears the drop, so after the session comes back none of them
    reports its vehicle observable until its connection is delivered again."""
    _shared, bs, fc = _fleet(3)
    fc.drop()
    fc.on_connect(fc, None, {}, _RC(), None)  # paho's reconnect; nothing delivered yet
    assert not any(b.transport_alive() for b in bs)


def test_a_vehicle_started_after_the_session_is_up_is_subscribed_at_once(paho) -> None:
    shared, _bs, fc = _fleet(1)
    late = Vda5050Binding(robot_id="amr-9", serial="amr-9", mqtt=shared)
    late.start()
    assert late._topics.state in fc.subscribed
    _online(fc, late)
    assert late.transport_alive()


def test_the_session_ends_with_the_last_vehicles_stop(paho) -> None:
    shared, (b1, b2), fc = _fleet(2)
    b1.stop()
    b1.stop()  # a second stop of the same binding releases nothing more
    assert not fc.disconnected and shared.users == 1 and b2.transport_alive()
    b2.stop()
    assert fc.disconnected and shared.users == 0


def test_a_lone_vehicle_keeps_its_own_client_and_pahos_limit(paho) -> None:
    b = Vda5050Binding(robot_id="amr-1", serial="amr-1")
    b.start()
    (fc,) = paho.instances
    assert fc.client_id == "swarmada-amr-1" and fc.max_inflight == PAHO_MAX_INFLIGHT
    b.stop()
    assert fc.disconnected


# ── the in-flight limit follows the fleet ────────────────────────────────────

def test_the_in_flight_limit_follows_the_fleets_size(paho) -> None:
    """Eleven for each vehicle (the most one can have in flight), never below paho's 20."""
    assert inflight_for(1) == PAHO_MAX_INFLIGHT
    assert inflight_for(2) == 22
    assert inflight_for(25) == 275
    _shared, _bs, fc = _fleet(25)
    assert fc.max_inflight == 275


def test_a_stop_is_sent_within_its_window_with_the_others_orders_in_flight(paho) -> None:
    """25 vehicles. The broker hasn't yet acknowledged an order to each of the other 24, which
    fills paho's default 20. amr-25's estop (startPause, then cancelOrder) must still leave the
    client, and its cancelOrder be acknowledged, within the stop's 2 s."""
    _shared, bs, fc = _fleet(25)
    *others, b = bs
    fc.hold = lambda topic: topic.endswith("/order") and topic != b._topics.order
    for o in others:
        o.command_move(o._robot_id, 5.0, 0.0)
    # The others' 24 orders await their PUBACK: sent, or queued unsent past the limit.
    assert len(fc.inflight) + len(fc.queued) == 24
    t0 = time.monotonic()
    b.command_estop(b._robot_id)
    ticket = b.stop_cancel(b._robot_id)
    deadline = t0 + 2.0  # a stop's budget
    while not Vda5050Binding.cancel_accepted(ticket):
        assert time.monotonic() < deadline, (
            f"amr-25's stop not acknowledged within 2 s: {len(fc.queued)} queued unsent")
        time.sleep(0.01)
    assert fc.sent.count(b._topics.instant_actions) == 2  # startPause and cancelOrder both sent


def test_a_namespace_stop_of_25_vehicles_at_their_worst_is_sent_at_once_and_acknowledged(
        paho) -> None:
    """Every vehicle at the most it can have in flight (mqtt_client._INFLIGHT_PER_VEHICLE): a
    pause, an order with the startPause before it, a preemption's cancelOrder and a
    factsheetRequest, all awaiting their PUBACK from a broker that is behind. Then a namespace
    stop reaches all 25 by its three paths (SafetyStream estop, ControlStream fence, edge
    estop): each vehicle's startPause and cancelOrder, three times over. Not one message may
    wait, queued unsent, for a place; and once the broker catches up, every vehicle's stop
    and cancelOrder is acknowledged, within the stop's 2 s."""
    _shared, bs, fc = _fleet(25)
    fc.hold = lambda topic: True  # the broker acknowledges nothing yet
    t0 = time.monotonic()
    for b in bs:
        rid = b._robot_id
        b.command_pause(rid)                   # startPause
        b.command_move(rid, 5.0, 0.0)          # startPause, order
        b.command_move(rid, 6.0, 0.0)          # in place: cancelOrder, the order held
        b.request_factsheet(rid)               # factsheetRequest
    tickets = {}
    for _path in ("SafetyStream estop", "ControlStream fence", "edge estop"):
        for b in bs:
            b.command_estop(b._robot_id)       # startPause, cancelOrder
            tickets[b._robot_id] = b.stop_cancel(b._robot_id)
    assert len(fc.inflight) == 25 * 11
    assert not fc.queued, f"{len(fc.queued)} messages queued unsent behind the limit"
    fc.release()
    assert time.monotonic() - t0 < 2.0
    for b in bs:
        assert Vda5050Binding.cancel_accepted(tickets[b._robot_id]), b._robot_id
        assert fc.sent.count(b._topics.instant_actions) == 1 + 1 + 1 + 1 + 6, b._robot_id
