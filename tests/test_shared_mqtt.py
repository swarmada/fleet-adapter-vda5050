# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""One MQTT client for every vehicle of a process, against a stand-in for paho-mqtt 2.1.

* Each publish returns paho's own MQTTMessageInfo, so one vehicle's acknowledgement never
  answers for another's cancelOrder.
* Each vehicle's messages reach that vehicle's binding only, and each binding hears the
  session go down and come back.
* A vehicle whose binding starts after the session is up has its topics subscribed at once, and
  one whose topic is registered while the session comes up has it subscribed too.
* The session ends with the last binding's stop, not the first.
* The in-flight limit follows the fleet's size (inflight_for). paho sends at most that many QoS 1
  messages awaiting their PUBACK and queues the rest unsent, so with the limit at paho's 20, a
  fleet's other traffic can keep a vehicle's stop from leaving the client within its 2 s.
* Under a broker that acknowledges each message a fixed time after it is sent, every vehicle's
  stop and cancelOrder in a namespace stop is acknowledged within 2 s of the stop: with each
  vehicle at the most one incident can put in flight, and with a burst of stops for robot ids
  this process doesn't serve on top."""

from __future__ import annotations

import heapq
import json
import sys
import threading
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


class _TimedPaho(_Paho):
    """A broker that acknowledges each message ``ack_s`` after paho sends it, on a thread of its
    own; paho's in-flight limit and queue as in _Paho. Each message's info records when it was
    published and when its PUBACK came."""

    ack_s = 1.0

    def __init__(self, callback_api_version, client_id="") -> None:
        super().__init__(callback_api_version, client_id)
        self.lock = threading.RLock()
        self.published: list[_Info] = []
        self._due: list = []  # (when, seq, info)
        self._seq = 0
        self._wake = threading.Condition(self.lock)
        self.closed = False
        threading.Thread(target=self._broker, daemon=True).start()

    def publish(self, topic, payload, qos=0) -> _Info:
        with self.lock:
            info = super().publish(topic, payload, qos)
            info.published_at = time.monotonic()
            info.acked_at = None
            self.published.append(info)
            return info

    def _send(self, info: _Info) -> None:
        self.sent.append(info.topic)
        self.inflight.append(info)
        self._seq += 1
        heapq.heappush(self._due, (time.monotonic() + self.ack_s, self._seq, info))
        self._wake.notify()

    def _broker(self) -> None:
        with self.lock:
            while not self.closed:
                if not self._due:
                    self._wake.wait(0.05)
                    continue
                when, _seq, info = self._due[0]
                now = time.monotonic()
                if now < when:
                    self._wake.wait(when - now)
                    continue
                heapq.heappop(self._due)
                info.acked_at = now
                self.puback(info)


def _install(monkeypatch, client_cls) -> None:
    client_mod = types.ModuleType("paho.mqtt.client")
    client_mod.Client = client_cls
    client_mod.CallbackAPIVersion = types.SimpleNamespace(VERSION1="v1", VERSION2="v2")
    mqtt_mod = types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod
    paho_mod = types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod
    monkeypatch.setitem(sys.modules, "paho", paho_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", client_mod)


@pytest.fixture
def paho(monkeypatch):
    _Paho.instances = []
    _install(monkeypatch, _Paho)
    return _Paho


@pytest.fixture
def timed_paho(monkeypatch):
    _Paho.instances = []
    _install(monkeypatch, _TimedPaho)
    yield _TimedPaho
    for fc in _Paho.instances:
        with fc.lock:
            fc.closed = True


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
    """Fourteen for each vehicle (the most one incident can put in flight), never below paho's
    20."""
    assert inflight_for(1) == PAHO_MAX_INFLIGHT
    assert inflight_for(2) == 28
    assert inflight_for(25) == 350
    _shared, _bs, fc = _fleet(25)
    assert fc.max_inflight == 350


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


# Each vehicle's messages in one incident, at the most it can have in flight
# (mqtt_client._INFLIGHT_PER_VEHICLE): 5 of its own work, then its stops.
_WORK = 5
_STOPS = 9  # 3 x (startPause, cancelOrder) for the emergency stops, 3 cancelOrders


def _incident(bs: list[Vda5050Binding], unserved: int) -> dict[str, list]:
    """Every vehicle at the most one incident can put in flight, all at once, with ``unserved``
    stops for a robot id this process doesn't serve coming before the stops (each stops every
    vehicle: startPause, cancelOrder). Returns each vehicle's stop messages, as paho's infos."""
    fc = _Paho.instances[0]
    for b in bs:
        rid = b._robot_id
        b.command_pause(rid)                   # startPause
        b.command_move(rid, 5.0, 0.0)          # startPause, order
        b.command_move(rid, 6.0, 0.0)          # in place: cancelOrder, the order held
        b.request_factsheet(rid)               # factsheetRequest
    for _ in range(unserved):
        for b in bs:
            b.command_estop(b._robot_id)       # startPause, cancelOrder
    stops: dict[str, list] = {b._robot_id: [] for b in bs}
    for path in ("SafetyStream estop", "ControlStream fence", "edge estop",
                 "ControlStream cancel", "lease expiry", "supersession"):
        for b in bs:
            before = len(fc.published)
            if path.endswith(("estop", "fence")):
                b.command_estop(b._robot_id)   # startPause, cancelOrder
            else:
                b.command_stop(b._robot_id)    # cancelOrder
            stops[b._robot_id] += fc.published[before:]
    return stops


@pytest.mark.parametrize(("case", "ack_s", "unserved"), [
    # One incident: 25 x 14 messages, all sent at once at the limit; the broker takes 1.2 s, so
    # a message queued for one round, past a limit one short per vehicle, is acknowledged at
    # 2.4 s, past the stop's 2 s.
    ("one incident", 1.2, 0),
    # The same with 3 stops for an unserved robot id on top (150 more messages, unbounded by
    # the factor, so some queue): with a broker that takes 0.5 s, every stop still makes it.
    ("one incident and an unserved-id burst", 0.5, 3),
])
def test_a_namespace_stop_of_25_vehicles_is_acknowledged_within_2_s(timed_paho, case, ack_s,
                                                                    unserved) -> None:
    """Every vehicle's stop and cancelOrder is acknowledged within 2 s of the stop."""
    timed_paho.ack_s = ack_s
    _shared, bs, fc = _fleet(25)
    with fc.lock:  # the whole incident at once, as one burst
        stops = _incident(bs, unserved)
        assert len(fc.published) == 25 * (_WORK + 2 * unserved + _STOPS)
        assert len(fc.inflight) == min(len(fc.published), fc.max_inflight)
    deadline = time.monotonic() + 2.5
    while time.monotonic() < deadline:
        with fc.lock:
            if all(i.acked_at is not None for msgs in stops.values() for i in msgs):
                break
        time.sleep(0.02)
    with fc.lock:
        late = {rid: [round((i.acked_at or float("inf")) - i.published_at, 2) for i in msgs
                      if i.acked_at is None or i.acked_at - i.published_at > 2.0]
                for rid, msgs in stops.items()}
        worst = max((i.acked_at - i.published_at for msgs in stops.values() for i in msgs
                     if i.acked_at is not None), default=float("nan"))
    print(f"{case}: limit {fc.max_inflight}, worst stop acknowledged after {worst:.2f} s")
    late = {rid: v for rid, v in late.items() if v}
    assert not late, f"{len(late)} vehicles' stops not acknowledged within 2 s: {late}"
    for b in bs:
        assert len(stops[b._robot_id]) == _STOPS


@pytest.mark.parametrize("when", ["first connect", "reconnect"])
def test_a_topic_registered_while_the_session_comes_up_is_subscribed(paho, monkeypatch,
                                                                     when) -> None:
    """Another vehicle's binding registers its topic while on_connect is subscribing the topics
    it found: the topic is subscribed, by on_connect or by the registration, never by neither."""
    entered, gate, gated = threading.Event(), threading.Event(), [when == "first connect"]
    plain = _Paho.subscribe

    def subscribe(self, topic, qos=0) -> None:
        if gated[0] and topic == "uagv/v2/acme/amr-1/state":
            entered.set()
            gate.wait(5)  # on_connect, between its snapshot of the topics and the session up
        plain(self, topic, qos)

    monkeypatch.setattr(_Paho, "subscribe", subscribe)
    mc = MqttClient(vehicles=2)
    mc.subscribe("uagv/v2/acme/amr-1/state", lambda t, p: None)
    if when == "first connect":
        coming_up = threading.Thread(target=mc.connect)
    else:
        mc.connect()
        p = _Paho.instances[0]
        p.drop()
        gated[0] = True
        coming_up = threading.Thread(target=p.on_connect, args=(p, None, {}, _RC(), None))
    coming_up.start()
    assert entered.wait(5)
    joining = threading.Thread(target=mc.subscribe,
                               args=("uagv/v2/acme/amr-2/state", lambda t, p: None))
    joining.start()
    joining.join(0.3)  # amr-2's binding registers its topic meanwhile
    gate.set()
    coming_up.join(5)
    joining.join(5)
    assert not coming_up.is_alive() and not joining.is_alive()
    assert mc.connected
    assert "uagv/v2/acme/amr-2/state" in _Paho.instances[0].subscribed
