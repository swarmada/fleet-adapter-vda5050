# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""The MQTT broker session, against a stand-in for paho-mqtt 2.x (no broker).

* connect() uses callback API version 2 and returns only once the broker has accepted the
  connection and every topic is subscribed; it raises when the broker cannot be reached,
  refuses, or does not answer.
* paho reconnects by itself after a drop. Each accepted (re)connect subscribes every topic
  again, since a clean session keeps none, and only then is the session counted as up.
* A drop counts the session as down at once. The binding then reports the vehicle as not
  observable, and its connection state, position and battery as unknown until the broker
  delivers them again."""

from __future__ import annotations

import json
import sys
import types
from typing import ClassVar

import pytest

from fleet_adapter_vda5050 import mqtt_client, profiles
from fleet_adapter_vda5050.mqtt_client import MqttClient
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"


class _RC:
    """paho's ReasonCode, as far as the client reads it."""

    def __init__(self, failure: bool = False, text: str = "Success") -> None:
        self.is_failure = failure
        self._text = text

    def __str__(self) -> str:
        return self._text


class _FakeClient:
    """paho.mqtt.client.Client, recording. loop_start() answers the connect as the network
    thread would, with ``answer`` (None: no answer)."""

    instances: ClassVar[list[_FakeClient]] = []
    answer: ClassVar[_RC | None] = _RC()
    refuse_tcp: ClassVar[bool] = False

    def __init__(self, callback_api_version, client_id="") -> None:
        self.callback_api_version = callback_api_version
        self.client_id = client_id
        self.subscribed: list[tuple[str, int]] = []
        self.published: list[tuple[str, str, int]] = []
        self.calls: list[str] = []
        self.reconnect_delay = None
        self.on_connect = self.on_disconnect = self.on_message = None
        _FakeClient.instances.append(self)

    def reconnect_delay_set(self, min_delay, max_delay) -> None:
        self.reconnect_delay = (min_delay, max_delay)

    def connect(self, host, port) -> None:
        self.calls.append(f"connect {host}:{port}")
        if _FakeClient.refuse_tcp:
            raise ConnectionRefusedError(61, "Connection refused")

    def loop_start(self) -> None:
        self.calls.append("loop_start")
        if _FakeClient.answer is not None:
            self.on_connect(self, None, {}, _FakeClient.answer, None)

    def loop_stop(self) -> None:
        self.calls.append("loop_stop")

    def disconnect(self) -> None:
        self.calls.append("disconnect")

    def subscribe(self, topic, qos=0) -> None:
        self.subscribed.append((topic, qos))

    def publish(self, topic, payload, qos=0) -> None:
        self.published.append((topic, payload, qos))

    # what paho's network thread does on its own
    def drop(self) -> None:
        self.on_disconnect(self, None, {}, _RC(True, "Unspecified error"), None)

    def reconnect(self, rc: _RC | None = None) -> None:
        self.on_connect(self, None, {}, rc or _RC(), None)

    def deliver(self, topic: str, payload: str) -> None:
        self.on_message(self, None, types.SimpleNamespace(topic=topic, payload=payload.encode()))


@pytest.fixture
def paho(monkeypatch):
    """Put the stand-in where ``import paho.mqtt.client`` finds it."""
    _FakeClient.instances = []
    _FakeClient.answer = _RC()
    _FakeClient.refuse_tcp = False
    client_mod = types.ModuleType("paho.mqtt.client")
    client_mod.Client = _FakeClient
    client_mod.CallbackAPIVersion = types.SimpleNamespace(VERSION1="v1", VERSION2="v2")
    mqtt_mod = types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod
    paho_mod = types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod
    monkeypatch.setitem(sys.modules, "paho", paho_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", client_mod)
    return _FakeClient


def _client() -> MqttClient:
    c = MqttClient("broker", 1883, client_id="swarmada-amr-1")
    c.subscribe("a/state", lambda t, p: None)
    c.subscribe("a/connection", lambda t, p: None)
    return c


# ── connect ──────────────────────────────────────────────────────────────────

def test_connect_uses_callback_api_v2_and_subscribes_once_accepted(paho) -> None:
    c = _client()
    c.connect()
    (fc,) = paho.instances
    assert fc.callback_api_version == "v2" and fc.client_id == "swarmada-amr-1"
    assert fc.subscribed == [("a/state", 1), ("a/connection", 1)]
    assert c.connected
    assert fc.reconnect_delay == (1, 10)


@pytest.mark.parametrize("case", ["refused", "no answer", "unreachable"])
def test_connect_raises_unless_the_broker_accepts(paho, monkeypatch, case) -> None:
    monkeypatch.setattr(mqtt_client, "_CONNECT_TIMEOUT_S", 0.05)
    if case == "refused":
        paho.answer = _RC(True, "Not authorized")
    elif case == "no answer":
        paho.answer = None
    else:
        paho.refuse_tcp = True
    c = _client()
    with pytest.raises(RuntimeError, match="broker broker:1883"):
        c.connect()
    assert not c.connected
    if case != "unreachable":
        assert paho.instances[0].calls[-2:] == ["disconnect", "loop_stop"]  # thread stopped
        assert paho.instances[0].subscribed == []


# ── a drop and paho's reconnect ──────────────────────────────────────────────

def test_a_reconnect_subscribes_every_topic_again(paho, capsys) -> None:
    c = _client()
    sessions = []
    c.on_session(sessions.append)
    c.connect()
    fc = paho.instances[0]
    fc.drop()
    assert not c.connected
    fc.reconnect()
    assert c.connected
    assert fc.subscribed == [("a/state", 1), ("a/connection", 1)] * 2
    assert sessions == [True, False, True]
    err = capsys.readouterr().err
    assert "session lost" in err and "subscribed again" in err


def test_a_refused_reconnect_stays_down(paho, capsys) -> None:
    c = _client()
    c.connect()
    fc = paho.instances[0]
    fc.drop()
    fc.reconnect(_RC(True, "Server unavailable"))
    assert not c.connected
    assert fc.subscribed == [("a/state", 1), ("a/connection", 1)]  # not again
    assert "refused the reconnect: Server unavailable" in capsys.readouterr().err


def test_stop_disconnects_then_stops_the_thread_and_is_idempotent(paho) -> None:
    c = _client()
    c.connect()
    fc = paho.instances[0]
    c.stop()
    c.stop()
    assert not c.connected
    assert fc.calls == ["connect broker:1883", "loop_start", "disconnect", "loop_stop"]


def test_publish_needs_a_connection_and_uses_qos_1(paho) -> None:
    c = _client()
    with pytest.raises(RuntimeError, match="not connected"):
        c.publish("a/order", "{}")
    c.connect()
    c.publish("a/order", "{}")
    assert paho.instances[0].published == [("a/order", "{}", 1)]


def test_messages_reach_their_handler_only(paho) -> None:
    got = []
    c = MqttClient("broker", 1883)
    c.subscribe("a/state", lambda t, p: got.append((t, p)))
    c.connect()
    fc = paho.instances[0]
    fc.deliver("a/state", "x")
    fc.deliver("b/state", "y")
    assert got == [("a/state", b"x")]


# ── the binding over the session ─────────────────────────────────────────────

def _state(x: float = 1.0, battery: int = 80) -> str:
    return json.dumps({"headerId": 1, "timestamp": "t1", "driving": False,
                       "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
                       "agvPosition": {"x": x, "y": 2.0, "theta": 0.0, "mapId": "m",
                                       "positionInitialized": True},
                       "batteryState": {"batteryCharge": battery},
                       "nodeStates": [], "actionStates": []})


def _started(paho) -> tuple[Vda5050Binding, _FakeClient]:
    b = Vda5050Binding(robot_id=ROBOT, serial="agv-1")
    b.start()
    return b, paho.instances[0]


def test_the_binding_subscribes_its_topics_and_is_live_once_the_vehicle_is_online(paho) -> None:
    b, fc = _started(paho)
    base = "uagv/v2/swarmada/agv-1"
    assert {t for t, _ in fc.subscribed} == {f"{base}/{k}" for k in
                                              ("connection", "state", "factsheet", "visualization")}
    assert not b.transport_alive()                       # no connection message yet
    fc.deliver(f"{base}/connection", '{"connectionState": "ONLINE"}')
    assert b.transport_alive()


def test_a_drop_makes_the_vehicle_unobservable_and_its_readings_unknown(paho) -> None:
    b, fc = _started(paho)
    base = "uagv/v2/swarmada/agv-1"
    fc.deliver(f"{base}/connection", '{"connectionState": "ONLINE"}')
    fc.deliver(f"{base}/state", _state(1.0))
    assert b.transport_alive() and b.pose(ROBOT) == (1.0, 2.0, 0)
    fc.drop()
    assert not b.transport_alive()
    assert "broker session is down" in b.transport_detail()
    assert b.connection_state(ROBOT) == profiles.CONN_OFFLINE
    assert b.pose(ROBOT) is None and b.battery_percent(ROBOT) is None
    fc.reconnect()                                       # session back; nothing delivered yet
    assert not b.transport_alive()
    fc.deliver(f"{base}/connection", '{"connectionState": "ONLINE"}')  # the retained one
    assert b.transport_alive()
    assert b.pose(ROBOT) is None                         # until the vehicle reports again
    fc.deliver(f"{base}/state", json.dumps({**json.loads(_state(4.0, 79)), "headerId": 2,
                                             "timestamp": "t2"}))
    assert b.pose(ROBOT) == (4.0, 2.0, 0) and b.battery_percent(ROBOT) == 79


def test_a_broker_that_refuses_fails_the_binding_start(paho) -> None:
    paho.answer = _RC(True, "Bad user name or password")
    b = Vda5050Binding(robot_id=ROBOT)
    with pytest.raises(RuntimeError, match="did not accept the connection"):
        b.start()
    assert not b.transport_alive()


def test_stop_makes_the_binding_unobservable(paho) -> None:
    b, fc = _started(paho)
    fc.deliver("uagv/v2/swarmada/agv-1/connection", '{"connectionState": "ONLINE"}')
    b.stop()
    assert not b.transport_alive()


def test_the_session_counts_as_up_only_once_every_topic_is_subscribed(paho) -> None:
    c = _client()
    seen = []
    c.on_session(lambda up: seen.append((up, list(paho.instances[-1].subscribed))))
    c.connect()
    fc = paho.instances[0]
    fc.drop()
    fc.reconnect()
    first, again = seen[0][1], seen[2][1]
    assert seen[0][0] and len(first) == 2          # both topics already sent when it said up
    assert seen[2][0] and len(again) == 4


def test_the_session_handler_hears_each_change_once(paho) -> None:
    c = _client()
    sessions = []
    c.on_session(sessions.append)
    c.connect()
    paho.instances[0].drop()
    c.stop()                                       # already down: no second "down"
    assert sessions == [True, False]


def test_a_connection_message_handled_after_a_drop_does_not_make_the_vehicle_observable(
        paho) -> None:
    b, fc = _started(paho)
    fc.drop()
    b.on_connection('{"connectionState": "ONLINE"}')  # one that was already on its way
    assert not b.transport_alive()                    # the session is still down


def test_a_drop_only_makes_the_vehicle_not_connected_and_a_fresh_online_restores_it(
        paho) -> None:
    # The drop's only effect: the vehicle reads as not connected. Nothing is sent to it (no
    # stop, no cancel of the binding's own), and the adapter reports it OFFLINE. A fresh
    # ONLINE from the broker restores it.
    from fleet_adapter.v1 import fleet_adapter_pb2 as pb
    from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter

    b, fc = _started(paho)
    base = "uagv/v2/swarmada/agv-1"
    fc.deliver(f"{base}/connection", '{"connectionState": "ONLINE"}')
    fc.deliver(f"{base}/state", _state(1.0))
    a = Adapter(pb, None, b, ROBOT)
    a._telemetry_interval = 0

    def one_cycle():
        passes = iter([False, True])
        a._shutdown = type("_Once", (), {"is_set": lambda self: next(passes),
                                         "set": lambda self: None})()
        a._telemetry_loop(None)
        return a._outbox.get_nowait().telemetry

    assert one_cycle().phase != pb.RobotPhase.ROBOT_PHASE_OFFLINE
    fc.drop()
    t = one_cycle()
    assert t.phase == pb.RobotPhase.ROBOT_PHASE_OFFLINE and not t.HasField("position")
    assert fc.published == []                         # nothing sent to the vehicle
    assert not b.is_online(ROBOT)
    fc.reconnect()
    fc.deliver(f"{base}/connection", '{"connectionState": "ONLINE"}')
    assert b.is_online(ROBOT) and b.transport_alive()
    assert one_cycle().phase != pb.RobotPhase.ROBOT_PHASE_OFFLINE
    assert fc.published == []
