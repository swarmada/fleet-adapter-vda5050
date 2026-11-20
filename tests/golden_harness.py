# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A harness that records what the adapter says on its wires, for the golden transcripts.

The adapter runs as shipped: ``main()`` with the single-vehicle flags, dialling a control plane
over gRPC. Two things stand in for the world around it:

* **The control plane** (:class:`Plane`): an in-process gRPC server for ``ControlStream`` and
  ``SafetyStream``, and a second one for an edge node's ``EdgeStream``. A test sends each
  control-plane message itself, and the server records each message the adapter sends.
* **The broker and the vehicle** (:class:`Broker`, :class:`Vehicle`): a stand-in for paho's
  client, so the adapter's own ``MqttClient`` runs unchanged. It records each publish and
  delivers the vehicle's replies on a thread of its own, as paho's network thread does. The
  vehicle answers orders and instant actions the way a VDA 5050 vehicle does, with a ``state``
  each time.

Nothing is recorded by time. A test waits for the message it expects next (:meth:`Transcript.expect`),
and the transcript takes, in order, every message on that wire up to it. Each wire keeps its own
order, so the transcript is the same on every run. Telemetry is the exception: it streams on a
timer, so it's left out and sampled only where a test asks (:meth:`Transcript.sample_telemetry`).
Timestamps, the adapter's per-process boot token, the edge node's port and the local latch's
random set ID are written as placeholders."""

from __future__ import annotations

import json
import queue
import re
import sys
import threading
import time
import types
from collections.abc import Callable
from concurrent import futures

import grpc
from conftest import PublishInfo
from google.protobuf import json_format

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter.v1 import fleet_adapter_pb2_grpc as pb_grpc
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles

ROBOT = "amr-1"
MANUFACTURER = "swarmada"
SERIAL = "amr-1"
WAIT_S = 5.0

_TIME_KEY = re.compile(r"(_ms|_at|^timestamp)$")


def _is_time(key: str) -> bool:
    """A clock reading (written as <t>); a duration such as lease_duration_ms is kept."""
    return bool(key) and bool(_TIME_KEY.search(key)) and "duration" not in key
_BOOT = re.compile(r"-[0-9a-f]{8}-(order|a)-")
_EDGE_ADDR = re.compile(r"^127\.0\.0\.1:\d+$")  # the edge server's port, picked per run


# ── normalising ──────────────────────────────────────────────────────────────

def _scrub(value, key: str = ""):
    if isinstance(value, dict):
        return {k: _scrub(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub(v, key) for v in value]
    if isinstance(value, str):
        if _is_time(key):
            return "<t>"
        if key == "set_id" and value:  # a fresh random ID for each edge stop
            return "<set>"
        if key == "adapter_version" and value:  # this build's version (test_contract.py)
            return "<version>"
        if _EDGE_ADDR.match(value):
            return "<edge>"
        return _BOOT.sub(r"-<boot>-\1-", value)
    if isinstance(value, (int, float)) and _is_time(key) and value:
        return "<t>"
    return value


def proto_line(msg) -> str:
    body = json_format.MessageToDict(msg, preserving_proto_field_name=True)
    return json.dumps(_scrub(body), sort_keys=True, separators=(",", ":"))


def mqtt_line(topic: str, payload: str) -> str:
    body = json.loads(payload)
    return topic.rsplit("/", 1)[-1] + " " + json.dumps(_scrub(body), sort_keys=True,
                                                         separators=(",", ":"))


def kind(msg) -> str:
    for field in ("payload", "msg"):
        try:
            which = msg.WhichOneof(field)
        except ValueError:
            continue
        if which:
            return which
    return ""


# ── the transcript ───────────────────────────────────────────────────────────

class Wire:
    """One direction of one stream, as the adapter sends it: the messages in arrival order."""

    def __init__(self, name: str, line: Callable) -> None:
        self.name, self._line = name, line
        self.items: list = []
        self.cursor = 0
        self.cond = threading.Condition()

    def add(self, item) -> None:
        with self.cond:
            self.items.append(item)
            self.cond.notify_all()


class Transcript:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.wires: dict[str, Wire] = {
            "control": Wire("control", proto_line), "safety": Wire("safety", proto_line),
            "edge": Wire("edge", proto_line), "mqtt": Wire("mqtt", lambda tp: mqtt_line(*tp))}
        self.telemetry = Wire("telemetry", proto_line)

    def step(self, text: str) -> None:
        self.lines.append(f"## {text}")

    def sent(self, wire: str, msg) -> None:
        self.lines.append(f"{wire}< {proto_line(msg)}")

    def note(self, text: str) -> None:
        self.lines.append(f"-- {text}")

    def expect(self, wire: str, match: Callable | str, timeout: float = WAIT_S):
        """Wait for the next message on ``wire`` that ``match`` accepts (a kind name, an
        MQTT topic suffix or actionType, or a predicate), and record every message on that
        wire up to and including it."""
        w = self.wires[wire]
        pred = match if callable(match) else _matcher(wire, match)
        deadline = time.monotonic() + timeout
        with w.cond:
            while True:
                for i in range(w.cursor, len(w.items)):
                    if pred(w.items[i]):
                        for item in w.items[w.cursor:i + 1]:
                            self.lines.append(f"{wire}> {w._line(item)}")
                        w.cursor = i + 1
                        return w.items[i]
                left = deadline - time.monotonic()
                if left <= 0:
                    seen = [w._line(x) for x in w.items[w.cursor:]]
                    raise AssertionError(f"{wire}: no {match!r} within {timeout:g} s; "
                                         f"unread: {seen}")
                w.cond.wait(left)

    def quiet(self, wire: str, seconds: float = 0.3) -> None:
        """Nothing more on ``wire`` for ``seconds``: what did come is recorded, so a message
        that should not have been sent shows in the transcript."""
        time.sleep(seconds)
        w = self.wires[wire]
        with w.cond:
            for item in w.items[w.cursor:]:
                self.lines.append(f"{wire}> {w._line(item)}")
            w.cursor = len(w.items)
        self.lines.append(f"{wire}: quiet")

    def sample_telemetry(self, match: Callable = lambda m: True, timeout: float = WAIT_S):
        """The first telemetry payload sent from now on that ``match`` accepts."""
        w = self.telemetry
        with w.cond:
            start = len(w.items)
        deadline = time.monotonic() + timeout
        with w.cond:
            while True:
                for item in w.items[start:]:
                    if match(item):
                        self.lines.append(f"telemetry> {proto_line(item)}")
                        return item
                start = len(w.items)
                left = deadline - time.monotonic()
                if left <= 0:
                    raise AssertionError(f"telemetry: no matching payload within {timeout:g} s")
                w.cond.wait(left)

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


def _matcher(wire: str, name: str) -> Callable:
    if wire == "mqtt":
        def match(item):
            topic, payload = item
            if topic.endswith("/" + name):
                return True
            body = json.loads(payload)
            return any(a.get("actionType") == name for a in body.get("actions", []))
        return match
    return lambda m: kind(m) == name


# ── the control plane ────────────────────────────────────────────────────────

class _Stream:
    """One server-side stream: what the test puts on ``out`` goes to the adapter. None ends the
    stream cleanly; an exception class ends it with an error."""

    def __init__(self) -> None:
        self.out: queue.Queue = queue.Queue()

    def responses(self, context):
        while True:
            msg = self.out.get()
            if msg is None:
                return
            if msg == "DROP":
                context.abort(grpc.StatusCode.UNAVAILABLE, "dropped by the test")
            yield msg


class Plane:
    def __init__(self, t: Transcript) -> None:
        self.t = t
        self.control: list[_Stream] = []
        self.safety: list[_Stream] = []
        self.edge: list[_Stream] = []
        self._new = threading.Condition()
        self._seq = 0
        plane = self

        def reader(requests, wire: str):
            def run():
                try:
                    for msg in requests:
                        if wire == "control" and kind(msg) == "telemetry":
                            t.telemetry.add(msg)
                        else:
                            t.wires[wire].add(msg)
                except grpc.RpcError:
                    pass
            threading.Thread(target=run, daemon=True).start()

        def serve(streams: list, requests, context, wire: str):
            s = _Stream()
            with plane._new:
                streams.append(s)
                plane._new.notify_all()
            reader(requests, wire)
            yield from s.responses(context)

        class Fleet(pb_grpc.FleetAdapterServiceServicer):
            def ControlStream(self, requests, context):
                yield from serve(plane.control, requests, context, "control")

            def SafetyStream(self, requests, context):
                yield from serve(plane.safety, requests, context, "safety")

        class Edge(pb_grpc.EdgeServiceServicer):
            def EdgeStream(self, requests, context):
                yield from serve(plane.edge, requests, context, "edge")

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        pb_grpc.add_FleetAdapterServiceServicer_to_server(Fleet(), self.server)
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.edge_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb_grpc.add_EdgeServiceServicer_to_server(Edge(), self.edge_server)
        self.edge_port = self.edge_server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.edge_server.start()

    def wait_stream(self, which: str, count: int, timeout: float = WAIT_S) -> _Stream:
        streams = getattr(self, which)
        with self._new:
            if not self._new.wait_for(lambda: len(streams) >= count, timeout):
                raise AssertionError(f"no {which} stream #{count} within {timeout:g} s")
            return streams[count - 1]

    def send(self, which: str, msg) -> None:
        """Send on the latest stream of ``which``, and record it."""
        self._seq += 1
        if which != "edge":
            msg.seq = self._seq
        self.t.sent(which, msg)
        getattr(self, which)[-1].out.put(msg)

    def drop_control(self) -> None:
        self.t.note("the control plane drops the ControlStream")
        self.control[-1].out.put("DROP")

    def stop(self) -> None:
        for streams in (self.control, self.safety, self.edge):
            for s in streams:
                s.out.put(None)
        self.server.stop(1.0)
        self.edge_server.stop(1.0)


# ── the broker and the vehicle ───────────────────────────────────────────────

class _RC:
    is_failure = False

    def __str__(self) -> str:
        return "Success"


class Vehicle:
    """A VDA 5050 vehicle, as far as the adapter reads one. It answers each order and each
    instant action with a ``state``, unless ``silent``."""

    def __init__(self) -> None:
        self.topics = profiles.get_profile("vda5050").topics(MANUFACTURER, SERIAL)
        self.lock = threading.RLock()
        self.silent = False
        self.header = 0
        self.order_id = ""
        self.nodes: list[dict] = []
        self.goal = (0.0, 0.0)
        self.x = self.y = 0.0
        self.paused = False
        self.driving = False
        self.actions: list[dict] = []

    def connection(self) -> str:
        return json.dumps({"headerId": 0, "timestamp": "2026-01-01T00:00:00.000Z",
                           "version": "2.0.0", "manufacturer": MANUFACTURER,
                           "serialNumber": SERIAL, "connectionState": "ONLINE"})

    def state(self) -> str:
        with self.lock:
            return self._state()

    def _state(self) -> str:
        self.header += 1
        return json.dumps({
            "headerId": self.header, "timestamp": f"2026-01-01T00:00:{self.header:02d}.000Z",
            "version": "2.0.0", "manufacturer": MANUFACTURER, "serialNumber": SERIAL,
            "orderId": self.order_id, "orderUpdateId": 0, "lastNodeId": "",
            "lastNodeSequenceId": 0, "driving": self.driving, "paused": self.paused,
            "operatingMode": "AUTOMATIC", "newBaseRequest": False,
            "nodeStates": self.nodes, "edgeStates": [], "actionStates": self.actions[-8:],
            "agvPosition": {"x": self.x, "y": self.y, "theta": 0.0, "mapId": "map",
                            "positionInitialized": True},
            "velocity": {"vx": 0.5 if self.driving else 0.0, "vy": 0.0, "omega": 0.0},
            "batteryState": {"batteryCharge": 80.0, "charging": False},
            "errors": [], "loads": [], "safetyState": {"eStop": "NONE", "fieldViolation": False}})

    def on_publish(self, topic: str, payload: str) -> list[tuple[str, str]]:
        with self.lock:
            return self._on_publish(topic, payload)

    def _on_publish(self, topic: str, payload: str) -> list[tuple[str, str]]:
        if self.silent:
            return []
        body = json.loads(payload)
        if topic == self.topics.order:
            self.order_id = body["orderId"]
            nodes = body.get("nodes", [])
            self.nodes = [{"nodeId": n["nodeId"], "sequenceId": n["sequenceId"], "released": True}
                          for n in nodes[1:]]
            last = nodes[-1].get("nodePosition", {}) if nodes else {}
            self.goal = (last.get("x", self.x), last.get("y", self.y))
            self.driving = bool(self.nodes) and not self.paused
        elif topic == self.topics.instant_actions:
            for a in body.get("actions", []):
                at = a["actionType"]
                if at == "startPause":
                    self.paused, self.driving = True, False
                elif at == "stopPause":
                    self.paused = False
                    self.driving = bool(self.nodes)
                elif at == "cancelOrder":
                    self.nodes, self.driving = [], False
                self.actions.append({"actionId": a["actionId"], "actionType": at,
                                     "actionStatus": "FINISHED"})
        else:
            return []
        return [(self.topics.state, self._state())]

    def arrive(self) -> tuple[str, str]:
        with self.lock:
            self.x, self.y = self.goal
            self.nodes, self.driving = [], False
            return (self.topics.state, self._state())


class Broker:
    """paho's client, for every MqttClient the adapter makes: publishes are recorded and handed
    to the vehicle; what the vehicle sends back is delivered on the broker's own thread. Once
    the adapter subscribes, the vehicle also sends a state every ``STATE_PERIOD_S``, as a
    vehicle does, unless it is silent."""

    STATE_PERIOD_S = 0.1

    def __init__(self, t: Transcript, vehicle: Vehicle) -> None:
        self.t, self.vehicle = t, vehicle
        self.clients: list = []
        self._q: queue.Queue = queue.Queue()
        self._stopped = threading.Event()
        self.state_seen = threading.Event()  # a state has reached the adapter
        self._ticking = False
        threading.Thread(target=self._run, daemon=True).start()

    def _tick(self) -> None:
        while not self._stopped.wait(self.STATE_PERIOD_S):
            if not self.vehicle.silent:
                self.deliver(self.vehicle.topics.state, self.vehicle.state())

    def _run(self) -> None:
        while True:
            fn = self._q.get()
            if fn is None:
                return
            fn()

    def post(self, fn) -> None:
        self._q.put(fn)

    def deliver(self, topic: str, payload: str) -> None:
        def run():
            for c in list(self.clients):
                if topic in c.subscribed and c.on_message is not None:
                    c.on_message(c, None, types.SimpleNamespace(topic=topic,
                                                                payload=payload.encode()))
                    if topic == self.vehicle.topics.state:
                        self.state_seen.set()
        self.post(run)

    def vehicle_says(self, topic: str, payload: str) -> None:
        self.deliver(topic, payload)

    def module(self) -> types.ModuleType:
        broker = self

        class Client:
            def __init__(self, callback_api_version, client_id="") -> None:
                self.client_id = client_id
                self.subscribed: set[str] = set()
                self.on_connect = self.on_disconnect = self.on_message = None
                broker.clients.append(self)

            def reconnect_delay_set(self, min_delay, max_delay) -> None: ...

            def connect(self, host, port) -> None: ...

            def loop_start(self) -> None:
                broker.post(lambda: self.on_connect(self, None, {}, _RC(), None))

            def loop_stop(self) -> None: ...

            def disconnect(self) -> None: ...

            def subscribe(self, topic, qos=0) -> None:
                self.subscribed.add(topic)
                if topic == broker.vehicle.topics.connection:  # retained
                    broker.deliver(topic, broker.vehicle.connection())
                    if not broker._ticking:
                        broker._ticking = True
                        threading.Thread(target=broker._tick, daemon=True).start()

            def publish(self, topic, payload, qos=0):
                broker.t.wires["mqtt"].add((topic, payload))
                for reply in broker.vehicle.on_publish(topic, payload):
                    broker.deliver(*reply)
                return PublishInfo(published=True)  # acknowledged, as a broker does at QoS 1

        mod = types.ModuleType("paho.mqtt.client")
        mod.Client = Client
        mod.CallbackAPIVersion = types.SimpleNamespace(VERSION1="v1", VERSION2="v2")
        return mod

    def stop(self) -> None:
        self._stopped.set()
        self._q.put(None)


def install_paho(monkeypatch, broker: Broker) -> None:
    client_mod = broker.module()
    mqtt_mod = types.ModuleType("paho.mqtt")
    mqtt_mod.client = client_mod
    paho_mod = types.ModuleType("paho")
    paho_mod.mqtt = mqtt_mod
    monkeypatch.setitem(sys.modules, "paho", paho_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_mod)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", client_mod)


# ── a run ────────────────────────────────────────────────────────────────────

class Run:
    """The adapter, its control plane, its broker and its vehicle, for one transcript."""

    def __init__(self, monkeypatch, state_dir) -> None:
        self.t = Transcript()
        self.vehicle = Vehicle()
        self.broker = Broker(self.t, self.vehicle)
        install_paho(monkeypatch, self.broker)
        self.plane = Plane(self.t)
        argv = ["--endpoint", f"127.0.0.1:{self.plane.port}", "--comms", "vda5050",
                "--robot-id", ROBOT, "--manufacturer", MANUFACTURER, "--serial", SERIAL,
                "--mqtt-host", "broker.test", "--insecure", "--state-dir", str(state_dir)]
        self.error: list[BaseException] = []

        def run():
            try:
                adapter_mod.main(argv)
            except BaseException as exc:  # noqa: BLE001 - reported by finish()
                self.error.append(exc)

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    # the session as every scenario starts it
    def register(self, session: int = 1, accepted: bool = True, edge: bool = False,
                 **ack) -> None:
        """The hello and the register on ControlStream session ``session``. The vehicle has
        reported a state first, so what the adapter does at registration does not depend on
        whether one came in time."""
        t, p = self.t, self.plane
        t.step(f"the adapter connects (ControlStream session {session})")
        p.wait_stream("safety", 1)
        p.wait_stream("control", session)
        if not self.broker.state_seen.wait(WAIT_S):
            raise AssertionError("the vehicle's state never reached the adapter")
        t.expect("control", "hello")
        p.send("control", pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
            accepted=True, negotiated_protocol_version="fleet_adapter.v1",
            negotiated_contract_version="1.4.0")))
        t.expect("control", "register")
        if edge:
            ack.setdefault("edge_endpoints", [pb.EdgeEndpoint(
                zone="zone-a", address=f"127.0.0.1:{p.edge_port}")])
        p.send("control", pb.ControlPlaneMessage(register_ack=pb.RegisterAck(
            accepted=accepted, telemetry_interval_seconds=1, **ack)))
        if accepted:
            t.expect("control", "capabilities")

    def vehicle_arrives(self) -> None:
        self.t.note("the vehicle reaches its goal")
        self.broker.vehicle_says(*self.vehicle.arrive())

    def command(self, command_id: int, **arm) -> None:
        self.plane.send("control", pb.ControlPlaneMessage(command=pb.Command(
            command_id=command_id, robot_id=arm.pop("robot_id", ROBOT), leader_term=1, **arm)))

    def finish(self) -> str:
        self.t.step("the control plane closes the streams")
        self.plane.stop()
        self.thread.join(10.0)
        self.broker.stop()
        assert not self.thread.is_alive(), "the adapter did not stop when its streams closed"
        assert not self.error, f"the adapter raised: {self.error[0]!r}"
        for name, w in self.t.wires.items():
            with w.cond:
                for item in w.items[w.cursor:]:
                    self.t.lines.append(f"{name}> {w._line(item)}")
                w.cursor = len(w.items)
        return self.t.text()
