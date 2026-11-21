# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A stream closed as superseded (RFC-0001 #fleet-adapter-protocol-connection-lifecycle).

One adapter identity is served by one live process. When the control plane accepts a second
ControlStream or SafetyStream under the same identity, it closes the earlier one with ABORTED
"superseded". A process whose stream is closed that way by a stream it didn't open MUST stop
every robot it drives and hold no work. This adapter only opens a stream after the previous one
has ended, so a superseded close is always another process's doing: it stops the vehicle
(cancelOrder, no pause, so the process that took over can give it work), refuses all later
work, releases the MQTT session the two processes would otherwise fight over, and stays up
without redialling, by any path: a drop's backoff, the simulated comms' reconnect and a clean
close included. An ordinary drop still redials.

A process serving several vehicles does all of that once, for every vehicle: the supersession is
the process's streams', so every vehicle is stopped and refuses work, the waits for rest run at
the same time, and each MQTT session is released once. With every vehicle on one MQTT client,
as a process serving several runs them, each releases it once and its session ends with the
last release, after every vehicle's cancelOrder. A vehicle brought up after the supersession
doesn't take the session back."""

from __future__ import annotations

import json
import threading
import time

import grpc
import pytest
from conftest import fleet_on_one_client, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import _Streams
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.process import FleetProcess
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
SUPERSEDED = ('superseded: a newer ControlStream for adapter "vda" in namespace "default" was '
              'accepted; one adapter identity is served on one stream, so this one is closed')
_IDLE = json.dumps({"driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
                    "paused": False, "orderId": "", "nodeStates": [], "actionStates": []})


class _Closed(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = "") -> None:
        super().__init__(details)
        self._code, self._details = code, details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def _wire(b) -> list[str]:
    return ["order" if t.endswith("/order") else json.loads(p)["actions"][0]["actionType"]
            for t, p in b.published]


def _assign_cmd(command_id: int, action_id: str, token: int) -> pb.ControlPlaneMessage:
    return pb.ControlPlaneMessage(command=pb.Command(
        command_id=command_id, robot_id=ROBOT, leader_term=1, assign_action=pb.AssignAction(
            action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
            assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}')))


_SESSION = [
    pb.ControlPlaneMessage(hello_ack=pb.HelloAck(accepted=True,
                                                 negotiated_contract_version="1.4.0")),
    pb.ControlPlaneMessage(register_ack=pb.RegisterAck(accepted=True,
                                                       telemetry_interval_seconds=1)),
]


class _Stub:
    """ControlStream and SafetyStream, scripted per call: messages, then the error that ends
    the stream. A stream scripted with no error stays open until the test drops it
    (``drop``) or the fixture closes it."""

    def __init__(self, control=(), safety=()) -> None:
        self.scripts = {"control": list(control), "safety": list(safety)}
        self.calls = {"control": 0, "safety": 0}
        self.open: dict = {}
        self.cancelled: list = []

    def _stream(self, which: str, opened: dict):
        self.calls[which] += 1
        n = opened["n"] = self.calls[which]
        script = self.scripts[which]
        msgs, error = script.pop(0) if script else ([], None)
        ev, slot = threading.Event(), {}
        self.open[(which, n)] = (ev, slot)
        yield from msgs
        if error is not None:
            raise error
        ev.wait()
        if slot.get("error") is not None:
            raise slot["error"]

    def drop(self, which: str, n: int, error) -> None:
        _until(lambda: (which, n) in self.open)
        ev, slot = self.open[(which, n)]
        slot["error"] = error
        ev.set()

    def close_all(self) -> None:
        for ev, _ in list(self.open.values()):
            ev.set()

    def ControlStream(self, _requests):
        return _Call(self, "control")

    def SafetyStream(self, _requests):
        return _Call(self, "safety")


class _Call:
    """One scripted stream, iterated as a gRPC response stream is; ``cancel`` ends it with
    CANCELLED, as a cancelled call does, and is recorded."""

    def __init__(self, stub: _Stub, which: str) -> None:
        self._stub, self._which, self._opened = stub, which, {}
        self._it = stub._stream(which, self._opened)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._it)

    def cancel(self) -> None:
        n = self._opened.get("n")
        if n is not None:
            self._stub.cancelled.append((self._which, n))
            self._stub.drop(self._which, n, _Closed(grpc.StatusCode.CANCELLED, "cancelled"))


@pytest.fixture
def adapter(monkeypatch):
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    monkeypatch.setattr(FleetProcess, "_start_edge", lambda self, link: None)
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.start = lambda: None  # no broker: bring-up is the stand-in's
    stops = []
    real_stop = b.stop
    b.stop = lambda: (stops.append(True), real_stop())
    b.on_state(_IDLE)
    a = Adapter(pb, None, b, ROBOT)
    a._stop_confirm_sleep = lambda s: None
    a.stops = stops
    a.stub = None
    yield a
    a._shutdown.set()
    if a.stub is not None:
        a.stub.close_all()


def _run(target, *args) -> threading.Thread:
    t = threading.Thread(target=target, args=args, daemon=True)
    t.start()
    return t


def _until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


def _refused_after(a) -> pb.AssignActionResult:
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(action_id="act-9", action_type=adapter_mod.ACTION_NAVIGATE,
                                 fencing_token=9, assignment_id="as-9",
                                 payload_json=b'{"x": 1, "y": 0}'), res)
    return res.assign_action


def test_a_superseded_control_stream_stops_the_vehicle_and_holds_no_work(adapter, capsys) -> None:
    a, b = adapter, adapter._robot
    stub = a.stub = _Stub(control=[(_SESSION + [_assign_cmd(1, "act-1", 7)],
                                    _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))])
    t = _run(a.run_control_stream, stub)
    _until(lambda: a.stops)
    assert _wire(b) == ["order", "cancelOrder"]  # stopped, and not paused
    assert a._current_action == ""
    assert a._interrupted == {"act-1": 7}
    refused = _refused_after(a)
    assert not refused.accepted and "superseded" in refused.message
    time.sleep(0.8)  # past the 0.5 s a redial waits
    assert stub.calls["control"] == 1  # not redialled
    assert t.is_alive()  # up until terminated
    assert "error: " in capsys.readouterr().err


def test_a_superseded_safety_stream_stops_the_vehicle_and_nothing_redials(adapter) -> None:
    a, b = adapter, adapter._robot
    stub = a.stub = _Stub(
        control=[(_SESSION + [_assign_cmd(1, "act-1", 7)], None)],
        safety=[([], _Closed(grpc.StatusCode.ABORTED,
                             SUPERSEDED.replace("ControlStream", "SafetyStream")))])
    tc = _run(a.run_control_stream, stub)
    _until(lambda: a._current_action == "act-1")
    ts = _run(a.run_safety_stream, stub)
    _until(lambda: a.stops)
    ts.join(2)
    assert _wire(b) == ["order", "cancelOrder"]
    assert a._interrupted == {"act-1": 7}
    assert not _refused_after(a).accepted
    assert stub.calls["safety"] == 1
    # The ControlStream is closed too, and a later drop of it redials nothing either.
    stub.drop("control", 1, _Closed(grpc.StatusCode.UNAVAILABLE, "connection reset"))
    time.sleep(0.8)
    assert stub.calls["control"] == 1
    assert tc.is_alive()


@pytest.mark.parametrize("closed", [
    _Closed(grpc.StatusCode.UNAVAILABLE, "connection reset"),
    _Closed(grpc.StatusCode.ABORTED, "aborted for another reason"),
], ids=["unavailable", "aborted-not-superseded"])
@pytest.mark.parametrize("stream", ["control", "safety"])
def test_an_ordinary_drop_still_redials_and_stops_nothing(adapter, stream, closed) -> None:
    a, b = adapter, adapter._robot
    stub = a.stub = _Stub(control=[(_SESSION + [_assign_cmd(1, "act-1", 7)], None),
                                   (_SESSION, None)],
                          safety=[([], closed), ([], None)])
    tc = _run(a.run_control_stream, stub)
    _until(lambda: a._current_action == "act-1")
    if stream == "control":
        stub.drop("control", 1, closed)  # the redial opens a second, which stays open
        _until(lambda: stub.calls["control"] == 2)
    else:
        ts = _run(a.run_safety_stream, stub)
        _until(lambda: stub.calls["safety"] == 2)
        assert ts.is_alive()
    assert "cancelOrder" not in _wire(b)
    assert not a.stops
    assert tc.is_alive()


# ── after a supersession, nothing is redialled, by any path ─────────────────
#
# A redial after MQTT is released takes the streams back from the process that took over, which
# then parks the vehicle as superseded: the robot's stops come to a process that can't deliver
# them. That is a robot that fails to stop.

SUPERSEDED_SAFETY = SUPERSEDED.replace("ControlStream", "SafetyStream")


def _both_open(a, stub) -> tuple[threading.Thread, threading.Thread]:
    tc = _run(a.run_control_stream, stub)
    _until(lambda: a._registered)
    ts = _run(a.run_safety_stream, stub)
    _until(lambda: ("safety", 1) in stub.open)
    return tc, ts


def test_a_safety_drop_then_a_supersession_does_not_redial_the_safety_stream(adapter) -> None:
    a = adapter
    stub = a.stub = _Stub(control=[(_SESSION, None)], safety=[([], None), ([], None)])
    tc, ts = _both_open(a, stub)
    stub.drop("safety", 1, _Closed(grpc.StatusCode.UNAVAILABLE, "connection reset"))
    time.sleep(0.15)  # in the 0.5 s backoff
    stub.drop("control", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))
    _until(lambda: a.stops)
    ts.join(1)
    assert not ts.is_alive()  # the SafetyStream loop has ended, past the backoff
    assert stub.calls["safety"] == 1
    assert tc.is_alive()


def test_after_a_drop_and_a_supersession_a_stop_for_the_robot_does_not_land_here(adapter) -> None:
    """The ControlStream drops; the SafetyStream is superseded during the backoff, and MQTT is
    released. Redialled, the ControlStream would take the robot's fence (here, e9) away from
    the process that took over: this process could neither deliver it (no MQTT) nor answer it
    (no SafetyStream). Not redialled, the fence goes to the process that can."""
    a, b = adapter, adapter._robot
    fence = pb.ControlPlaneMessage(command=pb.Command(
        command_id=2, robot_id=ROBOT, leader_term=1, estop=pb.Estop(estop_id="e9")))
    stub = a.stub = _Stub(control=[(_SESSION + [_assign_cmd(1, "act-1", 7)], None),
                                   (_SESSION + [fence], None)],
                          safety=[([], None)])
    tc, _ts = _both_open(a, stub)
    _until(lambda: a._current_action == "act-1")
    stub.drop("control", 1, _Closed(grpc.StatusCode.UNAVAILABLE, "connection reset"))
    time.sleep(0.15)  # in the 0.5 s backoff
    stub.drop("safety", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED_SAFETY))
    _until(lambda: a.stops)
    time.sleep(0.8)  # past the backoff
    assert stub.calls["control"] == 1  # the fence was never taken here
    assert _wire(b) == ["order", "cancelOrder"]  # stopped by the supersession, not paused
    assert tc.is_alive()


class _Sim:
    """The simulated comms' hooks the ControlStream loop and the RegisterAck read: a scenario
    drop whose reconnect time the test moves past."""

    def __init__(self) -> None:
        self.now = 0.0

    def stream_drop_window(self):
        return (0.0, 1.0)

    def elapsed(self) -> float:
        return self.now

    def reset_hardware_baseline(self) -> None:
        pass

    def hardware_delta(self) -> list:
        return []

    def manifest_hardware(self) -> list:
        return []


def test_the_simulated_comms_loop_does_not_redial_after_a_supersession(adapter) -> None:
    a = adapter
    a._sim = sim = _Sim()
    stub = a.stub = _Stub(control=[(_SESSION, None), (_SESSION, None)], safety=[([], None)])
    tc, _ts = _both_open(a, stub)
    a._reconnect_pending = True  # the scenario's drop
    stub.drop("control", 1, _Closed(grpc.StatusCode.CANCELLED, "dropped by the scenario"))
    time.sleep(0.15)  # waiting for the reconnect time
    stub.drop("safety", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED_SAFETY))
    _until(lambda: a.stops)
    sim.now = 2.0  # the reconnect time passes
    time.sleep(0.8)
    assert stub.calls["control"] == 1
    assert tc.is_alive()


def test_a_clean_close_after_a_supersession_leaves_the_process_up(adapter) -> None:
    a = adapter
    stub = a.stub = _Stub(control=[(_SESSION, None)], safety=[([], None)])
    tc, _ts = _both_open(a, stub)
    stub.drop("safety", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED_SAFETY))
    _until(lambda: a.stops)
    stub.drop("control", 1, None)  # the ControlStream ends without an error
    time.sleep(0.3)
    assert tc.is_alive() and not a._shutdown.is_set()
    assert stub.calls["control"] == 1


def test_a_pause_after_a_supersession_is_refused(adapter) -> None:
    a, b = adapter, adapter._robot
    stub = a.stub = _Stub(control=[(_SESSION, None)], safety=[([], None)])
    _both_open(a, stub)
    stub.drop("safety", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED_SAFETY))
    _until(lambda: a._superseded.is_set())
    res = pb.CommandResult()
    a._on_pause(pb.PauseCapabilities(), res)
    assert not res.pause.paused and "superseded" in res.pause.message
    _until(lambda: a.stops)
    assert "startPause" not in _wire(b)  # handed over unpaused


def test_a_fault_in_the_stop_sequence_still_releases_mqtt_and_stays_up(adapter, capsys) -> None:
    a, b = adapter, adapter._robot

    def broken(robot_id):
        raise TypeError("command_stop is broken")

    b.command_stop = broken
    stub = a.stub = _Stub(control=[(_SESSION, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))])
    tc = _run(a.run_control_stream, stub)
    _until(lambda: a.stops)
    time.sleep(0.3)
    assert tc.is_alive()
    assert "the stop sequence raised" in capsys.readouterr().err


def test_a_fence_after_a_safety_only_supersession_is_not_silently_lost(adapter) -> None:
    """Only the SafetyStream is superseded: the process that took over hasn't opened its
    ControlStream yet, so ours is still open. A fence sent on ours now could be neither answered
    (no SafetyStream) nor, once MQTT is released, delivered. So ours is closed at once, not
    redialled, and the control plane sends the fence to the process that took over."""
    a = adapter
    stub = a.stub = _Stub(control=[(_SESSION, None), (_SESSION, None)], safety=[([], None)])
    tc, _ts = _both_open(a, stub)
    stub.drop("safety", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED_SAFETY))
    _until(lambda: ("control", 1) in stub.cancelled, timeout=2)  # closed
    _until(lambda: a.stops)
    time.sleep(0.8)  # past the 0.5 s a redial waits
    assert stub.calls["control"] == 1  # and not redialled
    assert tc.is_alive()


# ── a fleet: the rule for every vehicle ──────────────────────────────────────

FLEET = ("amr-1", "amr-2", "amr-3")


def _fleet_process(monkeypatch, bindings) -> FleetProcess:
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    monkeypatch.setattr(FleetProcess, "_start_edge", lambda self, link: None)
    streams, cores = _Streams(), []
    for b in bindings:
        b.start = lambda: None
        b.stops = []
        b.stop = (lambda b=b, real=b.stop: (b.stops.append(True), real()))
        b.on_state(_IDLE)
        c = Adapter(pb, None, b, b._robot_id, streams=streams)
        c._stop_confirm_sleep = lambda s: None
        cores.append(c)
    p = FleetProcess(cores)
    p.stub = None
    return p


@pytest.fixture
def fleet(monkeypatch):
    """Three vehicles on one process's streams, each idle and live with an MQTT client of its
    own, none sending a state after a stop unless a test makes it."""
    p = _fleet_process(monkeypatch, [mark_transport_live(Vda5050Binding(robot_id=rid, serial=rid))
                                     for rid in FLEET])
    yield p
    p._streams.shutdown.set()
    if p.stub is not None:
        p.stub.close_all()


@pytest.fixture
def shared_fleet(monkeypatch):
    """The same three vehicles, all on one MQTT client (fleet_on_one_client)."""
    shared, broker, bindings = fleet_on_one_client(FLEET)
    p = _fleet_process(monkeypatch, bindings)
    p.shared, p.broker = shared, broker
    yield p
    p._streams.shutdown.set()
    if p.stub is not None:
        p.stub.close_all()


def _fleet_session() -> list:
    return [_SESSION[0]] + [pb.ControlPlaneMessage(register_ack=pb.RegisterAck(
        accepted=True, telemetry_interval_seconds=1)) for _ in FLEET]


def _working(p) -> None:
    """Every vehicle registered on the session and driving an action of its own."""
    _until(lambda: all(c._registered for c in p.cores))
    for i, c in enumerate(p.cores, start=1):
        res = pb.CommandResult()
        c._on_assign(pb.AssignAction(action_id=f"act-{i}", action_type=adapter_mod.ACTION_NAVIGATE,
                                     fencing_token=i, assignment_id=f"as-{i}",
                                     payload_json=b'{"x": 5, "y": 0}'), res)
        assert res.assign_action.accepted, c._robot_id


def _all_released(p) -> bool:
    return all(c._robot.stops for c in p.cores)


def test_a_superseded_stream_stops_every_vehicle_and_none_holds_work(fleet, capsys) -> None:
    p = fleet
    stub = p.stub = _Stub(control=[(_fleet_session(), None)])
    tc = _run(p.run_control_stream, stub)
    _working(p)
    stub.drop("control", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))
    _until(lambda: _all_released(p))
    for i, c in enumerate(p.cores, start=1):
        assert _wire(c._robot) == ["order", "cancelOrder"], c._robot_id  # stopped, not paused
        assert c._current_action == ""
        assert c._interrupted == {f"act-{i}": i}
        refused = _refused_after(c)
        assert not refused.accepted and "superseded" in refused.message
        res = pb.CommandResult()
        c._on_resume(pb.ResumeCapabilities(), res)
        assert not res.resume.resumed
        res = pb.CommandResult()
        c._on_pause(pb.PauseCapabilities(), res)
        assert not res.pause.paused
        assert c._robot.stops == [True]  # released once
    time.sleep(0.8)  # past the 0.5 s a redial waits
    assert stub.calls == {"control": 1, "safety": 0}  # nothing redialled
    assert tc.is_alive()
    err = capsys.readouterr().err
    assert err.count("SUPERSEDED: ControlStream closed") == 1  # recorded once, for the process
    for rid in FLEET:
        assert f"SUPERSEDED robot={rid}: the stop is NOT confirmed" in err
        assert f"SUPERSEDED robot={rid}: MQTT session released" in err


def test_every_cancel_order_goes_out_before_any_wait(fleet) -> None:
    p = fleet
    sent_before_a_wait: list = []
    real = Adapter._confirm_stop

    def confirm(self, since, **kw):
        sent_before_a_wait.append(sum(_wire(c._robot).count("cancelOrder") for c in p.cores))
        return real(self, since, **kw)

    p.cores[0].__class__._confirm_stop = confirm
    try:
        stub = p.stub = _Stub(control=[(_fleet_session(), None)])
        _run(p.run_control_stream, stub)
        _working(p)
        stub.drop("control", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))
        _until(lambda: _all_released(p))
    finally:
        p.cores[0].__class__._confirm_stop = real
    assert sent_before_a_wait == [3, 3, 3]


def test_the_waits_run_at_the_same_time(fleet) -> None:
    """No vehicle sends a state after the stop, so each wait runs its whole 2 s budget. One
    after another they would take 6 s; at the same time, about 2 s."""
    p = fleet
    for c in p.cores:
        c._stop_confirm_sleep = time.sleep
    stub = p.stub = _Stub(control=[(_fleet_session(), None)])
    _run(p.run_control_stream, stub)
    _working(p)
    t0 = time.monotonic()
    stub.drop("control", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))
    _until(lambda: _all_released(p), timeout=10)
    assert time.monotonic() - t0 < 3.5


def test_both_streams_superseded_release_each_session_once(fleet) -> None:
    p = fleet
    stub = p.stub = _Stub(control=[(_fleet_session(), None)], safety=[([], None)])
    tc = _run(p.run_control_stream, stub)
    _until(lambda: stub.calls["control"] == 1)
    ts = _run(p.run_safety_stream, stub)
    _until(lambda: stub.calls["safety"] == 1)
    stub.drop("safety", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED_SAFETY))
    _until(lambda: ("control", 1) in stub.cancelled, timeout=2)  # ours closed: no fence lands here
    _until(lambda: _all_released(p))
    time.sleep(0.8)
    assert [c._robot.stops for c in p.cores] == [[True]] * 3
    assert stub.calls == {"control": 1, "safety": 1}
    assert tc.is_alive()
    ts.join(2)


def test_a_fault_for_one_vehicle_still_stops_and_releases_the_others(fleet, capsys) -> None:
    p = fleet

    def broken(robot_id):
        raise TypeError("command_stop is broken")

    p.cores[1]._robot.command_stop = broken
    stub = p.stub = _Stub(control=[(_fleet_session(), None)])
    _run(p.run_control_stream, stub)
    _working(p)
    stub.drop("control", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))
    _until(lambda: _all_released(p))
    assert _wire(p.cores[0]._robot) == ["order", "cancelOrder"]
    assert _wire(p.cores[2]._robot) == ["order", "cancelOrder"]
    assert "SUPERSEDED robot=amr-2: the stop is NOT confirmed: the stop sequence raised" in (
        capsys.readouterr().err)


def test_a_superseded_fleet_on_one_mqtt_client_ends_its_session_once_after_every_stop(
        shared_fleet) -> None:
    """Every vehicle on one MQTT client: after a supersession each is stopped and releases the
    client once, and the client's session ends once, with the last release, after every
    vehicle's cancelOrder has gone out."""
    p = shared_fleet
    stub = p.stub = _Stub(control=[(_fleet_session(), None)])
    _run(p.run_control_stream, stub)
    _working(p)
    stub.drop("control", 1, _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))
    _until(lambda: _all_released(p))
    for c in p.cores:
        assert _wire(c._robot) == ["order", "cancelOrder"], c._robot_id
        assert c._robot.stops == [True] and not c._robot._mqtt_held, c._robot_id
    assert p.shared.users == 0
    assert p.broker.disconnects == [(0, len(p.broker.published))]  # once, last, after all


def _unstarted_fleet(monkeypatch):
    """FLEET on one MQTT client, none of them started yet: the client connects (a stand-in for
    paho's connect, counted) only when the first binding starts."""
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    shared, broker, bindings = fleet_on_one_client(FLEET)
    shared._client, shared._up, shared._users = None, False, 0
    for b in bindings:
        b._mqtt_held = b._started = False
        b.on_state(_IDLE)
    opens: list = []

    def _open() -> None:
        opens.append(True)
        shared._client, shared._up = broker, True
    shared._open = _open
    streams = _Streams()
    cores = [Adapter(pb, None, b, b._robot_id, streams=streams) for b in bindings]
    for c in cores:
        c._stop_confirm_sleep = lambda s: None
    p = FleetProcess(cores)
    return p, shared, broker, opens


def _supersede(p) -> None:
    p.cores[0]._on_superseded("ControlStream", _Closed(grpc.StatusCode.ABORTED, SUPERSEDED))


def test_a_vehicle_brought_up_after_a_supersession_is_not_started(monkeypatch) -> None:
    """The streams are superseded after two of three vehicles are up. Their bindings are released
    and the session ends; the third, brought up afterwards, isn't started: the client doesn't
    connect again, and nothing is published for it."""
    p, shared, broker, opens = _unstarted_fleet(monkeypatch)
    a1, a2, a3 = p.cores
    a1.bring_up_robot()
    a2.bring_up_robot()
    _supersede(p)
    assert shared.users == 0 and broker.disconnects == [(0, len(broker.published))]
    a3.bring_up_robot()
    assert opens == [True]  # connected once, for the first two only
    assert shared.users == 0 and shared._client is None
    assert not a3._robot._started and not a3._robot._mqtt_held
    assert list(a3._robot.published) == []


def test_a_vehicle_whose_start_was_under_way_at_the_supersession_is_released(
        monkeypatch) -> None:
    """The first vehicle's start is still connecting when the streams are superseded. The
    supersession's stop of it waits for the start (the vehicle's lock, held across it), then
    releases it: the client doesn't connect again, nothing is published for the vehicle, which
    has no order to cancel, and the session ends."""
    p, shared, broker, opens = _unstarted_fleet(monkeypatch)
    connecting, go = threading.Event(), threading.Event()
    opened = shared._open

    def slow_open() -> None:
        connecting.set()
        go.wait(5)
        opened()
    shared._open = slow_open
    a1 = p.cores[0]
    up = threading.Thread(target=a1.bring_up_robot, daemon=True)
    up.start()
    assert connecting.wait(5)
    # The supersession, on the stream reader's thread: recorded at once; its stop of this
    # vehicle waits for the vehicle's lock, which the bring-up holds while it connects.
    superseding = threading.Thread(target=_supersede, args=(p,), daemon=True)
    superseding.start()
    _until(lambda: a1._superseded.is_set())
    superseding.join(0.3)
    assert superseding.is_alive()  # held behind the start, not run while it connects
    go.set()
    up.join(5)
    superseding.join(5)
    assert not up.is_alive() and not superseding.is_alive()
    assert opens == [True]  # the one connect, and not again
    assert shared.users == 0 and shared._client is None
    assert broker.disconnects == [(0, len(broker.published))]
    assert not a1._robot._mqtt_held
    assert list(a1._robot.published) == []
