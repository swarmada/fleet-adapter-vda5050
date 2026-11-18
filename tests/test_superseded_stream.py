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
without redialling. An ordinary drop still redials."""

from __future__ import annotations

import json
import threading
import time

import grpc
import pytest
from conftest import mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
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

    def _stream(self, which: str):
        self.calls[which] += 1
        n = self.calls[which]
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
        return self._stream("control")

    def SafetyStream(self, _requests):
        return self._stream("safety")


@pytest.fixture
def adapter(monkeypatch):
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    monkeypatch.setattr(Adapter, "_edge_loop", lambda self: None)
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
    # The ControlStream that is still open then drops: it isn't redialled either.
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
