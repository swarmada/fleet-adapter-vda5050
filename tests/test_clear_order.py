# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A stop is answered before its clear.

A stop is commanded on the reader and answered from the vehicle's lane once rest is confirmed,
up to 2 s later. A clear for that stop can arrive on the SafetyStream meanwhile. Its
EstopClearAck, and the local report a clear that ends the local set brings, must come after the
stop's final EstopAck (STOPPED or FAILED), in that order, whichever stream the stop came on and
whether the clear came before or after its fence, and when the clear is read on the SafetyStream
while the fence's stop is still being commanded on the ControlStream.

Its lifted_at_ms is the time the latch lifted the stop, not the time the ack was sent; an ack
sent again for an ID lifted earlier carries the time it is sent."""

from __future__ import annotations

import json
import queue
import threading
import time

import pytest
from conftest import mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"


class _Stream:
    """One SafetyStream: what the adapter sends on it, and what the test sends it."""

    def __init__(self, requests) -> None:
        self.sent: list = []
        self.inbox: queue.Queue = queue.Queue()
        threading.Thread(target=lambda: self.sent.extend(requests), daemon=True).start()

    def __iter__(self):
        return self

    def __next__(self):
        msg = self.inbox.get()
        if msg == "END":
            raise StopIteration
        return msg

    def events(self, estop_id: str, start: int) -> list[str]:
        """What was sent about ``estop_id`` from ``sent[start]`` on, in order: "final" for a
        STOPPED or FAILED EstopAck, "clear" for an EstopClearAck, "local" for a local report.
        ``start`` is past the stream's opening report, so every later local report counts,
        wherever it falls."""
        out = []
        for m in list(self.sent)[start:]:
            kind = m.WhichOneof("payload")
            if kind == "estop_ack" and m.estop_ack.estop_id == estop_id \
                    and m.estop_ack.state != pb.ESTOP_STATE_STOPPING:
                out.append("final")
            elif kind == "estop_clear_ack" and m.estop_clear_ack.estop_id == estop_id:
                out.append("clear")
            elif kind == "local_stop":
                out.append("local")
        return out


class _Stub:
    def __init__(self) -> None:
        self.streams: list[_Stream] = []

    def SafetyStream(self, requests) -> _Stream:
        s = _Stream(requests)
        self.streams.append(s)
        return s


def _until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


@pytest.fixture
def adapter(monkeypatch):
    """A vehicle that reports rest, paused, only from ``rest_at`` (monotonic) on."""
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.on_state(json.dumps({"driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
                           "paused": False, "orderId": "", "nodeStates": [], "actionStates": []}))
    n = [100]
    b.rest_at = float("inf")

    def tick(dt: float) -> None:
        n[0] += 1
        rest = time.monotonic() >= b.rest_at
        b.on_state(json.dumps({
            "headerId": n[0], "timestamp": f"t-{n[0]}", "driving": not rest,
            "velocity": {"vx": 0.0 if rest else 0.6, "vy": 0.0, "omega": 0.0}, "paused": rest,
            "orderId": "", "nodeStates": [], "actionStates": []}))
    b.tick = tick
    a = Adapter(pb, None, b, ROBOT)
    a._negotiated = "1.4.0"
    a.stub = _Stub()
    yield a
    a._shutdown.set()
    for s in a.stub.streams:
        s.inbox.put("END")


def _fence(a, estop_id: str) -> None:
    a._handle_command(pb.Command(command_id=1, robot_id=ROBOT, leader_term=1,
                                 estop=pb.Estop(estop_id=estop_id)))


def _safety(stream, **payload) -> None:
    stream.inbox.put(pb.ControlPlaneSafetyMessage(robot_id=ROBOT, **payload))


@pytest.mark.parametrize("case", [
    "fence, clear",                 # the clear lifts the fenced stop while it waits
    "fence, clear, never at rest",  # ... and the stop is answered FAILED
    "estop, fence, clear",          # the stop came on the SafetyStream, then its fence
    "fence, clear ending the local set",
    "clear, fence",                 # the clear was kept until its fence came (_on_fence)
])
def test_a_stop_is_answered_before_its_clear(adapter, case) -> None:
    a, b, stub = adapter, adapter._robot, adapter.stub
    threading.Thread(target=a.run_safety_stream, args=(stub,), daemon=True).start()
    _until(lambda: len(stub.streams) == 1)
    s = stub.streams[0]
    _until(lambda: any(m.WhichOneof("payload") == "local_stop" for m in list(s.sent)))
    time.sleep(0.1)  # the opening report is all sent
    start = len(s.sent)
    b.rest_at = float("inf") if "never" in case else time.monotonic() + 0.8
    clear = pb.EstopClear(estop_id="e1", leader_term=1)
    if "local set" in case:
        clear.ends_local_set = a._latch.set_local(ROBOT).set_id

    if case.startswith("clear"):
        _safety(s, estop_clear=clear)
        _until(lambda: "e1" in a._latch._r(ROBOT).pending)
        _fence(a, "e1")
    else:
        if case.startswith("estop"):
            _safety(s, estop=pb.Estop(estop_id="e1"))
            _until(lambda: a._stops_in_flight == 1)
        _fence(a, "e1")
        _until(lambda: a._stops_in_flight >= 1)
        _safety(s, estop_clear=clear)
    _until(lambda: "clear" in s.events("e1", start), timeout=4)
    _until(lambda: a._stops_in_flight == 0)
    time.sleep(0.2)  # anything sent late would land now

    stops = 2 if case.startswith("estop") else 1  # a fence on a stop already latched stops too
    want = ["final"] * stops + ["clear"] + (["local"] if "local set" in case else [])
    assert s.events("e1", start) == want
    final = [m.estop_ack.state for m in s.sent if m.WhichOneof("payload") == "estop_ack"
             and m.estop_ack.estop_id == "e1" and m.estop_ack.state != pb.ESTOP_STATE_STOPPING]
    assert set(final) == {pb.ESTOP_STATE_FAILED if "never" in case else pb.ESTOP_STATE_STOPPED}


def _clear_acks(stream, estop_id: str, start: int) -> list:
    return [m.estop_clear_ack for m in list(stream.sent)[start:]
            if m.WhichOneof("payload") == "estop_clear_ack"
            and m.estop_clear_ack.estop_id == estop_id]


@pytest.mark.parametrize("path", ["fence, clear", "clear, fence"])
def test_a_clear_ack_carries_the_time_the_latch_lifted(adapter, path) -> None:
    """The latch lifts when the clear arrives (its fence already in) or when the fence arrives
    (its clear kept until then); the ack goes about 0.8 s later, after the stop's answer, and
    carries the lift's time. An ack sent again carries the time it is sent."""
    a, b, stub = adapter, adapter._robot, adapter.stub
    threading.Thread(target=a.run_safety_stream, args=(stub,), daemon=True).start()
    _until(lambda: len(stub.streams) == 1)
    s = stub.streams[0]
    _until(lambda: any(m.WhichOneof("payload") == "local_stop" for m in list(s.sent)))
    time.sleep(0.1)  # the opening report is all sent
    start = len(s.sent)
    clear = pb.EstopClear(estop_id="e1", leader_term=1)
    if path == "fence, clear":
        b.rest_at = time.monotonic() + 1.6
        _fence(a, "e1")
        time.sleep(0.8)  # into the stop's wait
        lifted_ms = time.time() * 1000
        _safety(s, estop_clear=clear)
    else:
        _safety(s, estop_clear=clear)
        _until(lambda: "e1" in a._latch._r(ROBOT).pending)
        b.rest_at = time.monotonic() + 0.8
        lifted_ms = time.time() * 1000
        _fence(a, "e1")
    _until(lambda: _clear_acks(s, "e1", start), timeout=4)
    seen_ms = time.time() * 1000
    assert seen_ms - lifted_ms >= 500  # the ack really went late, after the stop's wait
    assert s.events("e1", start) == ["final", "clear"]  # and after the stop's answer
    first = _clear_acks(s, "e1", start)[0]
    assert abs(first.lifted_at_ms - lifted_ms) <= 100, first.lifted_at_ms - lifted_ms

    time.sleep(0.3)
    resent_ms = time.time() * 1000
    _safety(s, estop_clear=clear)  # the same clear again: lifted already
    _until(lambda: len(_clear_acks(s, "e1", start)) == 2)
    again = _clear_acks(s, "e1", start)[1]
    assert abs(again.lifted_at_ms - resent_ms) <= 100, again.lifted_at_ms - resent_ms


def test_a_clear_read_while_its_fence_is_being_commanded_is_answered_after_the_stop(
        adapter) -> None:
    """The fence and its clear come on two streams, read on two threads. The clear is read after
    the fence has marked the stop fenced, while the fence is still commanding the stop (slowed
    here to 0.3 s). The clear must still be answered after the stop's own answer."""
    a, b, stub = adapter, adapter._robot, adapter.stub
    threading.Thread(target=a.run_safety_stream, args=(stub,), daemon=True).start()
    _until(lambda: len(stub.streams) == 1)
    s = stub.streams[0]
    _until(lambda: any(m.WhichOneof("payload") == "local_stop" for m in list(s.sent)))
    time.sleep(0.1)  # the opening report is all sent
    start = len(s.sent)
    b.rest_at = time.monotonic() + 0.8
    real = b.command_estop

    def slow_estop(robot_id) -> None:
        time.sleep(0.3)  # the fence commanding its stop
        real(robot_id)
    b.command_estop = slow_estop
    fencing = threading.Thread(target=_fence, args=(a, "e1"), daemon=True)
    fencing.start()
    _until(lambda: "e1" in a._latch._r(ROBOT).fenced)  # fenced, the stop not yet commanded
    _safety(s, estop_clear=pb.EstopClear(estop_id="e1", leader_term=1))
    fencing.join(5)
    _until(lambda: "clear" in s.events("e1", start), timeout=4)
    _until(lambda: a._stops_in_flight == 0)
    time.sleep(0.2)
    assert s.events("e1", start) == ["final", "clear"]
