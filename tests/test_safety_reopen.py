# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A stop's answer reaches the SafetyStream open when it is sent.

A stop is commanded on the reader and answered from the vehicle's lane once rest is confirmed,
up to 2 s later. If the SafetyStream drops and reopens meanwhile, the stream that reopened is
told the stop is latched, STOPPING; its final answer, STOPPED or FAILED, must come on that
stream too, not on the one that ended."""

from __future__ import annotations

import json
import queue
import threading
import time

import grpc
import pytest
from conftest import mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"


class _Dropped(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "connection reset"


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
        if msg is None:
            raise _Dropped()
        if msg == "END":
            raise StopIteration
        return msg

    def acks(self, estop_id: str) -> list:
        return [m.estop_ack for m in list(self.sent)
                if m.HasField("estop_ack") and m.estop_ack.estop_id == estop_id]


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


@pytest.mark.parametrize("via", ["estop", "fence"])
@pytest.mark.parametrize("confirmed", [True, False])
def test_a_stop_answered_after_the_safety_stream_reopened_is_answered_on_the_new_stream(
        adapter, via, confirmed) -> None:
    a, b, stub = adapter, adapter._robot, adapter.stub
    threading.Thread(target=a.run_safety_stream, args=(stub,), daemon=True).start()
    _until(lambda: len(stub.streams) == 1)
    first = stub.streams[0]
    t0 = time.monotonic()
    b.rest_at = t0 + 1.2 if confirmed else float("inf")  # after the reopen, or never
    if via == "estop":
        first.inbox.put(pb.ControlPlaneSafetyMessage(robot_id=ROBOT,
                                                     estop=pb.Estop(estop_id="e1")))
    else:
        a._handle_command(pb.Command(command_id=1, robot_id=ROBOT, leader_term=1,
                                     estop=pb.Estop(estop_id="e1")))
    _until(lambda: a._stops_in_flight == 1)
    first.inbox.put(None)  # the SafetyStream drops during the stop's wait
    _until(lambda: len(stub.streams) == 2)  # and reopens 0.5 s later
    second = stub.streams[1]
    _until(lambda: any(k.message != "latched" for k in second.acks("e1")), timeout=3)
    final = [k for k in second.acks("e1") if k.message != "latched"]
    assert [k.state for k in final] == [
        pb.ESTOP_STATE_STOPPED if confirmed else pb.ESTOP_STATE_FAILED]
    assert time.monotonic() - t0 < 2.6  # within its 2 s budget, give or take the looks
    assert [k.message for k in first.acks("e1")] in ([], ["latched"])  # nothing final on the old
    assert [k.state for k in second.acks("e1") if k.message == "latched"] == [
        pb.ESTOP_STATE_STOPPING]  # the reopened stream was told it was latched, STOPPING
