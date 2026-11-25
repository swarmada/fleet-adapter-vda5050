# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The edge per address: one EdgeStream for each edge node, however many vehicles are served
through it.

* Its members are the vehicles whose latest accepted RegisterAck listed its address, and each
  member tees it a PositionFrame.
* Its estop names no robot: every member's local latch is set and its stop commanded, then all
  of them are awaited in parallel, and the stop is answered once: STOPPED only if every member
  confirmed, else FAILED.
* Its EdgeStream, when it ends, is dialled again for as long as a vehicle is served through it,
  and its members tee the new stream a PositionFrame. A link with no member left is dropped, so
  a later RegisterAck that lists it dials it afresh. Nothing is dialled again after a
  supersession. Each stream reads the outbox it was opened with, so a stream that failed at once
  takes nothing meant for the next."""

from __future__ import annotations

import json
import queue
import threading
import time

import pytest
from conftest import mark_transport_live, reply_on_tick

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050.adapter import _Streams
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.process import FleetProcess
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

E1, E2 = "edge-1:7000", "edge-2:7000"


def _state() -> str:
    return json.dumps({
        "driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0}, "paused": False,
        "orderId": "", "nodeStates": [], "actionStates": [],
        "agvPosition": {"x": 1.0, "y": 2.0, "theta": 0.5, "mapId": "m", "positionInitialized": True}})


class _EdgeNodes:
    """Edge nodes by address: what each was dialled, what each stream received, and a queue
    for what each sends the adapter."""

    def __init__(self) -> None:
        self.dialled: list[str] = []
        self.got: dict[str, list] = {}
        self.out: dict[str, queue.Queue] = {}
        # An address's next stream fails at once; called with that stream's requests.
        self.fail_next: dict[str, object] = {}

        nodes = self

        class EdgeServiceStub:
            def __init__(self, channel):
                self.addr = channel

            def EdgeStream(self, requests):
                addr = self.addr
                late = nodes.fail_next.pop(addr, None)
                if late is not None:
                    late(requests)

                    def failed():
                        raise RuntimeError("the edge node refused the stream")
                        yield  # a generator, which raises on its first read as a failed call does
                    return failed()
                got = nodes.got.setdefault(addr, [])
                out = nodes.out.setdefault(addr, queue.Queue())
                threading.Thread(target=lambda: got.extend(requests), daemon=True).start()

                def responses():
                    while (msg := out.get()) is not None:
                        yield msg
                return responses()

        self.grpc = type("_Grpc", (), {"EdgeServiceStub": EdgeServiceStub})

    def dial(self, addr: str) -> str:
        self.dialled.append(addr)
        return addr

    def end(self, addr: str) -> None:
        """The edge node ends the stream it has open."""
        self.out[addr].put(None)

    def estop(self, addr: str, estop_id: str) -> None:
        _until(lambda: addr in self.out)
        self.out[addr].put(pb.EdgeControlMessage(estop=pb.Estop(estop_id=estop_id)))

    def acks(self, addr: str) -> list:
        return [m.estop_ack for m in self.got.get(addr, []) if m.HasField("estop_ack")]

    def frames(self, addr: str) -> list[str]:
        return [m.position.robot_id for m in self.got.get(addr, []) if m.HasField("position")]


def _until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


def _wire(core) -> list[str]:
    return [json.loads(p)["actions"][0]["actionType"] for t, p in core._robot.published
            if not t.endswith("/order")]


@pytest.fixture(autouse=True)
def _no_telemetry(monkeypatch):
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)


def _fleet(confirms=("amr-1", "amr-2")) -> tuple[FleetProcess, _EdgeNodes]:
    """Two vehicles on one set of streams, with lanes started; a vehicle in ``confirms``
    confirms its stops, the others never send a state after one."""
    nodes = _EdgeNodes()
    streams = _Streams(dial=nodes.dial)
    cores = []
    for rid in ("amr-1", "amr-2"):
        b = mark_transport_live(Vda5050Binding(robot_id=rid, serial=rid))
        b.on_state(_state())
        if rid in confirms:
            reply_on_tick(b, paused=True)
        cores.append(Adapter(pb, nodes.grpc, b, rid, streams=streams))
    p = FleetProcess(cores)
    p.start_lanes()
    p.new_session()
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    return p, nodes


def _register(p: FleetProcess, *addresses: list[str], accepted: bool = True) -> None:
    """The RegisterAcks for the registers waiting, oldest first: the nth lists addresses[n]."""
    for addrs in addresses:
        ack = pb.RegisterAck(accepted=accepted, telemetry_interval_seconds=1,
                             edge_endpoints=[pb.EdgeEndpoint(zone="z", address=a) for a in addrs])
        if not accepted:
            ack.rejection = pb.REGISTRATION_REJECTION_NOT_ADMITTED
        p.handle_control_plane(pb.ControlPlaneMessage(register_ack=ack), None)


def test_one_edge_stream_per_address_and_a_frame_per_member() -> None:
    p, nodes = _fleet()
    _register(p, [E1], [E1, E2])
    _until(lambda: len(nodes.frames(E1)) == 2 and len(nodes.frames(E2)) == 1)
    assert sorted(nodes.dialled) == [E1, E2]  # E1 once, though both vehicles listed it
    assert nodes.frames(E1) == ["amr-1", "amr-2"]
    assert nodes.frames(E2) == ["amr-2"]
    # Registering again on a new session lists the same nodes: nothing more is dialled.
    p.new_session()
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _register(p, [E1], [E1, E2])
    time.sleep(0.1)
    assert sorted(nodes.dialled) == [E1, E2]


def test_an_edge_stop_stops_every_member_and_is_answered_once() -> None:
    p, nodes = _fleet()
    a, b = p.cores
    _register(p, [E1], [E1])
    nodes.estop(E1, "x1")
    _until(lambda: nodes.acks(E1))
    time.sleep(0.2)  # no second answer follows
    acks = nodes.acks(E1)
    assert [(k.estop_id, k.state) for k in acks] == [("x1", pb.ESTOP_STATE_STOPPED)]
    assert acks[0].confirmed_at_ms >= acks[0].stop_initiated_at > 0
    for c in (a, b):
        assert _wire(c) == ["startPause", "cancelOrder"]
        assert c._latch.local(c._robot_id).latched
        assert c._stops_in_flight == 0


def test_an_edge_stop_is_failed_unless_every_member_confirmed() -> None:
    p, nodes = _fleet(confirms=("amr-1",))
    _register(p, [E1], [E1])
    nodes.estop(E1, "x1")
    _until(lambda: nodes.acks(E1), timeout=5)
    (ack,) = nodes.acks(E1)
    assert ack.state == pb.ESTOP_STATE_FAILED
    assert ack.message == "not confirmed: 'amr-2'"
    assert not ack.confirmed_at_ms


def test_every_member_is_commanded_at_once_and_awaited_in_parallel() -> None:
    p, nodes = _fleet(confirms=())  # neither confirms: each wait runs its whole budget (~2 s)
    a, b = p.cores
    _register(p, [E1], [E1])
    t0 = time.monotonic()
    nodes.estop(E1, "x1")
    _until(lambda: _wire(a) and _wire(b), timeout=1)
    assert time.monotonic() - t0 < 0.5  # both commanded before any wait ends
    _until(lambda: nodes.acks(E1), timeout=5)
    assert time.monotonic() - t0 < 3.5  # one budget, not two in turn
    assert [k.state for k in nodes.acks(E1)] == [pb.ESTOP_STATE_FAILED]


def test_members_follow_the_latest_accepted_register_ack() -> None:
    p, nodes = _fleet()
    a, b = p.cores
    _register(p, [E1], [E1])
    _until(lambda: len(nodes.frames(E1)) == 2)
    # A new session: amr-1 is now served through E2 only; amr-2's register is refused, so
    # its earlier registration still decides.
    p.new_session()
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _register(p, [E2])
    _register(p, [], accepted=False)
    nodes.estop(E1, "x1")
    _until(lambda: nodes.acks(E1))
    assert [k.state for k in nodes.acks(E1)] == [pb.ESTOP_STATE_STOPPED]
    assert _wire(a) == []
    assert _wire(b) == ["startPause", "cancelOrder"]


def test_an_edge_stop_with_no_member_is_answered_failed() -> None:
    p, nodes = _fleet()
    a, b = p.cores
    _register(p, [E1], [])
    p.new_session()
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _register(p, [], [])
    nodes.estop(E1, "x1")
    _until(lambda: nodes.acks(E1))
    (ack,) = nodes.acks(E1)
    assert ack.state == pb.ESTOP_STATE_FAILED
    assert ack.message == "no vehicle is served through this edge node"
    assert _wire(a) == [] and _wire(b) == []


def test_an_ended_edge_stream_is_dialled_again_and_its_estops_still_reach_the_vehicles() -> None:
    p, nodes = _fleet()
    a, b = p.cores
    _register(p, [E1], [E1])
    _until(lambda: len(nodes.frames(E1)) == 2)
    nodes.end(E1)
    _until(lambda: nodes.dialled == [E1, E1])  # after its backoff (0.5 s)
    _until(lambda: len(nodes.frames(E1)) == 4)  # each member tees the new stream a frame
    nodes.estop(E1, "x2")
    _until(lambda: nodes.acks(E1))
    assert [(k.estop_id, k.state) for k in nodes.acks(E1)] == [("x2", pb.ESTOP_STATE_STOPPED)]
    for c in (a, b):
        assert _wire(c) == ["startPause", "cancelOrder"]


def test_an_ended_edge_stream_with_no_vehicle_is_dropped_and_dialled_afresh_when_listed() -> None:
    p, nodes = _fleet()
    a, _b = p.cores
    _register(p, [E1], [])
    _until(lambda: len(nodes.frames(E1)) == 1)
    p.new_session()  # amr-1 is no longer served through E1
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _register(p, [], [])
    nodes.end(E1)
    time.sleep(1.0)  # past a redial's backoff
    assert nodes.dialled == [E1]  # not dialled again: no vehicle is served through it
    assert E1 not in p._edges
    p.new_session()  # listed again: dialled afresh, and its stops reach amr-1
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _register(p, [E1], [])
    _until(lambda: nodes.dialled == [E1, E1])
    nodes.estop(E1, "x3")
    _until(lambda: nodes.acks(E1))
    assert [(k.estop_id, k.state) for k in nodes.acks(E1)] == [("x3", pb.ESTOP_STATE_STOPPED)]
    assert _wire(a) == ["startPause", "cancelOrder"]


def test_an_ended_edge_stream_is_not_dialled_again_after_a_supersession() -> None:
    p, nodes = _fleet()
    _register(p, [E1], [E1])
    _until(lambda: len(nodes.frames(E1)) == 2)
    p.cores[0]._superseded.set()  # recorded, as _on_superseded records it
    nodes.end(E1)
    time.sleep(1.0)  # past a redial's backoff
    assert nodes.dialled == [E1]
    assert E1 not in p._edges


def test_a_stream_that_failed_at_once_takes_nothing_from_the_next() -> None:
    """gRPC reads a call's first request on a thread of its own, which can run only after a call
    that failed at once has ended and its outbox been replaced. That read takes from the outbox
    the call was opened with, so the next stream still gets each member's PositionFrame and the
    estop's answer."""
    p, nodes = _fleet()
    a, b = p.cores
    _register(p, [E1], [E1])
    _until(lambda: len(nodes.frames(E1)) == 2)
    link = p._edges[E1]
    taken: list = []

    def late(requests) -> None:  # the failed call's request thread, after the outbox is replaced
        opened = link.outbox

        def read() -> None:
            _until(lambda: link.outbox is not opened)
            taken.append(next(requests))  # and dropped: the call has ended
        threading.Thread(target=read, daemon=True).start()

    nodes.fail_next[E1] = late
    nodes.end(E1)
    # Dialled again after 0.5 s, and that stream fails at once; dialled again 1 s later.
    _until(lambda: nodes.dialled == [E1, E1, E1])
    _until(lambda: taken)  # the failed stream's late read was made
    nodes.estop(E1, "x4")
    _until(lambda: nodes.acks(E1))
    assert sorted(nodes.frames(E1)[2:]) == ["amr-1", "amr-2"]  # the third stream's: both
    assert [(k.estop_id, k.state) for k in nodes.acks(E1)] == [("x4", pb.ESTOP_STATE_STOPPED)]
    for c in (a, b):
        assert _wire(c) == ["startPause", "cancelOrder"]
