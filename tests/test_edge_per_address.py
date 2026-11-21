# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The edge per address: one EdgeStream for each edge node, however many vehicles are served
through it.

* Its members are the vehicles whose latest accepted RegisterAck listed its address, and each
  member tees it a PositionFrame.
* Its estop names no robot: every member's local latch is set and its stop commanded, then all
  of them are awaited in parallel, and the stop is answered once: STOPPED only if every member
  confirmed, else FAILED."""

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

        nodes = self

        class EdgeServiceStub:
            def __init__(self, channel):
                self.addr = channel

            def EdgeStream(self, requests):
                addr = self.addr
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
