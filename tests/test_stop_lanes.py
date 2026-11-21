# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Stops on lanes: a stop is commanded on the reader that received it, and waits for rest on its
vehicle's lane, so no vehicle's stop waits behind another's.

* While a stop is in flight (commanded, not yet answered) no work is taken and no pause is
  lifted, at every contract version.
* The wait takes the vehicle's lock for each look, not for the whole wait, so a command for that
  vehicle isn't held behind it.
* A fault before the wait is answered FAILED, and the stop is uncounted once."""

from __future__ import annotations

import json
import queue
import threading
import time

import pytest
from conftest import mark_registered, mark_transport_live, reply_on_tick
from swarmada_sdk.safety import ESTOP_STOPPED

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import _Streams
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.process import FleetProcess
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"


def _state(driving=False, vx=0.0, order_id="", nodes=0) -> str:
    return json.dumps({
        "driving": driving, "velocity": {"vx": vx, "vy": 0.0, "omega": 0.0}, "paused": False,
        "orderId": order_id, "nodeStates": [{"nodeId": f"n{i}"} for i in range(nodes)],
        "actionStates": []})


VEHICLES = {"idle": _state(), "driving": _state(driving=True, vx=0.8, order_id="x", nodes=2)}


def _wire(b) -> list[str]:
    return ["order" if t.endswith("/order") else json.loads(p)["actions"][0]["actionType"]
            for t, p in b.published]


def _drain(q: queue.Queue) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _assign(a, action_id, token):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 5, "y": 0}'), res)
    return res.assign_action


def _resume(a):
    res = pb.CommandResult()
    a._on_resume(pb.ResumeCapabilities(), res)
    return res.resume


def _estop(robot_id: str, estop_id: str) -> pb.ControlPlaneSafetyMessage:
    return pb.ControlPlaneSafetyMessage(robot_id=robot_id, estop=pb.Estop(estop_id=estop_id))


def _until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


# ── finding 3: no work in the window between the stop and the interrupt ─────

def _window(monkeypatch, negotiated: str, vehicle: str, during):
    """An estop whose stop is commanded, with ``during`` run in the window before it takes the
    action lock: the ControlStream reader taking a command just then."""
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation",
                        lambda robot, rid, since=None, **_: ESTOP_STOPPED)
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.on_state(vehicle)
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    a._negotiated = negotiated
    seen = {}
    real = a._interrupt_current

    def interrupt_after_a_command_lands(why, **kw):
        if "r" not in seen:
            seen["wire_before"] = _wire(b)
            seen["r"] = during(a)
        return real(why, **kw)

    a._interrupt_current = interrupt_after_a_command_lands
    a._on_safety_message(_estop(ROBOT, "e1"))
    acks = [m.estop_ack for m in _drain(a._safety_outbox) if m.HasField("estop_ack")]
    return a, b, seen, acks


@pytest.mark.parametrize("negotiated", ["1.4.0", "1.3.0", ""])
@pytest.mark.parametrize("vehicle", list(VEHICLES))
def test_no_order_follows_an_estop_from_an_assignment_in_the_window(
        monkeypatch, negotiated, vehicle) -> None:
    a, b, seen, acks = _window(monkeypatch, negotiated, VEHICLES[vehicle],
                               lambda a: _assign(a, "act-2", 6))
    r = seen["r"]
    assert seen["wire_before"] == ["startPause", "cancelOrder"]
    assert _wire(b) == ["startPause", "cancelOrder"]  # no order, and no stopPause before one
    assert b._held is None
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    # The 1.4.0 latch answers first, naming the stop; below it, the stop in flight does.
    assert r.message == ("under a stop: e1" if negotiated == "1.4.0" else "under a stop: in flight")
    assert a._current_action == "" and a._interrupted == {}
    assert [s.state for s in acks if s.estop_id == "e1"][-1] == pb.ESTOP_STATE_STOPPED
    assert a._stops_in_flight == 0


@pytest.mark.parametrize("negotiated", ["1.3.0", ""])
def test_no_stop_pause_follows_an_estop_from_a_resume_in_the_window(monkeypatch, negotiated) -> None:
    _a, b, seen, _ = _window(monkeypatch, negotiated, VEHICLES["driving"], _resume)
    assert not seen["r"].resumed
    assert _wire(b) == ["startPause", "cancelOrder"]


def test_work_is_taken_again_once_the_stop_is_answered(monkeypatch) -> None:
    a, _b, _, acks = _window(monkeypatch, "1.3.0", VEHICLES["idle"], lambda a: None)
    assert [s.state for s in acks] == [pb.ESTOP_STATE_STOPPED]
    assert _assign(a, "act-3", 7).accepted


# ── finding 1 and 2: one vehicle's wait holds no other stop, and no command ─

def _two(monkeypatch) -> FleetProcess:
    """amr-1 confirms its stops; amr-2 never sends a state after one, so its wait runs the
    whole budget and ends FAILED."""
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    streams = _Streams()
    cores = []
    for rid in ("amr-1", "amr-2"):
        b = mark_transport_live(Vda5050Binding(robot_id=rid, serial=rid))
        b.on_state(_state())
        if rid == "amr-1":
            reply_on_tick(b, paused=True)
        cores.append(mark_registered(Adapter(pb, None, b, rid, streams=streams)))
    p = FleetProcess(cores)
    for c in cores:
        c._negotiated = "1.4.0"
    getattr(p, "start_lanes", lambda: None)()
    return p


def _fence(robot_id: str, estop_id: str) -> pb.ControlPlaneMessage:
    return pb.ControlPlaneMessage(command=pb.Command(
        command_id=1, robot_id=robot_id, leader_term=1, estop=pb.Estop(estop_id=estop_id)))


@pytest.mark.parametrize("via", ["estop", "fence"])
def test_a_stop_does_not_wait_behind_another_vehicles_stop(monkeypatch, via) -> None:
    p = _two(monkeypatch)
    a, b = p.cores

    def reader():  # one stream reader: amr-2's stop arrives first, then amr-1's
        for rid, eid in (("amr-2", "e2"), ("amr-1", "e1")):
            if via == "estop":
                p.handle_safety(_estop(rid, eid))
            else:
                p.handle_control_plane(_fence(rid, eid), None)

    t0 = time.monotonic()
    threading.Thread(target=reader, daemon=True).start()
    _until(lambda: _wire(a._robot) == ["startPause", "cancelOrder"], timeout=3)
    assert time.monotonic() - t0 < 0.5  # not after amr-2's 2 s wait
    acks = []
    _until(lambda: acks.extend(m.estop_ack for m in _drain(p._streams.safety_outbox)
                               if m.HasField("estop_ack") and m.estop_ack.message != "latched")
           or any(k.estop_id == "e1" for k in acks), timeout=3)
    assert [(k.estop_id, k.state) for k in acks] == [("e1", pb.ESTOP_STATE_STOPPED)]
    # amr-2's wait is still running; its answer is FAILED once the budget is spent.
    _until(lambda: acks.extend(m.estop_ack for m in _drain(p._streams.safety_outbox)
                               if m.HasField("estop_ack") and m.estop_ack.message != "latched")
           or any(k.estop_id == "e2" for k in acks), timeout=5)
    assert (acks[-1].estop_id, acks[-1].state) == ("e2", pb.ESTOP_STATE_FAILED)
    assert a._stops_in_flight == 0 and b._stops_in_flight == 0


def test_a_command_for_a_vehicle_does_not_wait_out_its_stop(monkeypatch) -> None:
    p = _two(monkeypatch)
    b = p.cores[1]
    threading.Thread(target=p.handle_safety, args=(_estop("amr-2", "e2"),), daemon=True).start()
    _until(lambda: _wire(b._robot) == ["startPause", "cancelOrder"])
    done = threading.Event()

    def pause():
        p.handle_control_plane(pb.ControlPlaneMessage(command=pb.Command(
            command_id=2, robot_id="amr-2", leader_term=1, pause=pb.PauseCapabilities())), None)
        done.set()

    t0 = time.monotonic()
    threading.Thread(target=pause, daemon=True).start()
    assert done.wait(3)
    assert time.monotonic() - t0 < 0.5  # one look at most, not the stop's 2 s


# ── R1: a fault before the wait ─────────────────────────────────────────────

def test_a_fault_before_the_wait_is_answered_failed_and_uncounted(monkeypatch, capsys) -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.on_state(_state())

    def broken(robot_id):
        raise TypeError("command_estop is broken")

    b.command_estop = broken
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    a._negotiated = "1.3.0"
    a._on_safety_message(_estop(ROBOT, "e1"))  # the reader goes on
    acks = [m.estop_ack for m in _drain(a._safety_outbox) if m.HasField("estop_ack")]
    assert [(k.estop_id, k.state) for k in acks] == [("e1", pb.ESTOP_STATE_FAILED)]
    assert a._stops_in_flight == 0
    assert "error: " in capsys.readouterr().err
