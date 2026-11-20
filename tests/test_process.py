# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Several vehicles in one process: routing by robot_id, RegisterAck and DiscoverAck paired by
order, a reconnect registering every vehicle again, and the answers for a robot not served."""

from __future__ import annotations

import json
import queue

import pytest
from conftest import mark_transport_live
from swarmada_sdk.safety import ESTOP_STOPPED

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import _Streams
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.process import FleetProcess
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

IDS = ["amr-1", "amr-2", "amr-3"]
_IDLE = json.dumps({"driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
                    "paused": False, "orderId": "", "nodeStates": [], "actionStates": []})


def _fleet(ids=IDS) -> FleetProcess:
    streams = _Streams()
    cores = []
    for rid in ids:
        b = mark_transport_live(Vda5050Binding(robot_id=rid, serial=rid))
        b.on_state(_IDLE)
        cores.append(Adapter(pb, None, b, rid, streams=streams))
    return FleetProcess(cores)


def _drain(q: queue.Queue) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _wire(core) -> list[str]:
    return ["order" if t.endswith("/order") else json.loads(p)["actions"][0]["actionType"]
            for t, p in core._robot.published]


def _hello(p: FleetProcess, version: str = "1.4.0") -> None:
    p.handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version=version)), None)


def _register_ack(p: FleetProcess, **kw) -> None:
    p.handle_control_plane(pb.ControlPlaneMessage(register_ack=pb.RegisterAck(**kw)), None)


@pytest.fixture(autouse=True)
def _no_telemetry(monkeypatch):
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)


def _registered_fleet() -> FleetProcess:
    p = _fleet()
    p.new_session()
    _hello(p)
    for _ in IDS:
        _register_ack(p, accepted=True, telemetry_interval_seconds=1)
    _drain(p._streams.outbox)
    return p


def test_one_process_one_set_of_streams() -> None:
    p = _fleet()
    assert all(c._process is p for c in p.cores)
    assert len({id(c._outbox) for c in p.cores}) == 1
    assert len({id(c._safety_outbox) for c in p.cores}) == 1


def test_a_lone_vehicle_is_a_process_of_one() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    a = Adapter(pb, None, b, "amr-1")
    assert a._process.cores == [a]


def test_a_robot_id_served_twice_is_refused() -> None:
    streams = _Streams()
    cores = [Adapter(pb, None, mark_transport_live(Vda5050Binding(robot_id="amr-1")), "amr-1",
                     streams=streams) for _ in range(2)]
    with pytest.raises(ValueError, match="served twice"):
        FleetProcess(cores)


def test_the_hello_registers_every_vehicle_in_order() -> None:
    p = _fleet()
    p.new_session()
    _hello(p)
    sent = _drain(p._streams.outbox)
    assert [m.register.robot_id for m in sent] == IDS


def test_each_register_ack_answers_the_oldest_register() -> None:
    p = _fleet()
    p.new_session()
    _hello(p)
    _drain(p._streams.outbox)
    _register_ack(p, accepted=True, telemetry_interval_seconds=1)
    _register_ack(p, accepted=False, rejection=pb.REGISTRATION_REJECTION_NOT_ADMITTED,
                  message="no Robot yet")
    _register_ack(p, accepted=True, telemetry_interval_seconds=2)
    assert [c._registered for c in p.cores] == [True, False, True]
    assert p.cores[2]._telemetry_interval == 2.0
    sent = _drain(p._streams.outbox)
    assert [m.discover.robot_id for m in sent if m.HasField("discover")] == ["amr-2"]


def test_a_discover_ack_answers_the_oldest_announcement() -> None:
    p = _fleet()
    p.new_session()
    _hello(p)
    for _ in IDS:
        _register_ack(p, accepted=False, rejection=pb.REGISTRATION_REJECTION_NOT_ADMITTED)
    p.handle_control_plane(pb.ControlPlaneMessage(discover_ack=pb.DiscoverAck(
        accepted=True, ttl_seconds=10)), None)
    assert [c._discover_ttl for c in p.cores] == [10, 0, 0]
    assert [c._discover_acked.is_set() for c in p.cores] == [True, False, False]


def test_an_ack_with_nothing_waiting_is_dropped_in_a_fleet() -> None:
    p = _fleet()
    p.new_session()
    _register_ack(p, accepted=True)
    assert not any(c._registered for c in p.cores)


def test_a_new_session_registers_every_vehicle_again() -> None:
    p = _registered_fleet()
    session = p._streams.session
    p.new_session()
    assert p._streams.session == session + 1
    assert not any(c._registered for c in p.cores)
    _hello(p)
    assert [m.register.robot_id for m in _drain(p._streams.outbox)] == IDS


def test_a_register_for_a_replaced_session_is_not_sent() -> None:
    p = _fleet()
    p.new_session()
    old = p._streams.session
    p.new_session()
    assert not p.send_register(p.cores[0], p._streams.outbox, old)
    assert _drain(p._streams.outbox) == []


def test_a_command_reaches_only_the_vehicle_it_names() -> None:
    p = _registered_fleet()
    p.handle_control_plane(pb.ControlPlaneMessage(command=pb.Command(
        command_id=1, robot_id="amr-2", leader_term=1, assign_action=pb.AssignAction(
            action_id="act-1", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=1,
            assignment_id="as-1", payload_json=b'{"x": 5, "y": 0}'))), None)
    assert [_wire(c) for c in p.cores] == [[], ["order"], []]
    assert [c._current_action for c in p.cores] == ["", "act-1", ""]
    results = [m.command_result for m in _drain(p._streams.outbox) if m.HasField("command_result")]
    assert [(r.robot_id, r.assign_action.accepted) for r in results] == [("amr-2", True)]


def test_an_estop_stops_only_the_vehicle_it_names(monkeypatch) -> None:
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation",
                        lambda robot, rid, since=None: ESTOP_STOPPED)
    p = _registered_fleet()
    p.handle_safety(pb.ControlPlaneSafetyMessage(robot_id="amr-3", estop=pb.Estop(estop_id="e1")))
    assert [_wire(c) for c in p.cores] == [[], [], ["startPause", "cancelOrder"]]
    acks = [m for m in _drain(p._streams.safety_outbox) if m.HasField("estop_ack")]
    assert [(m.robot_id, m.estop_ack.state) for m in acks] == [("amr-3", pb.ESTOP_STATE_STOPPED)]
    assert [bool(c._latch.latched_ids(c._robot_id)) for c in p.cores] == [False, False, True]


def test_a_fence_stops_only_the_vehicle_it_names(monkeypatch) -> None:
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation",
                        lambda robot, rid, since=None: ESTOP_STOPPED)
    p = _registered_fleet()
    p.handle_control_plane(pb.ControlPlaneMessage(command=pb.Command(
        command_id=1, robot_id="amr-1", leader_term=1, estop=pb.Estop(estop_id="e1"))), None)
    assert [_wire(c) for c in p.cores] == [["startPause", "cancelOrder"], [], []]


def test_a_command_for_a_robot_not_served_is_refused_naming_those_served() -> None:
    p = _registered_fleet()
    p.handle_control_plane(pb.ControlPlaneMessage(command=pb.Command(
        command_id=1, robot_id="amr-9", leader_term=1, assign_action=pb.AssignAction(
            action_id="act-9", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=1))), None)
    (m,) = _drain(p._streams.outbox)
    assert not m.command_result.assign_action.accepted
    assert "'amr-1', 'amr-2', 'amr-3'" in m.command_result.assign_action.message
    assert all(_wire(c) == [] for c in p.cores)


def test_an_estop_for_a_robot_not_served_stops_none_once_all_are_registered() -> None:
    p = _registered_fleet()
    p.handle_safety(pb.ControlPlaneSafetyMessage(robot_id="amr-9", estop=pb.Estop(estop_id="e9")))
    (m,) = _drain(p._streams.safety_outbox)
    assert m.estop_ack.state == pb.ESTOP_STATE_FAILED
    assert all(_wire(c) == [] for c in p.cores)


def test_an_estop_for_a_robot_not_served_stops_every_unregistered_vehicle() -> None:
    p = _fleet()
    p.new_session()
    _hello(p)
    _register_ack(p, accepted=True)
    _register_ack(p, accepted=False, rejection=pb.REGISTRATION_REJECTION_INVALID_ROBOT_ID)
    _register_ack(p, accepted=False, rejection=pb.REGISTRATION_REJECTION_INVALID_ROBOT_ID)
    _drain(p._streams.safety_outbox)
    p.handle_safety(pb.ControlPlaneSafetyMessage(robot_id="amr-9", estop=pb.Estop(estop_id="e9")))
    assert [_wire(c) for c in p.cores] == [[], ["startPause", "cancelOrder"],
                                           ["startPause", "cancelOrder"]]
    (m,) = [m for m in _drain(p._streams.safety_outbox) if m.HasField("estop_ack")]
    assert m.estop_ack.state == pb.ESTOP_STATE_FAILED


def test_an_outcome_ack_is_offered_to_every_vehicle(monkeypatch) -> None:
    p = _registered_fleet()
    asked = []
    for c in p.cores:
        monkeypatch.setattr(c._retained, "acknowledge",
                            lambda rid, aid, tok, _a=asked: _a.append(rid) or rid == "amr-2")
    p.handle_control_plane(pb.ControlPlaneMessage(outcome_ack=pb.ActionOutcomeAck(
        action_id="act-1", fencing_token=4)), None)
    assert asked == IDS


def test_the_safety_stream_reports_every_vehicle_latch() -> None:
    p = _registered_fleet()
    _drain(p._streams.safety_outbox)
    p.on_safety_stream_open()
    reports = [m.robot_id for m in _drain(p._streams.safety_outbox) if m.HasField("local_stop")]
    assert reports == IDS


def test_an_estop_for_a_robot_not_served_during_start_up_stops_every_vehicle() -> None:
    """Behaviour change, for review. Before any registration is accepted, an estop for a robot
    this process doesn't serve stops every vehicle in it: any of them may be the one the stop
    was meant for, under an id the adapter doesn't know it by. A lone vehicle did this already.
    The answer is still FAILED: rest is not confirmed for the robot the stop named."""
    p = _fleet()
    p.new_session()
    _hello(p)
    _drain(p._streams.outbox)
    p.handle_safety(pb.ControlPlaneSafetyMessage(robot_id="amr-9", estop=pb.Estop(estop_id="e9")))
    assert [_wire(c) for c in p.cores] == [["startPause", "cancelOrder"]] * len(IDS)
    (m,) = [m for m in _drain(p._streams.safety_outbox) if m.HasField("estop_ack")]
    assert (m.robot_id, m.estop_ack.state) == ("amr-9", pb.ESTOP_STATE_FAILED)
    assert "no registration is accepted" in m.estop_ack.message
    assert not any(c._latch.latched_ids(c._robot_id) for c in p.cores)
