# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The simulated vehicle's reset: the robot-side act that lifts the local latch an edge stop set
(RFC-0001 #fleet-adapter-protocol-the-stop-latch). It is a local command on the vehicle, SIGUSR1
to the adapter's process, and nothing the control plane sends, and nothing on the adapter's
gRPC streams, can do it.

The latch is lifted only when the local latch is reset AND the EstopClear naming its set has
arrived, in either order. Until both, work stays refused."""

from __future__ import annotations

import signal

from conftest import mark_registered

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"


def _adapter() -> Adapter:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    a = Adapter(pb, None, robot, ROBOT)
    a._telemetry_started = True
    a._handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _drain(a._outbox)
    return mark_registered(a)


def _drain(q) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _accepted(a: Adapter, token: int) -> bool:
    r = pb.CommandResult()
    a._on_assign(pb.AssignAction(action_id=f"act-{token}", action_type=adapter_mod.ACTION_NAVIGATE,
                                 fencing_token=token, assignment_id=f"as-{token}",
                                 payload_json=b'{"x": 1, "y": 0}'), r)
    return r.assign_action.accepted


def _edge_stopped(a: Adapter) -> str:
    a._edge_stop("e-1", lambda ack: None)
    (rep,) = [m.local_stop for m in _drain(a._safety_outbox) if m.WhichOneof("payload") == "local_stop"]
    return rep.set_id


def _clear_naming(a: Adapter, set_id: str) -> None:
    # The stop and its fence, then the clear that names the local set.
    a._on_safety_message(pb.ControlPlaneSafetyMessage(robot_id=ROBOT, estop=pb.Estop(estop_id="s-1")))
    a._handle_command(pb.Command(command_id=1, robot_id=ROBOT, estop=pb.Estop(estop_id="s-1")))
    a._on_safety_message(pb.ControlPlaneSafetyMessage(robot_id=ROBOT, estop_clear=pb.EstopClear(
        estop_id="s-1", ends_local_set=set_id)))


def _reset_at_the_vehicle(a: Adapter) -> None:
    a._robot.request_reset()
    a._tick_robot()  # the telemetry loop's tick takes it


def test_the_reset_and_the_clear_together_lift_the_latch() -> None:
    a = _adapter()
    set_id = _edge_stopped(a)
    _reset_at_the_vehicle(a)
    reports = [m.local_stop for m in _drain(a._safety_outbox) if m.WhichOneof("payload") == "local_stop"]
    assert reports and not reports[-1].latched and reports[-1].set_id == set_id  # reset, set still waiting
    assert not _accepted(a, 5)
    _clear_naming(a, set_id)
    assert _accepted(a, 6)


def test_the_clear_first_then_the_reset_lifts_the_latch() -> None:
    a = _adapter()
    set_id = _edge_stopped(a)
    _clear_naming(a, set_id)
    assert not _accepted(a, 5)  # the clear alone ends the wait, not the latch
    _reset_at_the_vehicle(a)
    assert _accepted(a, 6)


def test_a_reset_without_an_edge_stop_changes_nothing() -> None:
    a = _adapter()
    _reset_at_the_vehicle(a)
    assert _accepted(a, 5)
    _reset_at_the_vehicle(a)  # taken once: a second tick finds no request
    assert not a._robot.take_reset()


def test_nothing_the_control_plane_sends_resets_the_local_latch() -> None:
    a = _adapter()
    set_id = _edge_stopped(a)
    for field in pb.Command.DESCRIPTOR.oneofs_by_name["command"].fields:
        cmd = pb.Command(command_id=1, robot_id=ROBOT)
        getattr(cmd, field.name).SetInParent()
        a._handle_command(cmd)
    for field in pb.ControlPlaneMessage.DESCRIPTOR.oneofs_by_name["payload"].fields:
        if field.name in ("hello_ack", "command"):
            continue
        msg = pb.ControlPlaneMessage()
        getattr(msg, field.name).SetInParent()
        a._handle_control_plane(msg, None)
    _clear_naming(a, set_id)
    a._tick_robot()
    assert a._latch.local(ROBOT).latched
    assert not _accepted(a, 50)


def test_main_puts_the_reset_on_sigusr1_for_the_simulated_vehicle(monkeypatch, capsys) -> None:
    handlers = {}
    monkeypatch.setattr(adapter_mod.signal, "signal", lambda sig, fn: handlers.setdefault(sig, fn))
    monkeypatch.setattr(Adapter, "run_safety_stream", lambda self, stub: None)
    made = []
    real_init = Adapter.__init__

    def init(self, *a, **kw):
        real_init(self, *a, **kw)
        made.append(self)

    monkeypatch.setattr(Adapter, "__init__", init)
    monkeypatch.setattr(Adapter, "run_control_stream", lambda self, stub: None)
    adapter_mod.main(["--insecure", "--scenario", "healthy-fleet"])
    assert signal.SIGUSR1 in handlers
    handlers[signal.SIGUSR1](signal.SIGUSR1, None)
    assert made[0]._robot.take_reset()
    assert "SIGUSR1" in capsys.readouterr().err
