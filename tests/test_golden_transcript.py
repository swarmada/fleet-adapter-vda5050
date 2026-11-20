# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Golden transcripts of one vehicle's traffic: what the adapter sends on ControlStream,
SafetyStream, EdgeStream and MQTT, for each scenario below, compared with tests/golden/.

They pin what the adapter says today, not whether it is right: the other tests do that. A
change that alters the traffic fails here until its transcript is regenerated and the diff
read. To regenerate: ``VDA_GOLDEN_UPDATE=1 pytest tests/test_golden_transcript.py``.

The harness (golden_harness.py) runs ``main()`` as shipped, with the single-vehicle flags, and
stands in only for the control plane, the edge node, the broker and the vehicle."""

from __future__ import annotations

import difflib
import json
import os
import pathlib

import pytest
from golden_harness import ROBOT, Run, kind

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod

GOLDEN = pathlib.Path(__file__).parent / "golden"
UPDATE = os.environ.get("VDA_GOLDEN_UPDATE") == "1"


def _check(name: str, text: str) -> None:
    path = GOLDEN / f"{name}.txt"
    if UPDATE:
        GOLDEN.mkdir(exist_ok=True)
        path.write_text(text)
        return
    assert path.exists(), f"{path} is missing: run with VDA_GOLDEN_UPDATE=1 and read it"
    want = path.read_text()
    if text != want:
        diff = "".join(difflib.unified_diff(want.splitlines(True), text.splitlines(True),
                                            "golden", "this run"))
        pytest.fail(f"the {name} transcript changed:\n{diff}")


def _assign(r: Run, command_id: int, action_id: str, token: int, x: float = 5.0,
            lease_ms: int = 0) -> None:
    r.command(command_id, assign_action=pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=json.dumps({"x": x, "y": 0}).encode(),
        lease_duration_ms=lease_ms, lease_generation=1 if lease_ms else None))


def _assign_and_drive(r: Run, command_id: int, action_id: str, token: int, **kw) -> None:
    r.t.step(f"assign {action_id}")
    _assign(r, command_id, action_id, token, **kw)
    r.t.expect("mqtt", "order")
    r.t.expect("control", "command_result")


@pytest.fixture
def run(monkeypatch, tmp_path):
    r = Run(monkeypatch, tmp_path)
    yield r
    if r.thread.is_alive():
        r.plane.stop()
        r.broker.stop()


def test_session(run) -> None:
    r, t = run, run.t
    r.register()
    t.step("heartbeat")
    r.plane.send("control", pb.ControlPlaneMessage(heartbeat=pb.HeartbeatRequest(robot_id=ROBOT)))
    t.expect("control", "heartbeat")
    t.step("telemetry, once the vehicle's position is known")
    t.sample_telemetry(lambda m: m.telemetry.HasField("position"))
    t.step("an outcome ack for an action this adapter keeps nothing for")
    r.plane.send("control", pb.ControlPlaneMessage(outcome_ack=pb.ActionOutcomeAck(
        action_id="act-none", fencing_token=1)))
    t.quiet("control")
    t.quiet("mqtt")
    _check("session", r.finish())


def test_assign_and_arrive(run) -> None:
    r, t = run, run.t
    r.register()
    _assign_and_drive(r, 1, "act-1", 7)
    t.step("the vehicle arrives")
    r.vehicle_arrives()
    t.expect("control", "action_status")
    t.step("the control plane reads the outcome")
    r.plane.send("control", pb.ControlPlaneMessage(outcome_ack=pb.ActionOutcomeAck(
        action_id="act-1", fencing_token=7)))
    t.quiet("control")
    t.quiet("mqtt")
    _check("assign_and_arrive", r.finish())


def test_renew_pause_resume_cancel(run) -> None:
    r, t = run, run.t
    r.register()
    _assign_and_drive(r, 1, "act-1", 7, lease_ms=30_000)
    t.step("renew the lease")
    r.command(2, renew_lease=pb.RenewActionLease(action_id="act-1", lease_generation=2,
                                                 lease_duration_ms=30_000))
    t.expect("control", "command_result")
    t.step("pause")
    r.command(3, pause=pb.PauseCapabilities(reason="test"))
    t.expect("mqtt", "startPause")
    t.expect("control", "command_result")
    t.step("resume")
    r.command(4, resume=pb.ResumeCapabilities(reason="test"))
    t.expect("mqtt", "stopPause")
    t.expect("control", "command_result")
    t.step("cancel")
    r.command(5, cancel_action=pb.CancelAction(action_id="act-1", reason="test"))
    t.expect("mqtt", "cancelOrder")
    t.expect("control", "command_result")
    t.quiet("control")
    t.quiet("mqtt")
    _check("renew_pause_resume_cancel", r.finish())


def test_safety_estop_and_clear(run) -> None:
    r, t = run, run.t
    r.register()
    _assign_and_drive(r, 1, "act-1", 7)
    t.step("an estop on the SafetyStream")
    r.plane.send("safety", pb.ControlPlaneSafetyMessage(robot_id=ROBOT, estop=pb.Estop(
        estop_id="e-1", reason="test", issued_by="test")))
    t.expect("mqtt", "startPause")
    t.expect("mqtt", "cancelOrder")
    t.expect("safety", "estop_ack")
    t.expect("control", "action_status")
    t.step("work under the stop is refused, and the latch reported")
    _assign(r, 2, "act-2", 8)
    t.expect("control", "command_result")
    t.expect("safety", "local_stop")
    t.step("the fence for the same stop on the ControlStream: the stop is commanded again")
    r.command(3, estop=pb.Estop(estop_id="e-1", reason="test", issued_by="test"))
    t.expect("mqtt", "cancelOrder")
    t.expect("safety", "estop_ack")
    t.step("the clear, which the fence lets take effect")
    r.plane.send("safety", pb.ControlPlaneSafetyMessage(robot_id=ROBOT, estop_clear=pb.EstopClear(
        estop_id="e-1", cleared_by="test", leader_term=1)))
    t.expect("safety", "estop_clear_ack")
    t.step("work after the clear: the stopPause goes before the order")
    _assign(r, 4, "act-3", 9)
    t.expect("mqtt", "order")
    t.expect("control", "command_result")
    t.quiet("control")
    t.quiet("safety")
    t.quiet("mqtt")
    _check("safety_estop_and_clear", r.finish())


def test_safety_estop_unconfirmed(run) -> None:
    r, t = run, run.t
    r.register()
    t.step("the vehicle stops answering; an estop")
    r.vehicle.silent = True
    r.plane.send("safety", pb.ControlPlaneSafetyMessage(robot_id=ROBOT, estop=pb.Estop(
        estop_id="e-1", reason="test", issued_by="test")))
    t.expect("mqtt", "cancelOrder")
    t.expect("safety", "estop_ack", timeout=8.0)
    t.quiet("safety")
    t.quiet("mqtt")
    _check("safety_estop_unconfirmed", r.finish())


def test_fence(run) -> None:
    r, t = run, run.t
    r.register()
    _assign_and_drive(r, 1, "act-1", 7)
    t.step("the fence: Command.estop on the ControlStream")
    r.command(2, estop=pb.Estop(estop_id="e-2", reason="test", issued_by="test"))
    t.expect("mqtt", "cancelOrder")
    t.expect("safety", "estop_ack")
    t.expect("control", "action_status")
    t.quiet("control")
    t.quiet("safety")
    t.quiet("mqtt")
    _check("fence", r.finish())


def test_edge_estop(run) -> None:
    r, t = run, run.t
    r.register(edge=True)
    t.step("the adapter dials the edge node and tees a position")
    r.plane.wait_stream("edge", 1)
    t.expect("edge", "position")
    t.step("an edge estop")
    r.plane.send("edge", pb.EdgeControlMessage(estop=pb.Estop(estop_id="edge-1", reason="test",
                                                              issued_by="edge")))
    t.expect("mqtt", "cancelOrder")
    t.expect("edge", "estop_ack")
    t.expect("safety", lambda m: kind(m) == "local_stop" and m.local_stop.latched)
    t.step("work under the local latch is refused, and the latch reported")
    _assign(r, 1, "act-1", 7)
    t.expect("control", "command_result")
    t.expect("safety", "local_stop")
    t.quiet("control")
    t.quiet("safety")
    t.quiet("mqtt")
    _check("edge_estop", r.finish())


def test_reconnect_keeps_the_running_action(run) -> None:
    r, t = run, run.t
    r.register()
    _assign_and_drive(r, 1, "act-1", 7)
    t.step("the ControlStream drops")
    r.plane.drop_control()
    r.register(session=2, authoritative_action_state=pb.RobotActionState(
        phase=pb.ROBOT_ACTION_PHASE_IN_PROGRESS, action_id="act-1", fencing_token=7))
    t.quiet("control")
    t.quiet("mqtt")
    _check("reconnect_keeps_the_running_action", r.finish())


def test_not_admitted_then_registered(run) -> None:
    r, t = run, run.t
    r.register(accepted=False, rejection=pb.REGISTRATION_REJECTION_NOT_ADMITTED,
               message="no Robot yet")
    t.expect("control", "discover")
    t.step("announced; the next register comes after min(ttl/2, 300) s")
    r.plane.send("control", pb.ControlPlaneMessage(discover_ack=pb.DiscoverAck(
        accepted=True, ttl_seconds=2)))
    t.expect("control", "register", timeout=6.0)
    r.plane.send("control", pb.ControlPlaneMessage(register_ack=pb.RegisterAck(
        accepted=True, telemetry_interval_seconds=1)))
    t.expect("control", "capabilities")
    t.quiet("control")
    _check("not_admitted_then_registered", r.finish())


def test_another_robot(run) -> None:
    r, t = run, run.t
    r.register()
    t.step("an assignment for another robot")
    r.command(1, robot_id="amr-9", assign_action=pb.AssignAction(
        action_id="act-9", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=1,
        assignment_id="as-1", payload_json=b'{"x": 1, "y": 0}'))
    t.expect("control", "command_result")
    t.step("a fence for another robot")
    r.command(2, robot_id="amr-9", estop=pb.Estop(estop_id="e-9", reason="test"))
    t.expect("safety", "estop_ack")
    t.step("a SafetyStream estop for another robot")
    r.plane.send("safety", pb.ControlPlaneSafetyMessage(robot_id="amr-9", estop=pb.Estop(
        estop_id="e-10", reason="test")))
    t.expect("safety", "estop_ack")
    t.step("a heartbeat for another robot is not answered")
    r.plane.send("control", pb.ControlPlaneMessage(heartbeat=pb.HeartbeatRequest(robot_id="amr-9")))
    t.quiet("control")
    t.quiet("safety")
    t.quiet("mqtt")
    _check("another_robot", r.finish())
