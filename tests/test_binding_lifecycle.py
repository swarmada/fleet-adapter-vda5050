# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The adapter drives the RobotBinding lifecycle, and asks whether it can see the robot
before it reports where the robot is (the same defect as in `fleet-adapter-ros2`,
in the third reference adapter).

`Vda5050Binding.start()` is what subscribes the `connection`, `state` and `factsheet`
topics and connects the broker, and it is what sets `_started` ("orders now publish;
before this they are cached only"). The adapter constructed the binding and never called
it, so `--comms vda5050` shipped inert: subscribed to nothing, connected to no broker,
publishing no orders — and not one call raised. `pose()` answered the origin,
`battery_percent()` answered 0, `phase_hint()` answered IDLE, and `is_stopped()` answered
False forever, which can never confirm a stop.

The root cause was that `RobotBinding` declared no lifecycle, so no call site knew there
was one to call — and the same silence covered the liveness half: VDA5050 declares
ONLINE / OFFLINE / CONNECTIONBROKEN on its own topic, the binding parsed it into
`connection_state()` / `is_online()`, and nothing read it. These tests pin the repair at
the seam rather than at the one line.

NO BROKER AND NO paho IS NEEDED for any of this, which is the point — the defect was in
what the adapter does before any transport exists, so the guard against its return must be
provable wherever the tests run.
"""

from __future__ import annotations

import pytest
from conftest import mark_transport_live, reply_on_tick
from swarmada_sdk.safety import ESTOP_FAILED, ESTOP_STOPPED

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import RobotBinding, SimulatedRobot
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

# ── the seam: the Protocol declares a lifecycle, and the adapter drives it ────

class _RecordingBinding:
    """Records the ORDER of lifecycle calls. Order is the assertion, not a detail:
    `spawn` is the first thing asked of the robot, and on a real binding nothing can be
    asked before the transport is up."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def start(self) -> None:
        self.calls.append("start")

    def stop(self) -> None:
        self.calls.append("stop")

    def spawn(self, robot_id: str) -> None:
        self.calls.append(f"spawn:{robot_id}")


def test_bring_up_starts_the_binding_before_spawning() -> None:
    binding = _RecordingBinding()
    Adapter(None, None, binding, "amr-1").bring_up_robot()
    assert binding.calls == ["start", "spawn:amr-1"]


def test_run_control_stream_brings_the_robot_up() -> None:
    """The gap `bring_up_robot()` alone leaves: a correct bring-up the shipped path never
    calls is exactly the defect again, one level up. `run_control_stream` is what `main()`
    invokes, so it is what must be pinned.

    Driven far enough to observe the bring-up and no further: the stub raises out of the
    first gRPC call, which happens after bring-up and proves the ordering too — the robot
    is up before the adapter says a word on the wire.
    """
    binding = _RecordingBinding()

    class _Boom(RuntimeError):
        pass

    class _Stub:
        def ControlStream(self, _requests):
            raise _Boom

    with pytest.raises(_Boom):
        Adapter(None, None, binding, "amr-1").run_control_stream(_Stub())

    assert binding.calls == ["start", "spawn:amr-1"]


def test_the_protocol_declares_the_lifecycle_and_the_liveness_predicate() -> None:
    """The seam itself. Without this the adapter's call site is an assumption about
    bindings rather than a requirement on them, which is the state this adapter shipped
    in — twice over, since the liveness signal it ignored was one it already computed."""
    assert "start" in RobotBinding.__protocol_attrs__
    assert "stop" in RobotBinding.__protocol_attrs__
    assert "transport_alive" in RobotBinding.__protocol_attrs__


def test_adapter_does_not_probe_for_a_lifecycle() -> None:
    """The rejected design, pinned as rejected.

    The alternative fix was for the adapter to DISCOVER the lifecycle
    (`if hasattr(robot, "start"): robot.start()`). Measured across all three reference
    adapters, it would in fact have worked on every binding as they exist today — all
    three already had a `start()`, and the two that shipped inert (`fleet-adapter-ros2`
    and this one) did so because nothing CALLED it, not because it was named something
    else.

    So it is rejected for what it ENCODES, not for a naming divergence anyone has
    observed: `if hasattr(...)` says "a binding with no lifecycle is fine", which is the
    assumption that was wrong, and it cannot tell an absent lifecycle from one spelled
    differently by a binding not yet written. The adapter must call `start()`
    unconditionally and fail loudly on a binding that has none.
    """

    class _NoLifecycle:
        def spawn(self, robot_id: str) -> None: ...

    with pytest.raises(AttributeError, match="start"):
        Adapter(None, None, _NoLifecycle(), "amr-1").bring_up_robot()


def test_a_failing_bring_up_is_fatal_not_swallowed() -> None:
    """`Vda5050Binding.start()` raises when paho is missing or the broker is unreachable.
    That must stop the run, not leave the adapter streaming plausible telemetry off a
    binding connected to nothing."""

    class _CannotConnect:
        def start(self) -> None:
            raise RuntimeError("MqttClient.connect() requires paho-mqtt")

        def spawn(self, robot_id: str) -> None:  # pragma: no cover - must not be reached
            raise AssertionError("spawn ran after start() failed")

    with pytest.raises(RuntimeError, match="paho-mqtt"):
        Adapter(None, None, _CannotConnect(), "amr-1").bring_up_robot()


def test_simulated_robot_satisfies_the_lifecycle_as_a_no_op() -> None:
    """The cost of putting the lifecycle in the contract: a binding with nothing to bring
    up must say so. Cheap, and deliberately visible — an absent lifecycle was not."""
    robot = SimulatedRobot()
    assert robot.start() is None
    assert robot.stop() is None
    assert robot.stop() is None          # idempotent
    assert robot.transport_alive() is True


def test_vda5050_teardown_is_idempotent_and_safe_before_start() -> None:
    """`stop()` did not exist at all before the lifecycle fix — not on the binding, not on the
    Protocol. It must be callable on a binding that was never started, because `main()`
    calls it in a `finally` that a failed bring-up also runs through."""
    b = Vda5050Binding(robot_id="amr-1")
    assert b.stop() is None
    assert b.stop() is None
    assert b.transport_alive() is False


def test_teardown_clears_liveness() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    assert b.transport_alive() is True
    b.stop()
    assert b.transport_alive() is False


# ── the detection half: an inert binding must not be able to hide ─────────────

def test_an_unstarted_binding_is_detected_on_tick() -> None:
    """`tick()` is the binding's only guaranteed main-thread callback, so it is where a
    transport that never existed is surfaced to a thread that is not itself broken. Before
    this, `tick()` was `pass`: 200 ticks, zero raises, on a binding talking to nobody."""
    b = Vda5050Binding(robot_id="amr-1")
    with pytest.raises(RuntimeError, match="NEVER STARTED"):
        b.tick(0.2)


def test_the_never_started_diagnosis_is_one_shot() -> None:
    """Deliberate, and the reason `transport_alive()` exists as well: an exception is a
    ONE-SHOT signal that the first thread to reach it consumes on behalf of every other.
    Pinned so that nobody 'fixes' it into a per-tick raise and concludes the predicate is
    redundant."""
    b = Vda5050Binding(robot_id="amr-1")
    with pytest.raises(RuntimeError, match="NEVER STARTED"):
        b.tick(0.2)
    assert b.tick(0.2) is None


def test_a_disconnected_robot_does_not_raise_on_tick() -> None:
    """The line between the two signals. A binding that started and then saw the robot go
    OFFLINE (or the MQTT last-will fire CONNECTIONBROKEN) is reporting a normal, recoverable
    robot state that VDA5050 has a field for — `transport_alive()` answers False for it every
    cycle, and turning it into an exception would be wrong. Never-started has no field, no
    message and no failed operation to raise from, which is why it gets the raise."""
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    b.on_connection('{"connectionState": "CONNECTIONBROKEN"}')
    assert b.tick(0.2) is None
    assert b.transport_alive() is False
    assert b.connection_state("amr-1") == profiles.CONN_BROKEN


def test_transport_alive_needs_both_the_session_and_the_robot() -> None:
    """Two independent failures, and `connection_state()` is what tells them apart: an
    un-started binding has no `connection` state because it never subscribed, which is not
    the same as a robot that subscribed and said OFFLINE."""
    b = Vda5050Binding(robot_id="amr-1")
    assert b.transport_alive() is False              # no session at all
    b.on_connection('{"connectionState": "ONLINE"}')
    assert b.transport_alive() is False, "ONLINE parsed off no subscription is not liveness"
    live = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    assert live.transport_alive() is True
    live.on_connection('{"connectionState": "OFFLINE"}')
    assert live.transport_alive() is False           # session up, robot gone


def test_phase_hint_does_not_report_IDLE_for_a_robot_it_cannot_see() -> None:
    """The accessor that was worse here than in ros2. `StateSnapshot.phase` defaults to
    IDLE, so the inert binding affirmatively reported the robot as parked and available
    while its own `connection_state()` said OFFLINE — not merely uninformative, but wrong
    in the reassuring direction."""
    b = Vda5050Binding(robot_id="amr-1")
    assert b.connection_state("amr-1") == profiles.CONN_OFFLINE
    assert b.phase_hint("amr-1") == ""


def test_an_inert_transport_still_answers_an_estop_with_FAILED() -> None:
    """The one place the raise must NOT escape.

    `run_safety_stream` and the edge loop call `confirm_estop` inside their response loops,
    and the safety stream is started BEFORE `bring_up_robot()` — so an estop can reach an
    un-started binding. If the raise escaped there, the thread would die and the control
    plane would get NO EstopAck. Silence on an estop is worse than FAILED: FAILED says
    "rest was not confirmed, escalate".

    FAILED is also the truthful answer. A binding subscribed to no `state` topic cannot
    confirm rest, and C5.3 forbids inferring it from silence.
    """
    a = Adapter(None, None, Vda5050Binding(robot_id="amr-1"), "amr-1")
    assert a._confirm_estop() == ESTOP_FAILED


def test_a_live_transport_still_reaches_the_real_confirm_estop() -> None:
    """The wrapper must not swallow the normal path: a binding that CAN confirm rest must
    still report STOPPED, or the guard above would have turned every estop into FAILED."""
    b = reply_on_tick(mark_transport_live(Vda5050Binding(robot_id="amr-1")), paused=True)
    a = Adapter(None, None, b, "amr-1")
    assert a._confirm_estop() == ESTOP_STOPPED


def test_the_wrapper_does_not_swallow_a_bug_in_the_safety_primitive() -> None:
    """Only the transport's RuntimeError is absorbed. A binding that fails some other way
    must still crash — a blanket `except Exception` on the estop path would hide exactly
    the class of defect the lifecycle fix is about."""

    class _Broken:
        def command_stop(self, robot_id: str) -> None:
            raise TypeError("command_stop is not callable like that")

        def command_estop(self, robot_id: str) -> None:
            raise TypeError("command_estop is not callable like that")

    a = Adapter(None, None, _Broken(), "amr-1")
    with pytest.raises(TypeError):
        a._confirm_estop()


def test_simulated_robot_ticks_forever_without_complaint() -> None:
    """The `$0` path must be untouched by all of the above. `SimulatedRobot` has no
    transport, its `start()` is a no-op, and its `tick()` advances the scenario clock —
    there is no un-started state for it to be in, and `--comms simulated` must not
    acquire one."""
    robot = SimulatedRobot()
    robot.start()
    robot.spawn("amr-1")
    for _ in range(200):
        robot.tick(0.2)
    assert robot.elapsed() == pytest.approx(40.0)
