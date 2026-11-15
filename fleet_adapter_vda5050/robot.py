# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The robot binding: how the VDA5050 Fleet Adapter talks to real robots.

`RobotBinding` is the seam between the Swarmada protocol and the robot side. This
adapter ships with `SimulatedRobot` so it is conformant out of the box with no MQTT
broker; the real binding is `Vda5050Binding` (MQTT/JSON), selected with
`--comms vda5050`.

SAFETY CONTRACT (never fake it): `is_stopped(robot_id)` MUST return True only when
the robot is *actually* at rest — confirmed from the robot, never inferred from a
timer or from silence. This is what makes emergency stop safe (CONFORMANCE.md C5).
"""

from __future__ import annotations

import math
import sys
import threading
from typing import Protocol

from fleet_adapter_vda5050 import task


class RobotBinding(Protocol):
    """The methods the adapter drives. Implemented by SimulatedRobot and by the
    VDA5050 binding (and, later, any ISO 21423 profile behind the same binding)."""

    # ── lifecycle ────────────────────────────────────────────────────────────
    # DECIDED (ported from fleet-adapter-ros2): the lifecycle belongs IN this contract,
    # and the adapter calls it unconditionally, rather than being discovered by the
    # adapter with `if hasattr(robot, "start")`.
    #
    # The decision is `fleet-adapter-ros2`'s, taken on the evidence measured across all
    # three reference adapters: ALL THREE BINDINGS ALREADY HAD A `start()`, two of the
    # three adapters never called it -- `fleet-adapter-ros2` and this one -- and all
    # three Protocols omitted it. The defect was never that bindings disagreed about
    # NAMING; it was that the contract did not require the call, and "Protocol silent"
    # tracked "call forgotten" almost perfectly.
    #
    # A `hasattr("start")` check would in fact have worked on all three bindings as they
    # exist today. It is rejected for what it ENCODES, not for a naming divergence
    # anyone has observed: `if hasattr(...)` literally says "a binding with no lifecycle
    # is fine", which is the assumption that was wrong, and it cannot tell an absent
    # lifecycle from one spelled differently by a binding not yet written.
    #
    # NOTHING ABOUT THIS ADAPTER ARGUED FOR DIVERGING. The one thing that is different
    # here is that the divergence would have been in the OTHER direction, and is worth
    # naming because it is the reason this repo's defect was the worse of the two:
    #
    #   `Vda5050Binding.start()` subscribes to the robot's `connection`, `state` and
    #   `factsheet` topics and connects the broker, and it is what sets `_started`
    #   ("orders now publish; before this they are cached only"). Nothing called it, so
    #   `--comms vda5050` built an object subscribed to nothing, connected to no broker,
    #   publishing no orders -- and NOT ONE CALL RAISED: pose() answered (0,0,0),
    #   battery_percent() answered 0, phase_hint() answered IDLE, and is_stopped()
    #   answered False forever, which can never confirm a stop. On the ros2 adapter that
    #   last one failed three estop conformance checks (C5.2, C8.3, C14.1) for a reason
    #   nobody could see; the same accessor here fails them the same way.
    #
    #   And unlike ros2, THIS binding already knew. VDA5050 has a protocol-level
    #   `connection` topic (ONLINE / OFFLINE / CONNECTIONBROKEN); the binding parsed it
    #   into `connection_state()` and `is_online()`, and the adapter read neither. The
    #   best liveness signal of the three adapters, computed and discarded. That is why
    #   `transport_alive()` below is declared here rather than left to each binding: a
    #   signal no caller is obliged to ask for is a signal nobody asks for.
    #
    # A binding with no lifecycle implements both as documented no-ops (see
    # `SimulatedRobot.start`), which is a visible, reviewable two lines; an absent
    # lifecycle was invisible.

    def start(self) -> None:
        """Bring the binding's transport up: open connections, subscribe, and start
        whatever background I/O the robot side needs. Called ONCE by the adapter before
        `spawn`, and before any command or telemetry read.

        MUST raise if the transport cannot be established. Returning normally is the
        adapter's only evidence that the binding is live, so a binding that cannot
        connect must not return quietly -- that is an inert binding reporting plausible
        values, the defect this method was added to prevent.

        A binding that needs no bring-up implements this as an explicit no-op."""

    def stop(self) -> None:
        """Tear the transport down: close connections and JOIN any background thread
        before the interpreter exits. MUST be idempotent, and MUST be safe to call when
        `start` was never called or raised.

        Declared alongside `start` deliberately. A lifecycle with a bring-up and no
        teardown is half a lifecycle: before the lifecycle fix this adapter had no `stop()` at
        all -- not on the binding, not on the Protocol -- so a `--comms vda5050` run left
        paho's network thread and the broker connection to the interpreter's exit."""

    def transport_alive(self) -> bool:
        """True only while this binding can still OBSERVE the robot — its transport is up
        and delivering, so the values the accessors below return are measurements rather
        than defaults or frozen copies.

        Declared here for the same reason as the lifecycle, and it is the other half of
        the same defect. The adapter must ask this BEFORE it publishes a
        `TelemetryPayload`, because a binding that cannot observe the robot still answers
        every accessor: `pose()` returns the origin, `battery_percent()` returns 0.
        Sending those is a **C6.2 violation** — *"never send a defaulted 0/false where the
        true reading is unknown"* — and the control plane cannot distinguish it from a
        robot genuinely parked at the origin on a flat battery.

        A predicate, deliberately, and not only an exception out of `tick()`. An
        exception is a ONE-SHOT signal, so whichever thread reaches it first consumes it
        on behalf of every other: an estop arriving before bring-up spends the binding's
        single `tick()` raise inside `confirm_estop`, and the telemetry loop that starts
        afterwards then sees a silent, apparently healthy binding and publishes defaults
        at full cadence forever. A predicate cannot be consumed, so it cannot be raced.

        A binding with no transport to lose returns True — `SimulatedRobot` does, because
        its readings are exactly as live as the simulation it models. Returning True is
        therefore the honest answer for it and NOT a stub."""

    # ── robot ────────────────────────────────────────────────────────────────

    def spawn(self, robot_id: str) -> None: ...

    def command_move(self, robot_id: str, x: float, y: float, yaw: float = 0.0) -> None:
        """Begin driving the robot toward a target pose (task dispatch)."""

    def command_stop(self, robot_id: str) -> None:
        """Command a safe stop. NOT a confirmation — that is `is_stopped`."""

    def command_estop(self, robot_id: str) -> None:
        """Command an emergency stop: halt at once and end the order, so the interrupted work
        cannot be resumed (VDA5050: startPause, then cancelOrder). NOT a confirmation — that
        is `is_stopped`. Declared rather than discovered, as the lifecycle is: the adapter
        calls it on every estop, whatever the binding."""

    def command_pause(self, robot_id: str) -> None:
        """Pause the robot's activity (VDA5050 startPause) — a REQUEST; confirmation
        that it has halted comes only from `is_stopped`. Keeps the order so it can
        resume, unlike command_stop which aborts it."""

    def command_resume(self, robot_id: str) -> None:
        """Resume a paused robot (VDA5050 stopPause)."""

    def paused_at_vehicle(self, robot_id: str) -> bool:
        """True when the robot reports a pause this binding did not set (a button on the
        vehicle, say): the adapter then refuses work, ROBOT_UNAVAILABLE. A binding whose robot
        has no pause of its own answers False."""

    def goal_refusal(self, robot_id: str, x: float, y: float) -> str:
        """Why the binding cannot route an order to a goal at (x, y), or "" when it can: the
        adapter then refuses the assignment, INVALID_ACTION, with this text. Optional: a
        binding without it routes nothing, and every goal can be sent (VDA5050: a goal off
        the lanes of a route graph, vda5050_binding.py)."""

    def route_unavailable(self, robot_id: str) -> str:
        """Why the binding can route no order now, or "" when it can: the adapter then
        refuses work, ROBOT_UNAVAILABLE. Optional, as goal_refusal is (VDA5050: the vehicle's
        position is unknown while a route graph is loaded)."""

    def tick(self, dt: float) -> None:
        """Advance any internal model by dt seconds (no-op for a real robot)."""

    def is_stopped(self, robot_id: str) -> bool:
        """CONFIRMED at rest — the ground truth the adapter waits on before it
        reports STOPPED. MUST reflect the real robot, never a timeout."""

    def state_seq(self, robot_id: str) -> int:
        """How many new state reports the binding has received from the robot. The adapter
        reads it once an estop is sent, and the SDK confirms the stop only from a report
        counted after that (swarmada_sdk.safety.await_estop_confirmation). An int that never
        falls; the state `is_stopped` reads is stored before the count rises. Optional: a
        binding whose state is current after every `tick()` declares
        `tick_keeps_state_current = True` instead (SimulatedRobot)."""

    def pose(self, robot_id: str) -> tuple[float, float, int] | None:
        """(x, y, floor) in the zone frame; None while the binding does not know it, and the
        adapter then sends no position (C6.2: never a defaulted origin)."""

    def reported_pose(self, robot_id: str) -> tuple[float, float, float | None, int] | None:
        """Optional: (x, y, yaw, floor), read as one, with yaw None when the robot gave no
        heading; None while unknown. When a binding has it, the adapter sends the heading
        too (VDA5050: a state's or a visualization's agvPosition)."""

    def battery_percent(self, robot_id: str) -> int | None:
        """0–100; None while unknown, and the adapter then sends no battery (C6.2)."""


class SimulatedRobot:
    """A minimal simulated robot with a REAL confirmed stop, so the adapter passes
    conformance immediately with no MQTT broker (`--comms simulated`, the default).

    Optionally scenario-driven: pass a scenario ``engine`` (see
    ``fleet_adapter_vda5050.scenarios``) to make the simulated battery, hardware,
    comms, and estop follow a named preset — the same presets as the in-tree
    simulator. With no engine it behaves exactly as the $0 skeleton, so conformance
    is unchanged. Keep the confirmed-stop contract — return `is_stopped=True` only
    when the robot has actually halted.
    """

    # How fast the simulated vehicle drives to its target: the speed the conformance fixture
    # assumes when it sizes a goal that keeps an action in flight.
    SPEED_MPS = 0.5

    # Its state is the simulation, current after every tick(): the SDK's estop wait reads
    # is_stopped() as it is and needs no state_seq (swarmada_sdk.safety).
    tick_keeps_state_current = True

    def __init__(self, engine=None) -> None:
        self._moving: dict[str, bool] = {}
        self._pos: dict[str, tuple[float, float]] = {}
        self._target: dict[str, tuple[float, float] | None] = {}  # the order's target; None: no order
        # A person's reset at the vehicle, asked for locally (SIGUSR1, see adapter.main) and taken
        # by the adapter's next tick. Nothing on the wire sets it.
        self._reset_requested = threading.Event()
        self._engine = engine
        self._elapsed = 0.0                 # seconds since spawn, advanced by tick()
        self._hw_prev: dict[str, str] = {}  # last-reported hardware status (delta base)
        self._order: dict[str, str] = {}    # the action behind the current order, for the log lines
        self._logged: dict[str, tuple] = {} # robot -> (seen, at): the last VDA_STATE line's values and time

    # ── RobotBinding contract (safety-critical; scenario never weakens it) ───────

    def start(self) -> None:
        """No-op, DELIBERATELY: a simulated robot has no transport to bring up.

        This is not an oversight and must not be deleted as dead code. `RobotBinding`
        declares a lifecycle (see the Protocol above) so that every binding answers the
        question "what does bring-up mean here?" in writing. For this one the answer is
        "nothing", and writing it down is what lets the adapter call `start()`
        unconditionally on whatever binding it was handed -- which is the fix for
        the defect where an adapter that could not call a lifecycle it did not know about
        shipped a `--comms vda5050` that talked to no broker."""

    def stop(self) -> None:
        """No-op, for the same reason as `start`: nothing to tear down. Idempotent
        trivially."""

    def transport_alive(self) -> bool:
        """Always True, and that is the honest answer rather than a stub: this robot's
        readings come from an in-process model, so they are exactly as live as the
        simulation. There is no transport that can die and leave the accessors returning
        frozen copies, which is the condition the adapter is asking about."""
        return True

    def spawn(self, robot_id: str) -> None:
        self._moving[robot_id] = False
        self._pos[robot_id] = (0.0, 0.0)
        self._target[robot_id] = None

    def command_move(self, robot_id: str, x: float, y: float, yaw: float = 0.0) -> None:
        # Driven there by tick(), at SPEED_MPS; yaw ignored by the sim, a real binding honours it.
        self._moving[robot_id] = True
        self._target[robot_id] = (x, y)

    def command_stop(self, robot_id: str) -> None:
        # The order ends where the robot is; a real robot confirms this via is_stopped.
        self._moving[robot_id] = False
        self._target[robot_id] = None

    def command_estop(self, robot_id: str) -> None:
        # The simulated vehicle stops at once, so its emergency stop is its stop.
        self.command_stop(robot_id)

    def command_pause(self, robot_id: str) -> None:
        self._moving[robot_id] = False  # startPause halts; is_stopped confirms it

    def command_resume(self, robot_id: str) -> None:
        self._moving[robot_id] = True   # stopPause resumes the held order

    def paused_at_vehicle(self, robot_id: str) -> bool:
        return False  # the simulated vehicle has no pause button

    def tick(self, dt: float) -> None:
        self._elapsed += dt  # advance the scenario clock
        for robot_id, moving in self._moving.items():
            target = self._target.get(robot_id)
            if not moving or target is None:
                continue
            (x, y), (tx, ty) = self._pos[robot_id], target
            dist = math.hypot(tx - x, ty - y)
            step = self.SPEED_MPS * dt
            if dist <= step:
                self._pos[robot_id] = target
                self._moving[robot_id] = False  # arrived: at rest, the order complete
            else:
                self._pos[robot_id] = (x + (tx - x) * step / dist, y + (ty - y) * step / dist)
        for robot_id in self._moving:
            self._log_state(robot_id)

    def note_order(self, robot_id: str, action: str) -> None:
        """Log an order the vehicle has taken. Log only: changes nothing the model does."""
        self._order[robot_id] = action
        x, y = self._target.get(robot_id) or self._pos.get(robot_id, (0.0, 0.0))
        print(f"VDA_ORDER robot={robot_id} order={action} x={x:.2f} y={y:.2f}", file=sys.stderr, flush=True)

    def _log_state(self, robot_id: str) -> None:
        """VDA_STATE: once a second while driving, and at each change. These lines stand for the
        model's values; they are not VDA5050 messages. `stopped` is the value the adapter uses."""
        stopped = self.is_stopped(robot_id)
        driving = bool(self._moving.get(robot_id))
        order = self._order.get(robot_id, "-") if self._target.get(robot_id) is not None else "-"
        seen = (order, driving, stopped)
        last = self._logged.get(robot_id)
        if last is not None and last[0] == seen and not (driving and self._elapsed - last[1] >= 1.0):
            return
        self._logged[robot_id] = (seen, self._elapsed)
        x, y = self._pos[robot_id]
        print(f"VDA_STATE robot={robot_id} order={order} driving={str(driving).lower()} x={x:.2f} y={y:.2f} "
              f"speed={self.SPEED_MPS if driving else 0.0:.2f} stopped={str(stopped).lower()}",
              file=sys.stderr, flush=True)

    def request_reset(self) -> None:
        """A reset at the vehicle: the robot-side half of lifting the local latch an edge stop
        set. Safe to call from a signal handler: it only raises a flag."""
        self._reset_requested.set()

    def take_reset(self) -> bool:
        """True once per reset asked for since the last call."""
        if self._reset_requested.is_set():
            self._reset_requested.clear()
            return True
        return False

    def task_state(self, robot_id: str) -> str:
        """The order's state: RUNNING on the way, SUCCEEDED once at the target, "" with no
        order (none given, or a stop ended it)."""
        target = self._target.get(robot_id)
        if target is None:
            return ""
        if self._moving.get(robot_id) or self._pos.get(robot_id) != target:
            return task.TASK_RUNNING
        return task.TASK_SUCCEEDED

    def is_stopped(self, robot_id: str) -> bool:
        return not self._moving.get(robot_id, False)

    def pose(self, robot_id: str) -> tuple[float, float, int]:
        x, y = self._pos.get(robot_id, (0.0, 0.0))
        return (x, y, 0)

    def battery_percent(self, robot_id: str) -> int:
        if self._engine is not None:
            curve = self._engine.battery_at(self._elapsed)  # battery-edge
            if curve is not None:
                return curve
        return 100

    # ── Scenario state the adapter maps onto the wire protocol ───────────────────

    def elapsed(self) -> float:
        return self._elapsed

    def hardware_delta(self) -> list:
        """Hardware components whose status changed since the last call (all on the
        first) — the delta-compressed hardware-fault stream. Empty with no engine."""
        if self._engine is None:
            return []
        changed = self._engine.hardware_delta(self._elapsed, self._hw_prev)
        for hs in changed:
            self._hw_prev[hs.name] = hs.status
        return changed

    def reset_hardware_baseline(self) -> None:
        """Forget which components were already reported, so the NEXT hardware_delta()
        returns every component again. Used to satisfy C6.1: the first TelemetryPayload
        after a (re)connect must be a full snapshot, never a resumed delta."""
        self._hw_prev.clear()

    def manifest_hardware(self) -> list:
        """The full hardware manifest for the initial CapabilitiesSnapshot."""
        return self._engine.hardware_at(0.0) if self._engine is not None else []

    def telemetry_suppressed(self) -> bool:
        return self._engine is not None and self._engine.telemetry_gap_at(self._elapsed)

    def stream_drop_window(self):
        return self._engine.stream_drop_window() if self._engine is not None else None

    def stream_down(self) -> bool:
        return self._engine is not None and self._engine.stream_down_at(self._elapsed)

    def estop_due(self) -> bool:
        return self._engine is not None and self._engine.estop_due(self._elapsed)
