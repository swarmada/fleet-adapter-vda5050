# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 Fleet Adapter — a safety-complete Swarmada Fleet Adapter.

Feature-basic but safety-complete (ADR-0005): the CONFORMANCE.md safety MUSTs are
wired from the audited `swarmada_sdk` primitives; optional commands are declined
with `unsupported = true` (C7). The robot side is a pluggable comms profile
(`--comms vda5050`, MQTT/JSON) behind the `RobotBinding` seam; `--comms simulated`
(default) passes the handshake (C1) at $0 with no MQTT broker.

Run:  python -m fleet_adapter_vda5050.adapter --endpoint localhost:9090
"""

from __future__ import annotations

import argparse
import os
import pathlib
import queue
import signal
import sys
import threading
import time
from collections.abc import Iterator

import grpc

# Make the package importable when run directly as a script (a real install
# `pip install -e .` makes this unnecessary).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from swarmada_sdk.safety import (
    ESTOP_FAILED,
    ESTOP_STOPPED,
    FenceDecision,
    FenceGuard,
    LeaseMonitor,
    await_estop_confirmation,
)

from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.latch import StopLatch
from fleet_adapter_vda5050.retained import Outcome, RetainedOutcomes
from fleet_adapter_vda5050.robot import SimulatedRobot

_TELEMETRY_INTERVAL = 0.2
# The robot is ticked by the time that passed since its last tick, at most this much at once,
# so a long gap (a suspended host) is not driven through in one step.
_MAX_TICK_S = 5.0

# The provisioning loop (C2.1; RFC-0001 #fleet-adapter-protocol-connection-lifecycle). A register
# refused NOT_ADMITTED is answered by announcing the robot (discover), and register is sent again
# every interval I until it is accepted. The DiscoverAck's ttl_seconds sets I = min(ttl/2, 300);
# with no TTL, I is 60 s. The loop waits this long for the DiscoverAck before it picks I.
_PROVISIONING_INTERVAL_S = 60.0
_DISCOVER_ACK_WAIT_S = 5.0


def _provisioning_interval(ttl_seconds: int) -> float:
    return min(ttl_seconds / 2.0, 300.0) if ttl_seconds > 0 else _PROVISIONING_INTERVAL_S


# The fleet-adapter CONTRACT version this adapter implements (ADR-0032). Distinct from
# adapter_version (this build) and protocol_version (the wire package identity, not a semver, which
# is why it cannot express compatibility). A control plane refuses REGISTRATION for an adapter that
# reports nothing or reports out of range, so this is not optional -- telemetry, heartbeat and
# emergency stop keep working either way. Bump it only when this adapter is re-qualified against a
# new contract (`make conformance`), and update the adapters/REGISTRY.md row with it.
#
# 1.4.0 is the stop path (RFC-0001 #fleet-adapter-protocol-the-stop-latch): a stop latches its
# ID and work is refused ROBOT_UNAVAILABLE until its EstopClear lifts it (latch.py); a command or
# a clear from a deposed leader is not acted on; work a stop interrupts is never continued; and
# the last action's terminal outcome is kept until an ActionOutcomeAck names it (retained.py).
# Each binds only a connection that negotiated 1.4.0; an older control plane sees 1.3.0's
# behaviour. An edge stop sets the robot's local latch: the simulated vehicle's reset (SIGUSR1, a
# local command at the vehicle) lifts it, and work resumes once the EstopClear that ends its set
# has arrived too. The reference control plane at v0.4 sends no such clear (RFC-0001, the
# edge-stop report's Not-met note), so there, after an edge stop, work stays refused.
CONTRACT_VERSION = "1.4.0"
_TERMINAL = (task_map.TASK_SUCCEEDED, task_map.TASK_FAILED, task_map.TASK_CANCELLED)

# The Command arms this adapter implements that act on the robot (its motion, its lease, a hold)
# or attest something about it (zone_constraints). Each is refused, before anything else runs,
# when Command.robot_id is not the robot this adapter serves (`_refuse_unserved`). The fence
# (`estop`) is answered FAILED on the SafetyStream (`_unserved_estop`). `scan` is not listed:
# it moves nothing and its snapshot names this adapter's own robot, as the simulation adapter
# answers it. Arms this adapter declines are declined (`unsupported`) whatever robot they name.
_ROBOT_ACTING_ARMS = frozenset({
    "assign_action", "renew_lease", "cancel_action", "pause", "resume", "zone_constraints",
})


def _at_least_14(version: str) -> bool:
    try:
        major, minor = (int(x) for x in version.split(".")[:2])
    except ValueError:
        return False
    return (major, minor) >= (1, 4)


def _now_ms() -> int:
    return int(time.time() * 1000)

# The action catalog this adapter serves (CapabilitiesSnapshot.supported_actions). A VDA5050
# order drives the vehicle to a node, so that is the one verb it offers; the payload keys are
# the ones task.goal_from_assign reads. An assignment of any other type -- or of no type -- is
# declined INVALID_ACTION, never run as the nearest thing the vehicle can do (C19.1, C19.3).
ACTION_NAVIGATE = "Navigate"
_NAVIGATE_PARAMS = (("x", "m", "number"), ("y", "m", "number"),
                    ("theta", "rad", "number"), ("mapId", "", "string"))

# Operating-constraint subjects this adapter can honour (FA-R5). None: nothing on a VDA5050
# order can switch a camera off or cap a speed per zone, so an assignment carrying ANY
# constraint is declined OPERATING_CONSTRAINT_UNMET with the subjects named (C17.1). A subject
# the adapter does not recognise lands in the same refusal (C17.2): an unrecognised rule is
# not an absent one.
HONOURED_SUBJECTS: frozenset[str] = frozenset()


def supported_actions(pb) -> list:
    return [pb.SupportedAction(
        action_type=ACTION_NAVIGATE,
        params=[pb.ActionParam(name=n, unit=u, kind=k, required=False)
                for n, u, k in _NAVIGATE_PARAMS],
        description="VDA5050 order to a node; optional x/y/theta target and mapId, "
                    "else the destination label")]


def unhonourable_subjects(constraints) -> list[str]:
    return [c.subject for c in constraints if c.subject not in HONOURED_SUBJECTS]


def zone_constraints_ack(pb, push) -> object:
    """Answer the zone_constraints push (FA-R7): the operating rules in force where the
    robot now is. This adapter honours no subject, so the answer is a receipt that says so.

    * An empty set is permission (a robot that has left a restricted region), acknowledged
      with nothing named -- never read as a lockout.
    * A set carrying rules is acknowledged with every subject named unhonourable. The
      robot is already in the zone; naming what it cannot comply with is all it can do, and
      the control plane can report it but not prevent it.
    * A set with unresolved zones is incomplete, and the claim fails closed: nothing here
      acknowledges having applied rules nobody could state. The zones are named, and the
      subjects that were known are still listed. The robot is not stopped: the push
      arrives after the crossing and does not make a zone safe (ADR-0059).
    """
    subjects = unhonourable_subjects(push.operating_constraints)
    unresolved = list(push.unresolved_zones)
    if unresolved:
        return pb.ZoneConstraintsAck(
            acknowledged=False, unhonourable_subjects=subjects,
            message="unresolved zone(s) " + ", ".join(unresolved) + ": their rules are "
                    "unknown, so this adapter cannot claim to have applied them")
    return pb.ZoneConstraintsAck(
        acknowledged=True, unhonourable_subjects=subjects,
        message=("this adapter honours none of these subjects; the robot is in a zone it "
                 "cannot comply with") if subjects else "")


class fleet_adapter_vda5050Adapter:
    def __init__(self, pb, pb_grpc, robot, robot_id: str, state_dir=None, dial=None, namespace: str = "") -> None:
        # The namespace in AdapterHello: empty takes it from the client certificate, the control
        # plane's binding. A name that differs from the certificate's is refused.
        self._namespace = namespace
        self._pb = pb
        self._pb_grpc = pb_grpc
        self._robot = robot
        self._robot_lock = threading.Lock()
        # The action the robot holds, and what is kept about it (_current_action, its token and
        # last state, _action_tokens, _interrupted), changes only under this lock. Several
        # threads end an action -- the SafetyStream and the EdgeStream (a stop), the telemetry
        # loop (lease expiry, a finished order, the drill), the ControlStream (a cancel, a
        # refused register, the RegisterAck's authoritative state) -- and each must find the
        # action, end it and report it as one step, or two of them end the same action twice
        # and the record that keeps interrupted work from being continued is lost. Taken
        # before _robot_lock when both are held; never the other way round.
        self._action_lock = threading.RLock()
        # Counts the emergency stops commanded, under _robot_lock. An estop commands the vehicle
        # before it takes _action_lock (a stop never waits on bookkeeping), so an assignment or
        # a resume already past its checks re-reads this under _robot_lock and gives up rather
        # than send an order or a stopPause after the stop.
        self._estop_epoch = 0
        self._robot_id = robot_id
        self._fence = FenceGuard()                              # C3
        self._lease = LeaseMonitor(on_expiry=self._self_stop)   # C4
        self._current_action = ""
        self._current_generation = 0
        self._current_fencing_token = None   # echoed on task-status streaming (C6.4)
        self._last_task_state = ""           # last streamed TaskState, for transition detect
        self._edge_addr: queue.Queue = queue.Queue()  # advertised edge endpoints (C8)
        self._outbox: queue.Queue = queue.Queue()
        self._safety_outbox: queue.Queue = queue.Queue()  # → SafetyStream (acks + drills)
        # Contract 1.4.0: the stop latch, the retained outcome, and the work a stop interrupted
        # (action ID -> the fencing token it was assigned with; its re-delivery starts nothing).
        # How a channel is opened, ControlStream and EdgeStream alike: mutual TLS, or plaintext
        # only when --insecure said so (main builds it). None dials nothing.
        self._dial = dial
        self._negotiated = ""
        self._latch = StopLatch()
        self._retained = RetainedOutcomes(state_dir)
        self._interrupted: dict[str, int] = {}
        self._action_tokens: dict[str, int] = {}
        self._shutdown = threading.Event()
        self._telemetry_started = False
        # True only between an ACCEPTED RegisterAck and the end of that ControlStream session.
        # A robot whose registration was refused, or not yet answered, is not under management
        # on this session, and no work is accepted for it (C13.2).
        self._registered = False
        # The ControlStream session, counted. The provisioning loop registers again only on the
        # session that was refused; a newer session registers on its own (C2.4).
        self._session = 0
        # The provisioning loop: the latest DiscoverAck's ttl_seconds, and an event set when a
        # DiscoverAck arrives, which the loop waits on before it picks its interval.
        self._discover_ttl = 0
        self._discover_acked = threading.Event()
        # C6.1: the first TelemetryPayload on any (re)connect must be a FULL
        # snapshot, not a resumed delta — otherwise a reconnected control plane
        # inherits a stale hardware picture it cannot detect.
        self._snapshot_pending = True
        # C2.3: per-robot cadence from RegisterAck; the adapter default until told.
        self._telemetry_interval = _TELEMETRY_INTERVAL

        # Scenario handle: a binding that exposes the scenario surface (SimulatedRobot
        # with an engine). None ⇒ no scenario (the vda5050 binding, or the $0 skeleton
        # with no --scenario) → the streams run exactly as before.
        self._sim = robot if hasattr(robot, "hardware_delta") else None
        self._estop_drilled = False
        self._reconnect_pending = False
        self._active_call = None
        # Transport-liveness log state: True so the FIRST degrade is announced, and so
        # the announcement is a transition rather than 5 lines a second.
        self._transport_was_live = True
        self._clock = time.monotonic
        self._last_tick: float | None = None

    # ── ControlStream ────────────────────────────────────────────────────────

    def _control_requests(self, outbox: queue.Queue | None = None) -> Iterator:
        """The session's requests: the hello, then what is put on ITS outbox. The outbox is
        bound when the session opens, so a dying session's generator cannot take a message
        put on the next session's outbox (a lost RegisterRobot, C2.4)."""
        pb = self._pb
        if outbox is None:
            outbox = self._outbox
        yield pb.AdapterMessage(hello=pb.AdapterHello(  # C1.2
            vendor="vda5050", adapter_version="0.1.0",
            protocol_version="fleet_adapter.v1", contract_version=CONTRACT_VERSION,
            namespace=self._namespace))
        while True:
            msg = outbox.get()
            if msg is None:
                return
            yield msg

    def bring_up_robot(self) -> None:
        """Bring the robot binding up, then spawn the robot on it.

        THE ONE PLACE the adapter drives `RobotBinding.start()`. It is called
        unconditionally -- no `hasattr`, no `--comms` branch -- because the Protocol
        declares the lifecycle (robot.py) and a binding with nothing to bring up says so
        with an explicit no-op.

        `start()` runs BEFORE `spawn()`, and that order is load-bearing rather than
        stylistic: `spawn` is the first thing the adapter asks OF the robot, and on
        `Vda5050Binding` nothing can be asked until the three MQTT subscriptions and the
        broker session exist. Spawning first would be addressing a transport that is not
        up yet.

        Separate from :meth:`run_control_stream` so the bring-up the shipped entry point
        performs can be driven WITHOUT a control plane or a gRPC stub -- which is what a
        harness needs, and the reason it matters is `fleet-adapter-ros2`'s: its R1 harness
        called `binding.start()` itself, so it exercised a started binding while the
        adapter did not, and R1 passed every assertion for months against a transport the
        shipped adapter never opened. A harness that supplies the step under
        test is not testing it. There is one bring-up implementation and it lives here.

        A failure here is fatal on purpose: `start()` raising stops the run, rather than
        letting the adapter register and stream plausible telemetry (pose `(0,0,0)`,
        battery `0`, phase `IDLE`, `is_stopped` permanently False) read off a binding
        connected to no broker."""
        with self._robot_lock:
            self._robot.start()
            self._robot.spawn(self._robot_id)

    def run_control_stream(self, stub) -> None:
        self.bring_up_robot()
        # The edge loop blocks until an endpoint is advertised (C8), so starting it
        # now is safe; it stays idle for zones with no edge node.
        threading.Thread(target=self._edge_loop, daemon=True).start()

        drop_window = self._sim.stream_drop_window() if self._sim else None
        if drop_window is None:
            # No scenario-driven drop: one logical ControlStream session, but resilient.
            # An UNEXPECTED transport drop (a demo port-forward blip, a manager restart)
            # raises RpcError from the response iterator; log and RE-OPEN with a fresh
            # Hello/Register rather than dying with a traceback. The gRPC channel
            # auto-heals once connectivity returns.
            #
            # The conformance harness faults this stream ON PURPOSE — twice: the
            # accepting-reconnect probe (C6.1/C2.2) and the contract-version refusal
            # probe (C13.2/C14.1). Without the re-open below the process exited on the
            # first fault, so all four of those checks could only ever be skipped
            # ("adapter did not redial within 5.0s") and the run printed an alarming
            # but expected traceback.
            while not self._shutdown.is_set():
                try:
                    # Fresh outbox per session. The dying generator is not stopped the
                    # instant its stream faults — it can still be blocked in get() — and
                    # with a shared queue it swallows the RegisterRobot the NEW session
                    # enqueues on hello_ack, so the control plane never sees the
                    # re-registration §9.2.5(b) requires.
                    outbox: queue.Queue = queue.Queue()
                    self._outbox = outbox
                    self._new_session()
                    for cp in stub.ControlStream(self._control_requests(outbox)):
                        self._handle_control_plane(cp, stub)
                except grpc.RpcError as exc:
                    if self._shutdown.is_set():
                        break
                    print(f"control stream dropped ({exc.code()}); reconnecting", file=sys.stderr)
                    time.sleep(0.5)
                    continue
                break  # clean close (response iterator ended without error)
            self._shutdown.set()
            return

        # comms-flaky: reconnect across a scenario-driven stream drop. Each pass is a
        # full Hello/Register handshake, exercising the RegisterRobot reconnect and the
        # persisted fencing/lease state (C3/C4) across the outage.
        while not self._shutdown.is_set():
            self._reconnect_pending = False
            # A fresh outbox per session, as above: what the dropped session left queued is
            # not sent ahead of the new session's RegisterRobot.
            outbox = queue.Queue()
            self._outbox = outbox
            self._new_session()
            call = stub.ControlStream(self._control_requests(outbox))
            self._active_call = call
            try:
                for cp in call:
                    self._handle_control_plane(cp, stub)
            except grpc.RpcError as exc:  # the drop surfaces as a CANCELLED RPC
                if not self._reconnect_pending:
                    raise
                print(f"control stream dropped ({exc.code()}); reconnecting", file=sys.stderr)
            finally:
                self._active_call = None
            if self._shutdown.is_set() or not self._reconnect_pending:
                break
            self._await_reconnect()
        self._shutdown.set()

    def _new_session(self) -> None:
        """A ControlStream session opens: nothing is registered on it yet."""
        self._session += 1
        self._registered = False
        self._latch.new_control_stream(self._robot_id)

    def _handle_control_plane(self, cp, stub) -> None:
        pb = self._pb
        kind = cp.WhichOneof("payload")
        if kind == "hello_ack":
            if not cp.hello_ack.accepted:
                raise RuntimeError(f"handshake rejected: {cp.hello_ack.message}")
            self._negotiated = cp.hello_ack.negotiated_contract_version
            self._outbox.put(pb.AdapterMessage(  # C2.1: register
                register=pb.RegisterRobot(robot_id=self._robot_id)))
            # A fresh ControlStream: the next payload must be a full snapshot (C6.1).
            self._snapshot_pending = True
            if not self._telemetry_started:  # C6: stream telemetry once registered
                self._telemetry_started = True
                threading.Thread(target=self._telemetry_loop, args=(stub,), daemon=True).start()
        elif kind == "register_ack":
            ack = cp.register_ack
            if not ack.accepted:
                self._on_register_refused(ack)
                return
            self._registered = True
            # Registered: the catalog, and any hardware manifest (C12.2), and a full snapshot
            # next (C6.1). Sent here rather than at the hello, because a robot admitted through
            # the provisioning loop had no Robot when the hello came, and what was sent for it
            # then was not kept.
            self._send_capabilities_snapshot()
            self._snapshot_pending = True
            for e in cp.register_ack.edge_endpoints:  # C8.1: dial each advertised edge node
                self._edge_addr.put(e.address)
            # C2.3 — adopt the per-robot cadence the control plane returned. Only a
            # POSITIVE value is a cadence: 0 means "not specified" and a negative value
            # is malformed — clamping that to the 1s minimum would let a buggy control
            # plane force the fastest permitted rate. Legitimate values are clamped to
            # the protocol's 1-30s range.
            secs = cp.register_ack.telemetry_interval_seconds
            if secs > 0:
                self._telemetry_interval = max(1.0, min(30.0, float(secs)))
            self._reconcile_action_state(cp.register_ack)
            self._send_retained()
        elif kind == "discover_ack":
            # Announced. The robot is not under management until a register is accepted.
            da = cp.discover_ack
            if not da.accepted:
                print(f"announce refused robot={self._robot_id}: {da.message}", file=sys.stderr)
            self._discover_ttl = da.ttl_seconds
            self._discover_acked.set()
        elif kind == "outcome_ack":
            oa = cp.outcome_ack
            if self._retained.acknowledge(self._robot_id, oa.action_id, oa.fencing_token):
                print(f"retained outcome acknowledged: action={oa.action_id}", file=sys.stderr)
        elif kind == "heartbeat":
            self._outbox.put(pb.AdapterMessage(
                heartbeat=pb.HeartbeatResponse(robot_id=cp.heartbeat.robot_id)))
        elif kind == "command":
            self._handle_command(cp.command)

    def _on_register_refused(self, ack) -> None:
        """A refused register (RFC-0001 #fleet-adapter-protocol-the-stop-latch, "A refused
        register"): the robot is not under management on this session, so the work held for it
        is stopped as interrupted work and no new work is accepted (C13.2). Telemetry and the
        SafetyStream carry on; an estop is honoured regardless (C14.1).

        Only NOT_ADMITTED -- no Robot exists for this robot yet -- is answered by announcing it
        (discover) and registering again each provisioning interval (C2.1). Any other refusal
        is not: announcing would be reaching for a robot the control plane said this adapter
        may not touch."""
        pb = self._pb
        self._registered = False
        self._interrupt_current("registration refused")
        if ack.rejection != pb.REGISTRATION_REJECTION_NOT_ADMITTED:
            # By number when this build does not know the code: proto3 enums are open, and a
            # name lookup that raised here would end the control stream and the process.
            code = (pb.RegistrationRejection.Name(ack.rejection)
                    if ack.rejection in pb.RegistrationRejection.values() else ack.rejection)
            print(f"register refused robot={self._robot_id} ({code}): {ack.message}",
                  file=sys.stderr)
            return
        self._discover_acked.clear()
        self._outbox.put(pb.AdapterMessage(discover=pb.DiscoverRobot(
            robot_id=self._robot_id, hardware=self._capability_components())))
        threading.Thread(target=self._provisioning_iteration,
                         args=(self._session, self._outbox), daemon=True).start()
        print(f"NOT_ADMITTED robot={self._robot_id}; announced for operator admission (C2.1)",
              file=sys.stderr)

    def _provisioning_iteration(self, session: int, outbox: queue.Queue) -> None:
        """Register again after the provisioning interval, on the session that was refused.

        The interval is the DiscoverAck's (min(ttl/2, 300)), or 60 s when none arrives with a
        TTL. A session that has been replaced, or a robot admitted meanwhile, ends the loop: the
        new session registers on its own. A register refused NOT_ADMITTED again starts the next
        iteration."""
        self._discover_acked.wait(_DISCOVER_ACK_WAIT_S)
        if self._shutdown.wait(_provisioning_interval(self._discover_ttl)):
            return
        if self._registered or session != self._session:
            return
        outbox.put(self._pb.AdapterMessage(
            register=self._pb.RegisterRobot(robot_id=self._robot_id)))

    def _handle_command(self, command) -> None:
        pb = self._pb
        result = pb.CommandResult(command_id=command.command_id, robot_id=command.robot_id)
        which = command.WhichOneof("command")
        if not self._serves(command.robot_id):
            # Before the term, the latch, the fence and the lease: nothing of this robot's
            # changes for a command addressed to another.
            if which == "estop":
                self._unserved_estop(command.robot_id, command.estop.estop_id, "fence")
                return
            if which in _ROBOT_ACTING_ARMS:
                self._refuse_unserved(command, which, result)
                self._outbox.put(pb.AdapterMessage(command_result=result))
                return
            self._log_unserved(command.robot_id, f"arm={which} command_id={command.command_id}",
                               "answered as for any robot: it moves nothing")
        # The term is kept for the robot (proto, Command.leader_term: "the highest term it has
        # seen for the robot"): only a command for the served robot is held to it or raises it.
        if self._serves(command.robot_id) and not self._latch.observe_term(
                self._robot_id, command.leader_term, is_stop=which == "estop"):
            # A deposed leader's command: not acted on, the result left empty.
            result.stale_leader_term = True
            self._outbox.put(pb.AdapterMessage(command_result=result))
            return
        if which == "estop":
            # The stop's fence: a stop, answered on the SafetyStream only.
            self._on_fence(command.estop.estop_id)
            return
        if which == "assign_action":
            self._on_assign(command.assign_action, result)
        elif which == "renew_lease":
            self._on_renew(command.renew_lease, result)
        elif which == "cancel_action":
            with self._action_lock:
                named = command.cancel_action.action_id
                if named and self._current_action and named != self._current_action:
                    # C3.9: a cancel for an action this robot no longer runs (it was preempted
                    # in place, or a stop ended it). That one is stopped already, and stopping
                    # the current one would abort the action that replaced it.
                    result.cancel_action.CopyFrom(pb.CancelActionResult(
                        acknowledged=True, disposition=pb.CANCEL_DISPOSITION_STOPPED_SAFELY,
                        cause=command.cancel_action.cause))
                    self._outbox.put(pb.AdapterMessage(command_result=result))
                    return
                cancelled, _ = self._take_current()
                with self._robot_lock:
                    self._robot.command_stop(self._robot_id)
                self._lease.release(self._robot_id)
                result.cancel_action.CopyFrom(pb.CancelActionResult(
                    acknowledged=True,
                    disposition=pb.CANCEL_DISPOSITION_STOPPED_SAFELY))
                if cancelled:
                    self._emit_action_status(cancelled, task_map.TASK_CANCELLED, None)
        elif which == "pause":
            self._on_pause(command.pause, result)
        elif which == "resume":
            self._on_resume(command.resume, result)
        elif which == "scan":
            self._on_scan(result)
        elif which == "zone_constraints":  # FA-R7, contract 1.3.0
            result.zone_constraints.CopyFrom(zone_constraints_ack(pb, command.zone_constraints))
        else:
            # C7.1: decline every optional command not implemented.
            result.unsupported = True
        self._outbox.put(pb.AdapterMessage(command_result=result))

    # ── robot_id: the one robot this adapter serves ─────────────────────────────

    def _serves(self, robot_id: str) -> bool:
        """True only for the robot_id this adapter registered. The control plane addresses
        every Command and every SafetyStream message by the robot's wire id, which is the id
        its adapter announced (the Robot's swarmada.io/robot-id). An empty robot_id names no
        robot, so it is never served."""
        return bool(robot_id) and robot_id == self._robot_id

    def _log_unserved(self, received: str, what: str, outcome: str) -> None:
        """Every message for a robot this adapter does not serve, at error level, naming both
        ids: an id mismatch with the control plane is loud, not silent."""
        print(f"error: UNSERVED robot={self._robot_id!r} addressed={received!r} {what}: "
              f"{outcome}", file=sys.stderr, flush=True)

    def _refuse_unserved(self, command, which: str, result) -> None:
        """Refuse a command addressed to a robot this adapter does not serve.

        One ControlStream carries the commands for every Robot bound to this adapter, and
        Command.robot_id selects which one (RFC-0001 #fleet-adapter-protocol-the-controlstream-
        service). This adapter serves exactly one vehicle, and a command for any other id run on
        it would move the wrong robot. So nothing here reaches the vehicle, the stop latch, the
        fencing mark, the lease or the current action, and each arm is answered with the
        refusal its result already defines. An assignment is declined ROBOT_UNAVAILABLE: the
        robot it names is not available through this adapter, so the control plane releases it
        to reschedule; INVALID_ACTION would wrongly say the action could not be served."""
        pb = self._pb
        why = (f"robot_id {command.robot_id!r} is not served by this adapter, which serves "
               f"{self._robot_id!r}; nothing was done")
        if which == "assign_action":
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE,
                message=why))
        elif which == "renew_lease":
            result.renew_lease.CopyFrom(pb.RenewActionLeaseResult(
                renewed=False, running=False, message=why))
        elif which == "cancel_action":
            result.cancel_action.CopyFrom(pb.CancelActionResult(acknowledged=False, message=why))
        elif which == "pause":
            result.pause.CopyFrom(pb.PauseResult(paused=False, message=why))
        elif which == "resume":
            result.resume.CopyFrom(pb.ResumeResult(resumed=False, message=why))
        elif which == "zone_constraints":
            result.zone_constraints.CopyFrom(pb.ZoneConstraintsAck(acknowledged=False))
        self._log_unserved(command.robot_id, f"arm={which} command_id={command.command_id}",
                           "refused; nothing was done")

    def _unserved_estop(self, received: str, estop_id: str, via: str) -> None:
        """An estop (on the SafetyStream) or a fence (Command.estop) for a robot this adapter
        does not serve: answered FAILED on the SafetyStream, never STOPPED. This adapter cannot
        confirm that robot is at rest, and FAILED is the answer that escalates; a STOPPED would
        confirm a stop nobody made. Nothing is latched: the stop is not this robot's.

        Whether this vehicle stops depends on the registration. Once one is accepted on this
        session, the adapter serves its robot, and a stop for another id is not this vehicle's:
        it does not stop. While none is accepted, the adapter serves no robot, and the likeliest
        reason a stop arrives under another id is a mismatch: this adapter's --robot-id is not
        the Robot's swarmada.io/robot-id, so its registration was refused and every stop for that
        Robot comes here under the other id. The vehicle is then stopped, as a stop for it would,
        so a misconfiguration never leaves it driving; the answer is still FAILED."""
        pb = self._pb
        ack = pb.EstopAck(estop_id=estop_id, state=pb.ESTOP_STATE_FAILED)
        why = (f"robot_id {received!r} is not served by this adapter, which serves "
               f"{self._robot_id!r}")
        if self._registered:
            self._log_unserved(received, f"{via} id={estop_id}",
                               "answered FAILED; no stop was commanded")
            ack.message = f"{why}; no stop was commanded and none can be confirmed"
        else:
            ack.stop_initiated_at = _now_ms()
            with self._robot_lock:
                self._command_estop()
            self._interrupt_current("emergency stop under a robot_id this adapter does not serve",
                                    command_stop=False)
            self._log_unserved(received, f"{via} id={estop_id}",
                               "answered FAILED; no accepted registration; vehicle stopped")
            ack.message = (f"{why}; no registration is accepted on this session, so this "
                           f"vehicle was stopped; rest is not confirmed for that robot")
        self._safety_outbox.put(pb.AdapterSafetyMessage(robot_id=received, estop_ack=ack))

    def _on_pause(self, pause, result) -> None:
        """Map Swarmada pause → VDA5050 startPause (pause the order without aborting
        it). require_stop_before_ack: ack paused=true only once the robot is CONFIRMED
        at rest (never inferred from the command or a timer)."""
        pb = self._pb
        with self._robot_lock:
            self._robot.command_pause(self._robot_id)
            paused = self._robot.is_stopped(self._robot_id) if pause.require_stop_before_ack else True
        result.pause.CopyFrom(pb.PauseResult(
            paused=paused,
            message="vda5050: startPause" if paused else "vda5050: pausing (awaiting confirmed stop)"))

    def _on_resume(self, resume, result) -> None:
        """Map Swarmada resume → VDA5050 stopPause (resume the held order). Refused while a
        stop is latched: an estop holds the vehicle paused, and a stopPause then would lift
        it before the stop's clear."""
        pb = self._pb
        epoch = self._estop_epoch
        if self._latch.refuses_work(self._robot_id):
            result.resume.CopyFrom(pb.ResumeResult(resumed=False, message="under a stop"))
            return
        with self._robot_lock:
            # Again where it counts: a stop commanded since the check above must not be lifted.
            if self._estop_epoch != epoch or self._latch.refuses_work(self._robot_id):
                result.resume.CopyFrom(pb.ResumeResult(resumed=False, message="under a stop"))
                return
            self._robot.command_resume(self._robot_id)
        result.resume.CopyFrom(pb.ResumeResult(resumed=True, message="vda5050: stopPause"))

    def _on_assign(self, task, result) -> None:
        # One step under the action lock: a stop arriving meanwhile on another thread finds
        # the action either not yet taken or taken, never half of it.
        with self._action_lock:
            self._on_assign_locked(task, result)

    def _on_assign_locked(self, task, result) -> None:
        pb = self._pb
        epoch = self._estop_epoch  # see _estop_epoch: re-read before the order is sent
        if task.action_id and self._interrupted.get(task.action_id) == task.fencing_token:
            # Interrupted work, re-delivered: accepted, and nothing starts. Never refused, since
            # a refusal says the robot never took that work.
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=True, accepted_fencing_token=task.fencing_token,
                message="interrupted by a stop; not continued"))
            return
        if self._latch.refuses_work(self._robot_id):
            # Under a stop: refused whatever the fencing token, and the latch reported.
            ids = sorted(self._latch.latched_ids(self._robot_id))
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE,
                message="under a stop: " + (", ".join(ids) if ids else "local latch")))
            self._report_latch()
            return
        unavailable = self._unavailable()
        if (unavailable and task.HasField("fencing_token")
                and task.fencing_token > self._fence.highest(self._robot_id)):
            # Refused BEFORE the fence's check, which raises the high-water mark on acceptance:
            # work the robot never took must not make a later assignment at that token look
            # stale.
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE,
                message=unavailable))
            return
        # Below or at the mark, or with no token, the check changes nothing: STALE, MISSING or
        # an identical re-delivery.
        decision = self._fence.check(
            self._robot_id, task.HasField("fencing_token"), task.fencing_token, task.assignment_id)
        if decision is FenceDecision.MISSING:
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_MISSING_FENCING_TOKEN))
            return
        if decision is FenceDecision.STALE:
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_STALE_FENCING_TOKEN))
            return
        # After the fencing branches, so a stale token is still refused STALE.
        if unavailable:
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE,
                message=unavailable))
            return
        # After the fencing branches on purpose: a superseded command is refused STALE, so
        # the control plane learns its assignment lost rather than a reason it never lost for.
        if task.action_type != ACTION_NAVIGATE:  # C19.1 / C19.3
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_INVALID_ACTION,
                message="unsupported action type: " + (task.action_type or "(empty)")))
            return
        unhonourable = unhonourable_subjects(task.operating_constraints)  # C17.1 / C17.2
        if unhonourable:
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False,
                rejection=pb.ASSIGN_ACTION_REJECTION_OPERATING_CONSTRAINT_UNMET,
                unhonourable_subjects=unhonourable))
            return
        goal = task_map.goal_from_assign(task.destination, task.payload_json)  # FleetTask → order target
        with self._robot_lock:
            if self._estop_epoch != epoch:
                # An emergency stop was commanded while this assignment was being checked. The
                # vehicle is paused and its order cancelled; an order now would follow the
                # stop (and, after an estop, lift the pause). Refused: the robot never took it.
                result.assign_action.CopyFrom(pb.AssignActionResult(
                    accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE,
                    message="an emergency stop arrived while this assignment was taken"))
                return
            self._robot.command_move(self._robot_id, goal.x, goal.y, goal.theta)
            if hasattr(self._robot, "note_order"):      # the simulated vehicle's log line
                self._robot.note_order(self._robot_id, task.action_id)
        self._current_action = task.action_id
        if task.HasField("fencing_token"):
            self._action_tokens[task.action_id] = task.fencing_token
        if task.lease_duration_ms:
            self._current_generation = task.lease_generation
            self._lease.grant(self._robot_id, task.lease_duration_ms / 1000.0, task.lease_generation)
        result.assign_action.CopyFrom(pb.AssignActionResult(
            accepted=True, accepted_fencing_token=task.fencing_token))  # C3.5
        # Task now RUNNING; echo the executing fencing_token so the control plane can
        # detect a robot still acting on a superseded assignment (C6.4).
        token = task.fencing_token if task.HasField("fencing_token") else None
        self._current_fencing_token = token
        self._last_task_state = task_map.TASK_RUNNING
        self._emit_action_status(task.action_id, task_map.TASK_RUNNING, token)

    def _unavailable(self) -> str:
        """Why the robot can take no work now, or "" when it can.

        * C13.2: it is not under management on this session (its registration refused, or
          not yet accepted).
        * It is paused at the vehicle: the vehicle reports a pause this adapter did not set,
          by its own button or by an earlier adapter process. An order would wait paused for
          as long as its lease is renewed, and the adapter does not release a pause it did not
          set. A pause pressed during this adapter's own estop pause is not seen here: the
          stopPause sent before the first order after the clear releases it (VDA5050 2.0.0:
          stopPause "can also restart vehicles that were stopped with a hardware button")."""
        if not self._registered:
            return "this robot's registration was not accepted on this session"
        paused = getattr(self._robot, "paused_at_vehicle", None)  # a binding written before it
        with self._robot_lock:
            if paused is not None and paused(self._robot_id):
                return "paused at the vehicle"
        return ""

    def _reconcile_action_state(self, ack) -> None:
        """C2.2 / C3.8 -- adopt the control plane's authoritative action state on RegisterAck.

        Across a connectivity gap the control plane is the source of truth: an action may
        have been cancelled while this adapter was away, and nothing local can say so.

        The fencing token is adopted first and unconditionally. It is the plane's own
        statement of the current mark, and after a failover the new plane legitimately
        holds one this adapter never saw; without adopting it, the adapter keeps honouring
        commands the old plane authorised. ``FenceGuard.adopt`` is monotonic, so a replayed
        RegisterAck cannot lower the mark.
        """
        if not ack.HasField("authoritative_action_state"):
            return
        pb = self._pb
        st = ack.authoritative_action_state
        if st.fencing_token:
            self._fence.adopt(self._robot_id, st.fencing_token)
        phase = st.phase
        if phase == pb.ROBOT_ACTION_PHASE_IN_PROGRESS:
            if st.action_id and st.action_id == self._current_action:
                return  # agrees with local state; keep executing under the same lease
            phase = pb.ROBOT_ACTION_PHASE_CANCELLED  # divergence is treated as a cancel
        if phase not in (pb.ROBOT_ACTION_PHASE_CANCELLED, pb.ROBOT_ACTION_PHASE_UNKNOWN,
                         pb.ROBOT_ACTION_PHASE_IDLE):
            return
        with self._action_lock:
            local, _ = self._take_current()
            with self._robot_lock:
                self._robot.command_stop(self._robot_id)
            self._lease.release(self._robot_id)
            reported = st.action_id or local
            if reported:
                state = (task_map.TASK_CANCELLED if phase == pb.ROBOT_ACTION_PHASE_CANCELLED
                         else task_map.TASK_STOPPED)
                self._emit_action_status(reported, state, None)

    def _on_renew(self, renew, result) -> None:
        pb = self._pb
        with self._action_lock:
            renewed = self._lease.renew(
                self._robot_id, renew.lease_duration_ms / 1000.0, renew.lease_generation)  # C4.3
            if renewed:
                self._current_generation = renew.lease_generation
            running = self._current_action == renew.action_id and self._current_action != ""  # C4.4
        result.renew_lease.CopyFrom(pb.RenewActionLeaseResult(
            renewed=renewed, running=running, current_generation=self._current_generation))

    def _emit_action_status(self, action_id: str, state_name: str,
                          fencing_token: int | None, progress_pct: int = 0, *,
                          message: str = "", end_cause: int = 0) -> None:
        """Send a ActionStatusUpdate (AdapterMessage.action_status). state_name is a
        task.TASK_* name bound here to the proto TaskState enum. When set, the
        executing fencing_token is echoed for superseded-assignment detection (C6.4)."""
        pb = self._pb
        if fencing_token is None:
            fencing_token = self._action_tokens.get(action_id)
        upd = pb.ActionStatusUpdate(
            action_id=action_id, state=getattr(pb, state_name), progress_pct=progress_pct)
        if message:
            upd.message = message
        if fencing_token is not None:
            upd.fencing_token = fencing_token
        if end_cause:
            upd.end_cause = end_cause
        if state_name in _TERMINAL and action_id in self._action_tokens:
            # Kept whatever the contract; reported only to a 1.4.0 control plane.
            self._retained.keep(self._robot_id, Outcome(
                action_id, state_name, message, self._action_tokens.pop(action_id), end_cause))
        self._outbox.put(pb.AdapterMessage(action_status=upd))

    def _send_retained(self) -> None:
        """The retained outcome, once the RegisterAck is processed, to a 1.4.0 control plane."""
        if not _at_least_14(self._negotiated):
            return
        out = self._retained.get(self._robot_id)
        if out is None:
            return
        pb = self._pb
        upd = pb.ActionStatusUpdate(action_id=out.action_id, state=getattr(pb, out.state),
                                    message=out.message, fencing_token=out.fencing_token,
                                    retained=True)
        if out.end_cause:
            upd.end_cause = out.end_cause
        self._outbox.put(pb.AdapterMessage(action_status=upd))

    def _take_current(self) -> tuple[str, int | None]:
        """Take the action the robot holds, with its fencing token, and clear it: the one way
        an action is ended. The caller holds the action lock, so of several threads ending the
        same action exactly one gets it and the others get ("", None)."""
        action = self._current_action
        token = self._action_tokens.get(action) if action else None
        self._current_action = ""
        self._last_task_state = ""
        self._current_fencing_token = None
        return action, token

    def _interrupt_current(self, why: str, *, command_stop: bool = True) -> None:
        """Interrupted work: the action ends STOPPED, then FAILED with cause ESTOP, and is never
        continued. Its re-delivery at the same token is answered accepted, nothing started.

        `command_stop` False is for a caller that has already commanded the stop (an estop,
        which stops the vehicle before anything else)."""
        pb = self._pb
        with self._action_lock:
            action, token = self._take_current()
            if not action:
                return
            if command_stop:
                with self._robot_lock:
                    self._robot.command_stop(self._robot_id)
            self._lease.release(self._robot_id)
            self._interrupted[action] = token or 0
            self._emit_action_status(action, task_map.TASK_STOPPED, None, message=why,
                                     end_cause=pb.ACTION_END_CAUSE_ESTOP)
            self._emit_action_status(action, task_map.TASK_FAILED, None,
                                     message=f"interrupted: {why}; not continued",
                                     end_cause=pb.ACTION_END_CAUSE_ESTOP)

    def _confirm_estop(self) -> str:
        """`confirm_estop`, but a BROKEN TRANSPORT still produces an answer.

        `confirm_estop` polls the binding's `tick()` and `is_stopped()`. Since the lifecycle
        fix, `Vda5050Binding.tick()` raises when its transport was never started — which is
        correct, and on this path alone it would make things worse if it escaped:

        * `run_safety_stream` and `_edge_loop` call this from inside their response loops,
          so an escaping exception kills that thread and the control plane gets **no
          EstopAck at all**. Silence on an estop is strictly less actionable than
          `FAILED`, which at least says "rest was not confirmed — escalate".
        * the safety stream is started BEFORE `bring_up_robot()` runs (see `main`), so
          there is a real window in which an estop can arrive at an un-started binding.
          Losing the safety thread permanently to a startup race would be a worse defect
          than the one being fixed.

        So the exception is caught, reported to stderr with its diagnosis intact, and
        mapped to ESTOP_FAILED. That mapping is not a fudge and not a weakening of C5:
        FAILED is the TRUE answer, because a binding subscribed to no `state` topic
        cannot confirm rest, and C5.3 forbids inferring rest from silence. The only thing
        that changes is that the reason now reaches the operator instead of being lost —
        previously this returned FAILED with no explanation anywhere, which is how
        C5.2 / C8.3 / C14.1 were misattributed for weeks on the ros2 adapter.

        Deliberately NOT a blanket `except Exception`: only the transport's own
        RuntimeError is absorbed. A bug in the safety primitive itself must still crash.
        """
        self._command_estop()
        return self._await_estop()

    def _command_estop(self) -> None:
        """Issue the emergency stop (`command_estop`) and return without confirming rest. The
        caller holds the robot lock. The transport's RuntimeError is reported, never raised:
        `_await_estop` then answers FAILED, the true answer (see `_confirm_estop`)."""
        self._estop_epoch += 1
        # A binding written before command_estop was declared still stops: with its stop. An
        # estop must never end in an AttributeError that kills the thread and leaves no ack.
        estop = getattr(self._robot, "command_estop", None) or self._robot.command_stop
        try:
            estop(self._robot_id)
        except RuntimeError as exc:
            print(f"estop could not be commanded - the robot binding's transport is not "
                  f"live: {exc}", file=sys.stderr)

    def _await_estop(self) -> str:
        """Wait for confirmed rest after `_command_estop`: STOPPED, or FAILED. The caller holds
        the robot lock."""
        try:
            return await_estop_confirmation(self._robot, self._robot_id)
        except RuntimeError as exc:
            print(f"estop could not be confirmed - the robot binding's transport is not "
                  f"live, so rest cannot be read from the robot and C5.3 forbids "
                  f"inferring it: {exc}", file=sys.stderr)
            return ESTOP_FAILED

    def _self_stop(self, robot_id: str) -> None:
        # Report the stop, then the action's end, before dropping it. Without them the
        # control plane sees only silence after the lease lapses, which is
        # indistinguishable from a robot that merely stopped reporting, so dual execution
        # cannot be ruled out (C4.2). STOPPED is the commanded stop and not terminal; the
        # FAILED that follows, with LEASE_EXPIRED, is what tells the control plane the
        # action ended.
        pb = self._pb
        with self._action_lock:
            token = self._current_fencing_token
            stopped, _ = self._take_current()
            with self._robot_lock:  # C4.2
                self._robot.command_stop(robot_id)
            if stopped:
                self._emit_action_status(stopped, task_map.TASK_STOPPED, token,
                                         message="assignment lease expired",
                                         end_cause=pb.ACTION_END_CAUSE_LEASE_EXPIRED)
                self._emit_action_status(stopped, task_map.TASK_FAILED, token,
                                         message="ended by lease expiry; the stop was commanded",
                                         end_cause=pb.ACTION_END_CAUSE_LEASE_EXPIRED)

    def _report_transport_not_live(self, detail: str) -> None:
        """Announce a transport that cannot observe the robot, on the TRANSITION only.

        At a 0.2 s cadence an unconditional log is 5 lines a second, which buries the
        diagnosis it is trying to deliver. Logged again if the transport recovers, so the
        operator sees an interval rather than an unbounded complaint."""
        if self._transport_was_live:
            self._transport_was_live = False
            print(f"telemetry DEGRADED to ROBOT_PHASE_OFFLINE: the robot binding cannot "
                  f"observe the robot, so position and battery are being OMITTED rather "
                  f"than sent as defaults (C6.2). {detail}", file=sys.stderr)

    def _transport_detail(self) -> str:
        """The binding's own reason for being unobservable, if it offers one.

        `transport_detail` is deliberately NOT on `RobotBinding`: the adapter degrades
        identically without it. Only the operator-facing sentence gets shorter, and
        demanding it of every binding would put a logging convenience in a safety
        contract."""
        detail = getattr(self._robot, "transport_detail", None)
        return detail(self._robot_id) if callable(detail) else ""

    def _telemetry_loop(self, stub) -> None:
        """Stream TelemetryPayload at the registered cadence (C6).

        Every cycle asks the binding whether it can still OBSERVE the robot, and stops
        claiming a position or a battery the moment it cannot. Two things make that
        necessary rather than defensive:

        * **It was a C6.2 violation, and this adapter had the evidence in hand.** An
          un-started binding answers every accessor — `pose()` with the origin,
          `battery_percent()` with 0 — and this loop published them at full cadence with
          a fresh `timestamp_ms`. C6.2 says *"never send a defaulted 0/false where the
          true reading is unknown"*, and a robot "parked at (0,0) on a 0% battery" is
          indistinguishable from a real one. What makes it this adapter's own fault
          rather than an oversight is that VDA5050 declares the answer on the
          `connection` topic and the binding already parsed it into `connection_state()`
          / `is_online()`: the signal was computed every cycle and read by nobody.
        * **`tick()` raising is not enough on its own.** It raises ONCE, so the first
          thread to reach it consumes the signal for all the others — and the safety
          stream starts before bring-up, so it can be the one that consumes it.

        OFFLINE is reported rather than a gap, deliberately. A gap is ambiguous between a
        crashed adapter, a network fault and a blind one; OFFLINE is the adapter saying
        "I am up and I cannot see the robot", in the field built for it, at the cadence
        the control plane already expects. Proto3 message fields have presence, so
        omitting `position` and `battery` is expressible and is what C6.2 asks for.

        `self._lease.tick()` keeps running throughout, deliberately: an un-renewed lease
        must still self-stop during a transport outage (C4), exactly as it does during
        the scenario stream drop."""
        pb = self._pb
        while not self._shutdown.is_set():
            with self._action_lock:  # an expiry ends the action: one step, as every other end
                self._lease.tick()  # C4 self-stop check — must keep running while degraded
            x = y = floor = battery = None
            with self._robot_lock:
                try:
                    self._tick_robot()  # advances the scenario clock too
                    live = self._robot.transport_alive()
                    detail = self._transport_detail() if not live else ""
                except RuntimeError as exc:
                    # tick() diagnosing a transport that was never started. Handled
                    # identically to the predicate answering False, so the outcome does
                    # not depend on which thread happened to consume the one-shot raise.
                    live, detail = False, str(exc)
                if live:
                    x, y, floor = self._robot.pose(self._robot_id)
                    battery = self._robot.battery_percent(self._robot_id)  # battery-edge curve
            self._maybe_estop_drill()   # estop-drill: confirmed stop on a timer
            self._maybe_stream_drop()   # comms-flaky: tear the stream down on a timer
            if self._sim is not None and self._sim.telemetry_suppressed():
                time.sleep(self._telemetry_interval)
                continue
            if not live:
                self._report_transport_not_live(detail)
                # OFFLINE, with position and battery ABSENT — never defaulted (C6.2).
                self._outbox.put(pb.AdapterMessage(telemetry=pb.TelemetryPayload(
                    robot_id=self._robot_id, timestamp_ms=int(time.time() * 1000),
                    phase=pb.RobotPhase.ROBOT_PHASE_OFFLINE,
                    current_action=self._current_action,
                    hardware=self._hardware_updates())))
                # Task progress is NOT emitted while blind: it derives from the binding's
                # latest `state`, which is frozen at whatever it held when the transport
                # stopped. Reporting SUCCEEDED off a frozen snapshot tells the control
                # plane an order finished when nothing is known about it.
                time.sleep(self._telemetry_interval)
                continue
            if not self._transport_was_live:
                self._transport_was_live = True
                print("telemetry RECOVERED: the binding can observe the robot again; "
                      "position and battery are being sent.", file=sys.stderr)
            phase = (pb.RobotPhase.ROBOT_PHASE_IN_PROGRESS if self._current_action
                     else pb.RobotPhase.ROBOT_PHASE_IDLE)
            self._outbox.put(pb.AdapterMessage(telemetry=pb.TelemetryPayload(  # C6
                robot_id=self._robot_id, timestamp_ms=int(time.time() * 1000),
                position=pb.RobotPosition(x=x, y=y, floor=floor),  # C6.2 explicit presence
                battery=pb.BatteryStatus(percent=battery), phase=phase,
                current_action=self._current_action,
                hardware=self._hardware_updates())))  # hardware-fault, delta-compressed
            self._maybe_emit_task_progress()  # stream order completion / failure (C6.4)
            time.sleep(self._telemetry_interval)

    def _tick_robot(self) -> None:
        """Advance the robot (and the scenario clock) by the time that passed since the last
        tick. Not by the telemetry cadence: the loop sleeps for whatever the RegisterAck set,
        1 s or more, and a fixed step then ran the simulated vehicle slower than the clock.
        The caller holds the robot lock."""
        now = self._clock()
        dt = 0.0 if self._last_tick is None else min(max(now - self._last_tick, 0.0), _MAX_TICK_S)
        self._last_tick = now
        self._robot.tick(dt)
        take_reset = getattr(self._robot, "take_reset", None)
        if take_reset is not None and take_reset():
            self._reset_at_robot()

    def _reset_at_robot(self) -> None:
        """A person reset the vehicle: its local latch is lifted, and reported. Reached only from
        the robot side (the binding's take_reset), never from a message on any stream."""
        loc = self._latch.reset_local(self._robot_id)
        if loc is None:
            print("reset at the vehicle: the local latch was not set; nothing changed",
                  file=sys.stderr)
            return
        print("reset at the vehicle: the local latch is lifted"
              + (f"; set {loc.set_id} still waits for the EstopClear that ends it, and work stays "
                 "refused until then" if loc.set_id else ""), file=sys.stderr)
        self._send_local_report()

    def _maybe_emit_task_progress(self) -> None:
        """Stream a TERMINAL task-status transition derived from the robot's order
        state (C6.4). The binding derives SUCCEEDED (the order reached its last node,
        not driving) / FAILED (a FATAL error) from the latest ``state``; on that
        transition the adapter reports it once — echoing the executing fencing token —
        and clears the task. RUNNING is already emitted on assign, so only terminals
        are streamed. The simulated vehicle reports SUCCEEDED once it has driven to
        its target; a binding without order state is a no-op."""
        action = self._current_action
        if not action or not hasattr(self._robot, "task_state"):
            return
        outcome = getattr(self._robot, "task_outcome", None)  # the state and why, read together
        detail = getattr(self._robot, "task_detail", None)
        with self._robot_lock:
            if outcome is not None:
                state, message = outcome(self._robot_id)
            else:
                state = self._robot.task_state(self._robot_id)
                message = detail(self._robot_id) if detail is not None else ""
        if state not in (task_map.TASK_SUCCEEDED, task_map.TASK_FAILED):
            return
        with self._action_lock:
            if self._current_action != action or state == self._last_task_state:
                return  # ended or replaced meanwhile: the state read was not this action's
            token = self._current_fencing_token
            self._take_current()             # terminal: the order is complete/failed
            if state == task_map.TASK_FAILED:
                # Nothing of a failed action may go on: the order before a preemption that timed
                # out, or a held order still to go out, would run with no action and no lease.
                # Not sent when the vehicle shows nothing to cancel.
                with self._robot_lock:
                    self._robot.command_stop(self._robot_id)
            # The lease ends with the action. Left running, it lapsed some 30 s later and the
            # self-stop sent the idle vehicle a cancelOrder it answered with a warning.
            self._lease.release(self._robot_id)
            failed = state == task_map.TASK_FAILED
            self._emit_action_status(
                action, state, token, progress_pct=0 if failed else 100, message=message,
                # Rejected, a FATAL error, or a preemption that timed out: the robot could not
                # complete it.
                end_cause=self._pb.ACTION_END_CAUSE_EXECUTION_FAILURE if failed else 0)

    # ── Scenario mapping (proto only; schedule/state live on the binding) ────────

    def _hw_status_enum(self, status: str):
        pb = self._pb
        return {
            "HEALTHY": pb.HARDWARE_STATUS_HEALTHY,
            "DEGRADED": pb.HARDWARE_STATUS_DEGRADED,
            "FAILED": pb.HARDWARE_STATUS_FAILED,
        }.get(status, pb.HARDWARE_STATUS_UNSPECIFIED)

    def _hardware_updates(self) -> list:
        if self._sim is None:
            return []
        pb = self._pb
        # C6.1 — clearing the delta baseline makes every component look changed, so
        # the whole inventory is sent on the first payload of a new stream.
        if self._snapshot_pending:
            self._sim.reset_hardware_baseline()
            self._snapshot_pending = False
        return [pb.HardwareStatusUpdate(
            component_name=hs.name, status=self._hw_status_enum(hs.status),
            degradation_reason=hs.reason) for hs in self._sim.hardware_delta()]

    def _capability_components(self) -> list:
        """The robot's full hardware manifest as proto HardwareComponents. From the
        scenario manifest on the sim path; empty for the $0 skeleton. For a live
        VDA5050 robot the capability self-description arrives via the factsheet."""
        if self._sim is None:
            return []
        pb = self._pb
        return [pb.HardwareComponent(
            name=hs.name, type=hs.type, model=hs.model,
            status=self._hw_status_enum(hs.status), degradation_reason=hs.reason)
            for hs in self._sim.manifest_hardware()]

    def _capabilities_snapshot(self):
        """The full CapabilitiesSnapshot: the action catalog, plus the hardware manifest when
        there is one. The one source for both the snapshot pushed at registration and the
        scan reply, so the two cannot disagree (C12.2). For a live VDA5050 robot the
        capability self-description arrives via the factsheet (binding.request_factsheet /
        factsheet()), so the manifest is empty there -- and the catalog is still sent."""
        pb = self._pb
        return pb.CapabilitiesSnapshot(
            robot_id=self._robot_id, hardware=self._capability_components(),
            supported_actions=supported_actions(pb),
            snapshot_ms=int(time.time() * 1000))

    def _send_capabilities_snapshot(self) -> None:
        # Pushed on every path, with or without a hardware manifest: the catalog is what the
        # control plane admits assignments against, and a live robot has one too.
        self._outbox.put(self._pb.AdapterMessage(capabilities=self._capabilities_snapshot()))

    def _on_scan(self, result) -> None:
        """Answer a ScanCapabilities request with a full CapabilitiesSnapshot (§6.10),
        instead of declining it."""
        result.scan.CopyFrom(self._capabilities_snapshot())

    def _maybe_estop_drill(self) -> None:
        if self._sim is None or self._estop_drilled or not self._sim.estop_due():
            return
        self._estop_drilled = True
        pb = self._pb
        # stop_initiated_at is stamped around the call that actually commands the
        # hardware, so it attests WHEN the stop was issued. C5.3 requires it:
        # reporting STOPPED with nothing attesting a hardware stop is exactly the
        # "inferred, not confirmed" case the estop protocol forbids.
        initiated_ms = int(time.time() * 1000)
        with self._robot_lock:
            self._command_estop()
        self._interrupt_current("estop drill (scenario)", command_stop=False)  # like a real estop
        with self._robot_lock:
            state = self._await_estop()  # C5 discipline: never faked
        self._safety_outbox.put(pb.AdapterSafetyMessage(
            robot_id=self._robot_id, estop_ack=pb.EstopAck(
                estop_id=f"drill-{self._robot_id}",
                stop_initiated_at=initiated_ms,
                state=(pb.ESTOP_STATE_STOPPED if state == ESTOP_STOPPED
                       else pb.ESTOP_STATE_FAILED),
                message="estop drill (scenario)")))

    def _maybe_stream_drop(self) -> None:
        if self._sim is None or self._reconnect_pending or not self._sim.stream_down():
            return
        self._reconnect_pending = True
        call = self._active_call
        if call is not None:
            call.cancel()           # raises CANCELLED in the control loop
        self._outbox.put(None)      # unblock the request generator so it returns

    def _await_reconnect(self) -> None:
        window = self._sim.stream_drop_window() if self._sim else None
        if window is None:
            return
        _, reconnect_at = window
        while not self._shutdown.is_set():
            if self._sim.elapsed() >= reconnect_at:
                return
            time.sleep(_TELEMETRY_INTERVAL)

    # ── SafetyStream (C5) ────────────────────────────────────────────────────

    def run_safety_stream(self, stub) -> None:
        """One SafetyStream at a time, re-opened when it drops. Each that opens is told what
        is latched first (contract 1.4.0)."""
        while not self._shutdown.is_set():
            outbox: queue.Queue = queue.Queue()  # fresh per stream, as the ControlStream's

            def requests(_o=outbox) -> Iterator:
                while True:
                    msg = _o.get()
                    if msg is None:
                        return
                    yield msg

            self._safety_outbox = outbox
            self._report_latch()
            try:
                for cp in stub.SafetyStream(requests()):
                    self._on_safety_message(cp)
            except grpc.RpcError as exc:
                if self._shutdown.is_set():
                    return
                print(f"safety stream dropped ({exc.code()}); reconnecting", file=sys.stderr)
                time.sleep(0.5)
                continue
            finally:
                outbox.put(None)
            return

    def _on_safety_message(self, cp) -> None:
        kind = cp.WhichOneof("payload")
        if kind in ("estop", "estop_clear") and not self._serves(cp.robot_id):
            if kind == "estop":
                self._unserved_estop(cp.robot_id, cp.estop.estop_id, "SafetyStream estop")
            else:
                # It lifts nothing here: the latch this adapter keeps is its own robot's.
                self._log_unserved(cp.robot_id, f"estop_clear id={cp.estop_clear.estop_id}",
                                   "ignored; nothing was lifted")
            return
        if kind == "estop":
            if _at_least_14(self._negotiated):
                self._latch.on_estop(self._robot_id, cp.estop.estop_id)
            self._stop_and_ack(cp.estop.estop_id, self._safety_outbox.put)
        elif kind == "estop_clear":
            self._on_clear(cp.estop_clear)

    def _stop_and_ack(self, estop_id: str, send) -> None:
        """Interrupt the work held, command the stop and answer it: STOPPED only once rest is
        confirmed, else FAILED. Stamped around the hardware stop so the ack carries evidence
        the stop was commanded, not merely asserted (C5.3)."""
        pb = self._pb
        # The stop first: the vehicle is halted before anything is recorded or reported. Then
        # the work it held is interrupted, and then rest is confirmed.
        initiated_ms = _now_ms()
        with self._robot_lock:
            self._command_estop()
        self._interrupt_current("emergency stop", command_stop=False)
        with self._robot_lock:
            state = self._await_estop()  # C5: confirmed, never inferred
        stopped = state == ESTOP_STOPPED
        self._latch.record_state(self._robot_id, estop_id, "STOPPED" if stopped else "FAILED")
        ack = pb.EstopAck(estop_id=estop_id, stop_initiated_at=initiated_ms,
                          state=pb.ESTOP_STATE_STOPPED if stopped else pb.ESTOP_STATE_FAILED)
        if stopped:
            ack.confirmed_at_ms = _now_ms()
        send(pb.AdapterSafetyMessage(robot_id=self._robot_id, estop_ack=ack))

    def _on_fence(self, estop_id: str) -> None:
        """The fence (Command.estop) on the current ControlStream: a stop, latched; a clear
        that came first takes effect with it."""
        stop, lifted = self._latch.on_fence(self._robot_id, estop_id)
        if stop:
            self._stop_and_ack(estop_id, self._safety_outbox.put)
        if lifted:
            self._send_clear_ack(estop_id)

    def _on_clear(self, clear) -> None:
        verdict = self._latch.on_clear(self._robot_id, clear.estop_id, clear.leader_term,
                                       clear.ends_local_set)
        print(f"estop clear id={clear.estop_id} term={clear.leader_term}: {verdict}",
              file=sys.stderr)
        if verdict == "ack":
            self._send_clear_ack(clear.estop_id)
            if clear.ends_local_set:
                self._send_local_report()

    def _send_clear_ack(self, estop_id: str) -> None:
        pb = self._pb
        self._safety_outbox.put(pb.AdapterSafetyMessage(
            robot_id=self._robot_id,
            estop_clear_ack=pb.EstopClearAck(estop_id=estop_id, lifted_at_ms=_now_ms())))

    def _send_local_report(self) -> None:
        pb = self._pb
        loc = self._latch.local(self._robot_id)
        self._safety_outbox.put(pb.AdapterSafetyMessage(
            robot_id=self._robot_id, local_stop=pb.LocalStopReport(
                latched=loc.latched, changed_at_ms=loc.changed_at_ms, source="robot",
                set_id=loc.set_id)))

    def _report_latch(self) -> None:
        """Every latched stop ID as an EstopAck with the robot's state, and the local latch:
        on each SafetyStream that opens, and with each refusal under the latch."""
        pb = self._pb
        states = {"STOPPING": pb.ESTOP_STATE_STOPPING, "STOPPED": pb.ESTOP_STATE_STOPPED,
                  "FAILED": pb.ESTOP_STATE_FAILED}
        for estop_id, state in sorted(self._latch.latched_ids(self._robot_id).items()):
            self._safety_outbox.put(pb.AdapterSafetyMessage(
                robot_id=self._robot_id, estop_ack=pb.EstopAck(
                    estop_id=estop_id, state=states.get(state, pb.ESTOP_STATE_STOPPING),
                    message="latched")))
        self._send_local_report()

    def _edge_stop(self, estop_id: str, send) -> None:
        """An edge stop: the robot's local latch is set (reported at once), then the same
        confirmed stop as any other, answered on the EdgeStream."""
        self._latch.set_local(self._robot_id)
        self._send_local_report()
        self._stop_and_ack(estop_id, lambda msg: send(msg.estop_ack))

    # ── EdgeStream (C8) ──────────────────────────────────────────────────────

    def _dial_edge(self, addr: str):
        """The edge node, dialled as the control plane is: same credentials, same rule."""
        if self._dial is None:
            raise RuntimeError(f"no dialler configured; not dialling edge node {addr}")
        return self._dial(addr)

    def _edge_loop(self) -> None:
        pb = self._pb
        addr = self._edge_addr.get()  # blocks until an edge endpoint is advertised (C8.1)
        estub = self._pb_grpc.EdgeServiceStub(self._dial_edge(addr))
        outbox: queue.Queue = queue.Queue()
        # C8.2: tee one PositionFrame so the edge node has a pose to evaluate, fire-and-forget.
        with self._robot_lock:
            x, y, floor = self._robot.pose(self._robot_id)
        outbox.put(pb.AdapterEdgeMessage(position=pb.PositionFrame(
            robot_id=self._robot_id,
            position=pb.RobotPosition(x=x, y=y, floor=floor))))

        def requests() -> Iterator:
            while True:
                msg = outbox.get()
                if msg is None:
                    return
                yield msg

        for ec in estub.EdgeStream(requests()):
            if ec.WhichOneof("msg") == "estop":  # C8.3: same confirmed discipline as C5
                self._edge_stop(ec.estop.estop_id,
                                lambda ack: outbox.put(pb.AdapterEdgeMessage(estop_ack=ack)))


def dialler(args, ap):
    """How this adapter opens a channel: mutual TLS from three files (the CA the control plane's
    certificate is signed by, this adapter's client certificate, and its key), or plaintext only
    with --insecure, which is for local tests. Anything else refuses to start (ap.error exits 2)
    before a robot is brought up or a channel opened: there is no insecure default."""
    files = {"--tls-ca": args.tls_ca, "--tls-cert": args.tls_cert, "--tls-key": args.tls_key}
    if args.insecure:
        if any(files.values()) or args.tls_server_name:
            ap.error("--insecure and the --tls-* flags together: give the three TLS files, or "
                     "--insecure for a local test, not both")
        print("INSECURE: --insecure was given, so the ControlStream, SafetyStream and EdgeStream "
              "carry no TLS and no client identity. For local tests only.", file=sys.stderr)
        return lambda target: grpc.insecure_channel(target)
    missing = [flag for flag, path in files.items() if not path]
    if missing:
        ap.error("mutual TLS needs --tls-ca, --tls-cert and --tls-key (missing: "
                 + ", ".join(missing) + "); --insecure dials without TLS, for local tests only")
    data = {}
    for flag, path in files.items():
        try:
            data[flag] = pathlib.Path(path).expanduser().read_bytes()
        except OSError as err:
            ap.error(f"{flag} {path}: {err.strerror or err}")
    creds = grpc.ssl_channel_credentials(root_certificates=data["--tls-ca"],
                                         private_key=data["--tls-key"],
                                         certificate_chain=data["--tls-cert"])
    options = ([("grpc.ssl_target_name_override", args.tls_server_name)]
               if args.tls_server_name else [])
    return lambda target: grpc.secure_channel(target, creds, options=options)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="VDA5050 Fleet Adapter")
    ap.add_argument("--endpoint", default="localhost:9090")
    ap.add_argument("--robot-id", default="robot-conformance-1")
    ap.add_argument("--comms", default="simulated", choices=["simulated", "vda5050"],
                    help="robot comms: 'simulated' ($0, no broker) or 'vda5050' (MQTT/JSON)")
    ap.add_argument("--mqtt-host", default="localhost")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--manufacturer", default="swarmada",
                    help="vda5050 comms: 'manufacturer' segment of the robot's MQTT topic "
                         "identity (interfaceName/version/MANUFACTURER/serialNumber). Must "
                         "match the robot's VDA5050 identity to bind a real AMR.")
    ap.add_argument("--serial", default="amr-1",
                    help="vda5050 comms: 'serialNumber' segment of the robot's MQTT topic "
                         "identity. Must match the robot's VDA5050 serial to bind a real AMR.")
    ap.add_argument("--scenario", default="healthy-fleet",
                    help="simulated-comms scenario preset name or path (default: healthy-fleet)")
    ap.add_argument("--fault-component", default="camera_front",
                    help="hardware-fault: component to degrade (default: camera_front)")
    ap.add_argument("--fault-at", type=float, default=30.0,
                    help="hardware-fault: seconds until the component degrades (default: 30)")
    ap.add_argument("--recover-at", type=float, default=90.0,
                    help="hardware-fault: seconds until the component recovers (default: 90)")
    ap.add_argument("--namespace", default="",
                    help="the namespace to declare in AdapterHello (default: none, so the control plane takes "
                         "it from this adapter's client certificate)")
    ap.add_argument("--state-dir", default="",
                    help="where the last action's terminal outcome is kept until the control "
                         "plane acknowledges it (contract 1.4.0), so a restart does not lose "
                         "it. Unset: kept in memory only, and a restart loses it")
    ap.add_argument("--tls-ca", default="",
                    help="the CA certificate (PEM) the control plane's server certificate is "
                         "signed by")
    ap.add_argument("--tls-cert", default="", help="this adapter's client certificate (PEM)")
    ap.add_argument("--tls-key", default="", help="this adapter's client key (PEM)")
    ap.add_argument("--tls-server-name", default="",
                    help="the name to check the server certificate against, when it is not the "
                         "endpoint's host")
    ap.add_argument("--insecure", action="store_true",
                    help="dial without TLS. For local tests only; never the default")
    args = ap.parse_args(argv)
    dial = dialler(args, ap)

    from fleet_adapter.v1 import fleet_adapter_pb2 as pb
    from fleet_adapter.v1 import fleet_adapter_pb2_grpc as pb_grpc

    if args.comms == "vda5050":
        from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding
        robot = Vda5050Binding(robot_id=args.robot_id,
                               manufacturer=args.manufacturer, serial=args.serial,
                               mqtt_host=args.mqtt_host, mqtt_port=args.mqtt_port)
    else:
        # Scenarios are additive tooling, not a safety requirement (ADR-0005): if
        # scenario support is unavailable (e.g. PyYAML missing) run the $0 skeleton.
        # A bad --scenario *name* is still a hard error.
        engine = None
        try:
            from fleet_adapter_vda5050.scenarios import (
                HardwareFaultOverrides,
                ScenarioEngine,
                load_scenario,
            )
        except ImportError as exc:
            print(f"scenario support unavailable ({exc}); running without a scenario. "
                  f"Install pyyaml to enable --scenario.", file=sys.stderr)
        else:
            scenario = load_scenario(args.scenario, HardwareFaultOverrides(
                component=args.fault_component, fault_at=args.fault_at, recover_at=args.recover_at))
            engine = ScenarioEngine(scenario)
        robot = SimulatedRobot(engine)  # $0 skeleton; scenario-driven when an engine is set
    adapter = fleet_adapter_vda5050Adapter(pb, pb_grpc, robot, args.robot_id,
                                           state_dir=args.state_dir or None, dial=dial,
                                           namespace=args.namespace)
    if args.comms == "simulated":
        # The simulated vehicle's reset: a local command at the vehicle, never a message.
        signal.signal(signal.SIGUSR1, lambda signum, frame: robot.request_reset())
        print(f"reset at the vehicle (lifts the local latch an edge stop set): send SIGUSR1 to "
              f"process {os.getpid()} (kill -USR1 {os.getpid()}; in a container, "
              f"docker kill --signal=USR1 <container>)", file=sys.stderr)
    print("retained outcome: " + (f"kept in {args.state_dir}" if args.state_dir else
                                  "kept in memory only (no --state-dir): a restart loses it"),
          file=sys.stderr)

    channel = dial(args.endpoint)
    stub = pb_grpc.FleetAdapterServiceStub(channel)
    threading.Thread(target=adapter.run_safety_stream, args=(stub,), daemon=True).start()  # C1.1
    try:
        adapter.run_control_stream(stub)
    finally:
        # The teardown half of the lifecycle the Protocol now declares. Before the lifecycle fix
        # there was no `stop()` anywhere in this adapter, so a `--comms vda5050` run left
        # paho's network thread and the broker session to the interpreter's exit. Calling
        # it here makes the shutdown explicit at the level that owns the robot; `stop()`
        # is idempotent and safe when `start()` never ran.
        robot.stop()


if __name__ == "__main__":
    main()
