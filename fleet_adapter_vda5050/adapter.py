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
import math
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
    EstopToken,
    FenceDecision,
    FenceGuard,
    LeaseMonitor,
    await_estop_confirmation,
)

from fleet_adapter_vda5050 import __version__
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.latch import StopLatch
from fleet_adapter_vda5050.process import FleetProcess
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
# The wait for a vehicle's own confirmation of a stop: the estop's budget
# (swarmada_sdk.safety.await_estop_confirmation's default, 40 x 0.05 s).
_STOP_CONFIRM_TICK_S = 0.05
_STOP_CONFIRM_TICKS = 40

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


def _is_superseded(exc) -> bool:
    """A stream the control plane closed because it accepted a newer one under this adapter
    identity: ABORTED, "superseded: ..." (RFC-0001 #fleet-adapter-protocol-connection-
    lifecycle). Any other close is a drop, and is redialled."""
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    if not callable(code) or not callable(details):
        return False
    return code() == grpc.StatusCode.ABORTED and "superseded" in (details() or "")


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


class _Streams:
    """What belongs to the process's streams rather than to one vehicle: the ControlStream and
    SafetyStream outboxes, the session count, the contract version the hello agreed, the
    shutdown, the dialler and the reconnect state. Every vehicle served on these streams shares
    one; a vehicle reads and writes it through attributes of the same names (_Shared)."""

    def __init__(self, dial=None, namespace: str = "") -> None:
        # The namespace in AdapterHello: empty takes it from the client certificate, the control
        # plane's binding. A name that differs from the certificate's is refused.
        self.namespace = namespace
        # How a channel is opened, ControlStream and EdgeStream alike: mutual TLS, or plaintext
        # only when --insecure said so (main builds it). None dials nothing.
        self.dial = dial
        self.outbox: queue.Queue = queue.Queue()
        self.safety_outbox: queue.Queue = queue.Queue()  # → SafetyStream (acks + drills)
        self.negotiated = ""
        self.shutdown = threading.Event()
        # The ControlStream session, counted. The provisioning loop registers again only on the
        # session that was refused; a newer session registers on its own (C2.4).
        self.session = 0
        self.reconnect_pending = False
        self.active_call = None
        # Set once one of these streams is closed as superseded by another process under this
        # adapter identity (_on_superseded): every vehicle is stopped, no work is held or taken
        # for any of them, and no stream is opened again. Of the streams, so of every vehicle.
        self.superseded = threading.Event()
        self.superseded_lock = threading.Lock()
        self.superseded_why = ""


class _Shared:
    """An attribute of the vehicle that lives on its _Streams: ``self._outbox`` reads and
    writes ``self._streams.outbox``."""

    def __set_name__(self, owner, name: str) -> None:
        self._key = name.lstrip("_")

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return getattr(obj._streams, self._key)

    def __set__(self, obj, value) -> None:
        setattr(obj._streams, self._key, value)


def _no_wait(_seconds: float) -> None:
    """The SDK's sleep for one look at a stop: the wait between looks is the adapter's own,
    taken with the robot lock released (_await_estop)."""


class _Lane:
    """One vehicle's worker. The part of a stop that waits (rest confirmed, then the answer)
    runs here, in the order the stops arrived, so the stream reader that began the stop goes on
    to the next message at once: with fifty vehicles on one SafetyStream, the fiftieth estop is
    commanded when it arrives, not after the waits of the forty-nine before it. Until the
    process starts it, a job runs in its caller, as every stop did before lanes."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._jobs: queue.Queue | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._jobs is not None:
                return
            self._jobs = queue.Queue()
            threading.Thread(target=self._run, args=(self._jobs,), name=f"lane-{self._name}",
                             daemon=True).start()

    def submit(self, job) -> None:
        jobs = self._jobs
        if jobs is None:
            job()
        else:
            jobs.put(job)

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until every job submitted so far has run. True if they all did in time."""
        done = threading.Event()
        self.submit(done.set)
        return done.wait(timeout)

    def _run(self, jobs: queue.Queue) -> None:
        while True:
            job = jobs.get()
            try:
                job()
            except Exception as exc:  # noqa: BLE001 - one job's fault must not end the lane
                print(f"error: vehicle {self._name}: a job on its lane raised {exc!r}",
                      file=sys.stderr, flush=True)


class fleet_adapter_vda5050Adapter:
    """One vehicle: its binding, its action, fence, lease, latch and retained outcome, and the
    logic that serves it. The streams it is served on are a _Streams, its own unless one is
    given."""

    _namespace = _Shared()
    _dial = _Shared()
    _outbox = _Shared()
    _safety_outbox = _Shared()
    _negotiated = _Shared()
    _shutdown = _Shared()
    _session = _Shared()
    _reconnect_pending = _Shared()
    _active_call = _Shared()
    _superseded = _Shared()
    _superseded_lock = _Shared()
    _superseded_why = _Shared()

    def __init__(self, pb, pb_grpc, robot, robot_id: str, state_dir=None, dial=None,
                 namespace: str = "", streams: _Streams | None = None) -> None:
        self._streams = streams if streams is not None else _Streams(dial, namespace)
        # The process this vehicle is served by (FleetProcess sets it). A vehicle built without
        # streams is a process of one, built at the end of __init__.
        self._process: FleetProcess | None = None
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
        # The stops commanded and not yet answered, under _robot_lock: counted with the command,
        # uncounted once the answer is sent. While any is in flight, no work is taken and no
        # pause is lifted, at every contract version (_on_assign, _on_resume).
        self._stops_in_flight = 0
        # Whether the streams were superseded (_on_superseded) is the streams' own
        # (_Streams.superseded): every vehicle served on them reads it.
        self._robot_id = robot_id
        # Where this vehicle's stops wait for rest and are answered (_stop_and_ack).
        self._lane = _Lane(robot_id)
        self._fence = FenceGuard()                              # C3
        self._lease = LeaseMonitor(on_expiry=self._self_stop)   # C4
        self._current_action = ""
        self._current_generation = 0
        self._current_fencing_token = None   # echoed on task-status streaming (C6.4)
        self._last_task_state = ""           # last streamed TaskState, for transition detect
        # Contract 1.4.0: the stop latch, the retained outcome, and the work a stop interrupted
        # (action ID -> the fencing token it was assigned with; its re-delivery starts nothing).
        self._latch = StopLatch()
        self._retained = RetainedOutcomes(state_dir)
        self._interrupted: dict[str, int] = {}
        self._action_tokens: dict[str, int] = {}
        # A superseded action's report waits for the vehicle's confirmation of its stop, on a
        # thread of its own (_end_superseded); kept so a test can join it.
        self._superseded_threads: list[threading.Thread] = []
        self._stop_confirm_sleep = time.sleep
        self._telemetry_started = False
        # True only between an ACCEPTED RegisterAck and the end of that ControlStream session.
        # A robot whose registration was refused, or not yet answered, is not under management
        # on this session, and no work is accepted for it (C13.2).
        self._registered = False
        # The outbox of the session whose registration was accepted. A report sent from another
        # thread goes out only on it, and only while it is still the current session's: the
        # reconnect loop installs a new session's outbox before that session registers (C2.1).
        self._registered_outbox: queue.Queue | None = None
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
        # Transport-liveness log state: True so the FIRST degrade is announced, and so
        # the announcement is a transition rather than 5 lines a second.
        self._transport_was_live = True
        self._clock = time.monotonic
        self._last_tick: float | None = None
        if streams is None:
            FleetProcess([self])

    # ── ControlStream ────────────────────────────────────────────────────────

    def _control_requests(self, outbox: queue.Queue | None = None) -> Iterator:
        """The session's requests: the hello, then what is put on ITS outbox. The outbox is
        bound when the session opens, so a dying session's generator cannot take a message
        put on the next session's outbox (a lost RegisterRobot, C2.4)."""
        pb = self._pb
        if outbox is None:
            outbox = self._outbox
        yield pb.AdapterMessage(hello=pb.AdapterHello(  # C1.2
            vendor="vda5050", adapter_version=__version__,
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
        """Run the process this vehicle is served by: bring every vehicle up, then the
        ControlStream."""
        self._process.run_control_stream(stub)

    def _run_control_stream(self, stub) -> None:
        """The ControlStream loop, run by the process's first vehicle; every message goes to
        the process (_handle_control_plane)."""
        self.bring_up_robot()

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
                    with self._superseded_lock:
                        # Under the lock a supersession is recorded under, so one the other
                        # stream recorded, during the backoff or before it, is seen here and
                        # nothing is dialled. A redial would take the streams back from the
                        # process that took over, after this one released MQTT: the robot's
                        # stops would then come to a process that can't deliver them.
                        if self._superseded.is_set():
                            break
                        outbox: queue.Queue = queue.Queue()
                        self._outbox = outbox
                        self._new_session()
                        stream = stub.ControlStream(self._control_requests(outbox))
                        self._active_call = stream  # closed by a SafetyStream supersession
                    for cp in stream:
                        self._handle_control_plane(cp, stub)
                except grpc.RpcError as exc:
                    if self._shutdown.is_set():
                        break
                    if _is_superseded(exc):
                        self._on_superseded("ControlStream", exc)
                    if self._superseded.is_set():
                        # Never redialled: another process serves this identity now. Up, and
                        # idle, until the process is terminated.
                        self._shutdown.wait()
                        break
                    print(f"control stream dropped ({exc.code()}); reconnecting", file=sys.stderr)
                    time.sleep(0.5)
                    continue
                break  # clean close (response iterator ended without error)
            self._park_if_superseded()
            self._shutdown.set()
            return

        # comms-flaky: reconnect across a scenario-driven stream drop. Each pass is a
        # full Hello/Register handshake, exercising the RegisterRobot reconnect and the
        # persisted fencing/lease state (C3/C4) across the outage.
        while not self._shutdown.is_set():
            self._reconnect_pending = False
            with self._superseded_lock:  # as above: nothing is dialled after a supersession
                if self._superseded.is_set():
                    break
                # A fresh outbox per session, as above: what the dropped session left queued
                # is not sent ahead of the new session's RegisterRobot.
                outbox = queue.Queue()
                self._outbox = outbox
                self._new_session()
                call = stub.ControlStream(self._control_requests(outbox))
                self._active_call = call
            try:
                for cp in call:
                    self._handle_control_plane(cp, stub)
            except grpc.RpcError as exc:  # the drop surfaces as a CANCELLED RPC
                if _is_superseded(exc):
                    self._on_superseded("ControlStream", exc)
                if self._superseded.is_set():
                    self._shutdown.wait()
                    break
                if not self._reconnect_pending:
                    raise
                print(f"control stream dropped ({exc.code()}); reconnecting", file=sys.stderr)
            finally:
                self._active_call = None
            if self._shutdown.is_set() or not self._reconnect_pending:
                break
            self._await_reconnect()
        self._park_if_superseded()
        self._shutdown.set()

    def _new_session(self) -> None:
        """A ControlStream session opens: nothing is registered on it yet, for any vehicle."""
        self._process.new_session()

    def _handle_control_plane(self, cp, stub) -> None:
        """A message on the ControlStream, for the process to route."""
        self._process.handle_control_plane(cp, stub)

    def _on_hello_ack(self, stub) -> None:
        """The hello is accepted (the process has kept the contract version): register."""
        self._process.send_register(self, self._outbox, self._session)  # C2.1
        # A fresh ControlStream: the next payload must be a full snapshot (C6.1).
        self._snapshot_pending = True
        if not self._telemetry_started:  # C6: stream telemetry once registered
            self._telemetry_started = True
            threading.Thread(target=self._telemetry_loop, args=(stub,), daemon=True).start()

    def _on_register_ack(self, ack) -> None:
        """The RegisterAck that answers this vehicle's register."""
        if not ack.accepted:
            self._on_register_refused(ack)
            return
        self._registered = True
        self._registered_outbox = self._outbox
        # Registered: the catalog, and any hardware manifest (C12.2), and a full snapshot
        # next (C6.1). Sent here rather than at the hello, because a robot admitted through
        # the provisioning loop had no Robot when the hello came, and what was sent for it
        # then was not kept.
        self._send_capabilities_snapshot()
        self._snapshot_pending = True
        # C8.1: this vehicle is served through the edge nodes this ack lists, and no others.
        self._process.edge_endpoints(self, [e.address for e in ack.edge_endpoints])
        # C2.3 — adopt the per-robot cadence the control plane returned. Only a
        # POSITIVE value is a cadence: 0 means "not specified" and a negative value
        # is malformed — clamping that to the 1s minimum would let a buggy control
        # plane force the fastest permitted rate. Legitimate values are clamped to
        # the protocol's 1-30s range.
        secs = ack.telemetry_interval_seconds
        if secs > 0:
            self._telemetry_interval = max(1.0, min(30.0, float(secs)))
        # The one line that says the robot is now under management on this session: until
        # it, work for the robot is refused here (C13.2) and the control plane cannot push it.
        print(f"REGISTERED robot={self._robot_id}; under management on this session "
              f"(contract {self._negotiated or 'not negotiated'}, telemetry every "
              f"{self._telemetry_interval:g} s)", file=sys.stderr, flush=True)
        self._reconcile_action_state(ack)
        self._send_retained()

    def _on_discover_ack(self, da) -> None:
        """The DiscoverAck that answers this vehicle's announcement. The robot is not under
        management until a register is accepted."""
        if not da.accepted:
            print(f"announce refused robot={self._robot_id}: {da.message}", file=sys.stderr)
        self._discover_ttl = da.ttl_seconds
        self._discover_acked.set()

    def _on_outcome_ack(self, oa) -> None:
        """An ActionOutcomeAck, offered to every vehicle: it clears only the outcome this
        vehicle kept under that action id and fencing token."""
        if self._retained.acknowledge(self._robot_id, oa.action_id, oa.fencing_token):
            print(f"retained outcome acknowledged: action={oa.action_id}", file=sys.stderr)

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
        self._process.send_discover(self, self._outbox, self._session, pb.AdapterMessage(
            discover=pb.DiscoverRobot(robot_id=self._robot_id,
                                      hardware=self._capability_components())))
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
        if self._registered:
            return
        self._process.send_register(self, outbox, session)  # nothing once the session is replaced

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
            if self._on_cancel(command.cancel_action, result):
                return  # answered later, off this reader (_finish_cancel)
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
        print(f"error: UNSERVED robot={self._process.served_text()} addressed={received!r} {what}: "
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
               f"{self._process.served_text()}; nothing was done")
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
               f"{self._process.served_text()}")
        unregistered = self._process.unregistered()
        if not unregistered:
            self._log_unserved(received, f"{via} id={estop_id}",
                               "answered FAILED; no stop was commanded")
            ack.message = f"{why}; no stop was commanded and none can be confirmed"
        else:
            # Every vehicle with no accepted registration is stopped: any of them may be the
            # one the stop was meant for, under an id this adapter doesn't know it by. Each is
            # commanded before any is interrupted.
            ack.stop_initiated_at = _now_ms()
            for c in unregistered:
                with c._robot_lock:
                    c._command_estop()  # its token is not needed: this stop is not confirmed
            for c in unregistered:
                c._interrupt_current("emergency stop under a robot_id this adapter does not "
                                     "serve", command_stop=False)
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
            if self._superseded.is_set():
                # The vehicle is handed over unpaused, so the process that took over can give
                # it work; and after MQTT is released a pause would publish nothing.
                result.pause.CopyFrom(pb.PauseResult(paused=False,
                                                     message=self._superseded_why))
                return
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
        if self._superseded.is_set():
            result.resume.CopyFrom(pb.ResumeResult(resumed=False, message=self._superseded_why))
            return
        if self._latch.refuses_work(self._robot_id) or self._stops_in_flight:
            result.resume.CopyFrom(pb.ResumeResult(resumed=False, message="under a stop"))
            return
        with self._robot_lock:
            # Again where it counts: a stop commanded since the check above must not be lifted.
            if (self._estop_epoch != epoch or self._stops_in_flight
                    or self._latch.refuses_work(self._robot_id)):
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
        if self._stops_in_flight:
            # A stop is commanded and not yet answered: refused whatever the fencing token, at
            # every contract version. Below 1.4.0 nothing else would refuse it, and an order
            # taken now would follow the stop and drive on.
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE,
                message="under a stop: in flight"))
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
        if decision is FenceDecision.REACK:
            # C3.4: an identical re-delivery of the current assignment is re-acked, and nothing is
            # started again (an assignment is never executed twice). Before the availability
            # check: refusing it while the vehicle cannot be observed would tell the control plane
            # this robot is not doing the work it may still be doing, and the plane would give
            # it to another robot. The lease is granted again, as the reference adapter does,
            # but only while the action is still the one running here.
            if task.lease_duration_ms and task.action_id and task.action_id == self._current_action:
                self._current_generation = task.lease_generation
                self._lease.grant(self._robot_id, task.lease_duration_ms / 1000.0,
                                  task.lease_generation)
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=True, accepted_fencing_token=task.fencing_token))
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
        # With a route graph (--route-graph) a goal off the lanes is refused: the order's last
        # leg would leave them. A binding without goal_refusal routes nothing, so it has none.
        goal_refusal = getattr(self._robot, "goal_refusal", None)
        refusal = goal_refusal(self._robot_id, goal.x, goal.y) if goal_refusal else ""
        if refusal:
            result.assign_action.CopyFrom(pb.AssignActionResult(
                accepted=False, rejection=pb.ASSIGN_ACTION_REJECTION_INVALID_ACTION,
                message=refusal))
            return
        with self._robot_lock:
            if self._estop_epoch != epoch or self._stops_in_flight:
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
          stopPause "can also restart vehicles that were stopped with a hardware button").
        * With a route graph, the vehicle's position is unknown: an order starts from it
          (``route_unavailable``, vda5050_binding.py)."""
        if self._superseded.is_set():
            return self._superseded_why
        if not self._registered:
            return "this robot's registration was not accepted on this session"
        # The vehicle cannot be observed: its connection is OFFLINE or CONNECTIONBROKEN, it has
        # sent no state within the lapse, or the broker session is down. An order now goes to
        # a topic nobody reads, and the control plane, which keeps such a robot Idle and Ready
        # by design, would hold the work on it. Refused, so the plane reschedules it.
        if not self._robot.transport_alive():
            detail = self._transport_detail()
            return "the vehicle is not connected" + (f": {detail}" if detail else "")
        paused = getattr(self._robot, "paused_at_vehicle", None)  # a binding written before it
        unroutable = getattr(self._robot, "route_unavailable", None)  # a binding that routes
        with self._robot_lock:
            if paused is not None and paused(self._robot_id):
                return "paused at the vehicle"
            why = unroutable(self._robot_id) if unroutable is not None else ""
            if why:
                return why
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
        newer_generation = False
        if phase in (pb.ROBOT_ACTION_PHASE_IN_PROGRESS, pb.ROBOT_ACTION_PHASE_REVOKING):
            local = self._current_action
            local_token = self._action_tokens.get(local) if local else None
            if st.action_id and st.action_id == local:
                if st.fencing_token and st.fencing_token == local_token:
                    # The same action under the same token: IN_PROGRESS keeps executing under
                    # the same lease; REVOKING keeps it too, and it self-stops here when that
                    # lease runs out.
                    return
                # The same name under another token: the control plane gave the action a newer
                # generation (requeued and assigned again while this adapter was away), which
                # this adapter never accepted and which may not be the work it is running; a
                # REVOKING reply revokes that newer generation, not this run. Never run under a
                # generation not given: stopped, and reported under the reply's token like any
                # other divergence.
                newer_generation = True
                print(f"reconcile: the registration reply names {local} "
                      f"{pb.RobotActionPhase.Name(phase).removeprefix('ROBOT_ACTION_PHASE_')} "
                      f"under token {st.fencing_token or 'none'}, and it runs here under token "
                      f"{local_token}: a generation this adapter never accepted; the vehicle is "
                      f"stopped", file=sys.stderr)
                phase = pb.ROBOT_ACTION_PHASE_CANCELLED  # divergence is treated as a cancel
            elif phase == pb.ROBOT_ACTION_PHASE_IN_PROGRESS:
                phase = pb.ROBOT_ACTION_PHASE_CANCELLED  # another action in progress: divergence
        named = st.action_id
        if phase == pb.ROBOT_ACTION_PHASE_REVOKING:
            # REVOKING naming another action: only a local action, which it does not name,
            # goes on below (the same name was decided above).
            if not self._current_action:
                return
        elif phase not in (pb.ROBOT_ACTION_PHASE_CANCELLED, pb.ROBOT_ACTION_PHASE_UNKNOWN,
                           pb.ROBOT_ACTION_PHASE_IDLE):
            return
        with self._action_lock:
            local, local_token = self._take_current()
            with self._robot_lock:
                self._robot.command_stop(self._robot_id)
                since = self._state_seq_now()
            self._lease.release(self._robot_id)
            # UNKNOWN naming the local action: the plane could not read it. Stopped above, and
            # ended like a run the reply does not name, so it gets a report under its own token
            # and its token entry goes.
            own_unknown = bool(local) and named == local and phase == pb.ROBOT_ACTION_PHASE_UNKNOWN
            if local and (local != named or own_unknown):
                # A local action the reply does not name is the plane's past: the robot is
                # bound to another action, or to none. Stopped above; reported once the
                # vehicle's own state confirms the stop (_end_superseded).
                self._action_tokens.pop(local, None)
                self._end_superseded_async(local, local_token, st.phase, named, since)
            if phase == pb.ROBOT_ACTION_PHASE_REVOKING:
                return  # the named action is not held here: nothing to stop or report
            reported = "" if own_unknown else named
            if reported:
                state = (task_map.TASK_CANCELLED if phase == pb.ROBOT_ACTION_PHASE_CANCELLED
                         else task_map.TASK_STOPPED)
                # C6.4: every report carries the token of the action it names: here the action
                # the reply names, under the reply's token, its current one. The action may be one
                # this adapter never received (an assignment made before this register was
                # processed), which has no token here, and a report without one is refused as an
                # adapter fault. Without one in the reply, its own token if held. No other token
                # is put on a report, so it can only match its own action.
                token = st.fencing_token or None
                if newer_generation:
                    # Not kept for a later connection: the action is live at that generation, and
                    # the plane neither reads nor acknowledges a kept CANCELLED for a live action,
                    # so it would be sent again on every reconnect.
                    self._action_tokens.pop(reported, None)
                elif token is not None and reported in self._action_tokens:
                    # Kept under the reply's token, the only one the plane acknowledges.
                    self._action_tokens[reported] = token
                self._emit_action_status(reported, state, token)

    def _state_seq_now(self) -> int | None:
        """The binding's state count, read right after a stop was sent (the caller holds the
        robot lock), or None when it has none (a simulator) or it does not answer an int."""
        state_seq = getattr(self._robot, "state_seq", None)
        if not callable(state_seq):
            return None
        try:
            seq = state_seq(self._robot_id)
        except Exception:  # noqa: BLE001 - the stop is sent; the wait then counts from None
            return None
        return seq if type(seq) is int else None

    def _end_superseded_async(self, action: str, token: int | None, reply_phase: int,
                              named: str, since: int | None) -> None:
        """Report a superseded local action once its stop is confirmed or the budget is spent,
        on a thread of its own: the wait must not hold the ControlStream's reader."""
        self._superseded_threads = [t for t in self._superseded_threads if t.is_alive()]
        t = threading.Thread(target=self._end_superseded,
                             args=(action, token, reply_phase, named, since), daemon=True)
        self._superseded_threads.append(t)
        t.start()

    def _end_superseded(self, action: str, token: int | None, reply_phase: int, named: str,
                        since: int | None) -> None:
        """The registration reply names another action, or none, or names ``action`` UNKNOWN (the
        plane could not read it), while this adapter ran ``action``; the vehicle was stopped (a
        cancelOrder). The action is reported STOPPED
        only once a state the vehicle sent after the stop confirms it (the estop's rule: a
        state counted after the stop, and the vehicle at rest); if none does within the stop
        budget, or new work is taken meanwhile, FAILED with the reason, which also says the
        outcome is unknown when the reply names ``action`` UNKNOWN. Always one report, and never silence: whatever goes wrong while waiting becomes the
        reason.

        Under the action's own token, and never retained or written to the token record: the
        plane has moved on, a kept outcome under an old generation is never acknowledged, and
        the same action id may already be running again under a newer token (_report_only).
        End cause SUPERSEDED with the action that displaced it when the reply names one; when it
        names none there is no displacing action, so no cause is stated (the proto: leave it
        unset rather than guess)."""
        pb = self._pb
        own_unknown = named == action and reply_phase == pb.ROBOT_ACTION_PHASE_UNKNOWN
        if own_unknown:
            why = ("the control plane could not read this action: its registration reply names "
                   "it UNKNOWN")
        else:
            what = (f"names {named} ({pb.RobotActionPhase.Name(reply_phase)})" if named
                    else "names no action")
            why = f"superseded: the control plane's registration reply {what}"
        try:
            failure = self._confirm_stop(since)
        except Exception as exc:  # noqa: BLE001 - never silence: the error becomes the reason
            failure = f"the stop could not be confirmed: {exc!r}"
        print(f"reconcile: {action} (token {token}) {why}; the vehicle was stopped and "
              + ("its state confirms it" if not failure else f"the stop was not confirmed: {failure}"),
              file=sys.stderr)
        # Nothing displaced an action the plane could not read: no cause, as when none is named.
        cause = (pb.ACTION_END_CAUSE_SUPERSEDED if named and not own_unknown
                 else pb.ACTION_END_CAUSE_UNSPECIFIED)
        if not failure:
            self._report_only(action, task_map.TASK_STOPPED, token, why, cause, named)
        else:
            reason = f"{why}; the stop was not confirmed: {failure}"
            if own_unknown:
                # The plane could not read the action and the stop is not confirmed: nobody can
                # establish what the run achieved. The v0.4 control plane drops
                # ACTION_STATE_UNKNOWN, and RFC-0001 has an adapter report FAILED itself, with a
                # message saying the outcome is unknown: that message is the only record that
                # the run may have done the job.
                reason += "; the outcome is unknown: the run may have done its work"
            self._report_only(action, task_map.TASK_FAILED, token, reason, cause, named)

    def _report_only(self, action: str, state_name: str, token: int | None, message: str,
                     end_cause: int, superseding: str) -> None:
        """Send one ActionStatusUpdate and nothing else: no retained outcome, and the token
        record (_action_tokens) untouched, so a newer run under the same action id keeps its
        own. Under the action lock, as every report of an ended action. Sent only on the session
        whose registration was accepted, while it is current; otherwise dropped (C2.1)."""
        pb = self._pb
        upd = pb.ActionStatusUpdate(action_id=action, state=getattr(pb, state_name), message=message)
        if token is not None:
            upd.fencing_token = token
        if end_cause:
            upd.end_cause = end_cause
            if end_cause == pb.ACTION_END_CAUSE_SUPERSEDED:
                upd.superseding_action_id = superseding
        with self._action_lock:
            outbox = self._outbox
            if not (self._registered and self._registered_outbox is outbox):
                # C2.1: nothing about the robot goes on a session before its registration is
                # accepted. A report that would is dropped, and said so.
                print(f"reconcile: the {pb.ActionState.Name(upd.state)} report of {action} is "
                      f"dropped: this session's registration has not been accepted (C2.1)",
                      file=sys.stderr)
                return
            outbox.put(pb.AdapterMessage(action_status=upd))

    def _on_cancel(self, cancel, result) -> bool:
        """A cancel: answered for what was witnessed, never for what was only commanded.

        STOPPED_SAFELY tells the control plane the robot is off the action, and it may then
        give the action to another robot at once (its executor guard). Otherwise the answer is
        CANCEL_DISPOSITION_STOP_UNCONFIRMED (contract 1.4.0), or acknowledged=false to a
        control plane below 1.4.0, which has no such answer.

        * The action it names is the one running: it is stopped, and STOPPED_SAFELY once a
          state counted after the stop shows the vehicle at rest and the broker has accepted
          the cancelOrder, within the estop's budget (_confirm_stop).
        * C3.9: it names an action the vehicle no longer runs, replaced in place by the one
          running. Nothing is stopped: that would abort the action that replaced it. It is
          STOPPED_SAFELY once a state received after the cancel shows the vehicle on the
          replacing order, which shows it off the one named (_witness_replaced).

        One look is taken here, on the reader. If it doesn't settle the answer, the rest of the
        wait and the answer run on a thread of their own, so no command behind the cancel waits
        for it, a fence least of all. Returns True when the answer is left to that thread."""
        replaced = ""
        with self._action_lock:
            named = cancel.action_id
            if named and self._current_action and named != self._current_action:
                replaced = self._current_action
                with self._robot_lock:
                    since = self._state_seq_now()
            else:
                cancelled, _ = self._take_current()
                with self._robot_lock:
                    self._robot.command_stop(self._robot_id)
                    since = self._state_seq_now()
                    # This stop's own cancelOrder: a later stop's can't answer for it.
                    ticket = getattr(self._robot, "stop_cancel", None)
                    ticket = ticket(self._robot_id) if ticket else None
                self._lease.release(self._robot_id)
                if cancelled:
                    self._emit_action_status(cancelled, task_map.TASK_CANCELLED, None)
        if replaced:
            action, cause = named, cancel.cause
            if getattr(self._robot, "runs_last_order", None) is None:
                self._answer_cancel(result, action, f"this vehicle can't report which order it "
                                    f"runs, so it can't be witnessed off this one for {replaced}",
                                    cause=cause)
                return False

            def confirm(looks: int = _STOP_CONFIRM_TICKS) -> str:
                return self._witness_replaced(since, replaced, looks)
        else:
            action, cause = named or cancelled, cancel.cause
            accepted = None
            if ticket is not None:
                def accepted(_robot_id: str) -> bool:
                    return self._robot.cancel_accepted(ticket)

            def confirm(looks: int = _STOP_CONFIRM_TICKS) -> str:
                return self._confirm_stop(since, accepted=accepted, looks=looks)
        if not confirm(looks=1):
            self._answer_cancel(result, action, "", cause=cause)
            return False
        threading.Thread(target=self._finish_cancel,
                         args=(result, action, confirm, cause, self._outbox),
                         daemon=True).start()
        return True

    def _finish_cancel(self, result, action: str, confirm, cause, outbox) -> None:
        """The rest of a cancel's wait, and its answer, off the reader. The answer goes on the
        outbox of the session the cancel came on: a newer session never had this cancel."""
        try:
            why = confirm()
        except Exception as exc:  # noqa: BLE001 - a cancel is answered, never left silent
            why = f"the wait raised {exc!r}"
        self._answer_cancel(result, action, why, cause=cause)
        outbox.put(self._pb.AdapterMessage(command_result=result))

    def _witness_replaced(self, since: int | None, replaced: str, looks: int) -> str:
        """C3.9: "" once a state received after the cancel (``state_seq`` > ``since``) shows
        the vehicle on the order the binding sent last, the one that replaced the action
        named, and none held behind a cancel (``runs_last_order``); else why not, within the
        estop's budget. A state that still shows the old order, or comes from before the
        cancel, witnesses nothing."""
        on_order = self._robot.runs_last_order
        state_seq = getattr(self._robot, "state_seq", None)
        if not callable(state_seq) or since is None:
            return "the vehicle's state count could not be read after the cancel"
        for look in range(looks):
            if look:
                self._stop_confirm_sleep(_STOP_CONFIRM_TICK_S)
            with self._robot_lock:
                seen = state_seq(self._robot_id)
                if type(seen) is int and seen > since and on_order(self._robot_id):
                    return ""
        budget = _STOP_CONFIRM_TICKS * _STOP_CONFIRM_TICK_S
        return (f"no state after the cancel showed the vehicle on the order of {replaced}, "
                f"which replaced it, within {budget:g} s")

    def _answer_cancel(self, result, action: str, why: str, cause=0) -> None:
        """STOPPED_SAFELY when ``why`` is empty (rest witnessed); else not confirmed."""
        pb = self._pb
        if not why:
            answer = pb.CancelActionResult(acknowledged=True,
                                           disposition=pb.CANCEL_DISPOSITION_STOPPED_SAFELY)
        else:
            message = f"the stop was commanded and not confirmed: {why}"
            print(f"error: cancel of {action or '(no action)'} for {self._robot_id!r}: "
                  f"{message}", file=sys.stderr, flush=True)
            if _at_least_14(self._negotiated):
                answer = pb.CancelActionResult(
                    acknowledged=True, disposition=pb.CANCEL_DISPOSITION_STOP_UNCONFIRMED,
                    message=message)
            else:
                answer = pb.CancelActionResult(acknowledged=False, message=message)
        if cause:
            answer.cause = cause
        result.cancel_action.CopyFrom(answer)

    def _confirm_stop(self, since: int | None, accepted=None,
                      looks: int = _STOP_CONFIRM_TICKS) -> str:
        """Wait for the vehicle's own confirmation of a cancelOrder stop: "" when a state
        counted after the stop (``state_seq`` > ``since``) shows it at rest
        (``stop_confirmed``, else ``is_stopped``), or the reason it was not confirmed within
        the stop budget, the estop's (_STOP_CONFIRM_TICKS x _STOP_CONFIRM_TICK_S). Never
        inferred from a timeout or from silence. A binding without ``state_seq`` (a
        simulator) is asked ``is_stopped`` alone. The binding is not ticked here: the
        telemetry loop does that, and states arrive on the transport's own thread. Once new
        work is taken, a state can no longer speak for this stop: not confirmed.

        ``accepted`` (a cancel's): the stop is confirmed only once it also answers True, the
        broker having accepted the cancelOrder, within the same budget. ``looks`` = 1 takes one
        look and doesn't wait."""
        confirmed = getattr(self._robot, "stop_confirmed", None) or self._robot.is_stopped
        rested = False
        state_seq = getattr(self._robot, "state_seq", None)
        counted = callable(state_seq)
        if counted and since is None:
            return "the vehicle's state count could not be read after the stop"
        for look in range(looks):
            if look:
                self._stop_confirm_sleep(_STOP_CONFIRM_TICK_S)
            if self._current_action:
                return f"new work ({self._current_action}) was taken before the stop was confirmed"
            with self._robot_lock:
                seen = state_seq(self._robot_id) if counted else None
                rested = bool((not counted or (type(seen) is int and seen > since)) and confirmed(
                    self._robot_id))
                if rested and (accepted is None or accepted(self._robot_id)):
                    return ""
        budget = _STOP_CONFIRM_TICKS * _STOP_CONFIRM_TICK_S
        if rested:
            return f"the broker did not acknowledge the cancelOrder within {budget:g} s"
        if counted and not self._robot.transport_alive():
            detail = self._transport_detail()
            return "the vehicle is not connected" + (f": {detail}" if detail else "")
        return f"no state after the stop showed the vehicle at rest within {budget:g} s"

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

        * `run_safety_stream` and the edge loop call this from inside their response loops,
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
        return self._await_estop(self._command_estop())

    def _command_estop(self) -> EstopToken | None:
        """Issue the emergency stop (`command_estop`) and return without confirming rest. The
        caller holds the robot lock. The transport's RuntimeError is reported, never raised:
        `_await_estop` then answers FAILED, the true answer (see `_confirm_estop`).

        Returns the token to pass to `_await_estop`: the binding's `state_seq` read once the
        stop was sent, so only a state that came after it confirms the stop. Not the SDK's
        `command_estop`, which stops with `command_stop` (a cancelOrder alone, no startPause).
        None when the binding has no `state_seq`, when the stop could not be sent, or when
        `state_seq` raised; the wait then reads the count when it begins, which is stricter.
        Returned and never kept: a token kept from an earlier stop would let a state from
        between the two stops confirm the later one."""
        self._estop_epoch += 1
        # A binding written before command_estop was declared still stops: with its stop. An
        # estop must never end in an AttributeError that kills the thread and leaves no ack.
        estop = getattr(self._robot, "command_estop", None) or self._robot.command_stop
        try:
            estop(self._robot_id)
        except RuntimeError as exc:
            print(f"estop could not be commanded - the robot binding's transport is not "
                  f"live: {exc}", file=sys.stderr)
            return None
        state_seq = getattr(self._robot, "state_seq", None)
        if not callable(state_seq):
            return None
        try:
            seq = state_seq(self._robot_id)
        except Exception as exc:  # noqa: BLE001 - the stop is sent; the wait then reads the count itself
            print(f"estop: the robot binding's state_seq raised after the stop was sent: "
                  f"{exc!r}", file=sys.stderr)
            return None
        return EstopToken(self._robot_id, seq, id(self._robot))

    def _await_estop(self, since: EstopToken | None, deadline: float | None = None) -> str:
        """Wait for confirmed rest after `_command_estop`, given the token it returned:
        STOPPED, or FAILED. The caller does not hold the robot lock. It is taken for each look
        and released between looks, as `_confirm_stop` does, so a command for this vehicle
        waits for one look at most, never for the stop's whole budget. Each look is the SDK's
        own check (`await_estop_confirmation`, one tick, against the same token); the budget
        is the SDK's default, _STOP_CONFIRM_TICKS looks _STOP_CONFIRM_TICK_S apart.

        ``deadline`` (``time.monotonic()``) also ends the wait, after one look at least: a stop
        on a lane is awaited until its own arrival plus the budget (_begin_stop), not for a full
        budget from when its turn comes, so a stop queued behind another stop's wait is still
        answered within its budget of arriving. The look after the deadline is still made: an
        answer is FAILED only once rest has been looked for and not found."""
        if since is None:
            # Read once, after the stop, as the SDK reads it on entry. Read again at every look,
            # a count would almost never be passed within the look.
            with self._robot_lock:
                since = self._estop_token_now()
        for _ in range(_STOP_CONFIRM_TICKS):
            with self._robot_lock:
                try:
                    state = await_estop_confirmation(self._robot, self._robot_id, since=since,
                                                     max_ticks=1, sleep=_no_wait)
                except RuntimeError as exc:
                    print(f"estop could not be confirmed - the robot binding's transport is "
                          f"not live, so rest cannot be read from the robot and C5.3 forbids "
                          f"inferring it: {exc}", file=sys.stderr)
                    return ESTOP_FAILED
            if state == ESTOP_STOPPED:
                return state
            if deadline is not None and time.monotonic() >= deadline:
                break
            self._stop_confirm_sleep(_STOP_CONFIRM_TICK_S)
        return ESTOP_FAILED

    def _estop_token_now(self) -> EstopToken | None:
        """The token for a stop already sent: the binding's count now. None when the binding
        has no count or it can't be read as an int (the SDK then reads it, and answers)."""
        state_seq = getattr(self._robot, "state_seq", None)
        if not callable(state_seq):
            return None
        try:
            seq = state_seq(self._robot_id)
        except Exception:  # noqa: BLE001 - the SDK reads it again, and its error is the answer
            return None
        return EstopToken(self._robot_id, seq, id(self._robot)) if type(seq) is int else None

    def _on_superseded(self, stream: str, exc) -> None:
        """A stream closed as superseded by another process under this adapter identity
        (RFC-0001 #fleet-adapter-protocol-connection-lifecycle: such a process "MUST stop every
        robot it drives" and "MUST also hold no work"). Once for the process, whichever stream
        comes first, and for every vehicle it serves (FleetProcess.stop_superseded):

        1. Work is refused from here on, for every vehicle (_unavailable, _on_resume,
           _on_pause read the streams' state), and no stream is opened again. After a
           SafetyStream supersession the ControlStream is closed too: a fence sent on it could
           be neither answered nor, once MQTT is released, delivered.
        2. Every vehicle is stopped with ``cancelOrder``, never paused: the process that took
           over refuses work on a vehicle paused by another process ("paused at the vehicle"),
           so a pause here would leave the robot out of service. Each epoch is counted, so an
           assignment or a resume already past its checks gives up rather than send after it.
        3. The work each held is interrupted ("stream superseded"); its reports go nowhere now.
        4. Rest is awaited for all of them at once, and each result logged: no stream is left
           to report it on.
        5. Each MQTT session is released, once, after the last wait. Both processes use the
           client id ``swarmada-<robot id>``, and an idle process that kept it would take it
           back on every reconnect, cutting the process that took over off the vehicle.

        This adapter opens a stream only after the previous one ended, so a superseded close
        is always a stream it did not open."""
        with self._superseded_lock:
            if self._superseded.is_set():
                return
            self._superseded_why = (f"this process's {stream} was superseded by another process "
                                    f"under the same adapter identity; it holds no work")
            self._superseded.set()
            # A SafetyStream superseded while our ControlStream is open (the process that took
            # over hasn't opened its own yet): a fence sent on ours now could be neither
            # answered, with no SafetyStream, nor delivered once MQTT is released. So ours is
            # closed too, and the control plane sends the robot's stops to the process that
            # took over. The dial records its call under this lock, so a call is either seen
            # here or never made.
            control = self._active_call if stream == "SafetyStream" else None
        if control is not None:
            control.cancel()
        print(f"error: SUPERSEDED: {stream} closed by the control plane: {exc.details()}. "
              f"Another process serves this adapter identity. Stopping every vehicle "
              f"({self._process.served_text()}), holding no work, not reconnecting; restart "
              f"this process to serve again", file=sys.stderr, flush=True)
        self._process.stop_superseded()

    def _begin_superseded_stop(self) -> int | None:
        """This vehicle's part of a supersession's stop, before any wait: ``cancelOrder``, the
        epoch counted. Returns the state count to confirm rest against (_confirm_stop)."""
        with self._robot_lock:
            self._estop_epoch += 1
            self._robot.command_stop(self._robot_id)
            return self._state_seq_now()

    def _park_if_superseded(self) -> None:
        """After a supersession the ControlStream loop neither redials nor returns: the
        process stays up, and idle, until it is terminated (main's teardown would otherwise
        run while it is still up)."""
        if self._superseded.is_set():
            self._shutdown.wait()

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
            position = battery = None
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
                    position = self._position_now()  # None while unknown: left out (C6.2)
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
            # C6.2: a position or battery the binding does not know is left out, never sent
            # as a defaulted origin or 0%.
            self._outbox.put(pb.AdapterMessage(telemetry=pb.TelemetryPayload(  # C6
                robot_id=self._robot_id, timestamp_ms=int(time.time() * 1000),
                position=position,
                battery=pb.BatteryStatus(percent=battery) if battery is not None else None,
                phase=phase,
                current_action=self._current_action,
                hardware=self._hardware_updates())))  # hardware-fault, delta-compressed
            self._maybe_emit_task_progress()  # stream order completion / failure (C6.4)
            time.sleep(self._telemetry_interval)

    def _position_now(self):
        """The robot's position as a RobotPosition, or None while the binding does not know
        it. With the binding's ``reported_pose`` (VDA5050), the heading is sent when the
        vehicle gave one; a binding with ``pose`` alone sends x, y and floor. The caller holds
        the robot lock."""
        pb = self._pb
        reported = getattr(self._robot, "reported_pose", None)
        if callable(reported):
            p = reported(self._robot_id)
            if p is None:
                return None
            x, y, yaw, floor = p
            pos = pb.RobotPosition(x=x, y=y, floor=floor)
            if yaw is not None:
                pos.yaw = yaw
            return pos
        p = self._robot.pose(self._robot_id)
        if p is None:
            return None
        x, y, floor = p
        return pb.RobotPosition(x=x, y=y, floor=floor)

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
        counted = False
        try:
            with self._robot_lock:
                self._stops_in_flight += 1
                counted = True
                since = self._command_estop()
            self._interrupt_current("estop drill (scenario)", command_stop=False)  # as an estop
            state = self._await_estop(since)  # C5 discipline: never faked
        finally:
            if counted:
                with self._robot_lock:
                    self._stops_in_flight -= 1
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
        self._process.start_lanes()
        while not self._shutdown.is_set():
            outbox: queue.Queue = queue.Queue()  # fresh per stream, as the ControlStream's

            def requests(_o=outbox) -> Iterator:
                while True:
                    msg = _o.get()
                    if msg is None:
                        return
                    yield msg

            self._safety_outbox = outbox
            self._process.on_safety_stream_open()
            try:
                with self._superseded_lock:  # as the ControlStream's: no dial after one
                    if self._superseded.is_set():
                        return
                    stream = stub.SafetyStream(requests())
                for cp in stream:
                    self._process.handle_safety(cp)
            except grpc.RpcError as exc:
                if self._shutdown.is_set():
                    return
                if _is_superseded(exc):
                    self._on_superseded("SafetyStream", exc)
                if self._superseded.is_set():
                    return  # never reopened: another process serves this identity now
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
            self._stop_and_ack(cp.estop.estop_id, self._send_safety)
        elif kind == "estop_clear":
            self._on_clear(cp.estop_clear)

    def _send_safety(self, msg) -> None:
        """Put ``msg`` on the SafetyStream open now. A stop is answered from its lane once its
        wait ends, up to 2 s after it arrived, and the SafetyStream may have dropped and reopened
        meanwhile: an answer bound to the outbox of the stream it came on would go to a stream
        that has ended, and the stream open now would only ever have reported it STOPPING."""
        self._safety_outbox.put(msg)

    def _stop_and_ack(self, estop_id: str, send) -> None:
        """A stop, in two halves. The first runs here, on the reader that received it: the
        stop is counted in flight and commanded, so the vehicle is halted before anything is
        recorded or reported, and then the work it held is interrupted. The second runs on the
        vehicle's lane (_finish_stop): rest is awaited, the stop is answered STOPPED only once
        rest is confirmed, else FAILED, and then it is uncounted. ``stop_initiated_at`` is
        stamped around the hardware stop, so the ack carries evidence the stop was commanded,
        not merely asserted (C5.3).

        A fault in the first half is answered FAILED at once, and the stop uncounted once: the
        reader goes on to the next message, which may be another vehicle's stop."""
        finish = self._begin_stop(estop_id, send)
        if finish is not None:
            self._lane.submit(finish)

    def _begin_stop(self, estop_id: str, send):
        """The stop's first half (_stop_and_ack). Returns the second half, for the lane, or
        None when the first half faulted and the stop is already answered FAILED. The stop's
        deadline is set here, on arrival: its wait ends then, however long it waited on the
        lane behind the stops before it (_await_estop)."""
        initiated_ms = _now_ms()
        deadline = time.monotonic() + _STOP_CONFIRM_TICKS * _STOP_CONFIRM_TICK_S
        counted = False
        try:
            with self._robot_lock:
                self._stops_in_flight += 1
                counted = True
                since = self._command_estop()
            self._interrupt_current("emergency stop", command_stop=False)
        except Exception as exc:  # noqa: BLE001 - a stop is answered, never left silent
            print(f"error: estop id={estop_id} for {self._robot_id!r} faulted before its wait: "
                  f"{exc!r}; answered FAILED", file=sys.stderr, flush=True)
            self._answer_stop(estop_id, initiated_ms, ESTOP_FAILED, send, counted)
            return None
        return lambda: self._finish_stop(estop_id, initiated_ms, since, send, deadline)

    def _finish_stop(self, estop_id: str, initiated_ms: int, since, send,
                     deadline: float) -> None:
        """The stop's second half, on the lane: rest confirmed, never inferred (C5), by the
        stop's own deadline, then the answer. The lane runs stops in the order they arrived, so
        they are answered in that order."""
        try:
            state = self._await_estop(since, deadline)
        except Exception as exc:  # noqa: BLE001 - a stop is answered, never left silent
            print(f"error: estop id={estop_id} for {self._robot_id!r}: its wait raised {exc!r}; "
                  f"answered FAILED", file=sys.stderr, flush=True)
            state = ESTOP_FAILED
        self._answer_stop(estop_id, initiated_ms, state, send, True)

    def _answer_stop(self, estop_id: str, initiated_ms: int, state: str, send,
                     counted: bool) -> None:
        pb = self._pb
        try:
            stopped = state == ESTOP_STOPPED
            self._latch.record_state(self._robot_id, estop_id, "STOPPED" if stopped else "FAILED")
            ack = pb.EstopAck(estop_id=estop_id, stop_initiated_at=initiated_ms,
                              state=pb.ESTOP_STATE_STOPPED if stopped else pb.ESTOP_STATE_FAILED)
            if stopped:
                ack.confirmed_at_ms = _now_ms()
            send(pb.AdapterSafetyMessage(robot_id=self._robot_id, estop_ack=ack))
        finally:
            if counted:
                with self._robot_lock:
                    self._stops_in_flight -= 1

    def _on_fence(self, estop_id: str) -> None:
        """The fence (Command.estop) on the current ControlStream: a stop, latched; a clear
        that came first takes effect with it."""
        stop, lifted = self._latch.on_fence(self._robot_id, estop_id)
        lifted_ms = _now_ms()  # when the kept clear took effect, if it did
        if stop:
            self._stop_and_ack(estop_id, self._send_safety)
        if lifted:
            # After the stop's own answer, which may still be waiting on the lane: the stop is
            # answered before its clear.
            self._lane.submit(lambda: self._send_clear_ack(estop_id, lifted_ms))

    def _on_clear(self, clear) -> None:
        """A SafetyStream clear. The latch decides it here, on the reader; the answer goes on
        the vehicle's lane, after the answer of any stop still waiting there, so a stop is
        answered before its clear (as in _on_fence). The local report a clear that ends the
        local set brings follows its EstopClearAck."""
        verdict = self._latch.on_clear(self._robot_id, clear.estop_id, clear.leader_term,
                                       clear.ends_local_set)
        lifted_ms = _now_ms() if verdict == "lifted" else None
        print(f"estop clear id={clear.estop_id} term={clear.leader_term}: {verdict}",
              file=sys.stderr)
        if verdict in ("lifted", "ack"):
            estop_id, ends_local = clear.estop_id, bool(clear.ends_local_set)

            def answer() -> None:
                self._send_clear_ack(estop_id, lifted_ms)
                if ends_local:
                    self._send_local_report()
            self._lane.submit(answer)

    def _send_clear_ack(self, estop_id: str, lifted_ms: int | None) -> None:
        """The EstopClearAck for ``estop_id``. ``lifted_ms`` is when the latch lifted the stop,
        taken where it lifted: the ack itself may go up to 2 s later, after the answer of a stop
        still waiting on the lane. None for an ack sent again for an ID lifted earlier: the
        latch keeps no time for a lift, so that ack carries the time it is sent. RFC-0001
        names ``lifted_at_ms`` without saying which time it holds."""
        pb = self._pb
        at = lifted_ms if lifted_ms is not None else _now_ms()
        self._safety_outbox.put(pb.AdapterSafetyMessage(
            robot_id=self._robot_id,
            estop_clear_ack=pb.EstopClearAck(estop_id=estop_id, lifted_at_ms=at)))

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
        """An edge stop for this vehicle alone: the process's edge stop with one member.
        ``send`` takes the EstopAck."""
        self._process.edge_stop([self], estop_id, send)

    # ── EdgeStream (C8) ──────────────────────────────────────────────────────

    def _dial_edge(self, addr: str):
        """The edge node, dialled as the control plane is: same credentials, same rule."""
        if self._dial is None:
            raise RuntimeError(f"no dialler configured; not dialling edge node {addr}")
        return self._dial(addr)

    def _edge_frame(self):
        """C8.2: the PositionFrame teed to an edge node, so it has a pose to evaluate. Only
        while the robot can be observed and its position is known, as for telemetry: None
        otherwise, never a frozen frame or one at the origin (C6.2)."""
        with self._robot_lock:
            position = self._position_now() if self._robot.transport_alive() else None
        if position is None:
            return None
        return self._pb.AdapterEdgeMessage(position=self._pb.PositionFrame(
            robot_id=self._robot_id, position=position, ts_ms=_now_ms()))


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
    ap.add_argument("--robot-id", default=None,
                    help="the robot id this adapter serves (default: robot-conformance-1)")
    ap.add_argument("--vehicles-file", default="",
                    help="vda5050 comms: a YAML list of the vehicles this process serves, each "
                         "with robot_id, serial and optionally manufacturer (vehicles.py). "
                         "Takes the place of --robot-id, --serial and --manufacturer; every "
                         "vehicle is served on one ControlStream and one MQTT client")
    ap.add_argument("--comms", default="simulated", choices=["simulated", "vda5050"],
                    help="robot comms: 'simulated' ($0, no broker) or 'vda5050' (MQTT/JSON)")
    ap.add_argument("--mqtt-host", default="localhost")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--manufacturer", default=None,
                    help="vda5050 comms: 'manufacturer' segment of the robot's MQTT topic "
                         "identity (interfaceName/version/MANUFACTURER/serialNumber). Must "
                         "match the robot's VDA5050 identity to bind a real AMR "
                         "(default: swarmada).")
    ap.add_argument("--serial", default=None,
                    help="vda5050 comms: 'serialNumber' segment of the robot's MQTT topic "
                         "identity. Must match the robot's VDA5050 serial to bind a real AMR "
                         "(default: amr-1).")
    ap.add_argument("--route-graph", default="",
                    help="vda5050 comms: a YAML lane graph (routing.py). Orders then follow its "
                         "lanes instead of one straight edge from the vehicle to the goal, and a "
                         "goal more than 0.05 m from every lane node is refused")
    ap.add_argument("--state-lapse-seconds", type=float, default=60.0,
                    help="vda5050 comms: when the vehicle is taken as connected from its states (its "
                         "retained connection message reads CONNECTIONBROKEN, or none has come "
                         "since the broker session began), how long that lasts without a new "
                         "state (default: 60; VDA 5050 has a vehicle send a state at least every "
                         "30 s)")
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
    if args.route_graph and args.comms != "vda5050":
        ap.error("--route-graph needs --comms vda5050")
    if not (math.isfinite(args.state_lapse_seconds) and args.state_lapse_seconds > 0):
        ap.error(f"--state-lapse-seconds is {args.state_lapse_seconds:g}, not a positive number")
    vehicles = _vehicles(args, ap)
    dial = dialler(args, ap)

    from fleet_adapter.v1 import fleet_adapter_pb2 as pb
    from fleet_adapter.v1 import fleet_adapter_pb2_grpc as pb_grpc

    if args.comms == "vda5050":
        from fleet_adapter_vda5050.routing import RouteGraph
        from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding
        graph = None
        if args.route_graph:
            try:
                graph = RouteGraph.load(args.route_graph)
            except ValueError as exc:
                ap.error(str(exc))
            on_map = f", map {graph.map_id}" if graph.map_id else ""
            print(f"route graph {args.route_graph}: {len(graph.node_ids)} nodes, "
                  f"{graph.edge_count} edges{on_map}", file=sys.stderr)
        # A fleet's vehicles share one MQTT client and its session (mqtt_client.py), under the
        # first vehicle's client id; a lone vehicle keeps a client of its own, as before.
        shared = None
        if len(vehicles) > 1:
            from fleet_adapter_vda5050.mqtt_client import MqttClient
            shared = MqttClient(args.mqtt_host, args.mqtt_port,
                                client_id=f"swarmada-{vehicles[0].robot_id}",
                                vehicles=len(vehicles))
        robots = [Vda5050Binding(robot_id=v.robot_id,
                                 manufacturer=v.manufacturer, serial=v.serial,
                                 mqtt_host=args.mqtt_host, mqtt_port=args.mqtt_port,
                                 route_graph=graph, state_lapse_s=args.state_lapse_seconds,
                                 mqtt=shared)
                  for v in vehicles]
        robot = robots[0]
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
        robots = [robot]
    if len(robots) == 1:
        adapter = fleet_adapter_vda5050Adapter(pb, pb_grpc, robot, vehicles[0].robot_id,
                                               state_dir=args.state_dir or None, dial=dial,
                                               namespace=args.namespace)
    else:
        # One process for the fleet: its streams, one ControlStream and one SafetyStream, are
        # every vehicle's; the first vehicle runs them, as a lone vehicle does (process.py).
        streams = _Streams(dial, args.namespace)
        cores = [fleet_adapter_vda5050Adapter(pb, pb_grpc, r, v.robot_id,
                                              state_dir=args.state_dir or None, streams=streams)
                 for r, v in zip(robots, vehicles)]
        FleetProcess(cores)
        adapter = cores[0]
        print(f"serving {len(cores)} vehicles from {args.vehicles_file} on one MQTT client "
              f"(swarmada-{vehicles[0].robot_id}): {adapter._process.served_text()}",
              file=sys.stderr)
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
        # is idempotent and safe when `start()` never ran. Every vehicle's: a shared client's
        # session ends with the last.
        for r in robots:
            r.stop()


def _vehicles(args, ap) -> list:
    """The vehicles this process serves: those --vehicles-file lists, or the one --robot-id,
    --serial and --manufacturer give (or their defaults). The file takes the place of those
    flags, so giving it with any of them is refused, as is giving it for the simulated comms,
    which serve one vehicle (ap.error exits 2)."""
    from fleet_adapter_vda5050.vehicles import DEFAULT_MANUFACTURER, Vehicle, load_vehicles

    if not args.vehicles_file:
        return [Vehicle(robot_id="robot-conformance-1" if args.robot_id is None else args.robot_id,
                        serial="amr-1" if args.serial is None else args.serial,
                        manufacturer=(DEFAULT_MANUFACTURER if args.manufacturer is None
                                      else args.manufacturer))]
    if args.comms != "vda5050":
        ap.error("--vehicles-file needs --comms vda5050: the simulated comms serve one vehicle")
    given = [flag for flag, value in (("--robot-id", args.robot_id), ("--serial", args.serial),
                                      ("--manufacturer", args.manufacturer)) if value is not None]
    if given:
        ap.error(f"--vehicles-file and {', '.join(given)} together: the file gives each "
                 f"vehicle's robot_id, serial and manufacturer")
    try:
        return load_vehicles(args.vehicles_file)
    except ValueError as exc:
        ap.error(str(exc))


if __name__ == "__main__":
    main()
