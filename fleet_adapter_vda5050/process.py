# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The process: the streams every vehicle it serves shares, and the router between them.

One ControlStream and one SafetyStream carry the messages for every robot this adapter serves
(RFC-0001 #fleet-adapter-protocol-the-controlstream-service). A :class:`FleetProcess` holds one
vehicle core (``fleet_adapter_vda5050Adapter``) per robot, all sharing one ``_Streams``, and
routes what arrives:

* **A Command, an Estop or an EstopClear** goes to the core its ``robot_id`` names, and to no
  other. One that names no robot served here goes to the first core, which gives today's
  answers for an unserved robot: a refusal, or an estop answered FAILED.
* **A RegisterAck or a DiscoverAck** names no robot. Each answers the oldest register (or
  discover) still unanswered on the session, so every register and discover is sent through
  :meth:`send_register` and :meth:`send_discover`, which record the order. A new session starts
  with none unanswered.
* **An ActionOutcomeAck** is offered to every core. ``RetainedOutcomes.acknowledge`` matches the
  action id and the fencing token, so only the core that kept the outcome clears it.

A single vehicle is a process of one, built by the core itself, so the shipped single-vehicle
flags run this same path."""

from __future__ import annotations

import queue
import sys
import threading
from collections import deque


class FleetProcess:
    def __init__(self, cores: list) -> None:
        if not cores:
            raise ValueError("a process serves at least one vehicle")
        streams = cores[0]._streams
        ids = [c._robot_id for c in cores]
        if len(set(ids)) != len(ids):
            raise ValueError(f"robot ids served twice: {sorted(i for i in set(ids) if ids.count(i) > 1)}")
        if any(c._streams is not streams for c in cores):
            raise ValueError("every vehicle in a process shares the process's streams")
        self.cores = list(cores)
        self._by_id = {c._robot_id: c for c in cores}
        self._first = cores[0]
        self._streams = streams
        self._pb = cores[0]._pb
        # Registers and discovers sent and not yet answered on the current session, oldest
        # first, with the session they were sent on.
        self._pair_lock = threading.Lock()
        self._pending_register: deque = deque()
        self._pending_discover: deque = deque()
        for c in cores:
            c._process = self

    # ── who is served ────────────────────────────────────────────────────────

    def core_for(self, robot_id: str):
        """The core that serves ``robot_id``, or None."""
        return self._by_id.get(robot_id) if robot_id else None

    def served_text(self) -> str:
        """The robots served, for a message: one by its id, a few by theirs, many by count."""
        ids = [c._robot_id for c in self.cores]
        if len(ids) <= 3:
            return ", ".join(repr(i) for i in ids)
        return f"{len(ids)} robots ({', '.join(repr(i) for i in ids[:3])}, ...)"

    # ── sessions and pairing ─────────────────────────────────────────────────

    def new_session(self) -> None:
        """A ControlStream session opens: nothing is registered or awaited on it yet."""
        with self._pair_lock:
            self._streams.session += 1
            self._pending_register.clear()
            self._pending_discover.clear()
        for c in self.cores:
            c._registered = False
            c._latch.new_control_stream(c._robot_id)

    def send_register(self, core, outbox: queue.Queue, session: int) -> bool:
        """Register ``core`` on ``outbox``, the outbox of ``session``, and record it as awaiting
        its RegisterAck. Nothing is sent once that session has been replaced: the new one
        registers on its own. Returns whether it was sent."""
        msg = self._pb.AdapterMessage(register=self._pb.RegisterRobot(robot_id=core._robot_id))
        with self._pair_lock:
            if session != self._streams.session:
                return False
            self._pending_register.append(core)
            outbox.put(msg)
        return True

    def send_discover(self, core, outbox: queue.Queue, session: int, msg) -> bool:
        """Announce ``core`` (``msg`` is its DiscoverRobot), recorded as awaiting a DiscoverAck."""
        with self._pair_lock:
            if session != self._streams.session:
                return False
            self._pending_discover.append(core)
            outbox.put(msg)
        return True

    def _answered(self, pending: deque, what: str):
        """The core an ack answers: the oldest unanswered. A process of one answers its one
        vehicle even with none recorded, as the adapter always has; a fleet drops an ack it
        can't pair, loudly."""
        with self._pair_lock:
            if pending:
                return pending.popleft()
        if len(self.cores) == 1:
            return self._first
        print(f"error: a {what} arrived with no {what.removesuffix('Ack').lower()} waiting for "
              f"one on this session; ignored", file=sys.stderr, flush=True)
        return None

    # ── the ControlStream ────────────────────────────────────────────────────

    def handle_control_plane(self, cp, stub) -> None:
        kind = cp.WhichOneof("payload")
        if kind == "hello_ack":
            if not cp.hello_ack.accepted:
                raise RuntimeError(f"handshake rejected: {cp.hello_ack.message}")
            self._streams.negotiated = cp.hello_ack.negotiated_contract_version
            for c in self.cores:
                c._on_hello_ack(stub)
        elif kind == "register_ack":
            core = self._answered(self._pending_register, "RegisterAck")
            if core is not None:
                core._on_register_ack(cp.register_ack)
        elif kind == "discover_ack":
            core = self._answered(self._pending_discover, "DiscoverAck")
            if core is not None:
                core._on_discover_ack(cp.discover_ack)
        elif kind == "outcome_ack":
            for c in self.cores:
                c._on_outcome_ack(cp.outcome_ack)
        elif kind == "heartbeat":
            rid = cp.heartbeat.robot_id
            if rid and self.core_for(rid) is None:
                # A per-robot heartbeat asks "is robot_id alive?", and the control plane takes
                # any reply naming the robot as yes: it's the exchange that decides whether a
                # robot is declared Offline. This adapter can't vouch for a robot it doesn't
                # serve, so it doesn't answer, and the exchange times out. An empty id is the
                # adapter-scoped question, and is answered.
                self._first._log_unserved(rid, "heartbeat",
                                          "not answered: this adapter can't say that robot is alive")
                return
            self._streams.outbox.put(self._pb.AdapterMessage(
                heartbeat=self._pb.HeartbeatResponse(robot_id=rid)))
        elif kind == "command":
            (self.core_for(cp.command.robot_id) or self._first)._handle_command(cp.command)

    def run_control_stream(self, stub) -> None:
        """Bring every vehicle up, then run the ControlStream. The first core runs the stream
        loop, as a lone vehicle always has; it hands every message here."""
        for c in self.cores[1:]:
            c.bring_up_robot()
            threading.Thread(target=c._edge_loop, daemon=True).start()
        self._first._run_control_stream(stub)

    # ── the SafetyStream ─────────────────────────────────────────────────────

    def run_safety_stream(self, stub) -> None:
        """The one SafetyStream, run by the first vehicle as a lone vehicle always has; every
        message comes back here (handle_safety)."""
        self._first.run_safety_stream(stub)

    def on_safety_stream_open(self) -> None:
        """Each SafetyStream that opens is told what is latched, for every vehicle."""
        for c in self.cores:
            c._report_latch()

    def handle_safety(self, cp) -> None:
        (self.core_for(cp.robot_id) or self._first)._on_safety_message(cp)

    def unregistered(self) -> list:
        """The vehicles with no registration accepted on this session."""
        return [c for c in self.cores if not c._registered]
