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
* **A stop** is commanded on the reader that received it, and waits for rest on its
  vehicle's lane (``_Lane``), so one vehicle's wait never holds another's stop.
* **An edge node** gets one EdgeStream, however many vehicles are served through it. Its members
  are the vehicles whose latest accepted RegisterAck listed its address; each gets a
  PositionFrame. Its estop names no robot: it stops every member, and is answered once
  (:meth:`edge_stop`).
* **An ActionOutcomeAck** is offered to every core. ``RetainedOutcomes.acknowledge`` matches the
  action id and the fencing token, so only the core that kept the outcome clears it.

A single vehicle is a process of one, built by the core itself, so the shipped single-vehicle
flags run this same path."""

from __future__ import annotations

import queue
import sys
import threading
from collections import deque

# How long a supersession waits for one vehicle's lane to finish its wait: the lane may first
# finish a stop that was already waiting there, and each wait is bounded at 2 s.
_SUPERSEDED_WAIT_S = 10.0


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
        # The edge nodes dialled, by address (_EdgeLink).
        self._edge_lock = threading.Lock()
        self._edges: dict[str, _EdgeLink] = {}
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
        self.start_lanes()
        for c in self.cores[1:]:
            c.bring_up_robot()
        self._first._run_control_stream(stub)

    # ── the SafetyStream ─────────────────────────────────────────────────────

    def start_lanes(self) -> None:
        """Each vehicle's lane gets its thread: from now on a stop's wait and its answer run
        there, and the reader that received the stop goes on at once."""
        for c in self.cores:
            c._lane.start()

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

    # ── a supersession ───────────────────────────────────────────────────────

    def stop_superseded(self) -> None:
        """Stop every vehicle after the streams were superseded (the core's _on_superseded has
        recorded it, once). Every ``cancelOrder`` is sent before any work is interrupted, and
        all of that before any wait. Rest is then awaited on each vehicle's lane, all at once,
        so one vehicle that can't be confirmed doesn't hold up the others; each result is
        logged. Each MQTT session is released once, after the last wait. A fault for one
        vehicle is logged and the rest go on."""
        begun: list = []
        for c in self.cores:
            try:
                begun.append((c, c._begin_superseded_stop(), ""))
            except Exception as fault:  # noqa: BLE001 - every other vehicle is still stopped
                begun.append((c, None, f"the stop sequence raised {fault!r}"))
        for c, _since, failed in begun:
            if not failed:
                try:
                    c._interrupt_current("stream superseded", command_stop=False)
                except Exception as fault:  # noqa: BLE001
                    print(f"error: SUPERSEDED robot={c._robot_id}: interrupting its work raised "
                          f"{fault!r}", file=sys.stderr, flush=True)
        waits: list = []
        for c, since, failed in begun:
            done, why = threading.Event(), [failed]

            def wait(c=c, since=since, done=done, why=why) -> None:
                try:
                    if not why[0]:
                        why[0] = c._confirm_stop(since)
                except Exception as fault:  # noqa: BLE001
                    why[0] = f"the stop sequence raised {fault!r}"
                finally:
                    done.set()
            c._lane.submit(wait)
            waits.append((c, done, why))
        for c, done, why in waits:
            # A lane runs a stop already waiting ahead first; each wait is bounded (2 s).
            if not done.wait(_SUPERSEDED_WAIT_S):
                why[0] = f"its wait did not end within {_SUPERSEDED_WAIT_S:g} s"
            print(f"error: SUPERSEDED robot={c._robot_id}: the stop "
                  + ("is confirmed: the vehicle is at rest" if not why[0]
                     else f"is NOT confirmed: {why[0]}"),
                  file=sys.stderr, flush=True)
        for c in self.cores:
            try:
                c._robot.stop()
                print(f"error: SUPERSEDED robot={c._robot_id}: MQTT session released",
                      file=sys.stderr, flush=True)
            except Exception as fault:  # noqa: BLE001
                print(f"error: SUPERSEDED robot={c._robot_id}: releasing the MQTT session "
                      f"raised {fault!r}", file=sys.stderr, flush=True)

    def unregistered(self) -> list:
        """The vehicles with no registration accepted on this session."""
        return [c for c in self.cores if not c._registered]

    # ── the EdgeStreams (C8) ─────────────────────────────────────────────────

    def edge_endpoints(self, core, addresses: list[str]) -> None:
        """``core``'s latest accepted RegisterAck listed ``addresses``: from now on it is
        served through those edge nodes and no others. An address listed for the first time
        is dialled (one EdgeStream for it, whoever listed it); a vehicle that joins an edge
        node's members tees it a PositionFrame (C8.2)."""
        new, joined = [], []
        with self._edge_lock:
            for addr, link in self._edges.items():
                if addr not in addresses and core in link.members:
                    link.members.remove(core)
            for addr in dict.fromkeys(addresses):
                link = self._edges.get(addr)
                if link is None:
                    link = self._edges[addr] = _EdgeLink(addr)
                    new.append(link)
                if core not in link.members:
                    link.members.append(core)
                    joined.append(link)
        for link in joined:
            frame = core._edge_frame()
            if frame is not None:
                link.outbox.put(frame)
        for link in new:
            self._start_edge(link)

    def _start_edge(self, link: _EdgeLink) -> None:
        threading.Thread(target=self._edge_loop, args=(link,), daemon=True).start()

    def _edge_loop(self, link: _EdgeLink) -> None:
        """One edge node's EdgeStream, dialled as the control plane is."""
        estub = self._first._pb_grpc.EdgeServiceStub(self._first._dial_edge(link.addr))

        def send(ack) -> None:
            link.outbox.put(self._pb.AdapterEdgeMessage(estop_ack=ack))

        for ec in estub.EdgeStream(link.requests()):
            if ec.WhichOneof("msg") == "estop":  # C8.3: same confirmed discipline as C5
                with self._edge_lock:
                    members = list(link.members)
                self.edge_stop(members, ec.estop.estop_id, send)

    def edge_stop(self, members: list, estop_id: str, send) -> None:
        """An edge estop names no robot: it stops every vehicle served through that edge node,
        and is answered by one EstopAck (``send`` takes it). Each member's local latch is set
        and reported, then every member's stop is commanded, and only then are they awaited, in
        parallel on their lanes. The answer goes once the last member has answered: STOPPED
        only if every member's rest was confirmed, else FAILED."""
        if not members:
            print(f"error: edge estop id={estop_id}: no vehicle served here is served through "
                  f"that edge node; answered FAILED", file=sys.stderr, flush=True)
            send(self._pb.EstopAck(estop_id=estop_id, state=self._pb.ESTOP_STATE_FAILED,
                                   message="no vehicle is served through this edge node"))
            return
        answer = _EdgeAnswer(self._pb, members, send)
        for c in members:
            c._latch.set_local(c._robot_id)
            c._send_local_report()
        finishes = [c._begin_stop(estop_id, answer.member_answered) for c in members]
        for c, finish in zip(members, finishes):
            if finish is not None:
                c._lane.submit(finish)


class _EdgeLink:
    """One edge node: its address, the vehicles served through it, and its EdgeStream's
    outbox."""

    def __init__(self, addr: str) -> None:
        self.addr = addr
        self.members: list = []  # under the process's _edge_lock
        self.outbox: queue.Queue = queue.Queue()

    def requests(self):
        while True:
            msg = self.outbox.get()
            if msg is None:
                return
            yield msg


class _EdgeAnswer:
    """The one EstopAck for an edge stop, from each member's answer. It goes once every
    member has answered: STOPPED only if every member's rest was confirmed, with the earliest
    stop and the latest confirmation; else FAILED, naming the vehicles not confirmed."""

    def __init__(self, pb, members: list, send) -> None:
        self._pb = pb
        self._send = send
        self._waiting = len(members)
        self._lock = threading.Lock()
        self._acks: list = []

    def member_answered(self, msg) -> None:
        """A member's answer, the AdapterSafetyMessage its stop would send."""
        with self._lock:
            self._acks.append((msg.robot_id, msg.estop_ack))
            self._waiting -= 1
            if self._waiting:
                return
        pb = self._pb
        acks = [a for _, a in self._acks]
        if len(acks) == 1:
            self._send(acks[0])
            return
        out = pb.EstopAck(estop_id=acks[0].estop_id,
                          stop_initiated_at=min(a.stop_initiated_at for a in acks))
        failed = sorted(rid for rid, a in self._acks if a.state != pb.ESTOP_STATE_STOPPED)
        if failed:
            out.state = pb.ESTOP_STATE_FAILED
            out.message = "not confirmed: " + ", ".join(repr(r) for r in failed)
        else:
            out.state = pb.ESTOP_STATE_STOPPED
            out.confirmed_at_ms = max(a.confirmed_at_ms for a in acks)
        self._send(out)
