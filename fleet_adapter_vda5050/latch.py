# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The stop latch (contract 1.4.0; RFC-0001 #fleet-adapter-protocol-the-stop-latch).

No work is given to a robot under a stop. Per robot this keeps:

* the latched stop IDs. A stop's ID latches when its ``Estop`` arrives on the SafetyStream or
  its fence (``Command.estop``) on the ControlStream, unless this process already cleared it;
* the fenced IDs: the fences that arrived on the current ControlStream. Only a fenced ID can be
  lifted, so a clear that overtakes its fence is kept until the fence arrives;
* the cleared IDs, so a clear sent again is acknowledged again and a late fence is ignored;
* the highest leader term seen;
* the local latch an edge stop sets, under a fresh 128-bit set ID. A reset at the robot lifts the
  local latch (reset_local); an EstopClear naming its set ends the wait for the clear. Work is
  refused until both have happened, in either order.

Everything lives in this process, as the latch section allows ("An adapter process that
restarts holds none of them"). What a latch does to the work held is the adapter's; this module
answers "is this robot latched" and "what must be reported". Every method is thread-safe: the
SafetyStream, the ControlStream and the EdgeStream all call in.
"""

from __future__ import annotations

import secrets
import threading
import time
from dataclasses import dataclass, field


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class _Robot:
    latched: dict[str, str] = field(default_factory=dict)  # stop ID -> last state reported
    fenced: set[str] = field(default_factory=set)
    cleared: set[str] = field(default_factory=set)
    pending: set[str] = field(default_factory=set)  # clears that came before their fence
    highest_term: int = 0
    local_set: bool = False  # set by an edge stop; only a reset at the robot lifts it
    local_set_id: str = ""  # the set whose EstopClear has not arrived; empty if none
    local_changed_ms: int = 0


@dataclass(frozen=True)
class LocalLatch:
    latched: bool
    set_id: str
    changed_at_ms: int


class StopLatch:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._robots: dict[str, _Robot] = {}

    def _r(self, robot_id: str) -> _Robot:
        return self._robots.setdefault(robot_id, _Robot())

    # ── leader term ──────────────────────────────────────────────────────────

    def observe_term(self, robot_id: str, term: int, *, is_stop: bool) -> bool:
        """True when a message at ``term`` may be acted on. A stop always may. 0 means no term
        was sent (a connection below 1.4.0) and is never stale for that alone."""
        with self._lock:
            r = self._r(robot_id)
            stale = 0 < term < r.highest_term
            r.highest_term = max(r.highest_term, term)
            return is_stop or not stale

    def highest_term(self, robot_id: str) -> int:
        with self._lock:
            return self._r(robot_id).highest_term

    # ── stops and clears ─────────────────────────────────────────────────────

    def on_estop(self, robot_id: str, estop_id: str) -> None:
        """An Estop on the SafetyStream. The caller stops the robot either way."""
        with self._lock:
            r = self._r(robot_id)
            if estop_id not in r.cleared:
                r.latched.setdefault(estop_id, "STOPPING")

    def on_fence(self, robot_id: str, estop_id: str) -> tuple[bool, bool]:
        """The fence on the current ControlStream. Returns (stop the robot, a kept clear took
        effect now and must be acknowledged). A fence for a cleared ID is ignored."""
        with self._lock:
            r = self._r(robot_id)
            if estop_id in r.cleared:
                return False, False
            r.latched.setdefault(estop_id, "STOPPING")
            r.fenced.add(estop_id)
            if estop_id in r.pending:
                r.pending.discard(estop_id)
                self._lift(r, estop_id)
                return True, True
            return True, False

    def on_clear(self, robot_id: str, estop_id: str, leader_term: int,
                 ends_local_set: str = "") -> str:
        """An EstopClear: "stale" (below the highest term: lifts nothing, not answered),
        "lifted" (lifted now: answer EstopClearAck), "ack" (lifted already: answer it again)
        or "pending" (its fence has not arrived: kept)."""
        with self._lock:
            r = self._r(robot_id)
            if 0 < leader_term < r.highest_term:
                return "stale"
            r.highest_term = max(r.highest_term, leader_term)
            if estop_id in r.cleared:
                verdict = "ack"
            elif estop_id in r.fenced:
                self._lift(r, estop_id)
                verdict = "lifted"
            else:
                r.pending.add(estop_id)
                return "pending"
            if ends_local_set and ends_local_set == r.local_set_id:
                r.local_set_id = ""  # the wait for the clear ends; a reset lifts the latch
                r.local_changed_ms = _now_ms()
            return verdict

    @staticmethod
    def _lift(r: _Robot, estop_id: str) -> None:
        r.latched.pop(estop_id, None)
        r.fenced.discard(estop_id)
        r.cleared.add(estop_id)

    def record_state(self, robot_id: str, estop_id: str, state: str) -> None:
        """The robot's state for a latched stop (STOPPING, STOPPED, FAILED), for reports."""
        with self._lock:
            r = self._r(robot_id)
            if estop_id in r.latched:
                r.latched[estop_id] = state

    def new_control_stream(self, robot_id: str) -> None:
        """A fence counts only on the ControlStream it came on: a new one starts uncounted."""
        with self._lock:
            self._r(robot_id).fenced.clear()

    def set_local(self, robot_id: str) -> LocalLatch:
        """An edge stop: the local latch is set under a fresh set ID."""
        with self._lock:
            r = self._r(robot_id)
            r.local_set = True
            r.local_set_id = secrets.token_hex(16)
            r.local_changed_ms = _now_ms()
            return LocalLatch(True, r.local_set_id, r.local_changed_ms)

    def reset_local(self, robot_id: str) -> LocalLatch | None:
        """A reset at the robot: the local latch is no longer set. The set, if its clear has not
        arrived, still waits for it. None when the latch was not set."""
        with self._lock:
            r = self._r(robot_id)
            if not r.local_set:
                return None
            r.local_set = False
            r.local_changed_ms = _now_ms()
            return LocalLatch(False, r.local_set_id, r.local_changed_ms)

    # ── what the adapter asks ────────────────────────────────────────────────

    def refuses_work(self, robot_id: str) -> bool:
        with self._lock:
            r = self._r(robot_id)
            return bool(r.latched) or r.local_set or bool(r.local_set_id)

    def latched_ids(self, robot_id: str) -> dict[str, str]:
        with self._lock:
            return dict(self._r(robot_id).latched)

    def local(self, robot_id: str) -> LocalLatch:
        with self._lock:
            r = self._r(robot_id)
            return LocalLatch(r.local_set, r.local_set_id, r.local_changed_ms)
