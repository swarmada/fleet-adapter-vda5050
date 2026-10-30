# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The retained outcome (contract 1.4.0; RFC-0001
#fleet-adapter-protocol-reconnect-action-state-reconciliation, The retained outcome).

The terminal outcome of the robot's last action, with the fencing token it was assigned with,
is kept until the control plane acknowledges it with an ``ActionOutcomeAck`` naming that action
and token, and is reported again (``retained = true``) after every RegisterAck until then.

A restart of the adapter does not end the retention, so with a state directory the outcome is
one JSON file per robot (mode 0600, written atomically). Without one it is kept in memory only,
and the adapter says so at start-up. A retained outcome is not work: nothing is continued for it.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Outcome:
    action_id: str
    state: str  # the ActionState name, e.g. ACTION_STATE_CANCELLED
    message: str
    fencing_token: int
    end_cause: int = 0


class RetainedOutcomes:
    def __init__(self, root: Path | str | None) -> None:
        self._root = Path(root).expanduser() if root else None
        self._lock = threading.Lock()
        self._mem: dict[str, Outcome] = {}

    @property
    def durable(self) -> bool:
        return self._root is not None

    def _path(self, robot_id: str) -> Path | None:
        return None if self._root is None else self._root / "retained" / f"{robot_id}.json"

    def keep(self, robot_id: str, outcome: Outcome) -> None:
        with self._lock:
            self._mem[robot_id] = outcome
            path = self._path(robot_id)
            if path is None:
                return
            try:
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                tmp = path.with_suffix(f".tmp-{threading.get_ident()}")
                tmp.write_text(json.dumps(asdict(outcome), sort_keys=True))
                os.chmod(tmp, 0o600)
                tmp.replace(path)
            except OSError as err:  # still kept in memory; lost only if the process restarts
                print(f"retained outcome for {robot_id} not saved: {err}", file=sys.stderr)

    def get(self, robot_id: str) -> Outcome | None:
        with self._lock:
            if robot_id in self._mem:
                return self._mem[robot_id]
            path = self._path(robot_id)
            if path is None or not path.exists():
                return None
            try:
                out = Outcome(**json.loads(path.read_text()))
            except (OSError, ValueError, TypeError) as err:
                print(f"retained outcome for {robot_id} unreadable, not reported: {err}",
                      file=sys.stderr)
                return None
            self._mem[robot_id] = out
            return out

    def acknowledge(self, robot_id: str, action_id: str, fencing_token: int) -> bool:
        """Drop the outcome when the ack names its action and token; True if dropped."""
        with self._lock:
            out = self._mem.get(robot_id)
            if out is None:
                path = self._path(robot_id)
                if path is not None and path.exists():
                    try:
                        out = Outcome(**json.loads(path.read_text()))
                    except (OSError, ValueError, TypeError):
                        out = None
            if out is None or out.action_id != action_id or out.fencing_token != fencing_token:
                return False
            self._mem.pop(robot_id, None)
            path = self._path(robot_id)
            if path is not None:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            return True
