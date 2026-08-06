# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Terminal task-status streaming (C6.4): the adapter streams a SUCCEEDED / FAILED
ActionStatusUpdate derived from the robot's order `state`, once, then clears the task.
A tiny fake pb records the emitted update so this stays proto-free."""

from __future__ import annotations

from fleet_adapter_vda5050 import task
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot


class _Pb:
    """Minimal proto stand-in — the enum values the adapter binds by name plus the
    two message constructors _emit_action_status uses."""

    ACTION_STATE_RUNNING = 1
    ACTION_STATE_SUCCEEDED = 2
    ACTION_STATE_FAILED = 3

    class ActionStatusUpdate:
        def __init__(self, action_id="", state=0, progress_pct=0):
            self.action_id, self.state, self.progress_pct = action_id, state, progress_pct
            self.fencing_token = None

    class AdapterMessage:
        def __init__(self, action_status=None, **_):
            self.action_status = action_status


class _Binding:
    """A binding that reports a scripted order-derived TaskState."""

    def __init__(self, state: str) -> None:
        self._state = state

    def task_state(self, robot_id: str) -> str:
        return self._state


def _adapter(binding) -> Adapter:
    return Adapter(_Pb(), None, binding, "amr-1")


def _drain(q) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def test_streams_succeeded_once_then_clears() -> None:
    a = _adapter(_Binding(task.TASK_SUCCEEDED))
    a._current_action = "t1"
    a._last_task_state = task.TASK_RUNNING
    a._current_fencing_token = 5

    a._maybe_emit_task_progress()
    msgs = _drain(a._outbox)
    assert len(msgs) == 1
    upd = msgs[0].action_status
    assert upd.action_id == "t1" and upd.state == _Pb.ACTION_STATE_SUCCEEDED
    assert upd.progress_pct == 100 and upd.fencing_token == 5  # C6.4 token echoed
    assert a._current_action == ""  # terminal → task cleared

    # A second poll must not re-emit (the task is done).
    a._maybe_emit_task_progress()
    assert _drain(a._outbox) == []


def test_streams_failed() -> None:
    a = _adapter(_Binding(task.TASK_FAILED))
    a._current_action = "t1"
    a._last_task_state = task.TASK_RUNNING

    a._maybe_emit_task_progress()
    msgs = _drain(a._outbox)
    assert msgs[0].action_status.state == _Pb.ACTION_STATE_FAILED


def test_running_is_not_streamed() -> None:
    # RUNNING was already reported on assign; the steady state is not re-streamed.
    a = _adapter(_Binding(task.TASK_RUNNING))
    a._current_action = "t1"
    a._last_task_state = task.TASK_RUNNING

    a._maybe_emit_task_progress()
    assert _drain(a._outbox) == []


def test_binding_without_order_state_is_noop() -> None:
    # The $0 SimulatedRobot has no task_state → nothing to stream (conformance path).
    a = _adapter(SimulatedRobot())
    a._current_action = "t1"

    a._maybe_emit_task_progress()
    assert _drain(a._outbox) == []
