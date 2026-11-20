# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Each stop is answered within its budget of its own arrival, and stops are answered in order.

A vehicle's stops wait for rest on its lane, one after another. A stop that arrives while
another is waiting must still be answered within 2 s of arriving, not 2 s after the one before
it ends: an estop, a fence and an edge estop for one vehicle in quick succession are each
answered within their own budget, in the order they arrived."""

from __future__ import annotations

import json
import threading
import time

import pytest
from conftest import mark_transport_live
from swarmada_sdk.safety import ESTOP_STOPPED

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
BUDGET_S = 2.0
SLACK_S = 0.25  # one look and one tick past the deadline, and the test's own reading


@pytest.fixture
def adapter(monkeypatch):
    """A vehicle that reports rest, paused, only from ``rest_at`` (monotonic) on, with its
    lane running."""
    monkeypatch.setattr(Adapter, "_telemetry_loop", lambda self, stub: None)
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.on_state(json.dumps({"driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
                           "paused": False, "orderId": "", "nodeStates": [], "actionStates": []}))
    n = [100]
    b.rest_at = float("inf")

    def tick(dt: float) -> None:
        n[0] += 1
        rest = time.monotonic() >= b.rest_at
        b.on_state(json.dumps({
            "headerId": n[0], "timestamp": f"t-{n[0]}", "driving": not rest,
            "velocity": {"vx": 0.0 if rest else 0.6, "vy": 0.0, "omega": 0.0}, "paused": rest,
            "orderId": "", "nodeStates": [], "actionStates": []}))
    b.tick = tick
    a = Adapter(pb, None, b, ROBOT)
    a._negotiated = "1.4.0"
    a._lane.start()
    yield a
    a._shutdown.set()


class _Answers:
    """Each stop's final answer (STOPPED or FAILED), stamped on the thread that sends it: the
    SafetyStream's answers through ``safety`` (the adapter's _send_safety), the edge node's
    through ``ack``."""

    def __init__(self, a) -> None:
        self.got: list[tuple[str, int, float]] = []  # (estop_id, state, monotonic)
        self._lock = threading.Lock()
        self._put = a._send_safety

    def safety(self, m) -> None:
        if m.WhichOneof("payload") == "estop_ack":
            self.ack(m.estop_ack)
        self._put(m)

    def ack(self, k) -> None:
        if k.state != pb.ESTOP_STATE_STOPPING:
            with self._lock:
                self.got.append((k.estop_id, k.state, time.monotonic()))


@pytest.mark.parametrize("confirmed", [True, False])
def test_an_estop_a_fence_and_an_edge_stop_are_each_answered_within_their_budget_in_order(
        adapter, confirmed) -> None:
    a, b = adapter, adapter._robot
    answers = _Answers(a)
    a._send_safety = answers.safety
    arrived = {}
    t0 = time.monotonic()
    b.rest_at = t0 + 1.0 if confirmed else float("inf")

    arrived["e1"] = time.monotonic()  # a SafetyStream estop
    a._on_safety_message(pb.ControlPlaneSafetyMessage(robot_id=ROBOT,
                                                      estop=pb.Estop(estop_id="e1")))
    time.sleep(0.1)
    arrived["f1"] = time.monotonic()  # its fence-only sibling on the ControlStream
    a._handle_command(pb.Command(command_id=1, robot_id=ROBOT, leader_term=1,
                                 estop=pb.Estop(estop_id="f1")))
    time.sleep(0.1)
    arrived["g1"] = time.monotonic()  # an edge node's estop, for this vehicle alone
    a._edge_stop("g1", answers.ack)

    deadline = time.monotonic() + 3 * BUDGET_S + 1
    while len(answers.got) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    got = list(answers.got)
    assert [g[0] for g in got] == ["e1", "f1", "g1"], got  # all answered, in arrival order
    gaps = {estop_id: round(at - arrived[estop_id], 3) for estop_id, _state, at in got}
    print(f"answered after arriving (s): {gaps}")
    assert all(g <= BUDGET_S + SLACK_S for g in gaps.values()), gaps
    want = pb.ESTOP_STATE_STOPPED if confirmed else pb.ESTOP_STATE_FAILED
    assert [g[1] for g in got] == [want] * 3


def test_a_stop_past_its_deadline_is_still_looked_at_once(adapter) -> None:
    """A stop whose turn on the lane comes after its deadline is answered from one look at the
    vehicle, never FAILED unlooked: here the vehicle is at rest, so it is STOPPED."""
    a, b = adapter, adapter._robot
    b.rest_at = 0.0  # at rest from the start
    with a._robot_lock:
        since = a._command_estop()
    assert a._await_estop(since, time.monotonic() - 1.0) == ESTOP_STOPPED
