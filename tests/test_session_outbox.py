# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Each ControlStream session's requests come from that session's own outbox.

A stream that has died is not stopped the instant it faults: its request generator can still
be blocked waiting for the next message. If it waited on whatever outbox the adapter holds now,
it could take the RegisterRobot queued for the NEXT session, and that session's
re-registration (C2.4) would never reach the control plane."""

from __future__ import annotations

import queue
import threading

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"
_DONE = object()  # the generator returned
_BLOCKED = object()  # the generator was still waiting when the time ran out


def _next_within(gen, seconds: float = 1.0):
    """The generator's next item, _DONE if it returned, or _BLOCKED, so a generator that waits
    on the wrong queue fails the test instead of hanging it."""
    out: list = []

    def step() -> None:
        try:
            out.append(next(gen))
        except StopIteration:
            out.append(_DONE)

    t = threading.Thread(target=step, daemon=True)
    t.start()
    t.join(seconds)
    return out[0] if out else _BLOCKED


def test_a_dying_sessions_generator_does_not_take_the_next_sessions_messages() -> None:
    a = Adapter(pb, None, SimulatedRobot(), ROBOT)
    old: queue.Queue = queue.Queue()
    gen = a._control_requests(old)
    assert next(gen).WhichOneof("payload") == "hello"
    new: queue.Queue = queue.Queue()
    a._outbox = new  # the next session opened
    new.put(pb.AdapterMessage(register=pb.RegisterRobot(robot_id=ROBOT)))
    old.put(None)  # the old session ends
    assert _next_within(gen) is _DONE
    assert new.get_nowait().WhichOneof("payload") == "register"  # still there for the new one


def test_without_an_outbox_the_generator_binds_the_current_one() -> None:
    a = Adapter(pb, None, SimulatedRobot(), ROBOT)
    bound = a._outbox
    gen = a._control_requests()
    next(gen)  # the hello; the outbox is bound from here
    a._outbox = queue.Queue()
    bound.put(pb.AdapterMessage(register=pb.RegisterRobot(robot_id=ROBOT)))
    msg = _next_within(gen)
    assert msg not in (_DONE, _BLOCKED) and msg.WhichOneof("payload") == "register"
