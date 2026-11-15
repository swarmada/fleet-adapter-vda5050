# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Safety tests for the VDA5050 binding (sub-item 4): confirmed stop / hold (C5),
shared by lease-expiry self-stop (C4).

The safety-critical property: STOPPED is reported ONLY from the robot's `state`
(driving=false + velocity ≈ 0) — never inferred from the stop command, a timer, or
an absent message (CONFORMANCE.md C5.3). The stop publishes a `cancelOrder`
instantAction — NOT `stopPause`, which in VDA5050 RESUMES motion.
"""

from __future__ import annotations

import json

from conftest import mark_transport_live, reply_on_tick
from swarmada_sdk.safety import ESTOP_FAILED, ESTOP_STOPPED, confirm_estop

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding


def _state(driving=False, vx=0.0, vy=0.0, omega=0.0, paused=False):
    return json.dumps({"driving": driving, "velocity": {"vx": vx, "vy": vy, "omega": omega},
                       "paused": paused})


def test_is_stopped_false_before_any_state() -> None:
    # No state observed → no evidence of rest → never claim STOPPED.
    b = Vda5050Binding(robot_id="amr-1")
    assert b.is_stopped("amr-1") is False


def test_is_stopped_false_while_driving_or_moving() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.on_state(_state(driving=True, vx=0.0))       # reports driving
    assert b.is_stopped("amr-1") is False
    b.on_state(_state(driving=False, vx=0.4))      # velocity nonzero despite driving=false
    assert b.is_stopped("amr-1") is False


def test_is_stopped_true_only_when_not_driving_still_and_paused() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.on_state(_state(driving=False, vx=0.005, omega=0.005))  # within sensor noise
    assert b.is_stopped("amr-1") is False  # at rest, but nothing says it will stay there
    b.on_state(_state(driving=False, vx=0.005, omega=0.005, paused=True))
    assert b.is_stopped("amr-1") is True


def test_command_stop_publishes_cancel_order_not_stop_pause() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.command_move("amr-1", 5.0, 5.0)
    assert b.pending_order("amr-1") is not None
    b.command_stop("amr-1")
    action = json.loads(b.last_action("amr-1"))
    assert action["actions"][0]["actionType"] == profiles.ACTION_CANCEL_ORDER
    assert action["actions"][0]["actionType"] != "stopPause"   # would RESUME — never on estop
    assert action["actions"][0]["blockingType"] == "HARD"
    assert b.pending_order("amr-1") is None                    # active order aborted
    # A stop is only a REQUEST: without state confirming rest, not yet STOPPED.
    assert b.is_stopped("amr-1") is False


def test_stop_uses_monotonic_header_id() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.command_move("amr-1", 1.0, 1.0)                 # headerId 1
    b.command_stop("amr-1")
    h1 = json.loads(b.last_action("amr-1"))["headerId"]
    b.command_stop("amr-1")
    h2 = json.loads(b.last_action("amr-1"))["headerId"]
    assert h2 > h1                                    # per-topic dedup key advances


# `confirm_estop` ticks the binding while it polls, and since the lifecycle fix `tick()` refuses
# a transport that was never brought up. These two are standing in for a LIVE robot that
# is reporting `state`, so they say so — see conftest.mark_transport_live. The estop
# behaviour of an INERT binding is covered on its own in test_binding_lifecycle.py.

def test_confirm_estop_stopped_when_state_confirms_rest() -> None:
    # The SDK's confirm_estop stops with command_stop (a cancelOrder, no startPause): only a
    # vehicle that reports itself paused, in a state sent after the stop, confirms it.
    b = reply_on_tick(mark_transport_live(Vda5050Binding(robot_id="amr-1")), paused=True)
    assert confirm_estop(b, "amr-1") == ESTOP_STOPPED


def test_confirm_estop_fails_from_a_state_older_than_the_stop() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    b.on_state(json.dumps({"headerId": 1, "timestamp": "t1", "driving": False, "paused": True,
                           "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0}}))
    assert confirm_estop(b, "amr-1", tick_dt=0.0, max_ticks=5) == ESTOP_FAILED


def test_confirm_estop_fails_when_robot_still_moving() -> None:
    # Stop commanded but state still shows motion → FAILED, never a false STOPPED.
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    b.on_state(_state(driving=True, vx=0.5))
    assert confirm_estop(b, "amr-1", tick_dt=0.0, max_ticks=5) == ESTOP_FAILED


def test_new_order_releases_hold() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.command_stop("amr-1")
    b.command_move("amr-1", 2.0, 3.0)                 # a fresh task supersedes the stop
    assert b.pending_order("amr-1") is not None
