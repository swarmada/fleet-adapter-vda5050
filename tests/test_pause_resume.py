# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Pause / resume mapping: Swarmada pause/resume Commands → VDA5050 native
startPause / stopPause instantActions (beyond the C1-C8 safety+task subset).

Pause pauses the active order WITHOUT aborting it (distinct from the confirmed-stop
cancelOrder safe-hold), so it can later resume; the physical halt is confirmed only
via is_stopped (the same never-faked contract as estop).
"""

from __future__ import annotations

import json

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.robot import SimulatedRobot
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding


def test_command_pause_publishes_start_pause() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.command_pause("amr-1")
    action = json.loads(b.last_action("amr-1"))
    assert action["actions"][0]["actionType"] == profiles.ACTION_START_PAUSE  # "startPause"


def test_command_resume_publishes_stop_pause() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.command_resume("amr-1")
    action = json.loads(b.last_action("amr-1"))
    assert action["actions"][0]["actionType"] == profiles.ACTION_STOP_PAUSE  # "stopPause"


def test_pause_resume_advance_monotonic_header_id() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.command_pause("amr-1")
    h1 = json.loads(b.last_action("amr-1"))["headerId"]
    b.command_resume("amr-1")
    h2 = json.loads(b.last_action("amr-1"))["headerId"]
    assert h2 > h1  # per-topic dedup key advances


def test_pause_preserves_the_active_order() -> None:
    # Unlike command_stop (cancelOrder, abort + hold), startPause keeps the order so
    # it can resume.
    b = Vda5050Binding(robot_id="amr-1")
    b.command_move("amr-1", 5.0, 5.0)
    assert b.pending_order("amr-1") is not None
    b.command_pause("amr-1")
    assert b.pending_order("amr-1") is not None  # order NOT aborted


def test_sim_robot_pause_halts_and_resume_moves() -> None:
    r = SimulatedRobot()
    r.spawn("r1")
    r.command_move("r1", 5.0, 0.0)
    assert not r.is_stopped("r1")
    r.command_pause("r1")
    assert r.is_stopped("r1")      # startPause halts; confirmed via is_stopped
    r.command_resume("r1")
    assert not r.is_stopped("r1")  # stopPause resumes the held order
