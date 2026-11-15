# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""FleetTask → VDA5050 order + task-status derivation (sub-item 3, targets C6.4/C7).

Pure: fake AssignAction fields / state snapshots, no broker. Asserts the order shape
(canonical two-node base order, monotonic ids) and the state→TaskState mapping.
"""

from __future__ import annotations

import json

from fleet_adapter_vda5050 import profiles, task
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

P = Vda5050Profile()


# ── goal_from_assign (protocol-side, profile-agnostic) ────────────────────────

def test_goal_from_payload_coordinates() -> None:
    payload = json.dumps({"x": 5.0, "y": -3.0, "theta": 1.57, "mapId": "L2"}).encode()
    g = task.goal_from_assign("dock-7", payload)
    assert (g.x, g.y, g.theta, g.map_id) == (5.0, -3.0, 1.57, "L2")
    assert g.label == "dock-7"


def test_goal_from_destination_string_and_named() -> None:
    g = task.goal_from_assign("2.5,4.0,0.5")
    assert (g.x, g.y, g.theta) == (2.5, 4.0, 0.5) and g.label == ""
    named = task.goal_from_assign("charging-A")
    assert named.label == "charging-A" and (named.x, named.y) == (0.0, 0.0)


def test_goal_ignores_garbage_payload() -> None:
    g = task.goal_from_assign("bay-2", b"\xff not json")
    assert g.label == "bay-2" and (g.x, g.y) == (0.0, 0.0)


# ── encode_order (VDA5050 wire) ───────────────────────────────────────────────

def test_encode_order_is_canonical_two_node_base_order() -> None:
    goal = profiles.OrderTarget(x=10.0, y=2.0, theta=0.5, map_id="L1")
    start = profiles.OrderTarget(x=0.0, y=0.0, map_id="L1")
    payload = json.loads(P.encode_order(
        header_id=7, order_id="amr-1-order-1", order_update_id=0,
        manufacturer="acme", serial="agv-7", start=start, goal=goal, timestamp="2026-07-08T00:00:00Z"))
    assert payload["headerId"] == 7
    assert payload["orderId"] == "amr-1-order-1" and payload["orderUpdateId"] == 0
    assert payload["version"] == "2.0.0"
    # node seq 0 → edge seq 1 → node seq 2, all released
    assert [n["sequenceId"] for n in payload["nodes"]] == [0, 2]
    assert payload["edges"][0]["sequenceId"] == 1
    assert all(n["released"] for n in payload["nodes"])
    goal_node = payload["nodes"][1]["nodePosition"]
    assert (goal_node["x"], goal_node["y"], goal_node["theta"], goal_node["mapId"]) == (10.0, 2.0, 0.5, "L1")


def test_encode_order_defaults_start_to_goal_when_pose_unknown() -> None:
    # No current pose → the base order still has a valid first node (VDA5050 requires
    # the AGV to be on node 0); it degenerates to the goal rather than being invalid.
    payload = json.loads(P.encode_order(
        header_id=1, order_id="o", order_update_id=0, manufacturer="m", serial="s",
        start=None, goal=profiles.OrderTarget(x=4.0, y=5.0), timestamp="t"))
    assert payload["nodes"][0]["nodePosition"]["x"] == 4.0


# ── task_state_from_snapshot (status from state) ──────────────────────────────

def test_task_state_running_until_arrived() -> None:
    snap = profiles.StateSnapshot(order_id="o1", active_nodes=1, driving=True)
    assert task.task_state_from_snapshot(snap, "o1") == task.TASK_RUNNING


def test_task_state_succeeded_when_last_node_reached() -> None:
    snap = profiles.StateSnapshot(order_id="o1", active_nodes=0, driving=False,
                                  node_states_reported=True)
    assert task.task_state_from_snapshot(snap, "o1") == task.TASK_SUCCEEDED


def test_task_state_failed_on_fatal_error() -> None:
    snap = profiles.StateSnapshot(order_id="o1", active_nodes=0, phase=profiles.PHASE_ERROR)
    assert task.task_state_from_snapshot(snap, "o1") == task.TASK_FAILED


def test_task_state_running_for_a_different_order() -> None:
    # The robot is idle on a stale/other order — not our completion.
    snap = profiles.StateSnapshot(order_id="other", active_nodes=0, driving=False)
    assert task.task_state_from_snapshot(snap, "o1") == task.TASK_RUNNING


# ── binding: command_move builds an order with monotonic ids ──────────────────

def test_binding_command_move_builds_order_and_bumps_ids() -> None:
    b = Vda5050Binding(robot_id="amr-1", manufacturer="acme", serial="agv-7", boot="b0")
    b.command_move("amr-1", 3.0, 4.0, 1.0)
    first = json.loads(b.pending_order("amr-1"))
    assert first["orderId"] == "amr-1-b0-order-1" and first["headerId"] == 1
    b.command_move("amr-1", 6.0, 7.0)
    second = json.loads(b.pending_order("amr-1"))
    assert second["orderId"] == "amr-1-b0-order-2"    # fresh orderId per task
    assert second["headerId"] == 2                    # monotonic headerId (dedup key)


def test_order_ids_differ_between_adapter_processes() -> None:
    """A vehicle reports a rejection until it accepts a new order: a restarted adapter's first
    order must not carry the id of the previous process's first order."""
    ids = set()
    for _ in range(20):
        b = Vda5050Binding(robot_id="amr-1")
        b.command_move("amr-1", 1.0, 0.0)
        ids.add(json.loads(b.pending_order("amr-1"))["orderId"])
    assert len(ids) == 20


def test_binding_order_uses_current_pose_as_start_node() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.on_state(json.dumps({"agvPosition": {"x": 1.0, "y": 1.0, "theta": 0.0,
                                           "mapId": "L1", "positionInitialized": True}}))
    b.command_move("amr-1", 9.0, 9.0)
    order = json.loads(b.pending_order("amr-1"))
    assert order["nodes"][0]["nodePosition"]["x"] == 1.0   # start = current pose
    assert order["nodes"][1]["nodePosition"]["x"] == 9.0   # goal
    assert order["nodes"][0]["nodePosition"]["mapId"] == "L1"
