# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""--route-graph: an order follows the lanes of a route graph instead of one straight edge.

* The route: from the vehicle's position onto the graph, the shortest path, the goal. A node
  within 0.05 m of the start or the goal is not sent (no zero-length edge).
* A start between two nodes enters by its edge, at whichever end makes the route shorter; a
  start more than 0.5 m from every edge goes straight to the nearest node, and is logged.
* The graph is checked at load, and a graph that is not connected is refused.
* With a graph, a goal more than 0.05 m from every lane node is refused INVALID_ACTION.
* The idLen check covers the graph's node ids.
* Without a graph, the order is the two-node order it always was.

The tests use examples/ks-aisles.yaml; one structural test loads the shipped
examples/warehouse-lanes.yaml. With WAREHOUSE_LANES_REFERENCE set to the path of the lane graph
it was generated as, another test checks that the copy has the same nodes, coordinates and
edges (not the same bytes: comments and other keys may differ)."""

from __future__ import annotations

import json
import math
import os
import pathlib

import pytest
from conftest import mark_registered, mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.robot import SimulatedRobot
from fleet_adapter_vda5050.routing import RouteGraph
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

EXAMPLES = pathlib.Path(__file__).resolve().parents[1] / "examples"
KS = EXAMPLES / "ks-aisles.yaml"
WAREHOUSE = EXAMPLES / "warehouse-lanes.yaml"
ROBOT = "amr-1"
ORDER = f"{ROBOT}-b0-order"


def _ks() -> RouteGraph:
    return RouteGraph.load(str(KS))


def _ids(route) -> list[str]:
    return [w.node_id for w in route.waypoints]


def _state(x=0.0, y=0.0, order_id="", nodes=0, driving=False, vx=0.0, errors=(),
           actions=()) -> str:
    return json.dumps({
        "agvPosition": {"x": x, "y": y, "theta": 0.0, "mapId": "local",
                        "positionInitialized": True},
        "driving": driving, "velocity": {"vx": vx, "vy": 0.0, "omega": 0.0},
        "orderId": order_id, "nodeStates": [{"nodeId": f"n{i}"} for i in range(nodes)],
        "actionStates": [{"actionId": a, "actionStatus": st} for a, st in actions],
        "errors": list(errors)})


def _binding(x=3.0, y=16.0, graph=True) -> Vda5050Binding:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0",
                                           route_graph=_ks() if graph else None))
    b.on_state(_state(x, y))
    return b


def _orders(b: Vda5050Binding) -> list[dict]:
    return [json.loads(p) for t, p in b.published if t.endswith("/order")]


def _node_ids(order: dict) -> list[str]:
    return [n["nodeId"] for n in order["nodes"]]


# ── the route ────────────────────────────────────────────────────────────────

def test_aisle_to_aisle_goes_round_by_the_dock_row_without_its_end_nodes() -> None:
    # aisle-1-south to aisle-2-south: both ends are nodes, so neither is sent again.
    assert _ids(_ks().route(3.0, 7.0, 9.0, 7.0)) == [
        "aisle-1-dock", "receiving-dock-1", "aisle-2-dock"]


def test_a_node_just_outside_the_tolerance_of_the_start_is_kept() -> None:
    # 0.06 m from aisle-1-south: a short leg, not a zero-length one.
    assert _ids(_ks().route(3.0, 7.06, 9.0, 7.0))[0] == "aisle-1-south"


def test_a_node_within_the_tolerance_of_either_end_is_dropped() -> None:
    ids = _ids(_ks().route(3.0, 7.04, 9.04, 7.0))
    assert ids == ["aisle-1-dock", "receiving-dock-1", "aisle-2-dock"]


def test_a_start_between_two_nodes_drives_on_toward_the_goal() -> None:
    # Between aisle-1-south (2.0 m behind) and aisle-1-middle (2.5 m ahead), bound north. The
    # nearest node is behind; entering by the edge, the vehicle does not turn back to it.
    assert _ids(_ks().route(3.0, 9.0, 3.0, 16.0)) == ["aisle-1-middle"]


def test_a_start_between_two_nodes_turns_back_when_the_goal_is_behind() -> None:
    assert _ids(_ks().route(3.0, 9.0, 3.0, 2.1)) == ["aisle-1-south"]


def test_a_start_beside_an_edge_enters_by_it() -> None:
    route = _ks().route(3.3, 9.0, 3.0, 16.0)  # 0.3 m beside the aisle
    assert _ids(route) == ["aisle-1-middle"] and route.off_graph_m is None


def test_a_start_off_the_graph_goes_to_the_nearest_node_and_says_how_far() -> None:
    route = _ks().route(6.0, 6.0, 9.0, 7.0)  # 3 m from aisle 1, between racks on a real floor
    assert _ids(route)[0] == "aisle-1-south"
    assert route.off_graph_m == pytest.approx(3.0)


def test_a_start_at_a_junction_takes_the_lane_toward_the_goal() -> None:
    # Just off aisle-1-dock, where the dock row meets aisle 1, bound up the aisle.
    assert _ids(_ks().route(3.1, 2.1, 3.0, 11.5)) == ["aisle-1-dock", "aisle-1-south"]


def test_a_start_past_the_end_of_a_spur_enters_by_it() -> None:
    # 0.3 m beyond charging-bay, at the end of its spur. The spur's far end is nearer by the
    # route, so the first leg runs up the spur, through charging-bay, to charge-spur.
    route = _ks().route(1.1, 0.8, 3.0, 7.0)
    assert _ids(route) == ["charge-spur", "aisle-1-dock"]
    assert route.off_graph_m is None


def test_a_start_at_a_corner_turns_onto_the_north_edge() -> None:
    # 0.2 m below the north edge by corner-nw, bound east: along the edge, not back to the corner.
    assert _ids(_ks().route(-4.0, 18.8, 9.0, 16.0)) == ["aisle-1-top", "aisle-2-top"]


def _near_an_edge(g: RouteGraph, x: float, y: float, within: float) -> bool:
    from fleet_adapter_vda5050.routing import _segment_distance
    return any(_segment_distance((x, y), g.position(a), g.position(b)) <= within + 1e-9
               for a, b in g.edges)


@pytest.mark.parametrize("path", [KS, WAREHOUSE])
def test_every_leg_stays_on_the_lanes(path) -> None:
    # 3000 random starts within 0.5 m of a lane, to random lane nodes. The first leg stays
    # within the start's own distance of the lanes, or within 0.05 m where the start's entry
    # node was dropped; every later leg is an edge of the graph.
    import random
    g = RouteGraph.load(str(path))
    rng = random.Random(4)
    edges = g.edges
    adjacent = {frozenset(e) for e in edges}
    ids = g.node_ids
    for _ in range(3000):
        a, b = rng.choice(edges)
        (ax, ay), (bx, by) = g.position(a), g.position(b)
        t, off = rng.random(), rng.uniform(-0.5, 0.5)
        dx, dy = bx - ax, by - ay
        n = math.hypot(dx, dy)
        sx, sy = ax + t * dx - off * dy / n, ay + t * dy + off * dx / n
        goal = rng.choice(ids)
        gx, gy = g.position(goal)
        route = g.route(sx, sy, gx, gy)
        assert route.off_graph_m is None
        pts = [(sx, sy)] + [(w.x, w.y) for w in route.waypoints] + [(gx, gy)]
        names = [None] + [w.node_id for w in route.waypoints] + [goal]
        for k in range(11):  # the first leg, sampled
            f = k / 10
            px, py = pts[0][0] + f * (pts[1][0] - pts[0][0]), pts[0][1] + f * (pts[1][1] - pts[0][1])
            assert _near_an_edge(g, px, py, max(abs(off), 0.05))
        for i in range(1, len(pts) - 1):
            if names[i] != names[i + 1]:
                assert frozenset((names[i], names[i + 1])) in adjacent, (sx, sy, goal, names)


def test_the_same_node_at_both_ends_needs_no_waypoint() -> None:
    assert _ids(_ks().route(3.0, 7.0, 3.0, 7.0)) == []


def test_over_the_north_edge_not_the_diagonal() -> None:
    assert _ids(_ks().route(3.0, 16.0, 21.0, 11.5)) == [
        "aisle-1-top", "aisle-2-top", "aisle-3-top", "aisle-4-top", "aisle-4-north"]


def test_nearest_names_the_node_and_its_distance() -> None:
    node, d = _ks().nearest(3.0, 9.0)
    assert node == "aisle-1-south" and d == pytest.approx(2.0)


# ── the graph, checked at load ───────────────────────────────────────────────

_TWO = {"a": [0.0, 0.0], "b": [1.0, 0.0]}


@pytest.mark.parametrize(("nodes", "edges", "says"), [
    ({}, [], "no nodes"),
    (_TWO, [["a", "c"]], "unknown node"),
    (_TWO, [["a", "a"], ["a", "b"]], "joins a node to itself"),
    (_TWO, [["a", "b"], ["b", "a"]], "listed twice"),
    (_TWO, [["a", "b", "c"]], "not a pair"),
    ({"a": [0.0, math.nan], "b": [1.0, 0.0]}, [["a", "b"]], "not a finite number"),
    ({"a": [0.0, math.inf], "b": [1.0, 0.0]}, [["a", "b"]], "not a finite number"),
    ({"a": [True, 0.0], "b": [1.0, 0.0]}, [["a", "b"]], "not a finite number"),
    ({"a": ["0", 0.0], "b": [1.0, 0.0]}, [["a", "b"]], "not a finite number"),
    ({"a": [0.0], "b": [1.0, 0.0]}, [["a", "b"]], r"not \[x, y\]"),
    ({"a": [0.0, 0.0], "b": [0.04, 0.0]}, [["a", "b"]], "closer than 0.05 m"),
    ({"start": [0.0, 0.0], "b": [1.0, 0.0]}, [["start", "b"]], "reserved"),
    ({"goal": [0.0, 0.0], "b": [1.0, 0.0]}, [["goal", "b"]], "reserved"),
    ({**_TWO, "c": [5.0, 5.0]}, [["a", "b"]], "not connected"),
    ({1: [0.0, 0.0], "b": [1.0, 0.0]}, [[1, "b"]], "not a non-empty string"),
    ({"a": [0.0, 0.0]}, [], "no edges"),
    (_TWO, [[["a"], "b"]], "not by two node ids"),
    ({"a": [0.0, 1e308], "b": [1.0, 0.0]}, [["a", "b"]], "within 1e\\+06 m"),
])
def test_a_bad_graph_is_refused(nodes, edges, says) -> None:
    with pytest.raises(ValueError, match=says):
        RouteGraph(nodes, edges)


def test_a_node_id_given_twice_is_refused(tmp_path) -> None:
    f = tmp_path / "g.yaml"
    f.write_text("nodes:\n  a: [0, 0]\n  b: [10, 0]\n  b: [0, 10]\nedges: [[a, b]]\n")
    with pytest.raises(ValueError, match="'b' is given twice"):
        RouteGraph.load(str(f))


def test_a_route_from_a_position_that_is_not_finite_is_refused() -> None:
    with pytest.raises(ValueError, match="no route"):
        _ks().route(math.nan, 7.0, 9.0, 7.0)


def test_a_file_that_is_not_a_graph_is_refused(tmp_path) -> None:
    for text in ("- just\n- a list\n", "nodes: [unclosed\n"):
        f = tmp_path / "g.yaml"
        f.write_text(text)
        with pytest.raises(ValueError, match="route graph"):
            RouteGraph.load(str(f))
    with pytest.raises(ValueError, match="route graph"):
        RouteGraph.load(str(tmp_path / "missing.yaml"))


def test_the_shipped_warehouse_lanes_load_and_cross_by_the_cross_aisle() -> None:
    g = RouteGraph.load(str(WAREHOUSE))  # loads only when connected (checked at load)
    ids = _ids(g.route(3.0, 7.0, 9.0, 7.0))  # aisle-1-south to aisle-2-south
    assert "aisle-1-middle" in ids and "aisle-2-middle" in ids  # along the cross-aisle (y 11.5)
    assert not [n for n in ids if "dock" in n]  # not round by the dock row


def _lanes(g: RouteGraph) -> tuple[dict[str, tuple[float, float]], set[frozenset[str]]]:
    return {n: g.position(n) for n in g.node_ids}, {frozenset(e) for e in g.edges}


@pytest.mark.skipif(not os.environ.get("WAREHOUSE_LANES_REFERENCE"),
                    reason="WAREHOUSE_LANES_REFERENCE is not set")
def test_the_shipped_warehouse_lanes_are_the_reference_lane_graph() -> None:
    # The lane graph, not the bytes: the same node ids at the same coordinates, the same
    # undirected edges, each once (a duplicate is refused at load).
    ref = RouteGraph.load(os.environ["WAREHOUSE_LANES_REFERENCE"])
    shipped = RouteGraph.load(str(WAREHOUSE))
    assert _lanes(shipped) == _lanes(ref)
    assert shipped.edge_count == ref.edge_count


# ── the order ────────────────────────────────────────────────────────────────

def test_an_order_with_via_is_one_released_base_with_no_heading_at_the_lane_nodes() -> None:
    via = [profiles.OrderTarget(x=1.0, y=0.0, label="a"), profiles.OrderTarget(x=1.0, y=1.0)]
    o = json.loads(Vda5050Profile().encode_order(
        header_id=1, order_id="o", order_update_id=0, manufacturer="m", serial="s",
        start=profiles.OrderTarget(x=0.0, y=0.0, theta=0.5),
        goal=profiles.OrderTarget(x=2.0, y=1.0, theta=1.5), timestamp="t", via=via))
    assert _node_ids(o) == ["start", "a", "via-1", "goal"]
    assert [n["sequenceId"] for n in o["nodes"]] == [0, 2, 4, 6]
    assert [(e["edgeId"], e["startNodeId"], e["endNodeId"], e["sequenceId"])
            for e in o["edges"]] == [("edge-0", "start", "a", 1), ("edge-1", "a", "via-1", 3),
                                     ("edge-2", "via-1", "goal", 5)]
    assert all(n["released"] for n in o["nodes"]) and all(e["released"] for e in o["edges"])
    thetas = [n["nodePosition"].get("theta") for n in o["nodes"]]
    assert thetas == [0.5, None, None, 1.5]


def test_an_order_without_via_is_the_two_node_order() -> None:
    o = json.loads(Vda5050Profile().encode_order(
        header_id=1, order_id="o", order_update_id=0, manufacturer="m", serial="s",
        start=profiles.OrderTarget(x=0.0, y=0.0), goal=profiles.OrderTarget(x=2.0, y=1.0),
        timestamp="t"))
    assert o["nodes"] == [
        {"nodeId": "start", "sequenceId": 0, "released": True, "actions": [],
         "nodePosition": {"x": 0.0, "y": 0.0, "theta": 0.0, "mapId": ""}},
        {"nodeId": "goal", "sequenceId": 2, "released": True, "actions": [],
         "nodePosition": {"x": 2.0, "y": 1.0, "theta": 0.0, "mapId": ""}}]
    assert o["edges"] == [{"edgeId": "edge-0", "sequenceId": 1, "released": True,
                           "startNodeId": "start", "endNodeId": "goal", "actions": []}]


# ── the binding ──────────────────────────────────────────────────────────────

def test_the_binding_routes_along_the_graph_on_the_vehicles_map() -> None:
    b = _binding(3.0, 16.0)                # aisle-1-north
    b.command_move(ROBOT, 21.0, 11.5)      # aisle-4-middle
    (o,) = _orders(b)
    assert _node_ids(o) == ["start", "aisle-1-top", "aisle-2-top", "aisle-3-top",
                            "aisle-4-top", "aisle-4-north", "goal"]
    assert all(n["nodePosition"]["mapId"] == "local" for n in o["nodes"])


def test_no_graph_keeps_the_two_node_order() -> None:
    b = _binding(3.0, 16.0, graph=False)
    b.command_move(ROBOT, 21.0, 11.5)
    assert _node_ids(_orders(b)[0]) == ["start", "goal"]


@pytest.mark.parametrize("position", [
    None,                                                       # no state yet
    {"x": 3.0, "y": 7.0, "theta": 0.0, "positionInitialized": False},
])
def test_an_unknown_position_makes_routing_unavailable(position) -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", route_graph=_ks()))
    if position is not None:
        b.on_state(json.dumps({"agvPosition": position, "driving": False}))
    assert b.route_unavailable(ROBOT) == (
        "the vehicle's position is unknown, and with a route graph an order starts from it")
    assert _binding().route_unavailable(ROBOT) == ""
    assert mark_transport_live(Vda5050Binding(robot_id=ROBOT)).route_unavailable(ROBOT) == ""


def test_a_held_order_sent_without_a_position_is_unrouted_and_logged(capsys) -> None:
    # Work is refused without a position; a held order can still go out on a state that has
    # none. It is sent unrouted, and said so.
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", route_graph=_ks()))
    b.command_move(ROBOT, 21.0, 11.5)
    assert _node_ids(_orders(b)[0]) == ["start", "goal"]
    assert f"order {ORDER}-1 is sent without a route" in capsys.readouterr().err


def test_a_routing_failure_sends_nothing_and_changes_nothing(monkeypatch) -> None:
    b = _binding(3.0, 9.0)
    b.command_estop(ROBOT)                 # an estop pause, lifted by the next order
    before = len(b.published)

    def broken(*_a):
        raise RuntimeError("route")

    monkeypatch.setattr(b._graph, "route", broken)
    with pytest.raises(RuntimeError):
        b.command_move(ROBOT, 3.0, 16.0)
    assert len(b.published) == before      # no stopPause, no order
    monkeypatch.undo()
    b.command_move(ROBOT, 3.0, 16.0)       # the pause is still there to lift
    kinds = [json.loads(p).get("actions", [{}])[0].get("actionType", "order")
             for _, p in b.published[before:]]
    assert kinds == ["stopPause", "order"]


def test_after_an_estop_the_routed_order_goes_out_behind_its_stop_pause() -> None:
    b = _binding(3.0, 9.0)
    b.command_estop(ROBOT)
    b.command_move(ROBOT, 3.0, 16.0)
    kinds = [json.loads(p).get("actions", [{}])[0].get("actionType", "order")
             for _, p in b.published]
    assert kinds == ["startPause", "cancelOrder", "stopPause", "order"]
    assert _node_ids(_orders(b)[0]) == ["start", "aisle-1-middle", "goal"]


def test_an_order_from_off_the_graph_is_logged_with_its_distance(capsys) -> None:
    b = _binding(6.0, 6.0)
    b.command_move(ROBOT, 9.0, 7.0)
    err = capsys.readouterr().err
    assert f"order {ORDER}-1 starts 3.00 m from the nearest lane" in err
    assert "straight to aisle-1-south" in err
    assert _node_ids(_orders(b)[0]) == [
        "start", "aisle-1-south", "aisle-1-dock", "receiving-dock-1", "aisle-2-dock", "goal"]


def test_an_order_on_the_graph_logs_nothing(capsys) -> None:
    b = _binding(3.0, 9.0)
    b.command_move(ROBOT, 3.0, 16.0)
    assert "nearest lane" not in capsys.readouterr().err


def test_a_held_order_is_routed_from_where_the_vehicle_is_when_it_goes_out() -> None:
    b = _binding(3.0, 7.0)
    b.command_move(ROBOT, 3.0, 16.0)
    b.on_state(_state(3.0, 9.0, order_id=f"{ORDER}-1", nodes=2, driving=True, vx=0.5))
    b.command_move(ROBOT, 3.0, 2.1)        # preempted on the aisle, bound back south
    assert len(_orders(b)) == 1            # held behind its cancelOrder
    (cancel,) = [json.loads(p)["actions"][0]["actionId"] for t, p in b.published
                 if t.endswith("/instantActions")]
    # The old order gone, the vehicle mid-edge: it stopped 0.5 m on.
    b.on_state(_state(3.0, 9.5, order_id=f"{ORDER}-1", actions=[(cancel, "FINISHED")]))
    _, second = _orders(b)
    assert second["orderId"] == f"{ORDER}-2"
    assert _node_ids(second) == ["start", "aisle-1-south", "goal"]
    assert second["nodes"][0]["nodePosition"]["y"] == 9.5


def test_a_rejected_routed_order_still_fails_its_action() -> None:
    b = _binding(3.0, 16.0)
    b.command_move(ROBOT, 21.0, 11.5)
    b.on_state(_state(3.0, 16.0, errors=[{
        "errorType": "validationError", "errorLevel": "WARNING", "errorDescription": "bad node",
        "errorReferences": [{"referenceKey": "orderId", "referenceValue": f"{ORDER}-1"}]}]))
    assert b.task_state(ROBOT) == task_map.TASK_FAILED


@pytest.mark.parametrize(("x", "y", "refused"), [
    (21.0, 11.5, False),     # aisle-4-middle
    (21.04, 11.5, False),    # within 0.05 m
    (21.06, 11.5, True),     # just outside
    (18.0, 15.0, True),      # between aisles 3 and 4
])
def test_a_goal_must_be_a_lane_node(x, y, refused) -> None:
    why = _binding().goal_refusal(ROBOT, x, y)
    assert bool(why) is refused
    if refused:
        assert "nearest lane node, aisle-" in why and " m away (at most 0.05 m)" in why


def test_without_a_graph_every_goal_can_be_sent() -> None:
    assert _binding(graph=False).goal_refusal(ROBOT, 18.0, 15.0) == ""


def test_the_refusal_names_the_node_and_the_distance() -> None:
    assert _binding().goal_refusal(ROBOT, 18.0, 16.0) == (
        "goal (18, 16) is not on the route graph: the nearest lane node, aisle-3-north, "
        "is 3.00 m away (at most 0.05 m)")


# ── idLen covers the graph's node ids ────────────────────────────────────────

def _factsheet(id_len: int) -> str:
    return json.dumps({"protocolLimits": {"maxStringLens": {"idLen": id_len}}})


def test_a_node_id_longer_than_idlen_is_logged_once_naming_it(capsys) -> None:
    b = _binding()
    b.on_factsheet(_factsheet(15))  # receiving-dock-N is 16 characters
    err = capsys.readouterr().err
    assert ("the route graph's node id receiving-dock-5 has 16; the vehicle will reject "
            "orders routed through it") in err
    b.on_factsheet(_factsheet(15))
    assert "node id" not in capsys.readouterr().err  # once for each limit


def test_node_ids_within_idlen_are_not_logged(capsys) -> None:
    b = _binding()
    b.on_factsheet(_factsheet(16))
    b.on_factsheet(_factsheet(64))
    assert "node id" not in capsys.readouterr().err


def test_no_graph_no_node_id_check(capsys) -> None:
    _binding(graph=False).on_factsheet(_factsheet(5))
    assert "node id" not in capsys.readouterr().err


# ── the adapter ──────────────────────────────────────────────────────────────

def _assign(a: Adapter, x: float, y: float, token: int = 5, action_id: str = "act-1"):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=json.dumps({"x": x, "y": y}).encode(),
        lease_duration_ms=30_000, lease_generation=1), res)
    return res.assign_action


def test_an_assignment_off_the_lanes_is_refused_invalid_action_and_sends_nothing() -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    res = _assign(a, 18.0, 15.0)
    assert not res.accepted
    assert res.rejection == pb.ASSIGN_ACTION_REJECTION_INVALID_ACTION
    assert res.message == ("goal (18, 15) is not on the route graph: the nearest lane node, "
                           "aisle-3-north, is 3.16 m away (at most 0.05 m)")
    assert b.published == []


def test_a_refused_goal_leaves_the_running_action_driving() -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, 21.0, 11.5, token=5, action_id="act-1").accepted
    res = _assign(a, 18.0, 15.0, token=6, action_id="act-2")
    assert res.rejection == pb.ASSIGN_ACTION_REJECTION_INVALID_ACTION
    assert a._current_action == "act-1" and a._current_fencing_token == 5
    assert len(_orders(b)) == 1 and not [p for t, p in b.published if "instantActions" in t]


def test_an_assignment_with_the_position_unknown_is_refused_unavailable() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", route_graph=_ks()))
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    res = _assign(a, 21.0, 11.5)
    assert res.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert res.message == ("the vehicle's position is unknown, and with a route graph an order "
                           "starts from it")
    assert b.published == []
    b.on_state(_state(3.0, 16.0))
    assert _assign(a, 21.0, 11.5, token=6).accepted  # refused before the fence: 6 is not stale


def test_an_assignment_to_a_lane_node_is_routed() -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, 21.0, 11.5).accepted
    assert "aisle-1-top" in _node_ids(_orders(b)[0])


def test_a_binding_without_goal_refusal_accepts_any_goal() -> None:
    a = mark_registered(Adapter(pb, None, SimulatedRobot(), ROBOT))
    assert _assign(a, 18.0, 15.0).accepted


# ── the flag ─────────────────────────────────────────────────────────────────

class _GotPast(Exception):
    """main() went past the check under test."""


def _trip(*_a, **_k):
    raise _GotPast


def test_route_graph_needs_vda5050_comms(monkeypatch, capsys) -> None:
    monkeypatch.setattr(adapter_mod, "dialler", _trip)  # checked before anything is dialled
    with pytest.raises(SystemExit) as exc:
        adapter_mod.main(["--insecure", "--route-graph", str(KS)])
    assert exc.value.code == 2
    assert "--route-graph needs --comms vda5050" in capsys.readouterr().err


def test_a_bad_route_graph_stops_the_adapter_before_a_binding_exists(
        monkeypatch, tmp_path, capsys) -> None:
    from fleet_adapter_vda5050 import vda5050_binding
    monkeypatch.setattr(vda5050_binding, "Vda5050Binding", _trip)
    f = tmp_path / "g.yaml"
    f.write_text("nodes: {a: [0, 0], b: [1, 0], c: [5, 5]}\nedges: [[a, b]]\n")
    with pytest.raises(SystemExit) as exc:
        adapter_mod.main(["--insecure", "--comms", "vda5050", "--route-graph", str(f)])
    assert exc.value.code == 2
    assert "not connected" in capsys.readouterr().err


def test_a_good_route_graph_reaches_the_binding(monkeypatch, capsys) -> None:
    from fleet_adapter_vda5050 import vda5050_binding
    seen = {}

    def binding(**kw):
        seen.update(kw)
        raise _GotPast

    monkeypatch.setattr(vda5050_binding, "Vda5050Binding", binding)
    with pytest.raises(_GotPast):
        adapter_mod.main(["--insecure", "--comms", "vda5050", "--route-graph", str(KS)])
    assert isinstance(seen["route_graph"], RouteGraph)
    assert f"route graph {KS}: 34 nodes, 36 edges" in capsys.readouterr().err
