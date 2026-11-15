# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""The route graph's optional ``map_id``, and the vehicle's ``mapId``.

* A graph may name the map its lanes are drawn on (``map_id``, a non-empty string). When it
  does and the vehicle reports another ``mapId``, the adapter logs an error, once for each pair
  of maps. Nothing is refused, and orders carry the vehicle's own mapId.
* An empty or missing reported mapId logs nothing.
* A mapId that is not a string (``null``, a number) reads as empty, so an order never carries
  ``"None"``."""

from __future__ import annotations

import json
import pathlib

import pytest
from conftest import mark_transport_live

from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.routing import RouteGraph
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
NODES = {"a": [0, 0], "b": [5, 0]}
EDGES = [["a", "b"]]
KS = pathlib.Path(__file__).resolve().parents[1] / "examples" / "ks-aisles.yaml"
MISMATCH = "the route graph is for map"


def _state(hid: int, map_id=None, *, position: bool = True) -> str:
    body = {"headerId": hid, "timestamp": f"t{hid}", "driving": False,
            "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
            "nodeStates": [], "actionStates": []}
    if position:
        body["agvPosition"] = {"x": 0.0, "y": 0.0, "theta": 0.0, "positionInitialized": True}
        if map_id is not ...:
            body["agvPosition"]["mapId"] = map_id
    return json.dumps(body)


def _binding(map_id: str | None = "warehouse") -> Vda5050Binding:
    graph = RouteGraph(NODES, EDGES, map_id)
    return mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", route_graph=graph))


def _mismatches(capsys) -> list[str]:
    return [ln for ln in capsys.readouterr().err.splitlines() if MISMATCH in ln]


# ── the file ─────────────────────────────────────────────────────────────────

def test_a_graph_file_may_name_its_map(tmp_path) -> None:
    f = tmp_path / "g.yaml"
    f.write_text("map_id: warehouse\nnodes: {a: [0, 0], b: [5, 0]}\nedges: [[a, b]]\n")
    assert RouteGraph.load(str(f)).map_id == "warehouse"


def test_a_graph_file_without_map_id_names_none() -> None:
    assert RouteGraph.load(str(KS)).map_id is None


@pytest.mark.parametrize("value", ['""', "7", "[a]", "{a: 1}", "true"])
def test_a_map_id_that_is_not_a_non_empty_string_stops_the_load(tmp_path, value) -> None:
    f = tmp_path / "g.yaml"
    f.write_text(f"map_id: {value}\nnodes: {{a: [0, 0], b: [5, 0]}}\nedges: [[a, b]]\n")
    with pytest.raises(ValueError, match="map_id is .*not a non-empty string"):
        RouteGraph.load(str(f))


def test_a_map_id_given_with_no_value_stops_the_load(tmp_path) -> None:
    f = tmp_path / "g.yaml"
    f.write_text("map_id:\nnodes: {a: [0, 0], b: [5, 0]}\nedges: [[a, b]]\n")
    with pytest.raises(ValueError, match="map_id is given with no value"):
        RouteGraph.load(str(f))


# ── the check ────────────────────────────────────────────────────────────────

def test_another_map_is_logged_once_for_each_pair(capsys) -> None:
    b = _binding()
    b.on_state(_state(1, "local"))
    b.on_state(_state(2, "local"))
    (line,) = _mismatches(capsys)
    assert "map 'warehouse' and the vehicle reports map 'local'" in line
    b.on_state(_state(3, "floor-2"))           # a second pair: said again
    b.on_state(_state(4, "local"))             # the first again: not
    (line,) = _mismatches(capsys)
    assert "reports map 'floor-2'" in line


def test_the_same_map_is_not_logged(capsys) -> None:
    b = _binding()
    b.on_state(_state(1, "warehouse"))
    assert _mismatches(capsys) == []


@pytest.mark.parametrize("map_id", ["", None, 7, ...])
def test_an_empty_or_missing_map_id_is_not_logged(capsys, map_id) -> None:
    b = _binding()
    b.on_state(_state(1, map_id))
    b.on_state(_state(2, position=False))
    assert _mismatches(capsys) == []


def test_a_graph_without_map_id_logs_nothing(capsys) -> None:
    b = _binding(map_id=None)
    b.on_state(_state(1, "local"))
    assert _mismatches(capsys) == []


def test_no_graph_logs_nothing(capsys) -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))
    b.on_state(_state(1, "local"))
    assert _mismatches(capsys) == []


def test_a_repeated_state_is_not_checked_again(capsys) -> None:
    b = _binding()
    b.on_state(_state(1, "local"))
    b._map_mismatch_reported.clear()          # as if not yet said
    b.on_state(_state(1, "local"))            # a redelivery: dropped before any check
    assert len(_mismatches(capsys)) == 1


def test_a_mismatch_refuses_nothing_and_orders_carry_the_vehicle_s_map() -> None:
    b = _binding()
    b.on_state(_state(1, "local"))
    b.command_move(ROBOT, 5.0, 0.0)
    (order,) = [json.loads(p) for t, p in b.published if t.endswith("/order")]
    assert {n["nodePosition"]["mapId"] for n in order["nodes"]} == {"local"}


# ── a mapId that is not a string ─────────────────────────────────────────────

@pytest.mark.parametrize("map_id", [None, 7, 1.5, True, ["m"], {"m": 1}])
def test_a_map_id_that_is_not_a_string_reads_as_empty(map_id) -> None:
    st = Vda5050Profile().parse_state(_state(1, map_id))
    assert st.map_id == ""


def test_an_order_never_carries_none_for_a_null_map_id() -> None:
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))
    b.on_state(_state(1, None))
    b.command_move(ROBOT, 5.0, 0.0)
    (payload,) = [p for t, p in b.published if t.endswith("/order")]
    assert "None" not in payload
    assert {n["nodePosition"]["mapId"] for n in json.loads(payload)["nodes"]} == {""}


def test_a_string_map_id_is_kept() -> None:
    assert Vda5050Profile().parse_state(_state(1, "local")).map_id == "local"


# ── the start-up line ────────────────────────────────────────────────────────

class _GotPast(Exception):
    """main() went past the check under test."""


def test_the_start_up_line_names_the_graph_s_map(monkeypatch, tmp_path, capsys) -> None:
    from fleet_adapter_vda5050 import vda5050_binding

    def _trip(**_kw):
        raise _GotPast

    monkeypatch.setattr(vda5050_binding, "Vda5050Binding", _trip)
    f = tmp_path / "g.yaml"
    f.write_text("map_id: warehouse\nnodes: {a: [0, 0], b: [5, 0]}\nedges: [[a, b]]\n")
    with pytest.raises(_GotPast):
        adapter_mod.main(["--insecure", "--comms", "vda5050", "--route-graph", str(f)])
    assert f"route graph {f}: 2 nodes, 1 edges, map warehouse" in capsys.readouterr().err
