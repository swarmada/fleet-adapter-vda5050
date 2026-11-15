# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A route graph: the lanes a VDA5050 order follows (``--route-graph``).

In VDA5050 the fleet master plans the route, and the vehicle drives the nodes and edges it is
sent; here the adapter is the master. Without a graph the adapter sends one straight edge from
the vehicle's position to the goal. With one, the order runs along the lanes: from the vehicle's
position onto the graph, the shortest path along it, and the goal.

The file is YAML. ``nodes`` maps a node id to ``[x, y]`` in metres, in the map frame the
vehicle reports its position in; ``edges`` lists undirected ``[a, b]`` pairs, each weighted by
its length. An optional ``map_id`` names the map the lanes are drawn on: a vehicle that reports
another ``mapId`` is logged as an error (vda5050_binding.py). Other top-level keys are ignored.

* **The goal is a lane node.** The adapter refuses a goal farther than :data:`NODE_TOLERANCE_M`
  from every node (:meth:`RouteGraph.nearest`), so the last leg never leaves the lanes.
* **The start enters the graph by its edge.** A vehicle stopped or preempted between two nodes
  is within :data:`EDGE_ENTRY_M` of the edge it was on, and its route starts at whichever end of
  that edge makes it shorter, not at the nearest node, which may lie behind it. A start farther
  from every edge goes straight to the nearest node, and :class:`Route` says how far it is.
* **No zero-length edge.** A node within :data:`NODE_TOLERANCE_M` of the start, or of the goal,
  is not sent: the start and goal nodes stand for it.
* **Checked at load** (:class:`RouteGraph`): a node id given twice, an unknown node in an
  edge, a self-loop, a duplicate edge, a graph with no edge, a coordinate that is not a finite
  number within :data:`MAX_COORD_M`, two nodes closer than :data:`NODE_TOLERANCE_M`, a node id
  the order uses for its own ends, and a graph that is not connected each stop the adapter. A
  connected graph always has a route, so from a known position the order never falls back to
  one straight edge across the floor. A position that is not finite has no route
  (:meth:`RouteGraph.route` raises ValueError): the binding refuses work then.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass

# A goal or a start this close to a node is at that node. Also the closest two nodes may be.
NODE_TOLERANCE_M = 0.05
# A start this close to an edge enters the graph by that edge.
EDGE_ENTRY_M = 0.5
# The order's own end nodes (profiles/vda5050.py); a graph node may not take either id.
RESERVED_IDS = frozenset({"start", "goal"})
# No coordinate is larger, so no edge length or route cost overflows.
MAX_COORD_M = 1e6


@dataclass(frozen=True)
class Waypoint:
    node_id: str
    x: float
    y: float


@dataclass(frozen=True)
class Route:
    """The lane nodes an order passes between its start and its goal, in order. ``off_graph_m``
    is the start's distance to the nearest edge when it is farther than :data:`EDGE_ENTRY_M`
    (the first leg then runs straight to the nearest node), else None."""

    waypoints: tuple[Waypoint, ...]
    off_graph_m: float | None = None


def _coord(node_id: str, value) -> tuple[float, float]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(f"route graph: node {node_id!r} is {value!r}, not [x, y]")
    out = []
    for v in value:
        if (isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                or abs(v) > MAX_COORD_M):
            raise ValueError(f"route graph: node {node_id!r} has coordinate {v!r}, "
                             f"not a finite number within {MAX_COORD_M:g} m")
        out.append(float(v))
    return out[0], out[1]


def _segment_distance(p: tuple[float, float], a: tuple[float, float],
                      b: tuple[float, float]) -> float:
    ax, ay = a
    dx, dy = b[0] - ax, b[1] - ay
    t = ((p[0] - ax) * dx + (p[1] - ay) * dy) / (dx * dx + dy * dy)  # no zero-length edge
    t = max(0.0, min(1.0, t))
    return math.dist(p, (ax + t * dx, ay + t * dy))


class RouteGraph:
    def __init__(self, nodes: dict, edges: list, map_id: str | None = None) -> None:
        if map_id is not None and not (isinstance(map_id, str) and map_id):
            raise ValueError(f"route graph: map_id is {map_id!r}, not a non-empty string")
        self._map_id = map_id
        if not isinstance(nodes, dict) or not nodes:
            raise ValueError("route graph: no nodes")
        if not isinstance(edges, list):
            raise ValueError("route graph: edges is not a list")  # noqa: TRY004 (one error for a bad file)
        if not edges:
            raise ValueError("route graph: no edges")
        self._nodes: dict[str, tuple[float, float]] = {}
        for k, v in nodes.items():
            if not isinstance(k, str) or not k:
                raise ValueError(f"route graph: node id {k!r} is not a non-empty string")
            if k in RESERVED_IDS:
                raise ValueError(f"route graph: node id {k!r} is reserved for the order's "
                                 "own start and goal nodes")
            self._nodes[k] = _coord(k, v)
        ids = sorted(self._nodes)
        for i, a in enumerate(ids):
            for b in ids[i + 1:]:
                d = math.dist(self._nodes[a], self._nodes[b])
                if d < NODE_TOLERANCE_M:
                    raise ValueError(f"route graph: nodes {a!r} and {b!r} are {d:.3f} m apart, "
                                     f"closer than {NODE_TOLERANCE_M} m")
        self._adj: dict[str, list[tuple[str, float]]] = {k: [] for k in self._nodes}
        self._edges: list[tuple[str, str]] = []
        seen: set[frozenset[str]] = set()
        for e in edges:
            if not isinstance(e, (list, tuple)) or len(e) != 2:
                raise ValueError(f"route graph: edge {e!r} is not a pair [a, b]")
            a, b = e
            if not (isinstance(a, str) and isinstance(b, str)):
                raise ValueError(f"route graph: edge {e!r} names a node by {a!r}, {b!r}, "  # noqa: TRY004
                                 "not by two node ids")
            if a not in self._nodes or b not in self._nodes:
                raise ValueError(f"route graph: edge {a}-{b} names an unknown node")
            if a == b:
                raise ValueError(f"route graph: edge {a}-{b} joins a node to itself")
            key = frozenset((a, b))
            if key in seen:
                raise ValueError(f"route graph: edge {a}-{b} is listed twice")
            seen.add(key)
            d = math.dist(self._nodes[a], self._nodes[b])
            self._adj[a].append((b, d))
            self._adj[b].append((a, d))
            self._edges.append((a, b))
        reached, todo = {ids[0]}, [ids[0]]
        while todo:
            for m, _ in self._adj[todo.pop()]:
                if m not in reached:
                    reached.add(m)
                    todo.append(m)
        if len(reached) != len(self._nodes):
            cut = sorted(set(self._nodes) - reached)
            raise ValueError(f"route graph: not connected; {len(cut)} node(s) cannot be reached "
                             f"from {ids[0]!r}, among them {cut[0]!r}")

    @classmethod
    def load(cls, path: str) -> RouteGraph:
        import yaml

        class _Loader(yaml.SafeLoader):
            """safe_load, except that a key given twice is an error: YAML keeps the last value,
            so a node id pasted twice would move that node and its edges without a word."""

        def _mapping(loader, node, deep=False):
            keys = set()
            for k, _ in node.value:
                key = loader.construct_object(k, deep=deep)
                if key in keys:
                    raise yaml.constructor.ConstructorError(
                        None, None, f"key {key!r} is given twice", k.start_mark)
                keys.add(key)
            return loader.construct_mapping(node, deep=deep)

        _Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)
        try:
            with open(path, encoding="utf-8") as f:
                doc = yaml.load(f, Loader=_Loader)  # a SafeLoader
        except (OSError, yaml.YAMLError) as exc:
            raise ValueError(f"route graph {path}: {exc}") from exc
        if not isinstance(doc, dict):
            raise ValueError(f"route graph {path}: not a mapping with nodes and edges")  # noqa: TRY004 (one error for a bad file)
        map_id = doc.get("map_id")
        if "map_id" in doc and map_id is None:  # "map_id:" with no value is not "no map id"
            raise ValueError(f"route graph {path}: map_id is given with no value")
        return cls(doc.get("nodes") or {}, doc.get("edges") or [], map_id)

    @property
    def map_id(self) -> str | None:
        """The map the lanes are drawn on, when the file names one."""
        return self._map_id

    @property
    def node_ids(self) -> list[str]:
        return list(self._nodes)

    @property
    def edge_count(self) -> int:
        return len(self._edges)

    @property
    def edges(self) -> list[tuple[str, str]]:
        return list(self._edges)

    def position(self, node_id: str) -> tuple[float, float]:
        return self._nodes[node_id]

    def nearest(self, x: float, y: float) -> tuple[str, float]:
        """The node nearest (x, y), and its distance. Ties go to the lower id."""
        k = min(self._nodes, key=lambda n: (math.dist(self._nodes[n], (x, y)), n))
        return k, math.dist(self._nodes[k], (x, y))

    def _entries(self, x: float, y: float) -> tuple[dict[str, float], float | None]:
        """Where a start at (x, y) enters the graph: each entry node with the distance to it,
        and the distance to the nearest edge when it is too far to enter by it."""
        p = (x, y)
        best = min(self._edges, key=lambda e: (
            _segment_distance(p, self._nodes[e[0]], self._nodes[e[1]]), e))
        off = _segment_distance(p, self._nodes[best[0]], self._nodes[best[1]])
        if off <= EDGE_ENTRY_M:
            return {n: math.dist(p, self._nodes[n]) for n in best}, None
        n, d = self.nearest(x, y)
        return {n: d}, off

    def route(self, sx: float, sy: float, gx: float, gy: float) -> Route:
        """The lane nodes to pass from (sx, sy) to the node nearest (gx, gy), in order, without
        a node within :data:`NODE_TOLERANCE_M` of either end. Raises ValueError for a start or
        a goal that is not finite: it has no route."""
        if not all(math.isfinite(v) for v in (sx, sy, gx, gy)):
            raise ValueError(f"route graph: no route from ({sx}, {sy}) to ({gx}, {gy})")
        sources, off = self._entries(sx, sy)
        goal, _ = self.nearest(gx, gy)
        dist = dict(sources)
        prev: dict[str, str] = {}
        heap = [(d, n) for n, d in sorted(sources.items())]
        heapq.heapify(heap)
        while heap:
            d, n = heapq.heappop(heap)
            if n == goal:
                break
            if d > dist[n]:
                continue
            for m, w in self._adj[n]:
                if d + w < dist.get(m, math.inf):
                    dist[m], prev[m] = d + w, n
                    heapq.heappush(heap, (d + w, m))
        path = [goal]  # connected, so the goal is always reached
        while path[-1] in prev:
            path.append(prev[path[-1]])
        path.reverse()
        if math.dist(self._nodes[path[0]], (sx, sy)) <= NODE_TOLERANCE_M:
            path = path[1:]
        if path and math.dist(self._nodes[path[-1]], (gx, gy)) <= NODE_TOLERANCE_M:
            path = path[:-1]
        return Route(tuple(Waypoint(n, *self._nodes[n]) for n in path), off)
