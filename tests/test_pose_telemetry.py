# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""The position sent to the control plane (TelemetryPayload.position).

* It is the agvPosition of the latest ``state`` or ``visualization`` that carried one. A
  vehicle may send visualization more often than state, so the map shows it moving between
  states. Visualization feeds this position only: stops, orders and tasks read the state.
* While the position is unknown (none reported, or ``positionInitialized: false``, or a
  reading that is not a finite number) no position is sent, never the origin (C6.2). The same
  for the battery: unknown is left out, never 0%.
* The heading is sent from ``theta`` when the vehicle gives one, and left out when not.
* A visualization delivered again (same headerId and timestamp) is dropped."""

from __future__ import annotations

import itertools
import json
import math
import threading

import pytest
from conftest import mark_transport_live

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.robot import SimulatedRobot
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
_N = iter(range(1, 10_000))


def _ap(x=3.0, y=4.0, theta=0.5, init=True, **extra) -> dict:
    ap = {"x": x, "y": y, "mapId": "m", "positionInitialized": init, **extra}
    if theta is not ...:
        ap["theta"] = theta
    return ap


def _state(ap=None, battery=77) -> str:
    n = next(_N)
    body = {"headerId": n, "timestamp": f"s{n}", "driving": False,
            "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
            "nodeStates": [], "actionStates": []}
    if ap is not None:
        body["agvPosition"] = ap
    if battery is not None:
        body["batteryState"] = {"batteryCharge": battery, "charging": False}
    return json.dumps(body)


def _vis(ap=None, hid=None, ts=None) -> str:
    body = {"headerId": hid if hid is not None else next(_N)}
    body["timestamp"] = ts if ts is not None else f"v{body['headerId']}"
    if ap is not None:
        body["agvPosition"] = ap
    return json.dumps(body)


def _binding() -> Vda5050Binding:
    return mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))


def _one_cycle(robot) -> pb.TelemetryPayload:
    """One pass of the telemetry loop, with the real generated messages: what it sent."""
    a = Adapter(pb, None, robot, ROBOT)
    a._telemetry_interval = 0
    passes = iter([False, True])

    class _Once:
        def is_set(self):
            return next(passes)

        def set(self):
            pass

    a._shutdown = _Once()
    a._telemetry_loop(None)
    return a._outbox.get_nowait().telemetry


# ── the profile ──────────────────────────────────────────────────────────────

def test_the_visualization_topic_is_in_the_topic_set() -> None:
    t = Vda5050Profile().topics("swarmada", "agv-1")
    assert t.visualization == "uagv/v2/swarmada/agv-1/visualization"


def test_a_visualization_reports_its_position() -> None:
    r = Vda5050Profile().parse_visualization(_vis(_ap(1.0, 2.0, 0.25), hid=5, ts="t5"))
    assert r.reported and r.position == profiles.Position(x=1.0, y=2.0, yaw=0.25, floor=0)
    assert (r.header_id, r.timestamp) == (5, "t5")


def test_a_position_that_is_not_known_is_reported_as_unknown() -> None:
    parse = Vda5050Profile().parse_visualization
    for ap in (_ap(init=False), _ap(x=math.nan), _ap(y=math.inf), _ap(x="3"), _ap(x=True),
               {"y": 1.0, "theta": 0.0}):
        r = parse(json.dumps({"agvPosition": ap}))
        assert r.reported and r.position is None, ap


def test_no_agv_position_reports_nothing() -> None:
    parse = Vda5050Profile().parse_visualization
    assert not parse(_vis()).reported
    assert not parse("not json").reported
    assert not parse(json.dumps({"agvPosition": [1, 2]})).reported
    assert not Vda5050Profile().parse_state(_state()).position_reported


def test_a_missing_or_unusable_theta_is_no_heading() -> None:
    parse = Vda5050Profile().parse_visualization
    assert parse(_vis(_ap(theta=...))).position.yaw is None
    assert parse(_vis(_ap(theta=math.nan))).position.yaw is None
    assert parse(_vis(_ap(theta=0.0))).position.yaw == 0.0  # 0 is a heading


# ── the binding ──────────────────────────────────────────────────────────────

def test_the_position_is_the_latest_reported_by_state_or_visualization() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 1.0)))
    assert b.reported_pose(ROBOT) == (1.0, 1.0, 0.5, 0)
    b.on_visualization(_vis(_ap(2.0, 1.0, 0.75)))
    assert b.reported_pose(ROBOT) == (2.0, 1.0, 0.75, 0)
    assert b.pose(ROBOT) == (2.0, 1.0, 0)
    b.on_state(_state(_ap(3.0, 1.0)))
    assert b.pose(ROBOT) == (3.0, 1.0, 0)


def test_a_message_without_agv_position_leaves_the_position_as_it_was() -> None:
    b = _binding()
    b.on_visualization(_vis(_ap(2.0, 1.0)))
    b.on_state(_state(None))                      # a state that does not say where it is
    b.on_visualization(_vis(None))
    assert b.pose(ROBOT) == (2.0, 1.0, 0)


def test_not_localized_makes_the_position_unknown() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 1.0)))
    b.on_visualization(_vis(_ap(init=False)))
    assert b.pose(ROBOT) is None and b.reported_pose(ROBOT) is None
    b.on_visualization(_vis(_ap(5.0, 5.0)))
    assert b.pose(ROBOT) == (5.0, 5.0, 0)
    b.on_state(_state(_ap(init=False)))
    assert b.pose(ROBOT) is None


def test_a_redelivered_visualization_is_dropped() -> None:
    b = _binding()
    old = _vis(_ap(1.0, 1.0), hid=7, ts="v7")
    b.on_visualization(old)
    b.on_visualization(_vis(_ap(2.0, 1.0), hid=8, ts="v8"))
    b.on_visualization(old)                       # delivered again: not applied
    assert b.pose(ROBOT) == (2.0, 1.0, 0)


def test_a_visualization_without_header_id_is_still_used() -> None:
    b = _binding()
    b.on_visualization(json.dumps({"agvPosition": _ap(4.0, 1.0)}))
    assert b.pose(ROBOT) == (4.0, 1.0, 0)


def test_a_visualization_changes_nothing_but_the_reported_position() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 1.0)))
    seq, state = b.state_seq(ROBOT), b._state
    b.on_visualization(_vis(_ap(9.0, 9.0)))
    assert b.state_seq(ROBOT) == seq and b._state is state
    assert (b._state.position.x, b._state.position.y) == (1.0, 1.0)  # orders start here
    b.command_move(ROBOT, 5.0, 1.0)
    (order,) = [json.loads(p) for t, p in b.published if t.endswith("/order")]
    assert (order["nodes"][0]["nodePosition"]["x"], order["nodes"][0]["nodePosition"]["y"]) \
        == (1.0, 1.0)


def test_an_order_from_a_state_with_no_theta_still_names_a_start_heading() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 1.0, theta=...)))
    b.command_move(ROBOT, 5.0, 1.0)
    (order,) = [json.loads(p) for t, p in b.published if t.endswith("/order")]
    assert order["nodes"][0]["nodePosition"]["theta"] == 0.0  # as before


def test_start_subscribes_to_visualization(monkeypatch) -> None:
    b = Vda5050Binding(robot_id=ROBOT, serial="agv-1")
    monkeypatch.setattr(b._mqtt, "connect", lambda: None)
    b.start()
    assert "uagv/v2/swarmada/agv-1/visualization" in b._mqtt.subscriptions


def test_battery_unknown_is_none() -> None:
    b = _binding()
    b.on_state(_state(_ap(), battery=None))
    assert b.battery_percent(ROBOT) is None
    b.on_state(_state(_ap(), battery=61))
    assert b.battery_percent(ROBOT) == 61


# ── what the telemetry sends ─────────────────────────────────────────────────

def test_telemetry_sends_the_reported_position_and_heading() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 2.0, 0.5)))
    b.on_visualization(_vis(_ap(3.0, 2.0, 1.25)))
    t = _one_cycle(b)
    assert t.HasField("position")
    assert (t.position.x, t.position.y, t.position.yaw, t.position.floor) == (3.0, 2.0, 1.25, 0)
    assert t.battery.percent == 77


def test_telemetry_leaves_out_an_unknown_position_never_the_origin() -> None:
    b = _binding()
    b.on_state(_state(_ap(7.0, 3.0, init=False)))  # online, not localized
    t = _one_cycle(b)
    assert not t.HasField("position")
    assert t.phase != pb.RobotPhase.ROBOT_PHASE_OFFLINE and t.battery.percent == 77


def test_telemetry_leaves_out_a_heading_the_vehicle_did_not_give() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 2.0, theta=...)))
    t = _one_cycle(b)
    assert t.HasField("position") and not t.position.HasField("yaw")
    assert t.position.HasField("x") and t.position.HasField("floor")


def test_telemetry_leaves_out_an_unknown_battery_never_zero() -> None:
    b = _binding()
    b.on_state(_state(_ap(), battery=None))
    t = _one_cycle(b)
    assert not t.HasField("battery") and t.HasField("position")


def test_a_simulated_robot_still_sends_x_y_and_floor() -> None:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    t = _one_cycle(robot)
    assert t.HasField("position") and not t.position.HasField("yaw")
    assert (t.position.x, t.position.y, t.position.floor) == (0.0, 0.0, 0)


# ── the edge tee ─────────────────────────────────────────────────────────────

def _edge_first_message(robot):
    """Run the edge loop against a stub edge node; what it sent first, or None."""
    a = Adapter(pb, None, robot, ROBOT)
    sent = []

    class _Stub:
        def __init__(self, _channel):
            pass

        def EdgeStream(self, requests):
            # The tee is queued before the stream opens: take the first message, if any is
            # there within 0.3 s, then close the stream (its requests then end, with none).
            t = threading.Thread(target=lambda: sent.extend(itertools.islice(requests, 1)),
                                 daemon=True)
            t.start()
            t.join(0.3)
            a._shutdown.set()  # so the ended stream isn't dialled again, and the loop returns
            return iter(())

    class _Grpc:
        EdgeServiceStub = _Stub

    a._pb_grpc = _Grpc()
    a._dial = lambda addr: object()
    a._process._start_edge = a._process._edge_loop  # inline: it returns once the stream ends
    a._process.edge_endpoints(a, ["edge:1"])
    return sent[0] if sent else None


def test_the_edge_tee_sends_the_position_and_heading() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 2.0, 0.5)))
    msg = _edge_first_message(b)
    assert msg is not None and msg.position.position.yaw == 0.5


def test_the_edge_tee_sends_no_frame_while_the_position_is_unknown() -> None:
    b = _binding()
    b.on_state(_state(_ap(init=False)))
    assert _edge_first_message(b) is None


def test_the_edge_tee_sends_no_frame_while_the_vehicle_cannot_be_seen() -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 2.0, 0.5)))
    b.on_connection(json.dumps({"connectionState": "CONNECTIONBROKEN"}))
    assert _edge_first_message(b) is None   # not the frozen (1, 2)


def test_the_edge_frame_carries_the_adapter_clock() -> None:
    import time
    b = _binding()
    b.on_state(_state(_ap(1.0, 2.0)))
    before = int(time.time() * 1000)
    msg = _edge_first_message(b)
    assert before <= msg.position.ts_ms <= int(time.time() * 1000)


# ── leaving ONLINE, and the order of the two topics ──────────────────────────

def _t(seconds: int) -> str:
    return f"2026-11-15T10:00:{seconds:02d}.000Z"


@pytest.mark.parametrize("away", ["OFFLINE", "CONNECTIONBROKEN"])
def test_what_was_reported_before_the_vehicle_left_is_not_sent_after_it_returns(away) -> None:
    b = _binding()
    b.on_state(_state(_ap(1.0, 2.0), battery=80))
    b.on_connection(json.dumps({"connectionState": away}))
    b.on_connection(json.dumps({"connectionState": "ONLINE"}))  # before any new state
    assert b.pose(ROBOT) is None and b.battery_percent(ROBOT) is None
    t = _one_cycle(b)
    assert not t.HasField("position") and not t.HasField("battery")
    b.on_state(_state(_ap(4.0, 2.0), battery=79))                # it reports again
    assert b.pose(ROBOT) == (4.0, 2.0, 0) and b.battery_percent(ROBOT) == 79


def test_a_visualization_older_than_the_position_reported_is_not_applied() -> None:
    b = _binding()
    state = json.loads(_state(_ap(5.0, 5.0)))
    state["timestamp"] = _t(2)
    b.on_state(json.dumps(state))
    b.on_visualization(_vis(_ap(1.0, 1.0), ts=_t(1)))   # older: a redelivery, a retained one
    assert b.pose(ROBOT) == (5.0, 5.0, 0)
    b.on_visualization(_vis(_ap(6.0, 5.0), ts=_t(3)))   # newer
    assert b.pose(ROBOT) == (6.0, 5.0, 0)
    b.on_visualization(_vis(_ap(2.0, 5.0), ts=_t(2)))   # older than that one
    assert b.pose(ROBOT) == (6.0, 5.0, 0)


def test_timestamps_that_cannot_be_read_or_compared_fall_back_to_arrival() -> None:
    b = _binding()
    b.on_visualization(_vis(_ap(1.0, 1.0), ts=_t(5)))
    b.on_visualization(_vis(_ap(2.0, 1.0), ts="not a time"))
    assert b.pose(ROBOT) == (2.0, 1.0, 0)
    b.on_visualization(_vis(_ap(3.0, 1.0), ts="2026-11-15T10:00:01"))  # no zone vs a zone
    b.on_visualization(_vis(_ap(4.0, 1.0), ts=_t(1)))
    assert b.pose(ROBOT) == (4.0, 1.0, 0)


def test_the_order_window_starts_again_when_the_vehicle_returns() -> None:
    # A vehicle that restarts may restart its clock: after it was away, its first report
    # stands, whatever its timestamp.
    b = _binding()
    b.on_visualization(_vis(_ap(1.0, 1.0), ts=_t(30)))
    b.on_connection(json.dumps({"connectionState": "OFFLINE"}))
    b.on_connection(json.dumps({"connectionState": "ONLINE"}))
    b.on_visualization(_vis(_ap(2.0, 1.0), ts=_t(1)))
    assert b.pose(ROBOT) == (2.0, 1.0, 0)


# ── readings that are not finite ─────────────────────────────────────────────

@pytest.mark.parametrize("field", ["batteryCharge", "batteryVoltage"])
def test_a_battery_reading_that_is_not_finite_loses_nothing_else(field) -> None:
    body = json.loads(_state(_ap(1.0, 2.0)))
    body["batteryState"][field] = math.nan
    st = Vda5050Profile().parse_state(json.dumps(body))
    assert st.position is not None and st.position_reported   # the state is kept
    if field == "batteryCharge":
        assert st.battery.percent is None
    else:
        assert st.battery.voltage is None and st.battery.percent == 77


@pytest.mark.parametrize("value", [math.nan, math.inf])
def test_an_order_update_id_that_is_not_finite_loses_nothing_else(value) -> None:
    body = json.loads(_state(_ap(1.0, 2.0)))
    body["orderUpdateId"] = value
    st = Vda5050Profile().parse_state(json.dumps(body))
    assert st.order_update_id == 0 and st.position is not None


def test_a_visualization_leaves_the_stop_and_route_checks_alone() -> None:
    from fleet_adapter_vda5050.routing import RouteGraph
    graph = RouteGraph({"a": [0, 0], "b": [5, 0]}, [["a", "b"]])
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", route_graph=graph))
    b.on_state(_state(_ap(init=False)))
    seen, stopped = b._states_seen, b.is_stopped(ROBOT)
    assert b.route_unavailable(ROBOT)                   # no position in the state
    b.on_visualization(_vis(_ap(1.0, 0.0)))
    assert b.route_unavailable(ROBOT)                   # still none: visualization is not read
    assert b._states_seen == seen and b.is_stopped(ROBOT) == stopped
    assert b.pose(ROBOT) == (1.0, 0.0, 0)               # only the reported position moved


def test_the_edge_tee_sends_no_frame_while_the_broker_session_is_down() -> None:
    # The vehicle never said it left (no connection message), but the binding cannot see it:
    # its broker session is not up. The position it holds is frozen, not current.
    b = Vda5050Binding(robot_id=ROBOT, boot="b0")        # not started: no broker session
    b.on_state(_state(_ap(1.0, 2.0)))
    assert b.pose(ROBOT) == (1.0, 2.0, 0) and not b.transport_alive()
    assert _edge_first_message(b) is None
