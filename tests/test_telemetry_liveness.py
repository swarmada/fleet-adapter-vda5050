# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The adapter must not publish telemetry it cannot observe (C6.2).

`Vda5050Binding` answers every accessor whether or not its transport is up: `pose()` with
the origin, `battery_percent()` with 0. `_telemetry_loop` published those at full cadence
with a fresh `timestamp_ms`, so a blind adapter was indistinguishable on the wire from a
robot parked at (0,0) on a flat battery.

That is a **C6.2 violation** in its own words — *"never send a defaulted 0/false where the
true reading is unknown"* — and not merely a staleness question.

What makes it this adapter's own fault rather than an oversight: VDA5050 declares the
answer on a topic of its own (ONLINE / OFFLINE / CONNECTIONBROKEN), the binding parsed it
into `connection_state()` and `is_online()` on every message, and no caller ever read it.
The best liveness signal of the three reference adapters, computed and discarded.

`tick()` raising was not sufficient on its own, and the reason is the interesting part: it
raises ONCE, so the first thread to reach it consumes the signal for every other thread.
These tests pin the predicate that cannot be consumed.

No broker and no paho: the binding's un-started state is the real one, and
`mark_transport_live` reaches the started one.
"""

from __future__ import annotations

from conftest import mark_transport_live

from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

_AT_REST = '{"agvPosition": {"x": 3.0, "y": 4.0, "theta": 0.0, "positionInitialized": true}, \
"batteryState": {"batteryCharge": 77, "charging": false}, \
"driving": false, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0}}'


class _Pb:
    """Just enough of the generated module for the telemetry path."""

    class RobotPhase:
        ROBOT_PHASE_UNSPECIFIED = 0
        ROBOT_PHASE_IDLE = 1
        ROBOT_PHASE_IN_PROGRESS = 3
        ROBOT_PHASE_OFFLINE = 6

    class RobotPosition:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class BatteryStatus:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class TelemetryPayload:
        def __init__(self, **kw):
            # Mirrors proto3 message-field presence: absent stays absent.
            self.position = kw.get("position")
            self.battery = kw.get("battery")
            self.phase = kw.get("phase")
            self.robot_id = kw.get("robot_id")
            self.timestamp_ms = kw.get("timestamp_ms")
            self.current_action = kw.get("current_action")

    class AdapterMessage:
        def __init__(self, telemetry=None, **_):
            self.telemetry = telemetry


def _one_cycle(robot) -> list:
    """Run exactly one pass of `_telemetry_loop` and return what it queued."""
    a = Adapter(_Pb(), None, robot, "amr-1")
    a._telemetry_interval = 0            # no real sleeping
    cycles = iter([False, True])         # one pass, then _shutdown looks set

    class _Once:
        def is_set(self):
            return next(cycles)

    a._shutdown = _Once()
    a._telemetry_loop(None)
    out = []
    while not a._outbox.empty():
        out.append(a._outbox.get_nowait())
    return out


def test_an_unobservable_robot_is_reported_OFFLINE_with_no_position_or_battery() -> None:
    """The C6.2 requirement, directly: unknown readings are ABSENT, not 0."""
    msgs = _one_cycle(Vda5050Binding(robot_id="amr-1"))      # never started
    assert len(msgs) == 1
    t = msgs[0].telemetry
    assert t.phase == _Pb.RobotPhase.ROBOT_PHASE_OFFLINE
    assert t.position is None, "a defaulted (0,0,0) position is exactly what C6.2 forbids"
    assert t.battery is None, "a defaulted 0% battery is exactly what C6.2 forbids"
    assert t.robot_id == "amr-1" and t.timestamp_ms          # still a well-formed payload


def test_the_old_behaviour_would_have_published_the_defaults() -> None:
    """Pins WHAT was wrong, so the regression is recognisable rather than abstract. These
    are the values the accessors return on an un-started binding, and they were published
    every 200 ms with a fresh timestamp — while the binding's own connection state said
    OFFLINE in the same breath."""
    b = Vda5050Binding(robot_id="amr-1")
    assert b.pose("amr-1") == (0.0, 0.0, 0)
    assert b.battery_percent("amr-1") == 0
    assert b.is_online("amr-1") is False     # the binding KNEW
    assert b.transport_alive() is False      # ...and the adapter now asks this first


def test_a_robot_that_went_OFFLINE_stops_being_reported_as_present() -> None:
    """The half a started binding can still fail. The broker session is up, so nothing
    raises and nothing is stale-looking — but `_state` is a FROZEN copy of the last
    reading, and republishing it with a fresh timestamp claims a measurement that was not
    taken."""
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    b.on_state(_AT_REST)
    assert _one_cycle(b)[0].telemetry.position.x == 3.0      # observable: reported
    b.on_connection('{"connectionState": "OFFLINE"}')
    t = _one_cycle(b)[0].telemetry
    assert t.phase == _Pb.RobotPhase.ROBOT_PHASE_OFFLINE
    assert t.position is None and t.battery is None


def test_an_observable_robot_reports_position_and_battery_normally() -> None:
    """The guard must not degrade a healthy adapter — otherwise it would turn every run
    OFFLINE, which is a worse failure than the one being fixed."""
    b = mark_transport_live(Vda5050Binding(robot_id="amr-1"))
    b.on_state(_AT_REST)
    t = _one_cycle(b)[0].telemetry
    assert t.phase == _Pb.RobotPhase.ROBOT_PHASE_IDLE
    assert (t.position.x, t.position.y) == (3.0, 4.0)
    assert t.battery.percent == 77


def test_the_simulated_path_is_unchanged() -> None:
    """`--comms simulated` is the conformance path and must be untouched: `SimulatedRobot`
    has no transport to lose, so it answers True and reports exactly as before."""
    from fleet_adapter_vda5050.robot import SimulatedRobot

    robot = SimulatedRobot()
    robot.spawn("amr-1")
    t = _one_cycle(robot)[0].telemetry
    assert t.phase == _Pb.RobotPhase.ROBOT_PHASE_IDLE
    assert (t.position.x, t.position.y) == (0.0, 0.0)
    assert t.battery.percent == 100
