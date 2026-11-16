# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""A vehicle taken as connected from its states.

A vehicle may publish its retained connection ONLINE only when it starts (vda-5050-lib 1.7.2).
After a broker restart or a network blip, the retained message stays its last will,
CONNECTIONBROKEN, while it is connected and sending states. So:

* after a broker drop, or while the connection reads CONNECTIONBROKEN or has not been received,
  a fresh counted state restores the vehicle as connected;
* a connection restored by a state lapses after ``state_lapse_s`` without a new state (default
  60 s: twice the 30 s VDA5050 allows between states);
* an explicit ONLINE restores it with no lapse; a later OFFLINE or CONNECTIONBROKEN wins at once;
  a vehicle that said OFFLINE itself is not restored by states;
* not connected means only that: nothing is sent to the vehicle because of it."""

from __future__ import annotations

import json

import pytest
from conftest import mark_transport_live

from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
ONLINE = '{"connectionState": "ONLINE"}'
OFFLINE = '{"connectionState": "OFFLINE"}'
BROKEN = '{"connectionState": "CONNECTIONBROKEN"}'


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def _binding(clock=None, lapse=60.0) -> tuple[Vda5050Binding, _Clock]:
    clock = clock or _Clock()
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0", clock=clock,
                                           state_lapse_s=lapse))
    b._connection, b._connection_known = profiles.CONN_OFFLINE, False  # nothing received yet
    return b, clock


_N = iter(range(1, 100_000))


def _state(header=True) -> str:
    n = next(_N)
    body = {"driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0},
            "nodeStates": [], "actionStates": []}
    if header:
        body.update(headerId=n, timestamp=f"t{n}")
    return json.dumps(body)


def test_a_state_restores_a_vehicle_whose_connection_reads_connectionbroken(capsys) -> None:
    b, _ = _binding()
    b.on_connection(BROKEN)
    assert not b.transport_alive()
    b.on_state(_state())
    assert b.transport_alive() and b.connection_state(ROBOT) == profiles.CONN_ONLINE
    err = capsys.readouterr().err
    assert "while its connection reads CONNECTIONBROKEN; taken as connected" in err


def test_a_state_restores_a_vehicle_after_a_broker_drop() -> None:
    b, _ = _binding()
    b.on_connection(ONLINE)
    b._on_broker_session(False)                 # the drop: connection unknown
    b.on_state(_state())
    assert b.transport_alive()


def test_a_state_restores_a_vehicle_whose_connection_was_never_received() -> None:
    b, _ = _binding()
    b.on_state(_state())
    assert b.transport_alive()


def test_a_state_in_flight_when_the_will_fired_restores_once_then_lapses() -> None:
    # The will (CONNECTIONBROKEN) fires as the vehicle drops; a state it sent just before is
    # still on its way and arrives after it. It restores the vehicle once. No state follows,
    # since the vehicle is gone, and the restored connection lapses.
    b, clock = _binding()
    b.on_connection(ONLINE)
    b.on_state(_state())
    b.on_connection(BROKEN)
    assert not b.transport_alive()
    b.on_state(_state())                        # in flight when the will fired
    assert b.transport_alive()
    clock.t += 60.0
    assert b.transport_alive()                  # within the window (60 s, inclusive)
    clock.t += 0.5
    assert not b.transport_alive()
    assert b.connection_state(ROBOT) == profiles.CONN_OFFLINE
    assert "none has come for 60 s" in b.transport_detail()
    assert b.published == []                    # nothing sent to the vehicle


def test_each_new_state_keeps_a_restored_connection_and_one_after_a_lapse_restores_it(
        capsys) -> None:
    b, clock = _binding()
    b.on_connection(BROKEN)
    b.on_state(_state())
    for _ in range(5):
        clock.t += 30.0
        b.on_state(_state())
        assert b.transport_alive()
    clock.t += 61.0
    assert not b.transport_alive()
    b.on_state(_state())
    assert b.transport_alive()
    assert capsys.readouterr().err.count("taken as connected") == 2  # restored, then again


def test_a_later_connectionbroken_wins_over_a_state_restored_connection() -> None:
    b, _ = _binding()
    b.on_connection(BROKEN)
    b.on_state(_state())
    assert b.transport_alive()
    b.on_connection(BROKEN)                     # a new will: the vehicle dropped again
    assert not b.transport_alive()


def test_a_later_offline_wins_and_states_do_not_restore_a_vehicle_that_said_offline() -> None:
    b, _ = _binding()
    b.on_connection(BROKEN)
    b.on_state(_state())
    b.on_connection(OFFLINE)                    # the vehicle's own orderly goodbye
    assert not b.transport_alive()
    b.on_state(_state())
    assert not b.transport_alive()


def test_an_explicit_online_restores_with_no_lapse() -> None:
    b, clock = _binding()
    b.on_connection(BROKEN)
    b.on_state(_state())                        # restored by a state...
    b.on_connection(ONLINE)                     # ...then the vehicle says ONLINE
    clock.t += 3600.0                           # no state for an hour
    assert b.transport_alive()
    b2, clock2 = _binding()
    b2.on_connection(ONLINE)
    clock2.t += 3600.0
    assert b2.transport_alive()


def test_only_a_counted_state_restores_the_connection() -> None:
    b, _ = _binding()
    b.on_connection(BROKEN)
    b.on_state(_state(header=False))            # no headerId: not counted
    assert not b.transport_alive()
    s = _state()
    b.on_state(s)
    assert b.transport_alive()
    b.on_connection(BROKEN)
    b.on_state(s)                               # a redelivery: dropped, restores nothing
    assert not b.transport_alive()


def test_a_lapse_only_makes_the_vehicle_not_connected() -> None:
    # The decisions that read "connected" answer as for any vehicle that cannot be seen, and
    # nothing is sent to the vehicle.
    b, clock = _binding()
    b.on_connection(BROKEN)
    b.on_state(_state())
    clock.t += 120.0
    assert not b.transport_alive()
    assert not b._nothing_to_cancel_locked()    # a stop asked for sends its cancel
    assert not b.paused_at_vehicle(ROBOT)
    assert b.phase_hint(ROBOT) == ""
    assert b.published == []


@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
def test_the_lapse_window_must_be_a_positive_finite_number(bad) -> None:
    with pytest.raises(ValueError, match="state_lapse_s"):
        Vda5050Binding(robot_id=ROBOT, state_lapse_s=bad)


def test_the_cli_sets_the_lapse_window(monkeypatch) -> None:
    from fleet_adapter_vda5050 import vda5050_binding
    seen = {}

    class _GotPast(Exception):
        pass

    def binding(**kw):
        seen.update(kw)
        raise _GotPast

    monkeypatch.setattr(vda5050_binding, "Vda5050Binding", binding)
    with pytest.raises(_GotPast):
        adapter_mod.main(["--insecure", "--comms", "vda5050", "--state-lapse-seconds", "90"])
    assert seen["state_lapse_s"] == 90.0
    with pytest.raises(_GotPast):
        adapter_mod.main(["--insecure", "--comms", "vda5050"])
    assert seen["state_lapse_s"] == 60.0


@pytest.mark.parametrize("bad", ["0", "-5", "nan", "inf"])
def test_the_cli_refuses_a_lapse_window_that_is_not_positive_and_finite(bad, capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        adapter_mod.main(["--insecure", "--comms", "vda5050", "--state-lapse-seconds", bad])
    assert exc.value.code == 2
    assert "--state-lapse-seconds" in capsys.readouterr().err
