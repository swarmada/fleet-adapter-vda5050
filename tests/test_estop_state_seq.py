# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""An emergency stop is confirmed only from a state the vehicle sent after it.

* ``state_seq`` counts the new states that carry a headerId and a timestamp; the adapter reads
  it once the stop is sent and hands the SDK's wait a token, so only a later state confirms.
* A QoS 1 redelivery (the same headerId and timestamp) is dropped: not stored, not counted.
* A state without an integer headerId or a timestamp is used but not counted (logged once).
* ``is_stopped`` needs the pause acknowledged: ``paused: true``, or the binding's latest
  startPause reported FINISHED. A FAILED startPause confirms nothing by itself and does not
  stand against ``paused: true``; a stopPause makes an earlier FINISHED count for nothing."""

from __future__ import annotations

import json

import pytest
from conftest import mark_registered, mark_transport_live, reply_on_tick
from swarmada_sdk.safety import ESTOP_FAILED, ESTOP_STOPPED, EstopToken

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
AT_REST = {"vx": 0.0, "vy": 0.0, "omega": 0.0}


def _state(hid: object = None, ts: str | None = None, *, paused: bool = False,
           driving: bool = False, x: float = 0.0, actions=(), nodes: int = 0,
           mode: str = "AUTOMATIC") -> str:
    """A VDA5050 state. ``hid`` and ``ts`` are its headerId and timestamp (left out when None);
    ``actions`` is (actionId, actionType, actionStatus) triples; ``nodes`` the nodeStates left."""
    body = {"driving": driving, "paused": paused, "operatingMode": mode,
            "velocity": AT_REST if not driving else {"vx": 0.5, "vy": 0.0, "omega": 0.0},
            "agvPosition": {"x": x, "y": 0.0, "theta": 0.0, "mapId": "m",
                            "positionInitialized": True},
            "actionStates": [{"actionId": a, "actionType": t, "actionStatus": s}
                             for a, t, s in actions],
            "nodeStates": [{"nodeId": f"n{i}", "sequenceId": 2 * i, "released": True}
                           for i in range(nodes)]}
    if hid is not None:
        body["headerId"] = hid
    if ts is not None:
        body["timestamp"] = ts
    return json.dumps(body)


def _binding() -> Vda5050Binding:
    return mark_transport_live(Vda5050Binding(robot_id=ROBOT))


def _wire(b: Vda5050Binding) -> list[str]:
    """What the binding published, in order: the instantAction type, or "order"."""
    out = []
    for topic, payload in b.published:
        if topic.endswith("/order"):
            out.append("order")
        else:
            out.extend(a["actionType"] for a in json.loads(payload)["actions"])
    return out


def _start_pauses(b: Vda5050Binding) -> list[str]:
    """The actionIds of the startPauses the binding published, in order."""
    out = []
    for topic, payload in b.published:
        if topic.endswith("/instantActions"):
            for a in json.loads(payload)["actions"]:
                if a["actionType"] == profiles.ACTION_START_PAUSE:
                    out.append(a["actionId"])
    return out


# ── state_seq and redeliveries ───────────────────────────────────────────────

def test_state_seq_counts_each_new_state() -> None:
    b = _binding()
    assert b.state_seq(ROBOT) == 0
    b.on_state(_state(1, "t1"))
    b.on_state(_state(2, "t2"))
    assert b.state_seq(ROBOT) == 2


def test_state_seq_is_zero_for_a_robot_this_binding_does_not_drive() -> None:
    b = _binding()
    b.on_state(_state(1, "t1"))
    assert b.state_seq("amr-2") == 0
    assert b.state_seq("") == 0


def test_a_redelivered_state_is_neither_stored_nor_counted_nor_acted_on(monkeypatch) -> None:
    b = _binding()
    old = _state(1, "t1", x=1.0)
    b.on_state(old)
    b.on_state(_state(2, "t2", x=2.0))
    seen = []
    monkeypatch.setattr(b, "_service_hold", lambda: seen.append("hold"))
    monkeypatch.setattr(b, "_cancel_late_order", lambda: seen.append("late"))
    b.on_state(old)  # QoS 1 delivers it again, after the newer state
    assert b.state_seq(ROBOT) == 2
    assert b.pose(ROBOT)[0] == 2.0  # the newer state stays
    assert seen == []


def test_a_restarted_vehicle_s_states_count_though_their_header_ids_repeat() -> None:
    b = _binding()
    b.on_state(_state(0, "boot1-a"))
    b.on_state(_state(1, "boot1-b"))
    b.on_state(_state(0, "boot2-a"))  # headerIds begin again; the timestamps are new
    b.on_state(_state(1, "boot2-b"))
    assert b.state_seq(ROBOT) == 4


def test_states_with_one_timestamp_and_different_header_ids_count() -> None:
    b = _binding()
    b.on_state(_state(1, "t"))
    b.on_state(_state(2, "t"))
    assert b.state_seq(ROBOT) == 2


def test_a_redelivery_is_looked_for_among_the_last_64_states() -> None:
    b = _binding()
    for i in range(65):
        b.on_state(_state(i, f"t{i}"))
    b.on_state(_state(1, "t1"))   # 64 states back: a redelivery
    assert b.state_seq(ROBOT) == 65
    b.on_state(_state(0, "t0"))   # 65 back, out of the window: counted
    assert b.state_seq(ROBOT) == 66


def test_a_state_without_header_id_or_timestamp_is_used_not_counted_and_logged_once(
        capsys) -> None:
    b = _binding()
    b.on_state(_state(None, "t1", x=1.0))
    assert b.pose(ROBOT)[0] == 1.0  # its telemetry is used
    b.on_state(_state(2, None, x=2.0))
    b.on_state(_state(True, "t3", x=3.0))  # a bool is not an integer headerId
    b.on_state(_state(5.0, "t4", x=4.0))   # nor is a float
    assert b.pose(ROBOT)[0] == 4.0
    assert b.state_seq(ROBOT) == 0
    err = capsys.readouterr().err
    assert err.count("no integer headerId or no timestamp") == 1
    assert "none confirms an emergency stop" in err


def test_a_vehicle_that_sends_no_header_id_never_confirms_an_estop() -> None:
    b = _binding()
    a = Adapter(None, None, b, ROBOT)

    def _tick(dt, tick=b.tick):
        b.on_state(_state(None, "t", paused=True))
        tick(dt)

    b.tick = _tick
    assert a._confirm_estop() == ESTOP_FAILED


# ── is_stopped: the pause acknowledged ───────────────────────────────────────

def test_at_rest_is_not_stopped_until_the_pause_is_acknowledged() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    b.on_state(_state(1, "t1"))
    assert b.is_stopped(ROBOT) is False
    b.on_state(_state(2, "t2", paused=True))
    assert b.is_stopped(ROBOT) is True


def test_the_latest_start_pause_finished_acknowledges_the_pause() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    (sp,) = _start_pauses(b)
    b.on_state(_state(1, "t1", actions=[(sp, "startPause", "FINISHED")]))
    assert b.is_stopped(ROBOT) is True


def test_a_failed_start_pause_does_not_acknowledge_the_pause() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    (sp,) = _start_pauses(b)
    b.on_state(_state(1, "t1", actions=[(sp, "startPause", "FAILED")]))
    assert b.is_stopped(ROBOT) is False


def test_a_failed_start_pause_does_not_stand_against_paused_true() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    (sp,) = _start_pauses(b)
    b.on_state(_state(1, "t1", paused=True, actions=[(sp, "startPause", "FAILED")]))
    assert b.is_stopped(ROBOT) is True


def test_an_earlier_start_pause_finished_does_not_acknowledge_a_later_one() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    b.command_estop(ROBOT)
    first, _ = _start_pauses(b)
    b.on_state(_state(1, "t1", actions=[(first, "startPause", "FINISHED")]))
    assert b.is_stopped(ROBOT) is False


def test_a_stop_pause_makes_an_earlier_start_pause_finished_count_for_nothing() -> None:
    b = _binding()
    b.command_pause(ROBOT)
    (sp,) = _start_pauses(b)
    b.command_resume(ROBOT)  # stopPause
    b.on_state(_state(1, "t1", actions=[(sp, "startPause", "FINISHED")]))
    assert b.is_stopped(ROBOT) is False


def test_the_start_pause_before_an_order_under_a_swarmada_pause_is_the_one_tracked() -> None:
    b = _binding()
    b.on_state(_state(1, "t1"))
    b.command_pause(ROBOT)
    b.command_move(ROBOT, 3.0, 0.0)  # under the pause: startPause, then the order
    sp = _start_pauses(b)[-1]
    assert len(_start_pauses(b)) == 2 and b._start_pause_id == sp
    assert _wire(b)[-2:] == ["startPause", "order"]
    b.on_state(_state(2, "t2", actions=[(sp, "startPause", "FINISHED")]))
    assert b.is_stopped(ROBOT) is True


# ── the adapter's token ──────────────────────────────────────────────────────

def test_the_adapter_passes_the_count_read_after_the_stop_was_sent(monkeypatch) -> None:
    b = _binding()
    b.on_state(_state(1, "t1"))
    sent = b.command_estop

    def command_estop(robot_id):
        sent(robot_id)
        b.on_state(_state(2, "t2"))  # arrives while the stop is sent, before the count is read

    b.command_estop = command_estop
    got = {}

    def wait(robot, rid, since=None, **_):
        got["since"] = since
        return ESTOP_FAILED

    monkeypatch.setattr(adapter_mod, "await_estop_confirmation", wait)
    a = Adapter(None, None, b, ROBOT)
    a._stop_confirm_sleep = lambda _s: None
    a._confirm_estop()
    assert got["since"] == EstopToken(ROBOT, 2, id(b))


def test_a_state_that_crossed_the_stop_does_not_confirm_it() -> None:
    # Sent by the vehicle before the stop, received while the stop is sent: at rest and paused,
    # but older than the stop. With no state after it, the stop is FAILED.
    b = _binding()
    sent = b.command_estop

    def command_estop(robot_id):
        sent(robot_id)
        b.on_state(_state(1, "t1", paused=True))

    b.command_estop = command_estop
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_FAILED


def test_an_estop_is_confirmed_from_a_state_after_it() -> None:
    b = reply_on_tick(_binding(), paused=True)
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_STOPPED


def test_a_second_estop_on_a_vehicle_already_paused_is_confirmed() -> None:
    # The vehicle refuses the second startPause (vda-5050-lib: "already paused") and stays
    # paused. The FAILED startPause confirms nothing by itself; paused: true does.
    b = _binding()
    a = Adapter(None, None, b, ROBOT)
    tick, n = b.tick, [0]

    def _tick(dt):
        n[0] += 1
        sp = _start_pauses(b)[-1]
        status = "FINISHED" if len(_start_pauses(b)) == 1 else "FAILED"
        b.on_state(_state(n[0], f"t{n[0]}", paused=True, actions=[(sp, "startPause", status)]))
        tick(dt)

    b.tick = _tick
    assert a._confirm_estop() == ESTOP_STOPPED
    assert a._confirm_estop() == ESTOP_STOPPED
    assert len(_start_pauses(b)) == 2


def test_a_failed_start_pause_on_a_vehicle_not_paused_is_answered_failed() -> None:
    b = _binding()
    tick, n = b.tick, [0]

    def _tick(dt):
        n[0] += 1
        sp = _start_pauses(b)[-1]
        b.on_state(_state(n[0], f"t{n[0]}", actions=[(sp, "startPause", "FAILED")]))
        tick(dt)

    b.tick = _tick
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_FAILED


def test_a_state_received_while_the_work_is_interrupted_confirms_the_stop(monkeypatch) -> None:
    b = _binding()
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    interrupt = a._interrupt_current

    def _interrupt(*args, **kw):
        b.on_state(_state(1, "t1", paused=True))  # the vehicle's report of the pause
        return interrupt(*args, **kw)

    monkeypatch.setattr(a, "_interrupt_current", _interrupt)
    acks = []
    a._stop_and_ack("e1", acks.append)
    assert acks[0].estop_ack.state == pb.ESTOP_STATE_STOPPED


def test_a_state_seq_that_raises_leaves_the_stop_sent_and_answered(capsys) -> None:
    b = _binding()

    def _raises(robot_id):
        raise RuntimeError("state_seq broke")

    b.state_seq = _raises
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_FAILED
    assert _start_pauses(b)  # the stop went out
    assert "state_seq raised after the stop was sent" in capsys.readouterr().err


def test_a_binding_without_state_seq_gets_no_token() -> None:
    robot = SimulatedRobot()
    robot.spawn(ROBOT)
    assert SimulatedRobot.tick_keeps_state_current is True
    assert Adapter(None, None, robot, ROBOT)._command_estop() is None


# ── the pause answer ─────────────────────────────────────────────────────────

def test_a_pause_is_answered_paused_false_until_the_vehicle_reports_it() -> None:
    b = _binding()
    b.on_state(_state(1, "t1"))  # at rest, from before the pause
    a = Adapter(pb, None, b, ROBOT)
    res = pb.CommandResult()
    a._on_pause(pb.PauseCapabilities(require_stop_before_ack=True), res)
    assert res.pause.paused is False
    assert "awaiting confirmed stop" in res.pause.message


def test_the_state_is_stored_and_counted_under_one_lock() -> None:
    # The SDK reads the count, then the state: a raised count must never be seen with a state
    # older than the one that raised it. Each time the binding's lock is let go, the state it
    # holds is at least as new as the count says.
    b = _binding()
    seen: list[tuple[int | None, int]] = []
    inner = b._lock

    class _Recording:
        def __enter__(self):
            inner.acquire()

        def __exit__(self, *exc):
            seen.append((b._state.header_id, b._state_seq))
            inner.release()

    b._lock = _Recording()
    for i in range(1, 4):
        b.on_state(_state(i, f"t{i}"))
    assert b.state_seq(ROBOT) == 3
    # States 1..3 carry headerIds 1..3: at every release the state and the count agree. A
    # count raised before the state is stored, or after, is seen out of step.
    assert all(hid is None or seq == hid for hid, seq in seen), seen
    assert (3, 3) in seen


# ── vda-5050-lib's virtual vehicle: a cancel lifts the pause ─────────────────
# Its cancelOrder ends the order with stopTraverse, which lifts the pause and publishes
# paused: false; and it drops an ended instantAction from its state after 5 publishes. The
# adapter polls every 50 ms, so it can see only the end of a burst of states.

def _burst(b: Vda5050Binding, make) -> None:
    """On the first tick, the vehicle sends every state ``make(start_pauses)`` lists."""
    tick, n = b.tick, [0]

    def _tick(dt):
        if n[0] == 0:
            for body in make(_start_pauses(b)):
                n[0] += 1
                b.on_state(_state(n[0], f"t{n[0]}", **body))
        tick(dt)

    b.tick = _tick


def test_an_estop_on_a_vehicle_whose_cancel_lifts_the_pause_is_confirmed() -> None:
    b = _binding()
    b.on_state(_state(100, "t100", driving=True, nodes=2))
    _burst(b, lambda sp: [
        {"nodes": 2},                                                     # stopped driving
        {"nodes": 2, "paused": True, "actions": [(sp[-1], "startPause", "FINISHED")]},
        {"nodes": 2, "actions": [(sp[-1], "startPause", "FINISHED")]},    # the cancel unpauses
        {"actions": [(sp[-1], "startPause", "FINISHED")]},                # the order is gone
        {}, {}, {}, {}, {},                                               # the FINISHED expires
    ])
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_STOPPED


def test_an_estop_under_a_swarmada_pause_whose_cancel_lifts_the_pause_is_confirmed() -> None:
    b = _binding()
    b.on_state(_state(100, "t100", nodes=2))
    b.command_pause(ROBOT)
    b.on_state(_state(101, "t101", nodes=2, paused=True))
    _burst(b, lambda sp: [
        {"nodes": 2, "paused": True, "actions": [(sp[-1], "startPause", "FAILED")]},
        {"nodes": 2},                                                     # the cancel unpauses
        {},                                                               # the order is gone
    ])
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_STOPPED


def test_a_pause_lifted_with_an_order_left_is_not_a_stop() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    (sp,) = _start_pauses(b)
    b.on_state(_state(1, "t1", nodes=2, paused=True, actions=[(sp, "startPause", "FINISHED")]))
    b.on_state(_state(2, "t2", nodes=2))  # unpaused, the order still there: it can drive on
    assert b.is_stopped(ROBOT) is False
    b.on_state(_state(3, "t3", actions=[("x", "pick", "RUNNING")]))
    assert b.is_stopped(ROBOT) is False   # an action not ended
    b.on_state(_state(4, "t4"))
    assert b.is_stopped(ROBOT) is True    # nothing left to drive


def _ack(b: Vda5050Binding, n: int, **kw) -> None:
    """A state that lists the binding's latest startPause FINISHED, paused: the pause seen."""
    sp = _start_pauses(b)[-1]
    b.on_state(_state(n, f"t{n}", paused=True, actions=[(sp, "startPause", "FINISHED")], **kw))


def test_a_pause_seen_before_an_order_does_not_stand_for_a_later_stop() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    _ack(b, 1)
    b.on_state(_state(2, "t2"))           # the pause lifted, nothing left
    assert b.is_stopped(ROBOT) is True
    b.command_move(ROBOT, 3.0, 0.0)       # stopPause, then the order
    b.on_state(_state(3, "t3"))           # sent before the vehicle took either
    assert b.is_stopped(ROBOT) is False


def test_a_pause_seen_before_the_next_start_pause_does_not_stand_for_it() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    _ack(b, 1)
    b.on_state(_state(2, "t2"))           # unpaused, nothing left
    assert b.is_stopped(ROBOT) is True
    b.command_estop(ROBOT)                # a new startPause: its own pause is needed
    b.on_state(_state(3, "t3"))
    assert b.is_stopped(ROBOT) is False


def test_only_a_counted_state_acknowledges_the_pause() -> None:
    b = _binding()
    b.command_estop(ROBOT)
    (sp,) = _start_pauses(b)
    b.on_state(_state(None, "t1", paused=True, actions=[(sp, "startPause", "FINISHED")]))
    b.on_state(_state(2, "t2"))           # no headerId above: not counted
    assert b.is_stopped(ROBOT) is False


def test_a_pause_seen_before_a_stop_pause_does_not_stand_after_it() -> None:
    b = _binding()
    b.command_pause(ROBOT)
    _ack(b, 1)
    b.command_resume(ROBOT)               # stopPause: the pause is over
    b.on_state(_state(2, "t2"))
    assert b.is_stopped(ROBOT) is False


def test_a_paused_state_that_does_not_name_the_start_pause_acknowledges_nothing() -> None:
    # The safety review's case: a Swarmada pause, a resume and a new order, then an estop. A
    # state the vehicle sent before it took the stopPause crosses the stop: paused, at rest.
    # It does not name the estop's startPause, so it says nothing about this stop; every later
    # state shows the pause lifted and the new order not yet taken.
    b = _binding()
    b.command_pause(ROBOT)
    _ack(b, 1)
    b.command_resume(ROBOT)
    b.command_move(ROBOT, 3.0, 0.0)
    sent = b.command_estop

    def command_estop(robot_id):
        sent(robot_id)
        b.on_state(_state(2, "t2", paused=True))  # crossed the stop

    b.command_estop = command_estop
    tick, n = b.tick, [2]

    def _tick(dt):
        n[0] += 1
        b.on_state(_state(n[0], f"t{n[0]}"))
        tick(dt)

    b.tick = _tick
    assert Adapter(None, None, b, ROBOT)._confirm_estop() == ESTOP_FAILED


def test_only_a_state_naming_this_start_pause_acknowledges_it() -> None:
    # No order in flight here, so only the naming rule stands between a crossing paused: true
    # and a confirmed stop.
    b = _binding()
    b.command_pause(ROBOT)
    _ack(b, 1)
    b.command_resume(ROBOT)
    b.command_estop(ROBOT)
    b.on_state(_state(2, "t2", paused=True))  # sent before the vehicle took the stopPause
    b.on_state(_state(3, "t3"))               # the stopPause taken; the startPause not yet
    assert b.is_stopped(ROBOT) is False
    _ack(b, 4)
    b.on_state(_state(5, "t5"))
    assert b.is_stopped(ROBOT) is True


def test_nothing_left_to_drive_is_no_stop_while_an_order_is_in_flight() -> None:
    b = _binding()
    b.on_state(_state(1, "t1"))
    b.command_move(ROBOT, 3.0, 0.0)       # order 1 out; the vehicle has not reported it yet
    b.command_estop(ROBOT)
    _ack(b, 2)
    b.on_state(_state(3, "t3"))           # the pause lifted, no node: but not the order sent
    assert b.is_stopped(ROBOT) is False
    (order,) = [json.loads(pl)["orderId"] for t, pl in b.published if t.endswith("/order")]
    b.on_state(json.dumps({**json.loads(_state(4, "t4")), "orderId": order}))
    assert b.is_stopped(ROBOT) is True    # it reports the order sent, ended: nothing in flight


@pytest.mark.parametrize("mode", ["MANUAL", "SERVICE", "TEACHIN", ""])
def test_nothing_left_to_drive_is_no_stop_where_a_person_can_drive(mode) -> None:
    # In MANUAL, SERVICE or TEACHIN a person can move the vehicle without an order; an
    # operatingMode not reported is not taken for AUTOMATIC.
    b = _binding()
    b.command_estop(ROBOT)
    _ack(b, 1, mode=mode)
    b.on_state(_state(2, "t2", mode=mode))  # the pause lifted, nothing left
    assert b.is_stopped(ROBOT) is False
    b.on_state(_state(3, "t3", mode=mode, paused=True))
    assert b.is_stopped(ROBOT) is True    # paused: true still confirms, in any mode


@pytest.mark.parametrize("mode", ["AUTOMATIC", "SEMIAUTOMATIC"])
def test_nothing_left_to_drive_is_a_stop_where_only_an_order_drives(mode) -> None:
    b = _binding()
    b.command_estop(ROBOT)
    _ack(b, 1, mode=mode)
    b.on_state(_state(2, "t2", mode=mode))
    assert b.is_stopped(ROBOT) is True


# ── actionIds and repeats ────────────────────────────────────────────────────

def test_action_ids_carry_the_process_token() -> None:
    # A restarted adapter's first startPause must not take an id the vehicle may still list
    # as FINISHED from the last process: that would confirm a stop the vehicle has not made.
    one = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    two = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    one.command_estop(ROBOT)
    two.command_estop(ROBOT)
    assert _start_pauses(one)[0] == f"{ROBOT}-{one._boot}-a-1"

    assert one._boot != two._boot and _start_pauses(one) != _start_pauses(two)
    two.on_state(_state(1, "t1", actions=[(_start_pauses(one)[0], "startPause", "FINISHED")]))
    assert two.is_stopped(ROBOT) is False


def test_a_dropped_repeat_is_logged_once_as_a_warning(capsys) -> None:
    b = _binding()
    b.on_state(_state(1, "t1"))
    b.on_state(_state(1, "t1"))
    b.on_state(_state(2, "t2"))
    b.on_state(_state(2, "t2"))
    err = capsys.readouterr().err
    assert err.count("vda5050: warning:") == 1
    assert "headerId 1 and timestamp 't1' came again and was dropped" in err


# ── a state that leaves out nodeStates or actionStates ───────────────────────

@pytest.mark.parametrize("missing", ["nodeStates", "actionStates"])
def test_nothing_left_to_drive_is_not_read_from_a_state_that_leaves_the_list_out(missing) -> None:
    # Without the list, what is left to drive is unknown, not nothing.
    b = _binding()
    b.command_estop(ROBOT)
    _ack(b, 1)
    body = json.loads(_state(2, "t2"))
    del body[missing]
    b.on_state(json.dumps(body))
    assert b.is_stopped(ROBOT) is False
    b.on_state(_state(3, "t3"))           # both lists, both empty
    assert b.is_stopped(ROBOT) is True


@pytest.mark.parametrize("missing", ["nodeStates", "actionStates"])
def test_paused_true_still_confirms_without_the_lists(missing) -> None:
    b = _binding()
    b.command_estop(ROBOT)
    body = json.loads(_state(1, "t1", paused=True))
    del body[missing]
    b.on_state(json.dumps(body))
    assert b.is_stopped(ROBOT) is True


def test_a_list_that_is_not_a_list_is_not_reported() -> None:
    from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
    st = Vda5050Profile().parse_state(json.dumps({"nodeStates": {}, "actionStates": "x"}))
    assert not st.node_states_reported and not st.action_states_reported
    st = Vda5050Profile().parse_state(json.dumps({"nodeStates": [], "actionStates": []}))
    assert st.node_states_reported and st.action_states_reported
