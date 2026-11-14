# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The stop paths: who ends an action, what reaches the vehicle, and what the fence keeps.

* One action is ended once. The SafetyStream, the EdgeStream, the telemetry loop and the
  ControlStream all end actions; under the action lock exactly one of them takes it, so the
  record that keeps interrupted work from being continued is never lost or overwritten.
* An estop is ``startPause``, then ``cancelOrder``: the vehicle halts at once and its order
  ends. It stays paused until the first order after the clear, which a ``stopPause`` precedes.
* A ``cancelOrder`` is not sent when it can change nothing: the vehicle reports it is at rest,
  with no order in progress and none of ours outstanding.
* A finished action releases its lease, so the lease never lapses on an idle vehicle.
* Work refused on a session whose registration was not accepted leaves the fencing mark where
  it was."""

from __future__ import annotations

import json
import threading
import time

from conftest import mark_registered, mark_transport_live
from swarmada_sdk.safety import ESTOP_STOPPED, LeaseMonitor

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050 import task as task_map
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"


# ── helpers ──────────────────────────────────────────────────────────────────

def _state(driving=False, vx=0.0, order_id="", nodes=0, actions=(), paused=False) -> str:
    return json.dumps({
        "driving": driving, "velocity": {"vx": vx, "vy": 0.0, "omega": 0.0}, "paused": paused,
        "orderId": order_id, "nodeStates": [{"nodeId": f"n{i}"} for i in range(nodes)],
        "actionStates": [{"actionId": f"a{i}", "actionStatus": st}
                         for i, st in enumerate(actions)]})


def _live_binding() -> Vda5050Binding:
    return mark_transport_live(Vda5050Binding(robot_id=ROBOT))


def _wire(b: Vda5050Binding) -> list[str]:
    """What the binding published, in order: the instantAction type, or "order"."""
    out = []
    for topic, payload in b.published:
        if topic.endswith("/order"):
            out.append("order")
        else:
            out.append(json.loads(payload)["actions"][0]["actionType"])
    return out


def _drain(q) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _statuses(a: Adapter) -> list[tuple[str, str]]:
    names = {v: k for k, v in pb.ActionState.items()}
    return [(m.action_status.action_id, names[m.action_status.state])
            for m in _drain(a._outbox) if m.WhichOneof("payload") == "action_status"]


def _assign(a: Adapter, action_id: str, token: int, assignment: str = "", lease_ms: int = 0):
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=assignment or f"as-{token}", payload_json=b'{"x": 5, "y": 0}',
        lease_duration_ms=lease_ms, lease_generation=1), res)
    return res.assign_action


def _adapter(robot=None) -> Adapter:
    robot = robot or SimulatedRobot()
    robot.spawn(ROBOT)
    return mark_registered(Adapter(pb, None, robot, ROBOT))


# ── #3: one action is ended once ─────────────────────────────────────────────

class _SlowStop(SimulatedRobot):
    """command_stop takes a while, so a second thread arrives while the first is inside it."""

    def command_stop(self, robot_id: str) -> None:
        time.sleep(0.05)
        super().command_stop(robot_id)


def _race(*calls) -> None:
    start = threading.Barrier(len(calls))

    def run(fn):
        start.wait()
        fn()

    threads = [threading.Thread(target=run, args=(fn,)) for fn in calls]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_two_threads_interrupting_one_action_end_it_once_and_keep_its_token() -> None:
    a = _adapter(_SlowStop())
    assert _assign(a, "act-1", 5).accepted
    _drain(a._outbox)
    _race(lambda: a._interrupt_current("emergency stop"),
          lambda: a._interrupt_current("registration refused"))
    assert _statuses(a) == [("act-1", "ACTION_STATE_STOPPED"), ("act-1", "ACTION_STATE_FAILED")]
    assert a._interrupted == {"act-1": 5}
    # So its re-delivery at the same token still starts nothing.
    r = _assign(a, "act-1", 5)
    assert r.accepted and "not continued" in r.message
    assert a._current_action == ""


def test_a_lease_expiry_racing_a_stop_ends_the_action_once() -> None:
    a = _adapter(_SlowStop())
    assert _assign(a, "act-1", 5).accepted
    _drain(a._outbox)
    _race(lambda: a._interrupt_current("emergency stop"), lambda: a._self_stop(ROBOT))
    statuses = _statuses(a)
    assert len(statuses) == 2 and statuses[1][1] == "ACTION_STATE_FAILED"
    assert {s[0] for s in statuses} == {"act-1"}


class _HeldMove(SimulatedRobot):
    """command_move waits until released, holding the assignment in the middle."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def command_move(self, robot_id, x, y, yaw=0.0) -> None:
        self.entered.set()
        self.release.wait(2.0)
        super().command_move(robot_id, x, y, yaw)


def test_a_stop_during_an_assignment_interrupts_the_assigned_action() -> None:
    """Without the lock the stop found no action yet, returned, and the assignment then left
    an action running that the stop never ended."""
    robot = _HeldMove()
    a = _adapter(robot)
    assigning = threading.Thread(target=lambda: _assign(a, "act-1", 5))
    assigning.start()
    assert robot.entered.wait(2.0)
    stopping = threading.Thread(target=lambda: a._interrupt_current("emergency stop"))
    stopping.start()
    time.sleep(0.05)  # the stop is now waiting on the assignment
    robot.release.set()
    assigning.join()
    stopping.join()
    assert ("act-1", "ACTION_STATE_FAILED") in _statuses(a)
    assert a._current_action == ""
    assert a._interrupted == {"act-1": 5}


def test_a_finished_order_is_not_reported_for_the_action_that_replaced_it() -> None:
    a = _adapter()
    assert _assign(a, "act-1", 5).accepted
    _drain(a._outbox)

    def succeeded_but_replaced(robot_id):
        a._current_action = "act-2"  # a new assignment landed while the state was read
        return task_map.TASK_SUCCEEDED

    a._robot.task_state = succeeded_but_replaced
    a._maybe_emit_task_progress()
    assert _statuses(a) == []
    assert a._current_action == "act-2"


# ── F5: a finished action releases its lease ─────────────────────────────────

def test_a_succeeded_action_releases_its_lease_so_it_never_lapses() -> None:
    now = [0.0]
    a = _adapter()
    a._lease = LeaseMonitor(on_expiry=a._self_stop, clock=lambda: now[0])
    assert _assign(a, "act-1", 5, lease_ms=30_000).accepted
    a._robot.tick(60.0)  # drives to the target: the order is complete
    a._maybe_emit_task_progress()
    assert ("act-1", "ACTION_STATE_SUCCEEDED") in _statuses(a)
    stops = []
    a._robot.command_stop = lambda robot_id: stops.append(robot_id)
    now[0] = 120.0
    a._lease.tick()
    assert stops == []          # no self-stop on the idle vehicle
    assert _statuses(a) == []   # and no FAILED/LEASE_EXPIRED for an action that succeeded


# ── F5: no cancelOrder with nothing to cancel ────────────────────────────────

def test_an_idle_vehicle_with_no_order_gets_no_cancel_order() -> None:
    b = _live_binding()
    b.on_state(_state())
    b.command_stop(ROBOT)
    assert _wire(b) == []
    assert b.suppressed_stops(ROBOT) == 1


def test_a_driving_vehicle_always_gets_the_cancel_order() -> None:
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.8, order_id="x", nodes=2))
    b.command_stop(ROBOT)
    b.command_stop(ROBOT)  # again while it is still moving: sent again
    assert _wire(b) == ["cancelOrder", "cancelOrder"]


def test_a_vehicle_still_moving_with_no_nodes_left_gets_the_cancel_order() -> None:
    """Decelerating after its last node, or reporting driving false while it still moves: not
    at rest, so the stop is sent."""
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.4))
    b.command_stop(ROBOT)
    b.on_state(_state(driving=False, vx=0.3))
    b.command_stop(ROBOT)
    assert _wire(b) == ["cancelOrder", "cancelOrder"]


def test_an_action_not_ended_gets_the_cancel_order() -> None:
    """At rest on its last node, with a node action still running or paused: a cancelOrder
    cancels it, so it is sent."""
    for status in ("WAITING", "INITIALIZING", "RUNNING", "PAUSED"):
        b = _live_binding()
        b.on_state(_state(actions=[status]))
        b.command_stop(ROBOT)
        assert _wire(b) == ["cancelOrder"], status


def test_ended_actions_do_not_keep_the_cancel_order() -> None:
    b = _live_binding()
    b.on_state(_state(actions=["FINISHED", "FAILED"]))
    b.command_stop(ROBOT)
    assert _wire(b) == []


def test_unreadable_action_states_count_as_not_ended() -> None:
    assert profiles.get_profile("vda5050").parse_state(
        json.dumps({"actionStates": "garbled"})).active_actions == 1


def test_a_cancel_does_not_end_an_order_the_vehicle_has_not_reported() -> None:
    """The order and the cancel travel on different topics. Until the vehicle reports the
    order, a later stop is sent again rather than skipped on the strength of the first."""
    b = _live_binding()
    b.on_state(_state())
    b.command_move(ROBOT, 3.0, 0.0)
    b.command_stop(ROBOT)
    b.command_stop(ROBOT)  # the vehicle has still not reported the order
    assert _wire(b) == ["order", "cancelOrder", "cancelOrder"]


def test_a_vehicle_at_rest_with_nodes_left_gets_the_cancel_order() -> None:
    b = _live_binding()
    b.on_state(_state(order_id="x", nodes=1))  # waiting between nodes
    b.command_stop(ROBOT)
    assert _wire(b) == ["cancelOrder"]


def test_an_order_not_yet_reported_by_the_vehicle_is_cancelled() -> None:
    """The vehicle's last state predates our order: it may be about to start it."""
    b = _live_binding()
    b.on_state(_state())
    b.command_move(ROBOT, 3.0, 0.0)
    b.command_stop(ROBOT)
    assert _wire(b) == ["order", "cancelOrder"]


def test_an_order_the_vehicle_reports_finished_is_not_cancelled() -> None:
    b = _live_binding()
    b.on_state(_state())
    b.command_move(ROBOT, 3.0, 0.0)
    order_id = json.loads(b.pending_order(ROBOT))["orderId"]
    b.on_state(_state(order_id=order_id, nodes=0))  # arrived
    b.command_stop(ROBOT)
    assert _wire(b) == ["order"]


def test_a_vehicle_that_cannot_be_observed_gets_the_cancel_order() -> None:
    b = _live_binding()
    b.on_state(_state())
    b.on_connection(json.dumps({"connectionState": "CONNECTIONBROKEN"}))
    b.command_stop(ROBOT)
    assert _wire(b) == ["cancelOrder"]


def test_no_state_yet_means_the_cancel_order_is_sent() -> None:
    b = _live_binding()
    b.command_stop(ROBOT)
    assert _wire(b) == ["cancelOrder"]


def test_the_register_reconcile_sends_no_cancel_order_to_an_idle_vehicle() -> None:
    b = _live_binding()
    b.on_state(_state())
    a = Adapter(pb, None, b, ROBOT)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            phase=pb.ROBOT_ACTION_PHASE_IDLE)))
    assert _wire(b) == []


def test_the_register_reconcile_cancels_an_order_still_on_the_vehicle() -> None:
    """After an adapter restart the vehicle may still run an order this process never sent."""
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.5, order_id="from-before-the-restart", nodes=2))
    a = Adapter(pb, None, b, ROBOT)
    a._reconcile_action_state(pb.RegisterAck(
        accepted=True, authoritative_action_state=pb.RobotActionState(
            phase=pb.ROBOT_ACTION_PHASE_IDLE)))
    assert _wire(b) == ["cancelOrder"]


# ── the estop: startPause, then cancelOrder; stopPause before the next order ──

def test_an_estop_is_start_pause_then_cancel_order() -> None:
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.8, order_id="x", nodes=2))
    b.command_estop(ROBOT)
    assert _wire(b) == ["startPause", "cancelOrder"]


def test_an_estop_on_an_idle_vehicle_still_pauses_and_cancels() -> None:
    """The last state can be a publish interval old: an estop never trusts it to skip the
    cancel, or a paused order it missed would be resumed by the next stopPause."""
    b = _live_binding()
    b.on_state(_state())
    b.command_estop(ROBOT)
    assert _wire(b) == ["startPause", "cancelOrder"]
    assert b.suppressed_stops(ROBOT) == 0


def test_the_first_order_after_an_estop_is_preceded_by_stop_pause_once() -> None:
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.8, order_id="x", nodes=2))
    b.command_estop(ROBOT)
    b.on_state(_state(order_id="x", paused=True))  # halted, paused, order cancelled
    b.published.clear()
    b.command_move(ROBOT, 1.0, 0.0)
    b.on_state(_state(order_id=f"{ROBOT}-order-1"))  # that order done
    b.command_move(ROBOT, 2.0, 0.0)
    assert _wire(b) == ["stopPause", "order", "order"]


def test_without_an_estop_an_order_has_no_stop_pause() -> None:
    b = _live_binding()
    b.command_move(ROBOT, 1.0, 0.0)
    assert _wire(b) == ["order"]


def test_a_stop_that_is_not_an_estop_never_pauses() -> None:
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.8, order_id="x", nodes=2))
    b.command_stop(ROBOT)
    b.on_state(_state(order_id="x"))  # cancelled
    b.command_move(ROBOT, 1.0, 0.0)
    assert _wire(b) == ["cancelOrder", "order"]


def test_the_adapter_estop_sends_one_pause_and_one_cancel(monkeypatch) -> None:
    """Before, the stop was commanded by the interrupt and again by the confirmation: two
    cancelOrders for one estop, before any pause."""
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation", lambda robot, rid: ESTOP_STOPPED)
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.8, order_id="x", nodes=2))
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    a._current_action = "act-1"
    a._action_tokens["act-1"] = 5
    acks = []
    a._stop_and_ack("e1", acks.append)
    assert _wire(b) == ["startPause", "cancelOrder"]
    assert acks[0].estop_ack.state == pb.ESTOP_STATE_STOPPED
    assert a._interrupted == {"act-1": 5}


def _race_assign_with(a: Adapter, monkeypatch, during):
    """Run an assignment, and `during` while it is past its checks (inside the goal lookup,
    before the order is sent). Returns the assignment's result."""
    entered, gate = threading.Event(), threading.Event()
    real = task_map.goal_from_assign

    def held(*args):
        entered.set()
        gate.wait(2.0)
        return real(*args)

    monkeypatch.setattr(adapter_mod.task_map, "goal_from_assign", held)
    res = pb.CommandResult()
    assigning = threading.Thread(target=lambda: a._on_assign(pb.AssignAction(
        action_id="act-1", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=5,
        assignment_id="as-5", payload_json=b'{"x": 5, "y": 0}'), res))
    assigning.start()
    assert entered.wait(2.0)
    other = threading.Thread(target=during)
    other.start()
    time.sleep(0.1)  # the stop has been commanded and now waits on the action lock
    gate.set()
    assigning.join()
    other.join()
    return res.assign_action


def test_an_estop_during_an_assignment_sends_no_order_and_no_stop_pause(monkeypatch) -> None:
    """The safety review's race: the estop commands the vehicle without the action lock, so an
    assignment already past the latch went on to send stopPause and its order after it."""
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation", lambda robot, rid: ESTOP_STOPPED)
    for vehicle in (_state(), _state(driving=True, vx=0.8, order_id="x", nodes=2)):
        b = _live_binding()
        b.on_state(vehicle)
        a = mark_registered(Adapter(pb, None, b, ROBOT))
        a._negotiated = "1.4.0"

        def estop(a=a):
            a._latch.on_estop(ROBOT, "e1")
            a._stop_and_ack("e1", lambda msg: None)

        r = _race_assign_with(a, monkeypatch, estop)
        assert _wire(b) == ["startPause", "cancelOrder"]
        assert not r.accepted and r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
        assert a._current_action == ""


class _HeldAfterOrder(Vda5050Binding):
    """Holds the assignment just after its order went out (note_order runs then)."""

    def __init__(self) -> None:
        super().__init__(robot_id=ROBOT)
        self.entered, self.gate = threading.Event(), threading.Event()

    def note_order(self, robot_id, action) -> None:
        self.entered.set()
        self.gate.wait(2.0)


def test_an_estop_just_after_the_order_went_out_stops_it_and_interrupts_the_action(
        monkeypatch) -> None:
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation", lambda robot, rid: ESTOP_STOPPED)
    b = mark_transport_live(_HeldAfterOrder())
    b.on_state(_state())
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assigning = threading.Thread(target=lambda: _assign(a, "act-1", 5))
    assigning.start()
    assert b.entered.wait(2.0)
    stopping = threading.Thread(target=lambda: a._stop_and_ack("e1", lambda msg: None))
    stopping.start()
    time.sleep(0.05)
    b.gate.set()
    assigning.join()
    stopping.join()
    assert _wire(b) == ["order", "startPause", "cancelOrder"]
    assert a._interrupted == {"act-1": 5}
    assert a._current_action == ""


def test_a_resume_racing_an_estop_does_not_lift_its_pause() -> None:
    b = _live_binding()
    a = Adapter(pb, None, b, ROBOT)
    calls = []

    def an_estop_lands_after_the_first_check(robot_id):
        calls.append(robot_id)
        if len(calls) == 1:
            a._estop_epoch += 1  # what _command_estop does under the robot lock
            return False
        return True

    a._latch.refuses_work = an_estop_lands_after_the_first_check
    res = pb.CommandResult()
    a._on_resume(pb.ResumeCapabilities(), res)
    assert not res.resume.resumed
    assert "stopPause" not in _wire(b)


def test_a_binding_without_command_estop_still_stops_and_answers(monkeypatch) -> None:
    monkeypatch.setattr(adapter_mod, "await_estop_confirmation", lambda robot, rid: ESTOP_STOPPED)

    class _Older(SimulatedRobot):
        command_estop = None  # written before command_estop was declared

    robot = _Older()
    robot.spawn(ROBOT)
    stops = []
    robot.command_stop = lambda robot_id: stops.append(robot_id)
    a = Adapter(pb, None, robot, ROBOT)
    acks = []
    a._stop_and_ack("e1", acks.append)
    assert stops == [ROBOT]
    assert acks[0].estop_ack.state == pb.ESTOP_STATE_STOPPED


def test_a_swarmada_resume_is_refused_while_a_stop_is_latched() -> None:
    b = _live_binding()
    a = Adapter(pb, None, b, ROBOT)
    a._latch.on_estop(ROBOT, "e1")
    res = pb.CommandResult()
    a._on_resume(pb.ResumeCapabilities(), res)
    assert not res.resume.resumed
    assert "stopPause" not in _wire(b)


# ── #5: refused work leaves the fencing mark where it was ────────────────────

def test_work_refused_while_unregistered_does_not_raise_the_fencing_mark() -> None:
    a = _adapter()
    assert _assign(a, "act-1", 5).accepted
    a._registered = False                          # the session's registration was refused
    r = _assign(a, "act-2", 7, assignment="as-7a")
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert a._fence.highest(ROBOT) == 5
    a._registered = True                           # admitted; the plane dispatches again
    r = _assign(a, "act-2", 7, assignment="as-7b")  # same token, a new assignment
    assert r.accepted


def test_below_the_mark_unregistered_work_is_still_stale_and_without_a_token_missing() -> None:
    a = _adapter()
    assert _assign(a, "act-1", 5).accepted
    a._registered = False
    assert _assign(a, "act-2", 4).rejection == pb.ASSIGN_ACTION_REJECTION_STALE_FENCING_TOKEN
    res = pb.CommandResult()
    a._on_assign(pb.AssignAction(action_id="act-3", action_type=adapter_mod.ACTION_NAVIGATE), res)
    assert res.assign_action.rejection == pb.ASSIGN_ACTION_REJECTION_MISSING_FENCING_TOKEN
    assert a._fence.highest(ROBOT) == 5


def test_profiles_names_used_here_are_the_wire_names() -> None:
    assert (profiles.ACTION_START_PAUSE, profiles.ACTION_STOP_PAUSE,
            profiles.ACTION_CANCEL_ORDER) == ("startPause", "stopPause", "cancelOrder")


# ── a pause at the vehicle: refused work ─────────────────────────────────────

def test_the_paused_field_is_read_only_when_it_is_true() -> None:
    parse = profiles.get_profile("vda5050").parse_state
    assert parse(json.dumps({"paused": True})).paused is True
    for value in (False, "true", 1, None):
        assert parse(json.dumps({"paused": value})).paused is False
    assert parse("{}").paused is False


def test_a_pause_this_binding_did_not_set_is_a_pause_at_the_vehicle() -> None:
    b = _live_binding()
    b.on_state(_state(paused=True))
    assert b.paused_at_vehicle(ROBOT)


def test_its_own_estop_pause_and_a_swarmada_pause_are_not() -> None:
    b = _live_binding()
    b.on_state(_state(paused=True))
    b.command_estop(ROBOT)
    assert not b.paused_at_vehicle(ROBOT)
    b = _live_binding()
    b.on_state(_state(paused=True))
    b.command_pause(ROBOT)
    assert not b.paused_at_vehicle(ROBOT)
    b.command_resume(ROBOT)  # resumed, and the vehicle still reports paused: not ours now
    assert b.paused_at_vehicle(ROBOT)


def test_a_vehicle_that_cannot_be_observed_is_not_reported_paused() -> None:
    b = _live_binding()
    b.on_state(_state(paused=True))
    b.on_connection(json.dumps({"connectionState": "OFFLINE"}))
    assert not b.paused_at_vehicle(ROBOT)


def test_work_is_refused_while_the_vehicle_is_paused_at_the_vehicle() -> None:
    b = _live_binding()
    b.on_state(_state(paused=True))
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    r = _assign(a, "act-1", 5)
    assert not r.accepted
    assert r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
    assert r.message == "paused at the vehicle"
    assert _wire(b) == []                # no order, and no stopPause to release it
    assert a._fence.highest(ROBOT) == 0  # and the fencing mark is where it was
    b.on_state(_state(paused=False))     # released at the vehicle
    assert _assign(a, "act-1", 5).accepted
    assert _wire(b) == ["order"]


def test_after_its_own_estop_the_paused_vehicle_takes_work_and_is_unpaused() -> None:
    b = _live_binding()
    b.on_state(_state(driving=True, vx=0.8, order_id="x", nodes=2))
    b.command_estop(ROBOT)
    b.on_state(_state(order_id="x", paused=True))  # halted, paused, order cancelled
    a = mark_registered(Adapter(pb, None, b, ROBOT))
    assert _assign(a, "act-1", 5).accepted
    assert _wire(b) == ["startPause", "cancelOrder", "stopPause", "order"]


def test_documented_a_pause_pressed_during_its_own_estop_pause_is_released_after_the_clear() -> (
        None):
    """The remaining case, pinned so that a change to it is deliberate. VDA5050 does not say
    who paused a vehicle; a button pressed while this binding's estop pause is in force looks
    like that pause, and the stopPause before the first order after the clear releases it
    (VDA5050 2.0.0: stopPause "can also restart vehicles that were stopped with a hardware
    button that triggered startPause (if configured)")."""
    b = _live_binding()
    b.on_state(_state())
    b.command_estop(ROBOT)
    b.on_state(_state(paused=True))  # the estop's pause, and now also the button: one flag
    assert not b.paused_at_vehicle(ROBOT)
    b.published.clear()
    b.command_move(ROBOT, 1.0, 0.0)
    assert _wire(b) == ["stopPause", "order"]


def test_a_binding_without_paused_at_vehicle_takes_work() -> None:
    class _Older(SimulatedRobot):
        paused_at_vehicle = None

    a = _adapter(_Older())
    assert _assign(a, "act-1", 5).accepted
