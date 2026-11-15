# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""A command or a stop for a robot this adapter does not serve.

One ControlStream and one SafetyStream carry the messages for every Robot bound to an adapter,
and robot_id selects which one. This adapter serves one vehicle.

* A command for another id, or for no id, is refused before anything runs: nothing reaches the
  vehicle, the stop latch, the fencing mark, the lease, the current action or the leader term.
  Each arm gets the refusal its result already defines.
* An estop on the SafetyStream, or a fence (Command.estop), for another id is answered FAILED
  on the SafetyStream, never STOPPED. This vehicle is not stopped and nothing is latched.
* A clear for another id lifts nothing.
* A scan, and an arm this adapter declines anyway, are answered as for any robot.
* Every one is logged at error level, naming the id received and the id served."""

from __future__ import annotations

import pytest
from conftest import mark_registered

from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.robot import SimulatedRobot

ROBOT = "amr-1"
OTHER = "amr-2"


class _Tripwire(SimulatedRobot):
    """The simulated vehicle, recording every method the adapter calls on it once armed."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.calls: list[str] = []

    def __getattribute__(self, name):
        value = super().__getattribute__(name)
        if (not name.startswith("_") and name not in ("armed", "calls")
                and callable(value) and super().__getattribute__("armed")):
            super().__getattribute__("calls").append(name)
        return value


def _drain(q) -> list:
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def _adapter() -> Adapter:
    robot = _Tripwire()
    robot.spawn(ROBOT)
    a = Adapter(pb, None, robot, ROBOT)
    a._telemetry_started = True  # no telemetry thread under test
    a._handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    _drain(a._outbox)
    return mark_registered(a)


def _assign_ours(a: Adapter, action_id: str = "act-1", token: int = 5, lease_ms: int = 30_000):
    return _cmd(a, ROBOT, assign_action=pb.AssignAction(
        action_id=action_id, action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=token,
        assignment_id=f"as-{token}", payload_json=b'{"x": 1, "y": 2}',
        lease_duration_ms=lease_ms, lease_generation=1)).assign_action


def _cmd(a: Adapter, robot_id: str, term: int = 0, **arm):
    a._handle_command(pb.Command(command_id=7, robot_id=robot_id, leader_term=term, **arm))
    results = [m.command_result for m in _drain(a._outbox)
               if m.WhichOneof("payload") == "command_result"]
    return results[-1] if results else None


def _cmd_only(a: Adapter, robot_id: str, term: int = 0, **arm):
    """The one message an unserved command may put on the ControlStream: its result."""
    a._handle_command(pb.Command(command_id=7, robot_id=robot_id, leader_term=term, **arm))
    msgs = _drain(a._outbox)
    assert [m.WhichOneof("payload") for m in msgs] == ["command_result"], msgs
    return msgs[0].command_result


def _state(a: Adapter) -> tuple:
    """Everything a command for another robot must leave as it was: this robot's action, fence,
    lease, latch and registration, and no record of any kind made for the other id."""
    r = a._latch._r(ROBOT)
    return (a._current_action, a._current_fencing_token, a._last_task_state,
            a._current_generation, dict(a._action_tokens), dict(a._interrupted),
            dict(a._fence._highest), dict(a._fence._assignment),
            dict(a._lease._deadline), dict(a._lease._generation),
            sorted(a._latch._robots), r.highest_term, dict(r.latched), set(r.fenced),
            set(r.cleared), set(r.pending), r.local_set, r.local_set_id,
            dict(a._retained._mem), a._registered, a._estop_epoch)


def _running(a: Adapter) -> Adapter:
    """This robot runs act-1 under a lease; the vehicle is then armed to record any call."""
    assert _assign_ours(a).accepted
    _drain(a._safety_outbox)
    a._robot.armed = True
    return a


_ARMS = {
    "assign_action": {"assign_action": pb.AssignAction(
        action_id="act-9", action_type=adapter_mod.ACTION_NAVIGATE, fencing_token=99,
        assignment_id="as-99", payload_json=b'{"x": 9, "y": 9}')},
    "renew_lease": {"renew_lease": pb.RenewActionLease(
        action_id="act-1", lease_duration_ms=60_000, lease_generation=2)},
    "cancel_action": {"cancel_action": pb.CancelAction(action_id="act-1")},
    "pause": {"pause": pb.PauseCapabilities()},
    "resume": {"resume": pb.ResumeCapabilities()},
    "zone_constraints": {"zone_constraints": pb.ZoneConstraints()},
}


def _refused(which: str, result) -> bool:
    if which == "assign_action":
        r = result.assign_action
        return (not r.accepted and r.rejection == pb.ASSIGN_ACTION_REJECTION_ROBOT_UNAVAILABLE
                and "not served" in r.message)
    if which == "renew_lease":
        r = result.renew_lease
        return not r.renewed and not r.running and "not served" in r.message
    if which == "cancel_action":
        r = result.cancel_action
        return (not r.acknowledged and r.disposition != pb.CANCEL_DISPOSITION_STOPPED_SAFELY
                and "not served" in r.message)
    if which == "pause":
        return not result.pause.paused and "not served" in result.pause.message
    if which == "resume":
        return not result.resume.resumed and "not served" in result.resume.message
    if which == "zone_constraints":
        return result.HasField("zone_constraints") and not result.zone_constraints.acknowledged
    raise AssertionError(which)


# ── commands ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("addressed", [OTHER, ""], ids=["another-id", "empty-id"])
@pytest.mark.parametrize("which", sorted(_ARMS))
def test_a_command_for_an_unserved_robot_is_refused_and_touches_nothing(
        which, addressed, capsys) -> None:
    a = _running(_adapter())
    before = _state(a)
    result = _cmd_only(a, addressed, term=50, **_ARMS[which])
    assert result.command_id == 7 and result.robot_id == addressed
    assert _refused(which, result)
    assert a._robot.calls == []            # the vehicle was not touched
    assert _state(a) == before             # nor the latch, fence, lease, action or term
    assert _drain(a._safety_outbox) == []
    err = capsys.readouterr().err
    assert f"error: UNSERVED robot='{ROBOT}' addressed='{addressed}' arm={which}" in err


def test_the_served_robot_still_takes_the_same_commands_afterwards() -> None:
    a = _running(_adapter())
    for which in sorted(_ARMS):
        _cmd(a, OTHER, **_ARMS[which])
    a._robot.armed = False
    renewed = _cmd(a, ROBOT, renew_lease=pb.RenewActionLease(
        action_id="act-1", lease_duration_ms=60_000, lease_generation=1)).renew_lease
    assert renewed.renewed and renewed.running
    assert _cmd(a, ROBOT, cancel_action=pb.CancelAction(action_id="act-1")).cancel_action.acknowledged
    assert _assign_ours(a, "act-2", 6).accepted


@pytest.mark.parametrize("arm", [{"pause": pb.PauseCapabilities()},
                                 {"scan": pb.ScanCapabilities()},
                                 {"validate_action": pb.ValidateAction()},
                                 {"estop": pb.Estop(estop_id="s-9")}],
                         ids=["acting-arm", "scan", "declined-arm", "fence"])
def test_an_unserved_command_with_a_newer_term_leaves_this_robots_term(arm) -> None:
    """The term is kept for the robot. Were it raised, this robot's own commands at the
    current term would be refused as a deposed leader's."""
    a = _adapter()
    _cmd(a, ROBOT, term=3, scan=pb.ScanCapabilities())
    _cmd(a, OTHER, term=9, **arm)
    assert a._latch.highest_term(ROBOT) == 3
    assert not _cmd(a, ROBOT, term=3, resume=pb.ResumeCapabilities()).stale_leader_term


def test_an_unserved_scan_below_this_robots_term_is_still_answered() -> None:
    """Not held to this robot's term either: it is not this robot's command."""
    a = _adapter()
    _cmd(a, ROBOT, term=5, scan=pb.ScanCapabilities())
    result = _cmd(a, OTHER, term=2, scan=pb.ScanCapabilities())
    assert not result.stale_leader_term and result.scan.robot_id == ROBOT


def test_a_scan_for_an_unserved_robot_is_answered_as_the_simulation_adapter_answers_it(
        capsys) -> None:
    """It moves nothing, and the snapshot names this adapter's own robot."""
    a = _adapter()
    result = _cmd(a, OTHER, scan=pb.ScanCapabilities())
    assert result.scan.robot_id == ROBOT
    assert f"addressed='{OTHER}' arm=scan" in capsys.readouterr().err


def test_an_arm_this_adapter_declines_is_declined_whatever_robot_it_names(capsys) -> None:
    a = _adapter()
    assert _cmd(a, OTHER, validate_action=pb.ValidateAction()).unsupported
    assert f"addressed='{OTHER}' arm=validate_action" in capsys.readouterr().err


# ── stops ────────────────────────────────────────────────────────────────────

def _acks(a: Adapter) -> list:
    return [m for m in _drain(a._safety_outbox) if m.WhichOneof("payload") == "estop_ack"]


@pytest.mark.parametrize("addressed", [OTHER, ""], ids=["another-id", "empty-id"])
def test_a_fence_for_an_unserved_robot_is_answered_failed_and_stops_nothing(
        addressed, capsys) -> None:
    a = _running(_adapter())
    before = _state(a)
    assert _cmd(a, addressed, term=50, estop=pb.Estop(estop_id="s-1")) is None  # no receipt
    (ack,) = _acks(a)
    assert ack.robot_id == addressed
    assert ack.estop_ack.estop_id == "s-1"
    assert ack.estop_ack.state == pb.ESTOP_STATE_FAILED
    assert not ack.estop_ack.HasField("stop_initiated_at")  # no stop was commanded
    assert "not served" in ack.estop_ack.message
    assert a._robot.calls == [] and _state(a) == before
    assert f"error: UNSERVED robot='{ROBOT}' addressed='{addressed}' fence id=s-1" in (
        capsys.readouterr().err)


@pytest.mark.parametrize("addressed", [OTHER, ""], ids=["another-id", "empty-id"])
def test_a_safety_stream_estop_for_an_unserved_robot_is_answered_failed_and_stops_nothing(
        addressed, capsys) -> None:
    a = _running(_adapter())
    before = _state(a)
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=addressed, estop=pb.Estop(estop_id="s-2")))
    (ack,) = _acks(a)
    assert (ack.robot_id, ack.estop_ack.estop_id) == (addressed, "s-2")
    assert ack.estop_ack.state == pb.ESTOP_STATE_FAILED
    assert a._robot.calls == [] and _state(a) == before
    a._robot.armed = False
    assert _assign_ours(a, "act-2", 6).accepted  # not latched: work is still taken
    assert f"addressed='{addressed}' SafetyStream estop id=s-2" in capsys.readouterr().err


def test_the_served_robots_estop_still_stops_it() -> None:
    a = _running(_adapter())
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=ROBOT, estop=pb.Estop(estop_id="s-3")))
    (ack,) = _acks(a)
    assert ack.robot_id == ROBOT and ack.estop_ack.state == pb.ESTOP_STATE_STOPPED
    assert "command_stop" in a._robot.calls or "command_estop" in a._robot.calls
    assert a._latch.refuses_work(ROBOT)


@pytest.mark.parametrize("addressed", [OTHER, ""], ids=["another-id", "empty-id"])
def test_a_clear_for_an_unserved_robot_before_the_fence_is_not_kept_for_it(addressed) -> None:
    """A clear that comes before its fence is kept and takes effect with the fence. One
    addressed to another robot must not be kept for this one: the fence would then lift this
    robot's own stop."""
    a = _adapter()
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=ROBOT, estop=pb.Estop(estop_id="s-5")))
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=addressed, estop_clear=pb.EstopClear(estop_id="s-5")))
    _cmd(a, ROBOT, estop=pb.Estop(estop_id="s-5"))  # this robot's fence
    assert a._latch.refuses_work(ROBOT) and "s-5" in a._latch.latched_ids(ROBOT)
    assert not [m for m in _drain(a._safety_outbox)
                if m.WhichOneof("payload") == "estop_clear_ack"]


@pytest.mark.parametrize("addressed", [OTHER, ""], ids=["another-id", "empty-id"])
def test_a_clear_for_an_unserved_robot_lifts_nothing(addressed, capsys) -> None:
    a = _adapter()
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=ROBOT, estop=pb.Estop(estop_id="s-4")))
    _cmd(a, ROBOT, estop=pb.Estop(estop_id="s-4"))  # its fence
    _drain(a._safety_outbox)
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=addressed, estop_clear=pb.EstopClear(estop_id="s-4")))
    assert a._latch.refuses_work(ROBOT) and "s-4" in a._latch.latched_ids(ROBOT)
    assert _drain(a._safety_outbox) == []  # no EstopClearAck
    assert f"addressed='{addressed}' estop_clear id=s-4" in capsys.readouterr().err
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=ROBOT, estop_clear=pb.EstopClear(estop_id="s-4")))
    assert not a._latch.refuses_work(ROBOT)  # its own clear still lifts it


# ── no accepted registration: an unserved stop also stops this vehicle ───────

def _unregistered() -> Adapter:
    """This adapter's registration not accepted on this session: it serves no robot. The
    likeliest cause of a stop under another id is then an id mismatch with the Robot."""
    a = _adapter()
    a._registered = False
    a._robot.armed = True
    return a


def _stopped(a: Adapter) -> bool:
    return "command_estop" in a._robot.calls or "command_stop" in a._robot.calls


@pytest.mark.parametrize("via", ["safety", "fence"])
@pytest.mark.parametrize("addressed", [OTHER, ""], ids=["another-id", "empty-id"])
def test_with_no_accepted_registration_an_unserved_stop_stops_the_vehicle_and_is_failed(
        addressed, via, capsys) -> None:
    a = _unregistered()
    if via == "safety":
        a._on_safety_message(pb.ControlPlaneSafetyMessage(
            robot_id=addressed, estop=pb.Estop(estop_id="s-6")))
    else:
        assert _cmd(a, addressed, estop=pb.Estop(estop_id="s-6")) is None
    (ack,) = _acks(a)
    assert ack.robot_id == addressed and ack.estop_ack.state == pb.ESTOP_STATE_FAILED
    assert ack.estop_ack.HasField("stop_initiated_at")  # a stop was commanded
    assert "vehicle was stopped" in ack.estop_ack.message
    assert _stopped(a)
    assert a._latch.latched_ids(ROBOT) == {} and sorted(a._latch._robots) in ([], [ROBOT])
    err = capsys.readouterr().err
    assert (f"error: UNSERVED robot='{ROBOT}' addressed='{addressed}'" in err
            and "no accepted registration; vehicle stopped" in err)


@pytest.mark.parametrize("via", ["safety", "fence"])
def test_once_a_registration_is_accepted_an_unserved_stop_does_not_stop_the_vehicle(
        via, capsys) -> None:
    a = _unregistered()
    a._handle_control_plane(pb.ControlPlaneMessage(
        register_ack=pb.RegisterAck(accepted=True)), None)
    _drain(a._outbox)
    _drain(a._safety_outbox)
    a._robot.calls.clear()
    if via == "safety":
        a._on_safety_message(pb.ControlPlaneSafetyMessage(
            robot_id=OTHER, estop=pb.Estop(estop_id="s-7")))
    else:
        _cmd(a, OTHER, estop=pb.Estop(estop_id="s-7"))
    (ack,) = _acks(a)
    assert ack.estop_ack.state == pb.ESTOP_STATE_FAILED
    assert not ack.estop_ack.HasField("stop_initiated_at")
    assert not _stopped(a)
    assert "no stop was commanded" in capsys.readouterr().err


def test_a_new_session_returns_to_stopping_until_its_registration_is_accepted() -> None:
    a = _adapter()  # registered
    a._new_session()
    a._robot.armed = True
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=OTHER, estop=pb.Estop(estop_id="s-8")))
    assert _stopped(a)


def test_with_no_accepted_registration_the_held_work_is_interrupted_once() -> None:
    """Interrupted work, as any stop leaves it: ended STOPPED then FAILED, not continued."""
    a = _adapter()
    assert _assign_ours(a).accepted
    _drain(a._outbox)
    a._registered = False
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=OTHER, estop=pb.Estop(estop_id="s-9")))
    states = [m.action_status.state for m in _drain(a._outbox)
              if m.WhichOneof("payload") == "action_status"]
    assert states == [pb.ACTION_STATE_STOPPED, pb.ACTION_STATE_FAILED]
    assert a._current_action == ""


def test_after_an_unserved_stop_with_no_registration_the_first_order_lifts_the_pause() -> None:
    """On a VDA 5050 vehicle that stop is startPause then cancelOrder, and nothing is latched. The
    binding keeps its estop pause, so once a registration is accepted the first order goes out
    behind a stopPause, as after a cleared stop; the vehicle is not left paused under it."""
    import json

    from conftest import mark_transport_live

    from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))
    b.on_state(json.dumps({"orderId": "", "driving": False, "paused": False,
                           "nodeStates": [], "actionStates": []}))
    a = Adapter(pb, None, b, ROBOT)
    a._telemetry_started = True
    a._handle_control_plane(pb.ControlPlaneMessage(hello_ack=pb.HelloAck(
        accepted=True, negotiated_contract_version="1.4.0")), None)
    assert not a._registered
    a._on_safety_message(pb.ControlPlaneSafetyMessage(
        robot_id=OTHER, estop=pb.Estop(estop_id="s-10")))
    b.on_state(json.dumps({"orderId": "", "driving": False, "paused": True,
                           "nodeStates": [], "actionStates": []}))  # halted, paused
    a._handle_control_plane(pb.ControlPlaneMessage(
        register_ack=pb.RegisterAck(accepted=True)), None)
    assert _assign_ours(a).accepted
    wire = []
    for topic, payload in b.published:
        msg = json.loads(payload)
        wire.append("order" if topic.endswith("/order") else msg["actions"][0]["actionType"])
    assert wire == ["startPause", "cancelOrder", "stopPause", "order"]
