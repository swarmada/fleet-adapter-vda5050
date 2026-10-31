"""The simulated vehicle's log lines: what `kubectl logs` shows of the model. Log only."""
from fleet_adapter_vda5050.robot import SimulatedRobot


def _robot():
    r = SimulatedRobot()
    r.spawn("amr")
    return r


def _lines(capsys, prefix):
    return [l for l in capsys.readouterr().err.splitlines() if l.startswith(prefix)]


def test_taking_an_order_is_logged_with_its_target(capsys):
    r = _robot()
    r.command_move("amr", 3.0, 4.5)
    r.note_order("amr", "act-1")
    assert _lines(capsys, "VDA_ORDER") == ["VDA_ORDER robot=amr order=act-1 x=3.00 y=4.50"]


def test_state_is_logged_once_a_second_while_driving(capsys):
    r = _robot()
    r.command_move("amr", 100.0, 0.0)
    r.note_order("amr", "act-1")
    for _ in range(25):
        r.tick(0.1)
    lines = _lines(capsys, "VDA_STATE")
    assert lines[0] == "VDA_STATE robot=amr order=act-1 driving=true x=0.05 y=0.00 speed=0.50 stopped=false"
    assert len(lines) == 3      # at 0.1 s, then 1.1 s and 2.1 s


def test_silent_when_idle_and_a_line_at_each_change(capsys):
    r = _robot()
    r.tick(0.1)
    capsys.readouterr()
    for _ in range(5):
        r.tick(0.1)
    assert _lines(capsys, "VDA_STATE") == []
    r.command_move("amr", 100.0, 0.0)
    r.note_order("amr", "act-1")
    r.tick(0.1)
    capsys.readouterr()
    r.command_pause("amr")
    r.tick(0.1)
    assert _lines(capsys, "VDA_STATE") == [
        "VDA_STATE robot=amr order=act-1 driving=false x=0.05 y=0.00 speed=0.00 stopped=true"]
    r.command_stop("amr")
    r.tick(0.1)
    assert _lines(capsys, "VDA_STATE") == [
        "VDA_STATE robot=amr order=- driving=false x=0.05 y=0.00 speed=0.00 stopped=true"]


def test_arrival_is_a_change_and_then_it_is_silent(capsys):
    r = _robot()
    r.command_move("amr", 0.04, 0.0)
    r.note_order("amr", "act-1")
    r.tick(0.1)
    assert _lines(capsys, "VDA_STATE") == [
        "VDA_STATE robot=amr order=act-1 driving=false x=0.04 y=0.00 speed=0.00 stopped=true"]
    r.tick(0.1)
    assert _lines(capsys, "VDA_STATE") == []


def test_the_stopped_value_is_the_one_the_adapter_reads(capsys):
    r = _robot()
    r.command_move("amr", 100.0, 0.0)
    r.tick(0.1)
    assert (" stopped=" + str(r.is_stopped("amr")).lower()) in capsys.readouterr().err
