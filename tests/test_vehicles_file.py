# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""--vehicles-file: one process serving the vehicles a YAML file lists.

* The file lists each vehicle's robot_id, serial and optionally manufacturer; a robot id or a
  (manufacturer, serial) pair given twice, an unknown key, a missing or empty value, a key given
  twice in an entry, or anything but a list of mappings is refused at start-up, naming the entry.
* It takes the place of --robot-id, --serial and --manufacturer, and needs --comms vda5050.
* Its vehicles are served by one process, on one MQTT client whose client id is the first
  vehicle's and whose in-flight limit is the fleet's (inflight_for).
* Without it, one vehicle is served as before, on a client of its own."""

from __future__ import annotations

import pytest

from fleet_adapter_vda5050 import adapter as adapter_mod
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter as Adapter
from fleet_adapter_vda5050.mqtt_client import PAHO_MAX_INFLIGHT, inflight_for
from fleet_adapter_vda5050.vehicles import Vehicle, load_vehicles

FLEET = """\
- robot_id: amr-1
  serial: 200-abc123
  manufacturer: MiR
- robot_id: amr-2
  serial: 200-abc124
  manufacturer: MiR
- robot_id: amr-3
  serial: s-7
"""


def _file(tmp_path, text: str) -> str:
    f = tmp_path / "vehicles.yaml"
    f.write_text(text)
    return str(f)


# ── the file ─────────────────────────────────────────────────────────────────

def test_each_vehicle_is_read_with_its_manufacturer_or_the_default(tmp_path) -> None:
    assert load_vehicles(_file(tmp_path, FLEET)) == [
        Vehicle("amr-1", "200-abc123", "MiR"), Vehicle("amr-2", "200-abc124", "MiR"),
        Vehicle("amr-3", "s-7", "swarmada")]


def test_one_serial_under_two_manufacturers_is_two_vehicles(tmp_path) -> None:
    text = ("- {robot_id: a, serial: s-1, manufacturer: MiR}\n"
            "- {robot_id: b, serial: s-1, manufacturer: OTTO}\n")
    assert [v.robot_id for v in load_vehicles(_file(tmp_path, text))] == ["a", "b"]


@pytest.mark.parametrize(("text", "says"), [
    ("robot_id: amr-1\nserial: s-1\n", "not a list of vehicles"),
    ("[]\n", "not a list of vehicles"),
    ("- amr-1\n", "entry 1: not a mapping"),
    ("- {robot_id: a, serial: s-1, robotId: b}\n", "entry 1: unknown key 'robotId'"),
    ("- {serial: s-1}\n", "entry 1: no robot_id"),
    ("- {robot_id: a}\n", "entry 1: no serial"),
    ("- {robot_id: '', serial: s-1}\n", "entry 1: robot_id is '', not a non-empty string"),
    ("- {robot_id: a, serial: 123}\n", "entry 1: serial is 123, not a non-empty string"),
    ("- {robot_id: a, serial: s-1, manufacturer: }\n", "manufacturer is None"),
    ("- {robot_id: a, serial: s-1}\n- {robot_id: a, serial: s-2}\n",
     "entry 2: robot_id 'a' is already entry 1"),
    ("- {robot_id: a, serial: s-1}\n- {robot_id: b, serial: s-1}\n",
     "entry 2: manufacturer 'swarmada' and serial 's-1' are already entry 1"),
    ("- robot_id: a\n  serial: s-1\n  robot_id: b\n", "key 'robot_id' is given twice"),
])
def test_a_bad_file_is_refused_naming_the_entry(tmp_path, text, says) -> None:
    path = _file(tmp_path, text)
    with pytest.raises(ValueError, match="vehicles file") as err:
        load_vehicles(path)
    assert says in str(err.value)


def test_a_file_that_cannot_be_read_is_refused(tmp_path) -> None:
    with pytest.raises(ValueError, match="vehicles file"):
        load_vehicles(str(tmp_path / "missing.yaml"))


# ── the command line ─────────────────────────────────────────────────────────

@pytest.mark.parametrize(("extra", "says"), [
    (["--comms", "simulated"], "--vehicles-file needs --comms vda5050"),
    (["--comms", "vda5050", "--robot-id", "amr-1"], "--vehicles-file and --robot-id together"),
    (["--comms", "vda5050", "--serial", "s-1"], "--vehicles-file and --serial together"),
    (["--comms", "vda5050", "--manufacturer", "MiR"],
     "--vehicles-file and --manufacturer together"),
])
def test_the_file_with_a_vehicle_flag_or_the_simulated_comms_is_refused(
        tmp_path, capsys, extra, says) -> None:
    with pytest.raises(SystemExit) as stop:
        adapter_mod.main(["--insecure", "--vehicles-file", _file(tmp_path, FLEET), *extra])
    assert stop.value.code == 2
    assert says in capsys.readouterr().err


def test_a_bad_file_stops_the_adapter_at_start_up(tmp_path, capsys) -> None:
    with pytest.raises(SystemExit) as stop:
        adapter_mod.main(["--insecure", "--comms", "vda5050", "--vehicles-file",
                          _file(tmp_path, "- {robot_id: a}\n")])
    assert stop.value.code == 2
    assert "entry 1: no serial" in capsys.readouterr().err


@pytest.fixture
def served(monkeypatch):
    """main() up to its streams: the vehicle it would run them for, and nothing dialled."""
    ran: list = []
    monkeypatch.setattr(Adapter, "run_control_stream", lambda self, stub: ran.append(self))
    monkeypatch.setattr(Adapter, "run_safety_stream", lambda self, stub: None)
    return ran


def test_the_files_vehicles_are_served_by_one_process_on_one_mqtt_client(
        tmp_path, capsys, served) -> None:
    adapter_mod.main(["--insecure", "--comms", "vda5050",
                      "--vehicles-file", _file(tmp_path, FLEET)])
    (first,) = served
    cores = first._process.cores
    assert [c._robot_id for c in cores] == ["amr-1", "amr-2", "amr-3"]
    assert len({id(c._streams) for c in cores}) == 1  # one ControlStream and SafetyStream
    bindings = [c._robot for c in cores]
    (client,) = {id(b._mqtt) for b in bindings}
    shared = bindings[0]._mqtt
    assert client == id(shared) and shared._client_id == "swarmada-amr-1"
    assert shared._max_inflight == inflight_for(3)
    assert [(b._manufacturer, b._serial) for b in bindings] == [
        ("MiR", "200-abc123"), ("MiR", "200-abc124"), ("swarmada", "s-7")]
    assert "serving 3 vehicles from" in capsys.readouterr().err


def test_without_the_file_one_vehicle_is_served_on_a_client_of_its_own(served) -> None:
    adapter_mod.main(["--insecure", "--comms", "vda5050", "--robot-id", "amr-9",
                      "--serial", "s-9", "--manufacturer", "MiR"])
    (first,) = served
    assert [c._robot_id for c in first._process.cores] == ["amr-9"]
    b = first._robot
    assert (b._manufacturer, b._serial) == ("MiR", "s-9")
    assert b._mqtt._client_id == "swarmada-amr-9"
    assert b._mqtt._max_inflight == PAHO_MAX_INFLIGHT


def test_the_one_vehicle_defaults_are_unchanged(served) -> None:
    adapter_mod.main(["--insecure", "--comms", "vda5050"])
    (first,) = served
    b = first._robot
    assert first._robot_id == "robot-conformance-1"
    assert (b._manufacturer, b._serial) == ("swarmada", "amr-1")
