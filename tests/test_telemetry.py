# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 `state` → protocol telemetry mapping (sub-item 2, targets C6).

Pure: fake `state` JSON, no broker and no paho. Asserts the numeric-only mapping —
position/battery/hardware/phase — and that opaque `errorReferences` never leak.
"""

from __future__ import annotations

import json

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

P = Vda5050Profile()


def test_position_from_agv_position() -> None:
    s = P.parse_state(json.dumps({"agvPosition": {"x": 3.0, "y": -4.0, "theta": 1.5,
                                                  "mapId": "level-2", "positionInitialized": True}}))
    assert (s.position.x, s.position.y, s.position.yaw) == (3.0, -4.0, 1.5)
    assert s.position.floor == 0  # mapId is a string identifier, not smuggled as a floor


def test_uninitialized_position_is_absent_not_zero() -> None:
    s = P.parse_state(json.dumps({"agvPosition": {"x": 9.9, "y": 9.9,
                                                  "positionInitialized": False}}))
    assert s.position is None  # never a bogus (0,0)/stale pose


def test_battery_charge_is_percent_not_fraction() -> None:
    # VDA5050 batteryCharge is already 0–100 %, unlike ROS BatteryState's 0.0–1.0.
    s = P.parse_state(json.dumps({"batteryState": {"batteryCharge": 82.4,
                                                   "charging": True, "batteryVoltage": 48.1}}))
    assert s.battery.percent == 82
    assert s.battery.charging is True
    assert s.battery.voltage == 48.1


def test_battery_charging_absent_is_unknown() -> None:
    s = P.parse_state(json.dumps({"batteryState": {"batteryCharge": 50}}))
    assert s.battery.charging is None  # unknown, never defaulted to False


def test_errors_map_to_hardware_status() -> None:
    s = P.parse_state(json.dumps({"errors": [
        {"errorType": "motorFault", "errorLevel": "FATAL", "errorDescription": "drive 2 overcurrent"},
        {"errorType": "lidarDirty", "errorLevel": "WARNING", "errorDescription": "clean sensor"},
    ]}))
    hw = {h.component_name: h for h in s.hardware}
    assert hw["motorFault"].status == profiles.STATUS_FAILED
    assert hw["motorFault"].reason == "drive 2 overcurrent"
    assert hw["lidarDirty"].status == profiles.STATUS_DEGRADED


def test_error_references_do_not_leak() -> None:
    # Numeric-only: the arbitrary errorReferences key/values must not appear anywhere.
    s = P.parse_state(json.dumps({"errors": [
        {"errorType": "e", "errorLevel": "WARNING", "errorDescription": "d",
         "errorReferences": [{"referenceKey": "secret", "referenceValue": "leak-me"}]},
    ]}))
    dumped = repr(s.hardware)
    assert "secret" not in dumped and "leak-me" not in dumped


def test_phase_precedence_fatal_then_manual_then_charging() -> None:
    fatal = P.parse_state(json.dumps({"errors": [{"errorType": "x", "errorLevel": "FATAL"}],
                                      "operatingMode": "MANUAL", "driving": True}))
    assert fatal.phase == profiles.PHASE_ERROR                      # FATAL wins
    manual = P.parse_state(json.dumps({"operatingMode": "SERVICE", "driving": True}))
    assert manual.phase == profiles.PHASE_MAINTENANCE               # manual mode over driving
    charging = P.parse_state(json.dumps({"operatingMode": "AUTOMATIC",
                                         "batteryState": {"charging": True}}))
    assert charging.phase == profiles.PHASE_CHARGING
    driving = P.parse_state(json.dumps({"operatingMode": "AUTOMATIC", "driving": True}))
    assert driving.phase == profiles.PHASE_ACTIVE
    idle = P.parse_state(json.dumps({"operatingMode": "AUTOMATIC", "driving": False}))
    assert idle.phase == profiles.PHASE_IDLE


def test_velocity_magnitudes_for_confirmed_stop() -> None:
    s = P.parse_state(json.dumps({"velocity": {"vx": 3.0, "vy": 4.0, "omega": -0.5},
                                  "driving": True}))
    assert s.velocity == (5.0, 0.5)  # hypot(3,4)=5, |omega|
    assert s.driving is True


def test_malformed_state_degrades_to_unknown() -> None:
    s = P.parse_state(b"\xff not json")
    assert s.position is None
    assert s.battery.percent is None and s.hardware == []


def test_binding_caches_and_maps_state() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.on_state(json.dumps({
        "agvPosition": {"x": 1.0, "y": 2.0, "theta": 0.25, "positionInitialized": True},
        "batteryState": {"batteryCharge": 61, "charging": False},
        "errors": [{"errorType": "estopButton", "errorLevel": "FATAL"}],
        "operatingMode": "AUTOMATIC",
    }))
    assert b.pose("amr-1") == (1.0, 2.0, 0)
    assert b.yaw("amr-1") == 0.25
    assert b.battery_percent("amr-1") == 61
    assert b.latest_battery("amr-1").charging is False
    assert b.latest_hardware("amr-1")[0].status == profiles.STATUS_FAILED
    assert b.phase_hint("amr-1") == profiles.PHASE_ERROR


def test_binding_defaults_before_any_state() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    assert b.pose("amr-1") == (0.0, 0.0, 0)
    assert b.battery_percent("amr-1") == 0
    assert b.latest_hardware("amr-1") == []
