# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 connection handling + comms-profile seam (sub-item 1).

Pure: no MQTT broker and no paho install — the profile parses the `connection`
message JSON and builds topics; the binding caches the state. This is what maps to
the adapter's online/registerable notion ahead of the C1 handshake to the control
plane.
"""

from __future__ import annotations

import json

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.profiles import get_profile
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding


# ── comms-profile seam ────────────────────────────────────────────────────────

def test_get_profile_returns_vda5050() -> None:
    p = get_profile("vda5050")
    assert p.name == "vda5050"
    assert isinstance(p, profiles.CommsProfile)  # runtime-checkable Protocol


def test_unknown_profile_rejected() -> None:
    # iso21423 is a planned follow-on; until registered it is an explicit error,
    # never a silent fallback.
    try:
        get_profile("iso21423")
    except ValueError as exc:
        assert "iso21423" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for an unregistered profile")


# ── VDA5050 topic layout ──────────────────────────────────────────────────────

def test_topic_layout_matches_vda5050_spec() -> None:
    t = Vda5050Profile().topics(manufacturer="acme", serial="agv-7")
    # {interfaceName}/v{majorVersion}/{manufacturer}/{serialNumber}/{topic}
    assert t.connection == "uagv/v2/acme/agv-7/connection"
    assert t.state == "uagv/v2/acme/agv-7/state"
    assert t.order == "uagv/v2/acme/agv-7/order"
    assert t.instant_actions == "uagv/v2/acme/agv-7/instantActions"


# ── connection parse (fail-safe) ──────────────────────────────────────────────

def test_parse_connection_states() -> None:
    p = Vda5050Profile()
    assert p.parse_connection(json.dumps({"connectionState": "ONLINE"})) == profiles.CONN_ONLINE
    assert p.parse_connection(json.dumps({"connectionState": "OFFLINE"})) == profiles.CONN_OFFLINE
    assert p.parse_connection(
        json.dumps({"connectionState": "CONNECTIONBROKEN"})) == profiles.CONN_BROKEN


def test_parse_connection_is_fail_safe() -> None:
    # Malformed / unknown → CONNECTIONBROKEN, never a false ONLINE.
    p = Vda5050Profile()
    assert p.parse_connection(b"\xff not json") == profiles.CONN_BROKEN
    assert p.parse_connection(json.dumps({"connectionState": "BOGUS"})) == profiles.CONN_BROKEN
    assert p.parse_connection(json.dumps({"no": "state"})) == profiles.CONN_BROKEN


# ── binding caches connection state ───────────────────────────────────────────

def test_binding_tracks_online_offline() -> None:
    b = Vda5050Binding(robot_id="amr-1", manufacturer="acme", serial="agv-7")
    assert b.is_online("amr-1") is False  # OFFLINE until the robot reports in
    b.on_connection(json.dumps({"connectionState": "ONLINE"}))
    assert b.is_online("amr-1") is True
    b.on_connection(json.dumps({"connectionState": "CONNECTIONBROKEN"}))
    assert b.is_online("amr-1") is False
    assert b.connection_state("amr-1") == profiles.CONN_BROKEN
