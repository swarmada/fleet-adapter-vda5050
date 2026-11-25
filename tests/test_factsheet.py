# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 factsheet → capabilities (beyond C1-C8): parse the AGV's `factsheet`
self-description and drive a `factsheetRequest` instantAction. Pure — no broker."""

from __future__ import annotations

import json

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.profiles.vda5050 import Vda5050Profile
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

P = Vda5050Profile()


def _factsheet(**overrides) -> str:
    doc = {
        "manufacturer": "acme",
        "serialNumber": "agv-7",
        "typeSpecification": {"seriesName": "examplebots-250", "agvClass": "CARRIER"},
        "protocolFeatures": {"agvActions": [
            {"actionType": "pick"}, {"actionType": "drop"}, {"actionType": "startPause"}]},
    }
    doc.update(overrides)
    return json.dumps(doc)


def test_parse_factsheet_extracts_type_and_actions() -> None:
    info = P.parse_factsheet(_factsheet())
    assert (info.manufacturer, info.serial) == ("acme", "agv-7")
    assert (info.series, info.agv_class) == ("examplebots-250", "CARRIER")
    assert info.actions == ["pick", "drop", "startPause"]


def test_parse_factsheet_dedups_and_skips_garbage_actions() -> None:
    payload = json.dumps({"protocolFeatures": {"agvActions": [
        {"actionType": "pick"}, {"actionType": "pick"}, {"noType": 1}, "junk"]}})
    assert P.parse_factsheet(payload).actions == ["pick"]


def test_parse_factsheet_malformed_is_empty() -> None:
    # A malformed factsheet degrades to empty (unknown), never a false-capable reading.
    assert P.parse_factsheet(b"\xff not json") == profiles.FactsheetInfo()


def test_request_factsheet_publishes_factsheet_request() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    b.request_factsheet("amr-1")
    action = json.loads(b.last_action("amr-1"))
    assert action["actions"][0]["actionType"] == profiles.ACTION_FACTSHEET_REQUEST


def test_on_factsheet_stores_parsed_capabilities() -> None:
    b = Vda5050Binding(robot_id="amr-1")
    assert b.factsheet("amr-1").actions == []      # nothing until the AGV reports
    b.on_factsheet(_factsheet())
    fs = b.factsheet("amr-1")
    assert fs.series == "examplebots-250" and "pick" in fs.actions
