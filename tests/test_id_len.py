# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The orderId against the vehicle's factsheet limit (protocolLimits.maxStringLens.idLen).

orderIds carry the robot id, a token unique to the adapter process and the order's number. A
vehicle that takes shorter ids rejects every order, and each action then ends FAILED. When a
factsheet arrives, and on each order sent, the binding checks the orderId against the limit
and logs an error naming both lengths, once for each limit."""

from __future__ import annotations

import json

from conftest import mark_transport_live

from fleet_adapter_vda5050 import profiles
from fleet_adapter_vda5050.vda5050_binding import Vda5050Binding

ROBOT = "amr-1"
FIRST = f"{ROBOT}-b0-order-1"
SHORT = len(FIRST) - 1  # a limit one character short of the first orderId


def _factsheet(id_len) -> str:
    return json.dumps({"protocolLimits": {"maxStringLens": {"idLen": id_len}}})


def _binding() -> Vda5050Binding:
    return mark_transport_live(Vda5050Binding(robot_id=ROBOT, boot="b0"))


def _errors(capsys) -> list[str]:
    return [line for line in capsys.readouterr().err.splitlines() if "idLen" in line]


def test_a_factsheet_limit_shorter_than_the_order_id_is_logged_with_both_lengths(capsys) -> None:
    b = _binding()
    b.on_factsheet(_factsheet(SHORT))
    (line,) = _errors(capsys)
    assert f"limits ids to {SHORT} characters" in line
    assert f"orderId {FIRST} has {len(FIRST)}" in line
    assert "the vehicle will reject its orders" in line


def test_a_limit_the_order_id_fits_is_not_logged(capsys) -> None:
    b = _binding()
    b.on_factsheet(_factsheet(len(FIRST)))
    b.on_factsheet(_factsheet(64))
    assert _errors(capsys) == []


def test_no_limit_in_the_factsheet_is_not_logged(capsys) -> None:
    b = _binding()
    b.on_factsheet(json.dumps({"typeSpecification": {"seriesName": "x"}}))
    b.on_factsheet(_factsheet(0))
    assert _errors(capsys) == []


def test_each_limit_is_logged_once(capsys) -> None:
    """A vehicle republishes its factsheet; the error is not repeated for the same limit."""
    b = _binding()
    b.on_factsheet(_factsheet(SHORT))
    b.on_factsheet(_factsheet(SHORT))
    b.command_move(ROBOT, 1.0, 0.0)
    assert len(_errors(capsys)) == 1
    b.on_factsheet(_factsheet(SHORT - 2))  # a different limit: said again
    assert len(_errors(capsys)) == 1


def test_an_order_id_that_grows_past_the_limit_is_logged_when_it_is_sent(capsys) -> None:
    """The id grows a digit at order 10: fits at startup, over the limit then."""
    b = _binding()
    b.on_factsheet(_factsheet(len(FIRST)))
    for _ in range(9):
        b.command_move(ROBOT, 1.0, 0.0)
    assert _errors(capsys) == []
    b.command_move(ROBOT, 1.0, 0.0)  # amr-1-b0-order-10: one character more
    (line,) = _errors(capsys)
    assert f"orderId {ROBOT}-b0-order-10 has {len(FIRST) + 1}" in line


def test_the_order_still_goes_out(capsys) -> None:
    """Logged, not refused: the vehicle's rejection ends the action FAILED with its reason."""
    b = _binding()
    b.on_factsheet(_factsheet(SHORT))
    b.command_move(ROBOT, 1.0, 0.0)
    assert [json.loads(p)["orderId"] for t, p in b.published if t.endswith("/order")] == [FIRST]


def test_the_check_uses_the_default_token_length(capsys) -> None:
    """The real token is 8 hex characters: robot id + 16 + the order's number."""
    b = mark_transport_live(Vda5050Binding(robot_id=ROBOT))
    b.on_factsheet(_factsheet(len(ROBOT) + 16))  # one short of the first orderId
    (line,) = _errors(capsys)
    assert f"has {len(ROBOT) + 17}" in line


def test_id_len_parse() -> None:
    parse = profiles.get_profile("vda5050").parse_factsheet
    assert parse(_factsheet(32)).id_len == 32
    for bad in (0, -1, True, 2.5, "32", None):
        assert parse(_factsheet(bad)).id_len is None
    assert parse(json.dumps({"protocolLimits": {"maxStringLens": "junk"}})).id_len is None
    assert parse(json.dumps({"protocolLimits": []})).id_len is None
    assert parse("not json").id_len is None
