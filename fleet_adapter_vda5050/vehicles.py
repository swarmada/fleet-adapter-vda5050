# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""The vehicles one process serves, read from ``--vehicles-file``.

The file is YAML: a list with one entry per vehicle, each a mapping with

* ``robot_id``: the Robot's ``swarmada.io/robot-id``, the id the control plane names it by;
* ``serial``: the vehicle's VDA 5050 ``serialNumber``;
* ``manufacturer`` (optional): its VDA 5050 ``manufacturer``, by default ``swarmada``.

For example::

    - robot_id: amr-1
      serial: 200-abc123
      manufacturer: examplebots
    - robot_id: amr-2
      serial: 200-abc124
      manufacturer: examplebots

Each robot id is given once, and so is each (manufacturer, serial) pair: two entries for one
vehicle would have two cores command it. A key given twice in an entry, a key not listed here,
an empty value or a value that isn't a string is an error, so a typo stops the process at start-up
rather than leaving a vehicle unserved or served under the wrong identity.

The manufacturer and the serial are each one level of the vehicle's MQTT topics
(``uagv/v2/<manufacturer>/<serial>/...``), so each is checked as one (identity_problem): a serial
holds only the characters VDA 5050 allows in a ``serialNumber``, A-Z a-z 0-9 _ . : -, and a
manufacturer none that MQTT gives a meaning in a topic: ``/`` (which would split it, so two
vehicles could share one topic), ``+`` and ``#`` (wildcards, which a published topic never
matches), ``$`` (which begins a broker's own topics) and NUL. Neither may be empty, which would
leave a level of the topics empty (``uagv/v2//amr-1/...``)."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MANUFACTURER = "swarmada"
_KEYS = ("robot_id", "serial", "manufacturer")
# VDA 5050 3.0.0, section 4.2: the serialNumber consists of A-Z a-z 0-9 _ . : -; "/" shall not
# be used in any of the topic's fields, and "+", "#" and "$" (reserved for broker-internal topics)
# should not be. The 2.0.0 text this adapter speaks wasn't to hand; vda-5050-lib 1.7.2 checks a
# serialNumber against the same characters ("not restricted to A-Z a-z 0-9 _ . : -").
_SERIAL_CHARS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.:-")
_NOT_IN_A_TOPIC_LEVEL = ("/", "+", "#", "$", "\0")


def identity_problem(manufacturer: str, serial: str) -> str | None:
    """Why ``manufacturer`` and ``serial`` can't be a vehicle's MQTT topic levels, or None when
    they can (the module docstring gives the rule)."""
    for name, value in (("serial", serial), ("manufacturer", manufacturer)):
        if not value:
            return f"{name} is empty, which would leave the vehicle's MQTT topics a level empty"
    bad = next((ch for ch in serial if ch not in _SERIAL_CHARS), None)
    if bad is not None:
        return (f"serial {serial!r} has {bad!r}, which VDA 5050 doesn't allow in a serialNumber: "
                f"only A-Z a-z 0-9 _ . : -")
    bad = next((ch for ch in manufacturer if ch in _NOT_IN_A_TOPIC_LEVEL), None)
    if bad is not None:
        return (f"manufacturer {manufacturer!r} has {bad!r}, which can't be in an MQTT topic "
                f"level: no '/', '+', '#', '$' or NUL")
    return None


@dataclass(frozen=True)
class Vehicle:
    robot_id: str
    serial: str
    manufacturer: str = DEFAULT_MANUFACTURER


def load_vehicles(path: str) -> list[Vehicle]:
    """The vehicles listed in the file at ``path``. RAISES ValueError, naming the file and the
    entry, for a file that can't be read or parsed, and for any entry the module docstring
    rules out."""
    import yaml

    class _Loader(yaml.SafeLoader):
        """safe_load, except that a key given twice is an error: YAML keeps the last value."""

    def _mapping(loader, node, deep=False):
        keys = set()
        for k, _ in node.value:
            key = loader.construct_object(k, deep=deep)
            if key in keys:
                raise yaml.constructor.ConstructorError(
                    None, None, f"key {key!r} is given twice", k.start_mark)
            keys.add(key)
        return loader.construct_mapping(node, deep=deep)

    _Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)
    try:
        with open(path, encoding="utf-8") as f:
            doc = yaml.load(f, Loader=_Loader)  # a SafeLoader
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"vehicles file {path}: {exc}") from exc
    if not isinstance(doc, list) or not doc:
        raise ValueError(f"vehicles file {path}: not a list of vehicles")
    vehicles: list[Vehicle] = []
    ids: dict[str, int] = {}
    identities: dict[tuple[str, str], int] = {}
    for n, entry in enumerate(doc, start=1):
        where = f"vehicles file {path}, entry {n}"
        if not isinstance(entry, dict):
            raise ValueError(f"{where}: not a mapping with robot_id and serial")  # noqa: TRY004
        unknown = sorted(str(k) for k in entry if k not in _KEYS)
        if unknown:
            raise ValueError(f"{where}: unknown key {unknown[0]!r}; the keys are "
                             f"{', '.join(_KEYS)}")
        for key in ("robot_id", "serial"):
            if key not in entry:
                raise ValueError(f"{where}: no {key}")
        for key, value in entry.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{where}: {key} is {value!r}, not a non-empty string")
        v = Vehicle(robot_id=entry["robot_id"], serial=entry["serial"],
                    manufacturer=entry.get("manufacturer", DEFAULT_MANUFACTURER))
        problem = identity_problem(v.manufacturer, v.serial)
        if problem:
            raise ValueError(f"{where}: {problem}")
        if v.robot_id in ids:
            raise ValueError(f"{where}: robot_id {v.robot_id!r} is already entry "
                             f"{ids[v.robot_id]}")
        if (v.manufacturer, v.serial) in identities:
            raise ValueError(f"{where}: manufacturer {v.manufacturer!r} and serial "
                             f"{v.serial!r} are already entry "
                             f"{identities[(v.manufacturer, v.serial)]}")
        ids[v.robot_id] = n
        identities[(v.manufacturer, v.serial)] = n
        vehicles.append(v)
    return vehicles
