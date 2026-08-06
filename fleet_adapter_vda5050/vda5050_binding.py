# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 robot binding — the VDA5050 CLASS behind the ``RobotBinding`` seam.

Wraps an MQTT transport + a ``CommsProfile`` (VDA5050 first; ISO 21423 later via the
same profile seam — see profiles/). Constructible without MQTT so the message
mapping is unit-testable at $0; :meth:`start` connects the broker (requires
``paho-mqtt``). Select it on the CLI with ``--comms vda5050``.

Subscribes to and parses ``connection`` (online/offline) and ``state`` (telemetry +
order progress, latest-wins); publishes ``order`` (task dispatch), a ``cancelOrder``
instantAction (confirmed-stop safe-hold), and ``startPause`` / ``stopPause``
instantActions (pause / resume the active order). ``is_stopped`` is CONFIRMED from
``state`` (not driving + velocity ≈ 0), never inferred — the binding is
safety-complete.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone

from fleet_adapter_vda5050 import profiles, task
from fleet_adapter_vda5050.mqtt_client import MqttClient


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# A robot is CONFIRMED at rest when its reported velocity is within sensor noise and
# it is not driving. Ground truth from `state` only — never a timer or silence (C5.3).
_EPS_LINEAR = 0.01   # m/s
_EPS_ANGULAR = 0.01  # rad/s


class Vda5050Binding:
    """VDA5050 implementation of ``RobotBinding`` (see robot.py)."""

    def __init__(self, robot_id: str = "robot-conformance-1",
                 manufacturer: str = "swarmada", serial: str = "amr-1",
                 profile_name: str = "vda5050",
                 mqtt_host: str = "localhost", mqtt_port: int = 1883) -> None:
        self._robot_id = robot_id
        self._manufacturer = manufacturer
        self._serial = serial
        self._profile = profiles.get_profile(profile_name)
        self._topics = self._profile.topics(manufacturer, serial)
        self._lock = threading.Lock()
        self._connection = profiles.CONN_OFFLINE  # until the robot reports ONLINE
        self._state = profiles.StateSnapshot()    # latest parsed telemetry
        self._factsheet = profiles.FactsheetInfo() # latest parsed factsheet (capabilities)
        self._header_id = 0                        # monotonic per-topic dedup key
        self._order_seq = 0                        # generates a unique orderId per task
        self._current_order_id = ""
        self._pending_order: str | None = None     # last order JSON built (test seam)
        self._last_action: str | None = None       # last instantAction JSON built (test seam)
        self._action_seq = 0                       # generates a unique actionId per stop
        self._has_state = False                    # True once a state message has arrived
        self._holding = False                      # a stop has been commanded; hold until a new order
        self._started = False                      # True once the broker is connected
        self._mqtt = MqttClient(mqtt_host, mqtt_port, client_id=f"swarmada-{robot_id}")

    # ── robot-side message callbacks (also the test entry points) ─────────────

    def on_connection(self, payload) -> None:
        state = self._profile.parse_connection(payload)
        with self._lock:
            self._connection = state

    def connection_state(self, robot_id: str) -> str:
        with self._lock:
            return self._connection

    def is_online(self, robot_id: str) -> bool:
        return self.connection_state(robot_id) == profiles.CONN_ONLINE

    def on_state(self, payload) -> None:
        snapshot = self._profile.parse_state(payload)  # latest-wins telemetry
        with self._lock:
            self._state = snapshot
            self._has_state = True

    def on_factsheet(self, payload) -> None:
        info = self._profile.parse_factsheet(payload)  # capabilities self-description
        with self._lock:
            self._factsheet = info

    def factsheet(self, robot_id: str) -> profiles.FactsheetInfo:
        """The latest parsed ``factsheet`` (the AGV's capability self-description)."""
        with self._lock:
            return self._factsheet

    def request_factsheet(self, robot_id: str) -> None:
        """Publish a VDA5050 ``factsheetRequest`` instantAction — asks the AGV to
        publish its ``factsheet``, which then arrives via :meth:`on_factsheet`."""
        self._publish_action(robot_id, profiles.ACTION_FACTSHEET_REQUEST, "factsheet")

    # ── RobotBinding: telemetry accessors (sub-item 2) ────────────────────────

    def spawn(self, robot_id: str) -> None:
        self._robot_id = robot_id

    def pose(self, robot_id: str) -> tuple[float, float, int]:
        with self._lock:
            p = self._state.position
        return (p.x, p.y, p.floor) if p is not None else (0.0, 0.0, 0)

    def yaw(self, robot_id: str) -> float:
        with self._lock:
            return self._state.position.yaw if self._state.position is not None else 0.0

    def battery_percent(self, robot_id: str) -> int:
        with self._lock:
            pct = self._state.battery.percent
        return pct if pct is not None else 0

    def latest_battery(self, robot_id: str) -> profiles.Battery:
        with self._lock:
            return self._state.battery

    def latest_hardware(self, robot_id: str) -> list[profiles.Hardware]:
        with self._lock:
            return list(self._state.hardware)

    def phase_hint(self, robot_id: str) -> str:
        with self._lock:
            return self._state.phase

    def operating_mode(self, robot_id: str) -> str:
        with self._lock:
            return self._state.operating_mode

    # ── RobotBinding: motion / safety ─────────────────────────────────────────

    def command_move(self, robot_id: str, x: float, y: float, yaw: float = 0.0) -> None:
        """Build a VDA5050 order for the target and publish it (when connected).
        Without a broker the order is only cached, so the encoding is testable at $0."""
        with self._lock:
            start_pos, map_id = self._state.position, self._state.map_id
            self._order_seq += 1
            order_id = f"{robot_id}-order-{self._order_seq}"
            self._current_order_id = order_id
            self._header_id += 1
            header_id = self._header_id
            self._holding = False  # a new order releases a prior stop-hold
        start = (profiles.OrderTarget(x=start_pos.x, y=start_pos.y, theta=start_pos.yaw,
                                      map_id=map_id) if start_pos is not None else None)
        goal = profiles.OrderTarget(x=x, y=y, theta=yaw, map_id=map_id)
        payload = self._profile.encode_order(
            header_id=header_id, order_id=order_id, order_update_id=0,
            manufacturer=self._manufacturer, serial=self._serial,
            start=start, goal=goal, timestamp=_now_iso())
        with self._lock:
            self._pending_order = payload
        if self._started:  # pragma: no cover — requires paho + a broker
            self._mqtt.publish(self._topics.order, payload)

    def pending_order(self, robot_id: str) -> str | None:
        """The last order JSON built by :meth:`command_move` (test seam)."""
        with self._lock:
            return self._pending_order

    def task_state(self, robot_id: str) -> str:
        """The protocol TaskState (name) for the current order, derived from the
        latest robot ``state`` — the task-status stream source (C6.4)."""
        with self._lock:
            snapshot, order_id = self._state, self._current_order_id
        return task.task_state_from_snapshot(snapshot, order_id)

    def command_stop(self, robot_id: str) -> None:
        """Publish a VDA5050 ``cancelOrder`` instantAction (abort the active order +
        hold). This is a REQUEST — confirmation that the robot is at rest comes only
        from :meth:`is_stopped` (state). Shared by confirmed estop (C5) and lease
        self-stop (C4). NB: never ``stopPause``, which would RESUME motion."""
        with self._lock:
            self._current_order_id = ""
            self._pending_order = None
            self._holding = True
            self._action_seq += 1
            self._header_id += 1
            header_id, action_id = self._header_id, f"{robot_id}-stop-{self._action_seq}"
        payload = self._profile.encode_instant_action(
            header_id=header_id, action_type=profiles.ACTION_CANCEL_ORDER,
            action_id=action_id, manufacturer=self._manufacturer, serial=self._serial,
            timestamp=_now_iso())
        with self._lock:
            self._last_action = payload
        if self._started:  # pragma: no cover — requires paho + a broker
            self._mqtt.publish(self._topics.instant_actions, payload)

    def command_pause(self, robot_id: str) -> None:
        """Publish a VDA5050 ``startPause`` instantAction — pauses the active order
        WITHOUT aborting it, so it can later resume. A REQUEST; the physical halt is
        confirmed only via :meth:`is_stopped` (state). Distinct from command_stop,
        which uses ``cancelOrder`` (abort + hold)."""
        self._publish_action(robot_id, profiles.ACTION_START_PAUSE, "pause")

    def command_resume(self, robot_id: str) -> None:
        """Publish a VDA5050 ``stopPause`` instantAction — resumes a paused order."""
        self._publish_action(robot_id, profiles.ACTION_STOP_PAUSE, "resume")

    def _publish_action(self, robot_id: str, action_type: str, tag: str) -> None:
        with self._lock:
            self._action_seq += 1
            self._header_id += 1
            header_id, action_id = self._header_id, f"{robot_id}-{tag}-{self._action_seq}"
        payload = self._profile.encode_instant_action(
            header_id=header_id, action_type=action_type,
            action_id=action_id, manufacturer=self._manufacturer, serial=self._serial,
            timestamp=_now_iso())
        with self._lock:
            self._last_action = payload
        if self._started:  # pragma: no cover — requires paho + a broker
            self._mqtt.publish(self._topics.instant_actions, payload)

    def last_action(self, robot_id: str) -> str | None:
        """The last instantAction JSON built by :meth:`command_stop` / pause / resume
        (test seam)."""
        with self._lock:
            return self._last_action

    def is_stopped(self, robot_id: str) -> bool:
        """CONFIRMED at rest: a ``state`` has been observed, the robot reports it is
        not driving, and the reported velocity is within sensor noise. Returns False
        until a state arrives — rest is never inferred from the stop command, a timer,
        or the absence of a message (C5.3)."""
        with self._lock:
            if not self._has_state:
                return False
            driving = self._state.driving
            linear, angular = self._state.velocity
        return (not driving) and linear <= _EPS_LINEAR and angular <= _EPS_ANGULAR

    def tick(self, dt: float) -> None:
        pass  # paho runs its own network loop; nothing to advance here.

    # ── MQTT runtime ──────────────────────────────────────────────────────────

    def start(self) -> None:
        """Subscribe to the robot's topics and connect to the broker. Requires
        paho-mqtt (optional dep) and a reachable MQTT broker."""
        self._mqtt.subscribe(self._topics.connection, lambda _t, p: self.on_connection(p))
        self._mqtt.subscribe(self._topics.state, lambda _t, p: self.on_state(p))
        self._mqtt.subscribe(self._topics.factsheet, lambda _t, p: self.on_factsheet(p))
        self._mqtt.connect()  # requires paho-mqtt + an MQTT broker
        self._started = True  # orders now publish; before this they are cached only
