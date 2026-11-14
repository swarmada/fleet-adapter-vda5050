# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""VDA5050 robot binding — the VDA5050 CLASS behind the ``RobotBinding`` seam.

Wraps an MQTT transport + a ``CommsProfile`` (VDA5050 first; ISO 21423 later via the
same profile seam — see profiles/). Constructible without MQTT so the message
mapping is unit-testable at $0; :meth:`start` connects the broker (requires
``paho-mqtt``), :meth:`stop` tears it down, and :meth:`transport_alive` reports whether
the robot can still be OBSERVED. Those three are the ``RobotBinding`` lifecycle;
the adapter drives them from ``bring_up_robot()`` and asks the predicate
before every telemetry publish. Select it on the CLI with ``--comms vda5050``.

Subscribes to and parses ``connection`` (online/offline) and ``state`` (telemetry +
order progress, latest-wins); publishes ``order`` (task dispatch), a ``cancelOrder``
instantAction (confirmed-stop safe-hold), and ``startPause`` / ``stopPause``
instantActions (pause / resume the active order). ``is_stopped`` is CONFIRMED from
``state`` (not driving + velocity ≈ 0), never inferred — the binding is
safety-complete.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime

from fleet_adapter_vda5050 import profiles, task
from fleet_adapter_vda5050.mqtt_client import MqttClient


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


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
        self._last_order_id = ""                   # the last order published; a stop does not clear it
        self._pending_order: str | None = None     # last order JSON built (test seam)
        self._last_action: str | None = None       # last instantAction JSON built (test seam)
        self._action_seq = 0                       # generates a unique actionId per stop
        self._has_state = False                    # True once a state message has arrived
        self._holding = False                      # a stop has been commanded; hold until a new order
        self._estop_paused = False                 # an estop paused the vehicle; stopPause before the next order
        self._suppressed_stops = 0                 # cancelOrders not sent: nothing to cancel (test seam)
        self._started = False                      # True once the broker is connected
        self._never_started_raised = False          # tick()'s never-started diagnosis is one-shot
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
        """The robot's phase as IT last reported it, or ``""`` (unknown) when this
        binding cannot observe it.

        The empty string is the lifecycle repair and it is not cosmetic. `StateSnapshot`
        defaults `phase` to `IDLE`, so an un-started binding — no subscription, no state
        message, ever — answered `'IDLE'`: it affirmatively reported the robot as parked
        and available while its own `connection_state()` said `OFFLINE`. That is worse
        than the ros2 adapter's inert binding, which was merely uninformative. A phase
        nobody has observed is unknown, and unknown has to be sayable."""
        if not self.transport_alive():
            return ""
        with self._lock:
            return self._state.phase

    def operating_mode(self, robot_id: str) -> str:
        with self._lock:
            return self._state.operating_mode

    # ── RobotBinding: motion / safety ─────────────────────────────────────────

    def command_move(self, robot_id: str, x: float, y: float, yaw: float = 0.0) -> None:
        """Build a VDA5050 order for the target and publish it (when connected).
        Without a broker the order is only cached, so the encoding is testable at $0.

        After an estop the vehicle is held in pause mode (:meth:`command_estop`). The first
        order after it is preceded by a ``stopPause``. On a contract 1.4.0 connection the
        adapter sends an order only once the stop is cleared (the stop latch refuses work until
        then), so lifting the pause here is the clear taking effect. Below 1.4.0 there is no
        latch: the next order the control plane sends lifts it, as the order after a
        ``cancelOrder`` always drove."""
        with self._lock:
            unpause = self._estop_paused
            self._estop_paused = False
        if unpause:
            self._publish_action(robot_id, profiles.ACTION_STOP_PAUSE, "resume")
        with self._lock:
            start_pos, map_id = self._state.position, self._state.map_id
            self._order_seq += 1
            order_id = f"{robot_id}-order-{self._order_seq}"
            self._current_order_id = order_id
            self._last_order_id = order_id
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
        self-stop (C4). NB: never ``stopPause``, which would RESUME motion.

        Not sent when there is nothing to cancel (:meth:`_nothing_to_cancel_locked`): the
        vehicle would answer it with a ``noOrderToCancel`` warning, which the adapter reports
        as a degraded component. The hold is taken either way. An estop always sends it
        (:meth:`command_estop`)."""
        self._cancel_order(robot_id, always=False)

    def _cancel_order(self, robot_id: str, *, always: bool) -> None:
        with self._lock:
            if not always and self._nothing_to_cancel_locked():
                self._current_order_id = ""
                self._pending_order = None
                self._holding = True
                self._suppressed_stops += 1
                return
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

    def command_estop(self, robot_id: str) -> None:
        """An emergency stop: ``startPause``, then ``cancelOrder``.

        VDA 5050 lets ``cancelOrder`` stop "immediately or on the next node", so on its own a
        stop could still drive on to the next node. ``startPause`` stops driving without
        reaching the next node. Sending it first halts the vehicle at once; the ``cancelOrder``
        then ends the order, so the interrupted work cannot be continued by a ``stopPause``.
        The vehicle stays paused until the next order (:meth:`command_move`).

        Never pause alone: a paused order is work the vehicle can resume. A REQUEST, as
        :meth:`command_stop` is; rest is confirmed only by :meth:`is_stopped`."""
        with self._lock:
            self._estop_paused = True
        self._publish_action(robot_id, profiles.ACTION_START_PAUSE, "estop-pause")
        # Always, even when the last state shows nothing to cancel: that state can be up to a
        # publish interval old, and a paused order the cancel missed would be resumed by the
        # next stopPause. A noOrderToCancel warning is the price of a stop.
        self._cancel_order(robot_id, always=True)

    def _nothing_to_cancel_locked(self) -> bool:
        """True only when a ``cancelOrder`` can change nothing, read from the vehicle's own
        ``state``. The caller holds the lock. All of these must hold:

        * the vehicle can be observed (broker session up, the robot ONLINE) and has reported
          a ``state``;
        * that state is at rest (not driving, velocity within noise) -- never a skip while
          the vehicle may be moving;
        * it lists no node still to be reached, so no order is in progress on it, and no
          action that has not ended (an action can run, or wait to resume, at rest on the
          last node, and a cancelOrder cancels it);
        * and no order this binding published is still outstanding: none was ever sent, or
          the vehicle reports the last one sent, ended. A stop does not end it here: an order
          is outstanding until the vehicle says so, so a cancelOrder that crossed the order on
          the way does not make a later stop look unnecessary."""
        if not (self._started and self._connection == profiles.CONN_ONLINE and self._has_state):
            return False
        st = self._state
        linear, angular = st.velocity
        at_rest = (not st.driving) and linear <= _EPS_LINEAR and angular <= _EPS_ANGULAR
        if not at_rest or st.active_nodes or st.active_actions:
            return False
        return not self._last_order_id or st.order_id == self._last_order_id

    def suppressed_stops(self, robot_id: str) -> int:
        """cancelOrders not sent because there was nothing to cancel (test seam)."""
        with self._lock:
            return self._suppressed_stops

    def command_pause(self, robot_id: str) -> None:
        """Publish a VDA5050 ``startPause`` instantAction — pauses the active order
        WITHOUT aborting it, so it can later resume. A REQUEST; the physical halt is
        confirmed only via :meth:`is_stopped` (state). Distinct from command_stop,
        which uses ``cancelOrder`` (abort + hold)."""
        self._publish_action(robot_id, profiles.ACTION_START_PAUSE, "pause")

    def command_resume(self, robot_id: str) -> None:
        """Publish a VDA5050 ``stopPause`` instantAction — resumes a paused order. The adapter
        refuses a resume while a stop is latched, so this never lifts an estop's pause early."""
        with self._lock:
            self._estop_paused = False
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
        """paho runs its own network loop, so there is no model to advance -- but this is
        the binding's only guaranteed MAIN-THREAD callback (the adapter's telemetry loop
        drives it every tick), which makes it the one place a transport that was never
        brought up can be surfaced to a thread that is not itself broken.

        Raises ONCE, and only for the never-started case. A binding that started and then
        went `OFFLINE` / `CONNECTIONBROKEN` is NOT an error here: VDA5050 reports that on
        the `connection` topic as a normal, recoverable event (a robot powering down, the
        MQTT last-will firing), and `transport_alive()` already answers False for it every
        cycle. Raising would turn a robot state the protocol has a field for into an
        exception. Never-started has no field, no message and no failed operation to raise
        from, which is exactly why it needs one."""
        with self._lock:
            report = not self._started and not self._never_started_raised
            if report:
                self._never_started_raised = True
        if report:
            raise RuntimeError(
                "this Vda5050Binding was NEVER STARTED: it is subscribed to no MQTT "
                "topic, connected to no broker, and publishes no orders (they are cached "
                "only), so no `connection`, `state` or `factsheet` message has ever "
                "arrived and none ever will. Every reading it reports is a default, not a "
                "measurement -- pose() is the origin, battery_percent() is 0, and "
                "is_stopped() is False forever, which can never confirm a stop (C5.3). "
                "Nothing raised to say so because there is no failed operation to raise "
                "from. The adapter brings the transport up in adapter.bring_up_robot(); a "
                "caller that constructs a Vda5050Binding outside that path must call "
                "start() itself.")

    # ── MQTT runtime (the RobotBinding lifecycle — see robot.py) ──────────────

    def start(self) -> None:
        """Subscribe to the robot's topics and connect to the broker. Requires
        paho-mqtt (optional dep) and a reachable MQTT broker.

        RAISES if the broker cannot be reached or paho is missing — `MqttClient.connect`
        does not swallow either — and that is the contract `RobotBinding.start` states:
        returning normally is the adapter's only evidence that this binding is live.

        Driven by `adapter.bring_up_robot()` on every run. Until the lifecycle fix nothing called
        it at all, so `--comms vda5050` shipped subscribed to nothing (see robot.py)."""
        self._mqtt.subscribe(self._topics.connection, lambda _t, p: self.on_connection(p))
        self._mqtt.subscribe(self._topics.state, lambda _t, p: self.on_state(p))
        self._mqtt.subscribe(self._topics.factsheet, lambda _t, p: self.on_factsheet(p))
        self._mqtt.connect()  # requires paho-mqtt + an MQTT broker
        with self._lock:
            self._started = True  # orders now publish; before this they are cached only

    def stop(self) -> None:
        """Stop paho's network loop and disconnect. Idempotent, and safe when `start`
        was never called or raised — `MqttClient.stop` is a no-op with no client.

        This adapter had no teardown at all before the lifecycle fix, on the binding or on the
        Protocol, so a `--comms vda5050` run left paho's thread and the broker session to
        the interpreter's exit. `_started` is cleared first so that a teardown racing a
        telemetry tick makes the binding report NOT observable rather than briefly
        observable-but-disconnected."""
        with self._lock:
            self._started = False
        self._mqtt.stop()

    def transport_alive(self) -> bool:
        """True only while this binding can OBSERVE the robot: the broker session is up
        AND the robot's own `connection` topic says `ONLINE`.

        Both halves are required and they fail differently.

        * `_started` False means there is no transport AT ALL — no subscription, so the
          `connection` state is not stale, it was never fetched. This is the
          state the adapter used to ship.
        * `ONLINE` is the robot's own declaration, and VDA5050 gives it explicitly
          (`OFFLINE` on a clean disconnect, `CONNECTIONBROKEN` from the MQTT last-will).
          A broker session that is up while the robot is gone still delivers nothing, and
          `_state` then holds a FROZEN copy of the last reading rather than a measurement
          — which is the condition C6.2 is about.

        This is the signal the binding always computed (`connection_state()`,
        `is_online()`) and the adapter never read. Declaring it on `RobotBinding` is what
        obliges a caller to ask; use `connection_state()` to tell the three apart."""
        with self._lock:
            return self._started and self._connection == profiles.CONN_ONLINE

    def transport_detail(self, robot_id: str = "") -> str:
        """Why `transport_alive()` is False, for the operator-facing log. Not on the
        Protocol — the adapter degrades identically without it (see
        `_report_transport_not_live`); it only makes the reason reach a human."""
        with self._lock:
            started, conn = self._started, self._connection
        if not started:
            return ("the binding was never started: no broker session and no "
                    "subscriptions")
        return f"the robot's VDA5050 connection state is {conn}"
