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
``state`` (not driving, velocity ≈ 0, the pause acknowledged), never inferred, and
``state_seq`` counts new states, so an estop is confirmed only from a state sent after
it — the binding is safety-complete.

A new order while the vehicle still has one is a preemption: ``cancelOrder`` first, and the
new order once the vehicle reports the old one gone (:meth:`Vda5050Binding.command_move`).
"""

from __future__ import annotations

import math
import secrets
import sys
import threading
import time
from collections import deque
from datetime import UTC, datetime

from fleet_adapter_vda5050 import profiles, task
from fleet_adapter_vda5050.mqtt_client import MqttClient
from fleet_adapter_vda5050.routing import NODE_TOLERANCE_M, RouteGraph


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _vehicle_time(stamp: str) -> datetime | None:
    """A VDA5050 header timestamp (ISO 8601, e.g. 2026-11-15T10:00:00.000Z), or None when it
    cannot be read: then messages are taken in the order they arrive."""
    try:
        return datetime.fromisoformat(stamp) if stamp else None
    except ValueError:
        return None


def _before(a: datetime, b: datetime) -> bool:
    """a is earlier than b. False when the two cannot be compared (one with a time zone, one
    without): arrival order then decides."""
    try:
        return a < b
    except TypeError:
        return False


# A robot is CONFIRMED at rest when its reported velocity is within sensor noise and
# it is not driving. Ground truth from `state` only — never a timer or silence (C5.3).
_EPS_LINEAR = 0.01   # m/s
_EPS_ANGULAR = 0.01  # rad/s

# How long a preempting order waits for the vehicle to end the order before it. VDA5050 lets
# the vehicle stop "on the next node" and finish actions it cannot interrupt, and its state
# can be a publish interval old (vda-5050-lib's default is 30 s). Past this the new order is
# dropped and its action FAILED.
_PREEMPT_TIMEOUT_S = 60.0

# How many recent states a redelivery is looked for among. QoS 1 redelivers a message the
# broker has not seen acknowledged, after a reconnect: the last few, not the last hundred.
_STATE_DEDUP_WINDOW = 64
# The same for visualization, which carries only a position: a redelivery would move the
# reported position back for one telemetry frame.
_VISUALIZATION_DEDUP_WINDOW = 16

# The operatingMode values in which only an order moves the vehicle. In MANUAL, SERVICE or
# TEACHIN a person can drive it without one (VDA5050 2.0.0, 6.10.6).
_ORDER_DRIVEN_MODES = frozenset({"AUTOMATIC", "SEMIAUTOMATIC"})


class Vda5050Binding:
    """VDA5050 implementation of ``RobotBinding`` (see robot.py)."""

    def __init__(self, robot_id: str = "robot-conformance-1",
                 manufacturer: str = "swarmada", serial: str = "amr-1",
                 profile_name: str = "vda5050",
                 mqtt_host: str = "localhost", mqtt_port: int = 1883,
                 preempt_timeout_s: float = _PREEMPT_TIMEOUT_S, clock=time.monotonic,
                 boot: str | None = None, route_graph: RouteGraph | None = None) -> None:
        self._robot_id = robot_id
        self._graph = route_graph  # the lanes orders follow (routing.py); None: one straight edge
        self._manufacturer = manufacturer
        self._serial = serial
        self._profile = profiles.get_profile(profile_name)
        self._topics = self._profile.topics(manufacturer, serial)
        self._lock = threading.Lock()
        # Serialises every order and instantAction this binding publishes, with the decision
        # to send it. Taken before _lock, never after, and never held while waiting: the
        # estop's pause and cancel cannot fall either side of a held order going out.
        self._publish_lock = threading.RLock()
        self._connection = profiles.CONN_OFFLINE  # until the robot reports ONLINE
        self._state = profiles.StateSnapshot()    # latest parsed telemetry
        self._factsheet = profiles.FactsheetInfo() # latest parsed factsheet (capabilities)
        self._header_id = 0                        # monotonic per-topic dedup key
        self._order_seq = 0                        # generates a unique orderId per task
        # In every orderId, so ids are unique across adapter processes. A vehicle reports a
        # rejection until it accepts a new order; without this, a restarted adapter's first
        # order would carry the id of the previous process's first order, and a rejection the
        # vehicle still shows for that one would look like a rejection of the new one.
        self._boot = boot if boot is not None else secrets.token_hex(4)
        self._id_len_reported: int | None = None   # the idLen an over-long orderId was logged for
        self._node_id_len_reported: int | None = None  # ...and an over-long lane node id
        self._current_order_id = ""
        self._last_order_id = ""                   # the last order published; a stop does not clear it
        self._pending_order: str | None = None     # last order JSON built (test seam)
        self._last_action: str | None = None       # last instantAction JSON built (test seam)
        self._action_seq = 0                       # generates a unique actionId per stop
        self._has_state = False                    # True once a state message has arrived
        self._states_seen = 0                      # state messages received, less redeliveries
        # state_seq: states with a headerId and a timestamp, less redeliveries. Only these
        # can confirm an estop (:meth:`state_seq`).
        self._state_seq = 0
        # (headerId, timestamp) of the last _STATE_DEDUP_WINDOW counted states: a QoS 1
        # redelivery is byte for byte the state it repeats.
        self._recent_states: deque[tuple[int, str]] = deque(maxlen=_STATE_DEDUP_WINDOW)
        self._unidentified_reported = False        # a state with no headerId or timestamp, logged once
        self._repeat_reported = False              # a dropped repeat, logged once
        self._lists_missing_reported = False       # a state without nodeStates or actionStates, logged once
        self._map_mismatch_reported: set[tuple[str, str]] = set()  # (graph's, vehicle's), each logged once
        # The position sent to the control plane: the one in the latest state or visualization
        # that carried an agvPosition, None when that said the position is unknown. Only for
        # telemetry: stops, orders and tasks read the state's own position.
        self._pose: profiles.Position | None = None
        # The vehicle's timestamp of that position, when it could be read: a visualization
        # older than it is not applied (MQTT orders each topic, not the two together).
        self._pose_at: datetime | None = None
        # A state has come since the vehicle was last ONLINE: until then the battery reading
        # is from before it went away, and is not sent.
        self._battery_current = False
        # (headerId, timestamp) of recent visualizations: a redelivery is dropped.
        self._recent_visualizations: deque[tuple[int, str]] = deque(maxlen=_VISUALIZATION_DEDUP_WINDOW)
        # The actionId of this binding's latest startPause, until a stopPause goes out: its
        # FINISHED confirms the pause (:meth:`is_stopped`).
        self._start_pause_id = ""
        # A counted state that lists that startPause as ended has shown the vehicle paused
        # (paused: true, or the startPause FINISHED). Kept
        # when a later state shows the pause lifted, as a cancelOrder lifts it on
        # vda-5050-lib's virtual vehicle; cleared by the next startPause, a stopPause or an
        # order.
        self._pause_acked = False
        self._holding = False                      # a stop has been commanded; hold until a new order
        self._recancelled = False                  # this stop's one late cancel has been sent
        self._stop_cancel_id = ""                  # the cancelOrder the last stop sent
        self._stop_cancel_ended = False            # ...and the vehicle has reported it ended
        self._estop_paused = False                 # an estop paused the vehicle; stopPause before the next order
        self._pause_requested = False              # a Swarmada pause (command_pause) is in force
        self._suppressed_stops = 0                 # cancelOrders not sent: nothing to cancel (test seam)
        # A preemption in progress (command_move): the order held, the cancelOrder sent for
        # the order before it, and when the hold gives up.
        self._held: tuple[str, float, float, float] | None = None  # (orderId, x, y, yaw)
        self._held_cancel_id = ""
        self._held_deadline = 0.0
        self._open_cancel_id = ""                  # the last cancelOrder sent, if no order since
        self._open_cancel_at = 0                   # _states_seen when it was sent
        self._preempt_timeout_s = preempt_timeout_s
        self._clock = clock
        self._failed_order_id = ""                 # a held order that timed out
        self._failed_detail = ""
        self._started = False                      # True once the broker is connected
        self._never_started_raised = False          # tick()'s never-started diagnosis is one-shot
        self._mqtt = MqttClient(mqtt_host, mqtt_port, client_id=f"swarmada-{robot_id}")

    # ── robot-side message callbacks (also the test entry points) ─────────────

    def on_connection(self, payload) -> None:
        state = self._profile.parse_connection(payload)
        with self._lock:
            self._connection = state
            if state != profiles.CONN_ONLINE:
                # Away: what it last reported is not where it is when it comes back (pushed,
                # relocalised, recharged). Unknown until it reports again.
                self._pose, self._pose_at, self._battery_current = None, None, False

    def connection_state(self, robot_id: str) -> str:
        with self._lock:
            return self._connection

    def is_online(self, robot_id: str) -> bool:
        return self.connection_state(robot_id) == profiles.CONN_ONLINE

    def on_state(self, payload) -> None:
        """A ``state`` from the vehicle: stored (latest wins), counted, and then any held order
        or late cancel is seen to.

        A state seen before, by its headerId and timestamp, is a QoS 1 redelivery and is
        dropped: not stored (it is older than the current one), not counted, nothing seen to.
        A vehicle that restarts begins its headerIds again, with new timestamps, so its states
        still count. A state without an integer headerId or a timestamp cannot be told from a
        redelivery: it is stored and seen to, but not counted in :meth:`state_seq`, so it
        never confirms an estop. That is logged once.

        The state is stored before the counts rise, under one lock, so a reader that sees a
        count raised reads a state at least as new as the one that raised it (the SDK's
        ``state_seq`` contract)."""
        snapshot = self._profile.parse_state(payload)
        key = ((snapshot.header_id, snapshot.timestamp)
               if snapshot.header_id is not None and snapshot.timestamp else None)
        report = warn = False
        missing: list[str] = []
        other_map: str | None = None  # the vehicle's map, when it is not the graph's
        with self._publish_lock:
            with self._lock:
                repeat = key is not None and key in self._recent_states
                if repeat:
                    warn, self._repeat_reported = not self._repeat_reported, True
                else:
                    self._state = snapshot
                    self._has_state = True
                    self._states_seen += 1
                    self._battery_current = True
                    if snapshot.position_reported:
                        self._pose, self._pose_at = snapshot.position, _vehicle_time(snapshot.timestamp)
                    if key is not None:
                        self._recent_states.append(key)
                        self._state_seq += 1
                        # Only a state that names this startPause as ended: one the vehicle
                        # sent after it took the startPause. A paused: true from a state that
                        # crossed it could be a pause the vehicle was about to leave.
                        pid = self._start_pause_id
                        if pid and pid in snapshot.ended_actions and (
                                snapshot.paused or pid in snapshot.finished_actions):
                            self._pause_acked = True
                    elif not self._unidentified_reported:
                        self._unidentified_reported = report = True
                    graph_map = self._graph.map_id if self._graph is not None else None
                    pair = (graph_map or "", snapshot.map_id)
                    if (graph_map and snapshot.map_id and snapshot.map_id != graph_map
                            and pair not in self._map_mismatch_reported):
                        self._map_mismatch_reported.add(pair)
                        other_map = snapshot.map_id
                    if not snapshot.lists_reported and not self._lists_missing_reported:
                        self._lists_missing_reported = True
                        missing = [name for name, ok in (
                            ("nodeStates", snapshot.node_states_reported),
                            ("actionStates", snapshot.action_states_reported)) if not ok]
            if repeat:
                if warn:
                    print(f"vda5050: warning: a state with headerId {key[0]} and timestamp "
                          f"{key[1]!r} came again and was dropped. MQTT redelivers a message "
                          "after a reconnect; a vehicle that sends one headerId and timestamp "
                          "for two different states loses the second. Logged once.",
                          file=sys.stderr)
                return
            if other_map is not None:
                print(f"vda5050: error: the route graph is for map {self._graph.map_id!r} and "
                      f"the vehicle reports map {other_map!r}; orders follow the graph's lanes "
                      "in the frame the vehicle reports its position in. Logged once for each "
                      "pair of maps.", file=sys.stderr)
            if missing:
                print(f"vda5050: error: a state from the vehicle has no {' or '.join(missing)} "
                      "list (or is not a VDA 5050 state at all), so what it has left to do is "
                      "unknown. Until a state has both "
                      "lists, every stop sends its cancelOrder, an order waits behind a cancel "
                      "for a state with both (and fails when its hold times out), and no task "
                      "is marked done. Logged once.", file=sys.stderr)
            if report:
                print("vda5050: error: a state from the vehicle has no integer headerId or no "
                      "timestamp, so a redelivered state cannot be told from a new one. Such "
                      "states are used, but none confirms an emergency stop, which is then "
                      "answered FAILED. Logged once.", file=sys.stderr)
            self._service_hold()
            self._cancel_late_order()

    def on_visualization(self, payload) -> None:
        """A ``visualization`` from the vehicle: its position, for the position sent to the
        control plane (:meth:`reported_pose`) and nothing else. A vehicle may send it more
        often than its state. A redelivery, by headerId and timestamp, is dropped; one with no
        agvPosition changes nothing."""
        report = self._profile.parse_visualization(payload)
        key = ((report.header_id, report.timestamp)
               if report.header_id is not None and report.timestamp else None)
        at = _vehicle_time(report.timestamp)
        with self._lock:
            if key is not None:
                if key in self._recent_visualizations:
                    return
                self._recent_visualizations.append(key)
            if not report.reported:
                return
            if at is not None and self._pose_at is not None and _before(at, self._pose_at):
                return  # older than the position already reported (a redelivery, a retained one)
            self._pose, self._pose_at = report.position, at

    def state_seq(self, robot_id: str) -> int:
        """How many new states, each with a headerId and a timestamp, this binding has received
        from its vehicle: the SDK's ``await_estop_confirmation`` confirms a stop only from a
        state counted after the stop was sent. Never falls. 0 for a robot id that is not this
        binding's, so a stop is never confirmed for a robot it does not drive."""
        if robot_id != self._robot_id:
            return 0
        with self._lock:
            return self._state_seq

    def _cancel_late_order(self) -> None:
        """Once per stop: the vehicle has taken the order this binding stopped, after the stop's
        cancelOrder ended. Since the stop, no order has been sent and none is held; the vehicle
        has reported that cancel ended (FINISHED, FAILED, or refused by an error naming it;
        kept once seen, since a vehicle drops an ended instantAction from its state after a few
        publishes); and it now reports the last order sent as its own, with a node or an action
        left, or without a nodeStates or actionStates list (unknown is not nothing left). Sends
        a cancelOrder.

        How it happens: the cancel reached the vehicle before the order (MQTT orders one
        client's messages per topic only) and found nothing to cancel; or the vehicle rejected
        the order, then took it when it was delivered again (QoS 1, after a reconnect). The
        action is ended and its lease released, so without this the vehicle would drive an
        order nothing supervises. Not on a state that merely crossed the cancel: the cancel
        must be reported ended first. Once only: a vehicle that refuses a cancel while it runs
        the order is not answered with a cancel on every state."""
        with self._publish_lock:
            with self._lock:
                st, last = self._state, self._last_order_id
                if self._stop_cancel_id and self._stop_cancel_id in st.ended_actions:
                    self._stop_cancel_ended = True
                late = (self._holding and self._stop_cancel_ended and not self._recancelled
                        and self._held is None and bool(last) and st.order_id == last
                        and not st.nothing_left)  # a missing list: work may be left
                if late:
                    self._recancelled = True
            if late:
                print(f"vda5050: the vehicle is running order {last}, which was stopped; "
                      "cancelling it again", file=sys.stderr)
                self._send_cancel(self._robot_id)

    def _service_hold(self) -> None:
        """Send a held order whose wait is over, or time the hold out (:meth:`_take_held_locked`)."""
        with self._publish_lock:
            with self._lock:
                release = self._take_held_locked()
            if release is not None:
                self._send_order(self._robot_id, *release)

    def on_factsheet(self, payload) -> None:
        info = self._profile.parse_factsheet(payload)  # capabilities self-description
        with self._lock:
            self._factsheet = info
            self._check_id_len_locked()
            self._check_node_id_len_locked()

    def _order_id_locked(self, seq: int) -> str:
        """The orderId of order number ``seq``. The caller holds the lock."""
        return f"{self._robot_id}-{self._boot}-order-{seq}"

    def _action_id_locked(self, robot_id: str, seq: int) -> str:
        """The actionId of instantAction number ``seq``: ``robot-token-a-N``. It carries the
        process token, as an orderId does: a vehicle lists an ended action for a while, and an
        id a restarted adapter used again could look like this process's startPause FINISHED,
        which confirms a stop (:meth:`is_stopped`). Four characters shorter than an orderId at
        the same number, so a vehicle that takes this binding's orders takes its stops too.
        The caller holds the lock."""
        return f"{robot_id}-{self._boot}-a-{seq}"

    def _check_id_len_locked(self, order_id: str | None = None) -> None:
        """Log an error when the longest id this binding sends is longer than the vehicle's
        factsheet allows (``protocolLimits.maxStringLens.idLen``). The longer of two: the
        orderId (``order_id``, else the next one), and the next actionId. The orderId, unless
        far more instantActions than orders have been sent. An actionId the vehicle rejects is
        an instantAction it refuses, an emergency stop's among them; an orderId, an order.
        Checked when a factsheet arrives, and again on each order and instantAction sent,
        since the ids grow a digit as they mount. Logged once for each limit. Nothing is refused here: a rejected
        order ends its action FAILED with the vehicle's reason, and a stop the vehicle does
        not take is answered FAILED. The caller holds the lock."""
        limit = self._factsheet.id_len
        if limit is None or self._id_len_reported == limit:
            return
        if order_id is None:
            order_id = self._order_id_locked(self._order_seq + 1)
        action_id = self._action_id_locked(self._robot_id, self._action_seq + 1)
        kind, longest = max(("orderId", order_id), ("actionId", action_id),
                            key=lambda k: len(k[1]))
        if len(longest) <= limit:
            return
        self._id_len_reported = limit
        what = ("its orders" if kind == "orderId"
                else "its instantActions, an emergency stop's startPause among them")
        print(f"vda5050: error: the vehicle's factsheet limits ids to {limit} characters "
              f"(protocolLimits.maxStringLens.idLen), and {kind} {longest} has "
              f"{len(longest)}; the vehicle will reject {what}. Use a shorter robot id.",
              file=sys.stderr)

    def _check_node_id_len_locked(self) -> None:
        """Log an error when a lane node id of the route graph is longer than the vehicle's
        factsheet allows (idLen also bounds nodeId): the vehicle would reject every order routed
        through that node. Checked when a factsheet arrives, since the graph does not change;
        logged once for each limit, naming the longest id. The caller holds the lock."""
        limit = self._factsheet.id_len
        if (self._graph is None or limit is None
                or self._node_id_len_reported == limit):
            return
        longest = max(self._graph.node_ids, key=lambda n: (len(n), n))
        if len(longest) <= limit:
            return
        self._node_id_len_reported = limit
        print(f"vda5050: error: the vehicle's factsheet limits ids to {limit} characters "
              f"(protocolLimits.maxStringLens.idLen), and the route graph's node id {longest} "
              f"has {len(longest)}; the vehicle will reject orders routed through it. Use "
              f"shorter node ids in the route graph.", file=sys.stderr)

    def route_unavailable(self, robot_id: str) -> str:
        """Why no order can be routed now, or "" when one can: with a route graph, the
        vehicle's position must be known (a state with an initialised, finite
        ``agvPosition``). The order starts there; from an unknown position it would be one
        edge from the goal to itself, which a vehicle that plans its own way to the first node
        could drive straight across the floor. The adapter refuses work then,
        ROBOT_UNAVAILABLE. Without a graph, "" always."""
        if self._graph is None:
            return ""
        with self._lock:
            pos = self._state.position
        if pos is None or not (math.isfinite(pos.x) and math.isfinite(pos.y)):
            return ("the vehicle's position is unknown, and with a route graph an order starts "
                    "from it")
        return ""

    def goal_refusal(self, robot_id: str, x: float, y: float) -> str:
        """Why a goal at (x, y) cannot be routed, or "" when it can. With a route graph the
        goal must be a lane node (within routing.NODE_TOLERANCE_M of one): the order's last
        leg runs straight from the node nearest the goal, and off the lanes that leg could
        cross a rack. Without a graph every goal can be sent."""
        if self._graph is None:
            return ""
        if not (math.isfinite(x) and math.isfinite(y)):
            return f"goal ({x:g}, {y:g}) is not a position"
        node, d = self._graph.nearest(x, y)
        if d <= NODE_TOLERANCE_M:
            return ""
        return (f"goal ({x:g}, {y:g}) is not on the route graph: the nearest lane node, "
                f"{node}, is {d:.2f} m away (at most {NODE_TOLERANCE_M} m)")

    def factsheet(self, robot_id: str) -> profiles.FactsheetInfo:
        """The latest parsed ``factsheet`` (the AGV's capability self-description)."""
        with self._lock:
            return self._factsheet

    def request_factsheet(self, robot_id: str) -> None:
        """Publish a VDA5050 ``factsheetRequest`` instantAction — asks the AGV to
        publish its ``factsheet``, which then arrives via :meth:`on_factsheet`."""
        self._publish_action(robot_id, profiles.ACTION_FACTSHEET_REQUEST)

    # ── RobotBinding: telemetry accessors (sub-item 2) ────────────────────────

    def spawn(self, robot_id: str) -> None:
        self._robot_id = robot_id

    def pose(self, robot_id: str) -> tuple[float, float, int] | None:
        """(x, y, floor) last reported, from a state or a visualization; None while the
        position is unknown (none reported yet, or the vehicle not localized). Never the
        origin in place of an unknown position (C6.2)."""
        p = self.reported_pose(robot_id)
        return (p[0], p[1], p[3]) if p is not None else None

    def reported_pose(self, robot_id: str) -> tuple[float, float, float | None, int] | None:
        """(x, y, yaw, floor) last reported, read as one; yaw None when the vehicle gave no
        heading. None while the position is unknown. The position sent to the control plane."""
        with self._lock:
            p = self._pose
        return (p.x, p.y, p.yaw, p.floor) if p is not None else None

    def yaw(self, robot_id: str) -> float:
        with self._lock:
            p = self._state.position
        return p.yaw if p is not None and p.yaw is not None else 0.0

    def battery_percent(self, robot_id: str) -> int | None:
        """The state's battery charge; None while unknown, never a 0% reading (C6.2), and
        None from the time the vehicle leaves ONLINE until its next state."""
        with self._lock:
            return self._state.battery.percent if self._battery_current else None

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

        **Preemption.** VDA5050 has a vehicle reject an order with a new orderId while an order
        is active, unless it stitches onto that order's base. So when the vehicle may still
        have an order (:meth:`_order_active_locked`), this sends a ``cancelOrder`` and holds
        the new order. It goes out on the first ``state`` that shows the old order gone
        (:meth:`_take_held_locked`), from the vehicle's position then. A second order while one
        is held replaces it; the cancel already sent serves both, as does a stop's cancel still
        in progress when the order comes. A stop or an estop drops it
        (:meth:`_cancel_order`). If no such state comes within the preemption timeout, the
        order is dropped and :meth:`task_state` answers FAILED, so the action ends rather than
        running to its timeout on an orderId the vehicle never took; the adapter then commands a
        stop, which cancels whatever is left of the order before.

        After an estop the vehicle is held in pause mode (:meth:`command_estop`). The first
        order after it is preceded by a ``stopPause``, sent with the order itself (a held order
        keeps the pause until it goes out), unless a Swarmada pause is in force
        (:meth:`_send_order`). On a contract 1.4.0 connection the adapter sends an
        order only once the stop is cleared (the stop latch refuses work until then), so
        lifting the pause here is the clear taking effect. Below 1.4.0 there is no latch: the
        next order the control plane sends lifts it, as the order after a ``cancelOrder``
        always drove.

        VDA5050 2.0.0: stopPause "can also restart vehicles that were stopped with a hardware
        button that triggered startPause (if configured)", and a vehicle does not report which
        of the two paused it. So a pause pressed on the vehicle while this binding's own estop
        pause is in force is released by this stopPause, once the stop is cleared. A pause
        pressed when this binding has none in force is refused work instead
        (:meth:`paused_at_vehicle`)."""
        with self._publish_lock:
            with self._lock:
                self._order_seq += 1
                order_id = self._order_id_locked(self._order_seq)
                self._current_order_id = order_id
                held = self._held is not None
                preempt = held or self._order_active_locked()
                if preempt:
                    self._held = (order_id, x, y, yaw)
                    self._pending_order = None
                open_cancel = self._cancel_in_progress_locked()
            if not preempt:
                self._send_order(robot_id, order_id, x, y, yaw)
                return
            if held:
                return  # the cancelOrder already sent ends the order this one waits on
            cancel_id = open_cancel or self._send_cancel(robot_id)
            with self._lock:
                self._held_cancel_id = cancel_id
                self._held_deadline = self._clock() + self._preempt_timeout_s

    def _cancel_in_progress_locked(self) -> str:
        """The actionId of a stop's cancelOrder still in progress, or "". It ends the order as
        well, and a second cancel would only be refused, with a noOrderToCancel warning. In
        progress means the vehicle lists it as not ended, or no state has come since it was
        sent. Not merely "not reported ended": a vehicle drops an ended instantAction from
        its state after a few publishes, and a cancel taken as pending long after it ended
        would leave every later preemption waiting on nothing. The caller holds the lock."""
        cid = self._open_cancel_id
        if cid and (cid in self._state.active_action_ids
                    or self._states_seen == self._open_cancel_at):
            return cid
        return ""

    def _send_order(self, robot_id: str, order_id: str, x: float, y: float, yaw: float) -> None:
        """Publish the order now, with the start node at the vehicle's position now. The caller
        holds the publish lock.

        Before it: a ``stopPause`` if an estop paused the vehicle, unless a Swarmada pause is in
        force. Then the pause stays: a ``startPause`` goes first instead, since a vehicle may
        leave pause mode on a cancelOrder (vda-5050-lib's virtual AGV does), and only a
        Swarmada resume lifts that pause."""
        # The route first, from the vehicle's position now: nothing is sent or changed before
        # it, so a routing failure leaves no pause lifted and no order taken as sent.
        with self._lock:
            start_pos, map_id = self._state.position, self._state.map_id
        start = (profiles.OrderTarget(x=start_pos.x, y=start_pos.y,
                                      theta=start_pos.yaw if start_pos.yaw is not None else 0.0,
                                      map_id=map_id) if start_pos is not None else None)
        goal = profiles.OrderTarget(x=x, y=y, theta=yaw, map_id=map_id)
        via = self._via(order_id, start, goal)
        with self._lock:
            pause = self._pause_requested
            unpause = self._estop_paused
            self._estop_paused = False
        if pause:  # before the estop's stopPause: a Swarmada pause stays
            self._publish_action(robot_id, profiles.ACTION_START_PAUSE)
        elif unpause:
            self._publish_action(robot_id, profiles.ACTION_STOP_PAUSE)
        with self._lock:
            self._check_id_len_locked(order_id)
            self._last_order_id = order_id
            # The vehicle has work again: a new stop needs a new pause. Today a stopPause or a
            # startPause always goes first and clears it already; kept so no later path
            # carries a pause over an order.
            self._pause_acked = False
            self._open_cancel_id = ""
            self._header_id += 1
            header_id = self._header_id
            self._holding = False  # a new order releases a prior stop-hold
        payload = self._profile.encode_order(
            header_id=header_id, order_id=order_id, order_update_id=0,
            manufacturer=self._manufacturer, serial=self._serial,
            start=start, goal=goal, timestamp=_now_iso(), via=via)
        with self._lock:
            self._pending_order = payload
        if self._started:  # pragma: no cover — requires paho + a broker
            self._mqtt.publish(self._topics.order, payload)

    def _via(self, order_id: str, start: profiles.OrderTarget | None,
             goal: profiles.OrderTarget) -> list[profiles.OrderTarget]:
        """The lane nodes an order passes on its way (routing.py), from the vehicle's position
        now: none without a route graph. An assignment is refused while the position is unknown
        (:meth:`route_unavailable`); a held order can still go out on a state without one, and is
        then sent unrouted, with an error logged."""
        if self._graph is None:
            return []
        if start is None or not (math.isfinite(start.x) and math.isfinite(start.y)):
            print(f"vda5050: error: order {order_id} is sent without a route: the vehicle's "
                  "position is unknown", file=sys.stderr)
            return []
        route = self._graph.route(start.x, start.y, goal.x, goal.y)
        if route.off_graph_m is not None:
            first = route.waypoints[0].node_id if route.waypoints else "the goal"
            print(f"vda5050: error: order {order_id} starts {route.off_graph_m:.2f} m from the "
                  f"nearest lane, at ({start.x:g}, {start.y:g}); its first leg runs straight "
                  f"to {first}. Start and admit the vehicle on a lane node.", file=sys.stderr)
        return [profiles.OrderTarget(x=w.x, y=w.y, map_id=goal.map_id, label=w.node_id)
                for w in route.waypoints]

    def _order_active_locked(self) -> bool:
        """True when the vehicle may still have an order a new one would be rejected behind:
        its last ``state`` lists a node still to be reached or an action that has not ended,
        or leaves out its nodeStates or actionStates list (what is left is unknown), or it
        does not yet report the last order this binding sent (that order may be on its way)
        and has not reported rejecting it. The caller holds the lock. False when the vehicle
        cannot be observed or has
        reported no state: there is nothing to wait on then, and the order goes out as
        before."""
        if not (self._started and self._connection == profiles.CONN_ONLINE and self._has_state):
            return False
        st = self._state
        if not st.nothing_left:  # a node, an action not ended, or a list not reported
            return True
        return (bool(self._last_order_id) and st.order_id != self._last_order_id
                and not self._rejected_locked(self._last_order_id))

    def _rejected_locked(self, order_id: str) -> tuple[str, str] | None:
        """``(errorType, errorDescription)`` when the vehicle rejected ``order_id``, or None.
        The caller holds the lock.

        Rejected: this binding sent that order (it is the last one sent; a held order has not
        been), the vehicle's last ``state`` carries an error naming its orderId, and the vehicle
        does not report it as its own. An error naming the order the vehicle has taken is about
        running it, not a rejection. The error type is not matched, so a type the vehicle
        chose itself still counts. VDA5050 2.0.0 6.6.4 has the vehicle report a rejection until
        it accepts a new order.

        Only an ``orderId`` reference counts. A headerId alone is not attributed: header ids
        restart with this process. This binding never sends an order update (every order has
        a new orderId and orderUpdateId 0), so an orderUpdateId cannot name a rejection here."""
        if not order_id or order_id != self._last_order_id:
            return None
        st = self._state
        if st.order_id == order_id:
            return None
        return st.order_errors.get(order_id)

    def _take_held_locked(self) -> tuple[str, float, float, float] | None:
        """The held order, taken, when the last ``state`` shows the order before it gone; or
        None. The caller holds both locks. Gone means the vehicle reports the preemption's
        cancelOrder ended (FINISHED, or refused with an error naming it), with both lists
        reported, no node left, no action that has not ended, and not driving. Only the cancel's own end counts, and
        only the vehicle can report it, so no state that crossed the cancel on the way can
        release the order. MQTT orders one client's messages per topic only; were the held
        order to overtake its cancel on another topic, the cancel would end it.

        A vehicle that does not list instantActions in its state (VDA5050 2.0.0 has it list
        them) never reports the cancel ended: the hold times out and the action fails.

        Also where the hold times out, on a state or on :meth:`tick`."""
        if self._held is None:
            return None
        st = self._state
        gone = (self._held_cancel_id in st.ended_actions
                and st.nothing_left and not st.driving)
        if gone:
            held, self._held, self._held_cancel_id = self._held, None, ""
            return held
        if self._clock() >= self._held_deadline:
            order_id = self._held[0]
            self._held, self._held_cancel_id = None, ""
            self._open_cancel_id = ""  # not waited on again: the next preemption sends its own
            self._failed_order_id = order_id
            self._failed_detail = (
                f"preempted, but the vehicle did not report its previous order ended within "
                f"{self._preempt_timeout_s:g} s; order {order_id} was not sent")
            print(f"vda5050: {self._failed_detail}", file=sys.stderr)
        return None

    def held_order(self, robot_id: str) -> str:
        """The orderId of the order held for a preemption, or "" (test seam)."""
        with self._lock:
            return self._held[0] if self._held is not None else ""

    def pending_order(self, robot_id: str) -> str | None:
        """The last order JSON built by :meth:`command_move` (test seam)."""
        with self._lock:
            return self._pending_order

    def task_state(self, robot_id: str) -> str:
        """The protocol TaskState (name) for the current order, derived from the
        latest robot ``state`` — the task-status stream source (C6.4)."""
        return self.task_outcome(robot_id)[0]

    def task_detail(self, robot_id: str) -> str:
        """Why :meth:`task_state` answers FAILED for the current order, when this binding knows;
        "" otherwise."""
        return self.task_outcome(robot_id)[1]

    def task_outcome(self, robot_id: str) -> tuple[str, str]:
        """:meth:`task_state` and :meth:`task_detail` read under one lock, from one ``state``:
        read apart, a state arriving between them could give FAILED with no reason."""
        with self._lock:
            snapshot, order_id = self._state, self._current_order_id
            if order_id and order_id == self._failed_order_id:
                return task.TASK_FAILED, self._failed_detail  # a preempting order never sent
            rejected = self._rejected_locked(order_id)
            if rejected is not None:
                why = ": ".join(part for part in rejected if part)
                return task.TASK_FAILED, (f"the vehicle rejected order {order_id}"
                                          + (f": {why}" if why else ""))
        return task.task_state_from_snapshot(snapshot, order_id), ""

    def command_stop(self, robot_id: str) -> None:
        """Publish a VDA5050 ``cancelOrder`` instantAction (abort the active order +
        hold). This is a REQUEST — confirmation that the robot is at rest comes only
        from :meth:`is_stopped` (state). Shared by confirmed estop (C5) and lease
        self-stop (C4). NB: never ``stopPause``, which would RESUME motion.

        Not sent when there is nothing to cancel (:meth:`_nothing_to_cancel_locked`): the
        vehicle would answer it with a ``noOrderToCancel`` warning, which the adapter reports
        as a degraded component. The hold is taken either way. An estop always sends it
        (:meth:`command_estop`). An order held for a preemption is dropped either way."""
        self._cancel_order(robot_id, always=False)

    def _cancel_order(self, robot_id: str, *, always: bool) -> None:
        with self._publish_lock:
            with self._lock:
                self._current_order_id = ""
                self._pending_order = None
                self._holding = True
                self._recancelled = False  # a new stop: its own late cancel, if it comes to that
                self._stop_cancel_id, self._stop_cancel_ended = "", False
                self._held, self._held_cancel_id = None, ""  # a stop ends a preemption too
                if not always and self._nothing_to_cancel_locked():
                    self._suppressed_stops += 1
                    return
            cancel_id = self._send_cancel(robot_id)
            with self._lock:
                self._stop_cancel_id = cancel_id

    def _send_cancel(self, robot_id: str) -> str:
        """Publish a ``cancelOrder`` and return its actionId."""
        with self._publish_lock:
            with self._lock:
                self._action_seq += 1
                self._header_id += 1
                header_id = self._header_id
                action_id = self._action_id_locked(robot_id, self._action_seq)
                self._check_id_len_locked()
            payload = self._profile.encode_instant_action(
                header_id=header_id, action_type=profiles.ACTION_CANCEL_ORDER,
                action_id=action_id, manufacturer=self._manufacturer, serial=self._serial,
                timestamp=_now_iso())
            with self._lock:
                self._last_action = payload
                self._open_cancel_id, self._open_cancel_at = action_id, self._states_seen
            if self._started:  # pragma: no cover — requires paho + a broker
                self._mqtt.publish(self._topics.instant_actions, payload)
            return action_id

    def command_estop(self, robot_id: str) -> None:
        """An emergency stop: ``startPause``, then ``cancelOrder``.

        VDA 5050 lets ``cancelOrder`` stop "immediately or on the next node", so on its own a
        stop could still drive on to the next node. ``startPause`` stops driving without
        reaching the next node. Sending it first halts the vehicle at once; the ``cancelOrder``
        then ends the order, so the interrupted work cannot be continued by a ``stopPause``.
        The vehicle stays paused until the next order (:meth:`command_move`).

        Never pause alone: a paused order is work the vehicle can resume. A REQUEST, as
        :meth:`command_stop` is; rest is confirmed only by :meth:`is_stopped`.

        Under the publish lock, so an order held for a preemption goes out before both or not
        at all (the cancel drops it)."""
        with self._publish_lock:
            with self._lock:
                self._estop_paused = True
            self._publish_action(robot_id, profiles.ACTION_START_PAUSE)
            # Always, even when the last state shows nothing to cancel: that state can be up to
            # a publish interval old, and a paused order the cancel missed would be resumed by
            # the next stopPause. A noOrderToCancel warning is the price of a stop.
            self._cancel_order(robot_id, always=True)

    def _nothing_to_cancel_locked(self) -> bool:
        """True only when a ``cancelOrder`` can change nothing, read from the vehicle's own
        ``state``. The caller holds the lock. All of these must hold:

        * the vehicle can be observed (broker session up, the robot ONLINE) and has reported
          a ``state``;
        * that state is at rest (not driving, velocity within noise) -- never a skip while
          the vehicle may be moving;
        * it carries both lists (``StateSnapshot.nothing_left``: a list left out is unknown,
          not empty), and lists no node still to be reached, so no order is in progress on
          it, and no action that has not ended (an action can run, or wait to resume, at rest
          on the last node, and a cancelOrder cancels it);
        * and no order this binding published is still outstanding: none was ever sent, or
          the vehicle reports the last one sent, ended. A stop does not end it here: an order
          is outstanding until the vehicle says so, so a cancelOrder that crossed the order on
          the way does not make a later stop look unnecessary. Nor does a rejection: that the
          vehicle rejected an order is read from an error naming it (:meth:`_rejected_locked`),
          and VDA5050 lets any error name an order. The stop after a rejection sends its
          cancelOrder, at the price of a noOrderToCancel warning."""
        if not (self._started and self._connection == profiles.CONN_ONLINE and self._has_state):
            return False
        st = self._state
        linear, angular = st.velocity
        at_rest = (not st.driving) and linear <= _EPS_LINEAR and angular <= _EPS_ANGULAR
        if not at_rest or not st.nothing_left:  # a list not reported is not "nothing"
            return False
        return not self._last_order_id or st.order_id == self._last_order_id

    def paused_at_vehicle(self, robot_id: str) -> bool:
        """True when the vehicle reports it is paused and this binding did not pause it (no
        estop pause and no Swarmada pause in force): a pause at the vehicle, by its button, or
        one an earlier adapter process set and this one does not know of. The adapter refuses
        work then. An order would wait paused, its action RUNNING for as long as its lease is
        renewed, and this binding will not send the stopPause that releases a pause it did not
        set. Read from the last ``state`` while the vehicle can be observed; False otherwise."""
        with self._lock:
            if not (self._started and self._connection == profiles.CONN_ONLINE
                    and self._has_state):
                return False
            return self._state.paused and not (self._estop_paused or self._pause_requested)

    def suppressed_stops(self, robot_id: str) -> int:
        """cancelOrders not sent because there was nothing to cancel (test seam)."""
        with self._lock:
            return self._suppressed_stops

    def command_pause(self, robot_id: str) -> None:
        """Publish a VDA5050 ``startPause`` instantAction — pauses the active order
        WITHOUT aborting it, so it can later resume. A REQUEST; the physical halt is
        confirmed only via :meth:`is_stopped` (state). Distinct from command_stop,
        which uses ``cancelOrder`` (abort + hold)."""
        with self._lock:
            self._pause_requested = True
        self._publish_action(robot_id, profiles.ACTION_START_PAUSE)

    def command_resume(self, robot_id: str) -> None:
        """Publish a VDA5050 ``stopPause`` instantAction — resumes a paused order. The adapter
        refuses a resume while a stop is latched, so this never lifts an estop's pause early."""
        with self._lock:
            self._estop_paused = False
            self._pause_requested = False
        self._publish_action(robot_id, profiles.ACTION_STOP_PAUSE)

    def _publish_action(self, robot_id: str, action_type: str) -> None:
        with self._publish_lock:
            with self._lock:
                self._action_seq += 1
                self._header_id += 1
                header_id = self._header_id
                action_id = self._action_id_locked(robot_id, self._action_seq)
                self._check_id_len_locked()
                # Before it is sent, so its FINISHED cannot arrive first. A stopPause ends the
                # pause: an earlier startPause's FINISHED then confirms nothing.
                if action_type == profiles.ACTION_START_PAUSE:
                    self._start_pause_id, self._pause_acked = action_id, False
                elif action_type == profiles.ACTION_STOP_PAUSE:
                    self._start_pause_id, self._pause_acked = "", False
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
        """CONFIRMED at rest and paused: a ``state`` has been observed, the robot reports it is
        not driving, the reported velocity is within sensor noise, and the vehicle has
        acknowledged the pause. Returns False until a state arrives — rest is never inferred
        from the stop command, a timer, or the absence of a message (C5.3).

        The pause is acknowledged when the state reports ``paused: true``, or reports this
        binding's latest ``startPause`` FINISHED (VDA5050 2.0.0: a finished startPause means
        the vehicle "stands still" and reports paused). Every caller asks after a startPause:
        the estop's, or a Swarmada pause. A vehicle at rest between nodes has not stopped
        until it has paused, since a stopPause or its order could move it on. A startPause
        that FAILED confirms nothing by itself, and does not stand against ``paused: true``:
        a vehicle already paused refuses a second startPause and stays paused.

        Or: a counted state that lists this startPause as ended has shown the vehicle paused,
        and it now has nothing left to drive: no node, no action that has not ended, and the
        last order this binding sent is the one it reports (none in flight). A state that
        leaves out ``nodeStates`` or ``actionStates`` says nothing about what is left, and
        does not confirm by this rule. The estop's
        cancelOrder can lift the pause (vda-5050-lib's virtual vehicle lifts it when it ends
        an order), and a vehicle drops an ended instantAction from its state after a few
        publishes, so the latest state may show neither; with no order left, nothing moves
        it. Only in AUTOMATIC or SEMIAUTOMATIC operatingMode: in MANUAL, SERVICE or TEACHIN a
        person can move the vehicle without an order."""
        with self._lock:
            if not self._has_state:
                return False
            st, pause_id, acked = self._state, self._start_pause_id, self._pause_acked
            last_order = self._last_order_id
        linear, angular = st.velocity
        if st.driving or linear > _EPS_LINEAR or angular > _EPS_ANGULAR:
            return False
        if st.paused or (bool(pause_id) and pause_id in st.finished_actions):
            return True
        return (acked and st.nothing_left
                and st.operating_mode in _ORDER_DRIVEN_MODES
                and (not last_order or st.order_id == last_order))

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
        self._service_hold()  # a preemption that has waited too long times out here too
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
        if self._topics.visualization:
            self._mqtt.subscribe(self._topics.visualization,
                                 lambda _t, p: self.on_visualization(p))
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
