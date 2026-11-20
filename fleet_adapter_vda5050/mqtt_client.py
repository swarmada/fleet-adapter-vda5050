# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Thin MQTT transport for the VDA5050 comms profile.

Wraps ``paho-mqtt`` (2.x, callback API version 2) so the profile and binding stay
transport-agnostic and testable without a broker: ``paho`` is an optional dependency
imported lazily in :meth:`connect`, and the message parsing in ``profiles/`` needs
neither ``paho`` nor a live broker. Subscriptions registered before :meth:`connect` are
applied on connect, so the binding can wire its handlers at construction time.

The broker session can drop and come back: paho's network thread reconnects by itself.
Each time the broker accepts the connection (``on_connect``) every registered topic is
subscribed again, since a clean session keeps no subscriptions, and only then is the
session counted as up (:attr:`MqttClient.connected`). When it drops (``on_disconnect``)
it is counted as down at once. The binding is told of both (:meth:`MqttClient.on_session`),
so it never reports a reading from before the drop as current.

VDA5050 runs over MQTT at QoS 1 (at-least-once); the binding drops a redelivered
``state`` by its ``headerId`` and ``timestamp`` (``Vda5050Binding.on_state``).

One client can serve every vehicle of a process: each vehicle's binding subscribes its own
topics, adds its own session handler, and counts as a user from its :meth:`connect` to its
:meth:`stop`. The broker session is opened by the first user and ended by the last. Each
:meth:`publish` returns paho's own ``MQTTMessageInfo`` for that message, so one vehicle's
acknowledgement never answers for another's.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable

_Handler = Callable[[str, bytes], None]

# How long connect() waits for the broker to accept the connection before it gives up.
_CONNECT_TIMEOUT_S = 10.0
# paho's reconnect backoff after a drop, in seconds: the first retry after 1 s, then up to 10 s.
_RECONNECT_MIN_S = 1
_RECONNECT_MAX_S = 10
# paho's own limit on QoS 1 messages awaiting their PUBACK (paho-mqtt 2.1: 20). Past it, a publish
# is queued and not yet sent, so it can't be acknowledged.
PAHO_MAX_INFLIGHT = 20
# The most QoS 1 messages one vehicle can have awaiting their PUBACK at once when a stop reaches
# it, each publish being one message (vda5050_binding.py: _send_order, _send_cancel,
# _publish_action):
#   2  its last order, and the startPause or stopPause _send_order puts before it
#   1  a preemption's cancelOrder (the order it holds goes out only once that cancel has ended,
#      and a stop drops it)
#   1  a pause or a resume (startPause, stopPause)
#   1  a factsheetRequest
#   6  the stop's startPause and cancelOrder, on each of the three paths one stop can come by:
#      the SafetyStream estop, its ControlStream fence and an edge node's estop
# 11 in all. A message whose effect the vehicle has already reported (an order it drives, a
# cancel it reports ended) is taken as acknowledged: a broker acknowledges a QoS 1 publish when
# it takes it, before delivering it. So the late cancelOrder for an order still running after a
# stop (on_state) adds none: it is sent only once the vehicle reports that stop's cancelOrder
# ended, and takes its place.
_INFLIGHT_PER_VEHICLE = 11


def inflight_for(vehicles: int) -> int:
    """The in-flight limit for a client that serves ``vehicles`` vehicles: never below paho's own,
    and the most one vehicle can have in flight (_INFLIGHT_PER_VEHICLE) for each, so a whole
    fleet's stop is sent at once and none of its messages waits, queued unsent behind other
    vehicles' messages, for a PUBACK to free its place. The one place the limit is decided."""
    return max(PAHO_MAX_INFLIGHT, _INFLIGHT_PER_VEHICLE * max(int(vehicles), 1))


class MqttClient:
    """Minimal MQTT client: register topic handlers, connect, publish. The network
    loop runs on paho's own thread once connected."""

    def __init__(self, host: str = "localhost", port: int = 1883,
                 client_id: str = "swarmada-vda5050", vehicles: int = 1) -> None:
        self._host = host
        self._port = port
        self._client_id = client_id
        # The vehicles this client serves set how many messages it may have in flight.
        self._max_inflight = inflight_for(vehicles)
        self._client = None
        self._subs: dict[str, _Handler] = {}
        # Held over a topic's registration and, on a connect, over the snapshot of the topics,
        # their subscribes and the session coming up: a topic is subscribed by one or the other.
        self._lock = threading.Lock()
        self._up = False                                  # the broker accepted us; subscribed
        self._first = threading.Event()                   # the first on_connect has been seen
        self._first_failure = ""                          # ...and why it failed, if it did
        self._session_handlers: list[Callable[[bool], None]] = []
        # The bindings that connected and haven't stopped: the session ends with the last.
        self._users = 0
        self._use_lock = threading.Lock()                 # connect() and stop(), one at a time

    def subscribe(self, topic: str, handler: _Handler) -> None:
        """Register a handler for a topic; applied on the next :meth:`connect`, and again
        on every reconnect. One registered while the session is up (another vehicle's binding
        connected first) is subscribed at once.

        Under the lock :meth:`_on_connect` holds from its snapshot of the topics until the
        session is up: a topic registered while the session comes up is in that snapshot, or
        is registered once the session is up and subscribed here. Without it, a topic
        registered after the snapshot but before the session was up was subscribed by
        neither, and its vehicle heard nothing until the next reconnect."""
        with self._lock:
            self._subs[topic] = handler
            client = self._client
            if client is not None and self._up:
                client.subscribe(topic, qos=1)

    def on_session(self, handler: Callable[[bool], None]) -> None:
        """Also call ``handler(True)`` each time the broker session comes up (subscriptions
        sent), and ``handler(False)`` each time it goes down. Called on paho's thread. Each
        vehicle's binding adds its own."""
        self._session_handlers.append(handler)

    @property
    def subscriptions(self) -> tuple[str, ...]:
        """Registered topics (test seam — assertable without a broker)."""
        return tuple(self._subs)

    @property
    def connected(self) -> bool:
        """True while the broker session is up: accepted, and every topic subscribed again.
        False before the first connect, after a drop until paho reconnects, and after
        :meth:`stop`."""
        with self._lock:
            return self._up

    def connect(self) -> None:
        """Connect to the broker and start paho's network thread; return once the broker
        has accepted the connection and the topics are subscribed. RAISES RuntimeError when
        paho is missing, the broker cannot be reached, refuses the connection, or does not
        answer within the timeout: returning normally is the binding's evidence that it is
        live (``RobotBinding.start``). Counts the caller as a user until its :meth:`stop`; for
        a user after the first, the session is already open, and it returns at once."""
        with self._use_lock:
            if self._client is not None:
                self._users += 1
                return
            self._open()
            self._users = 1

    def _open(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError(
                "MqttClient.connect() requires paho-mqtt and an MQTT broker. Use "
                "--comms simulated for a $0 run, or install paho-mqtt and point "
                "--mqtt-host at your VDA5050 broker."
            ) from exc
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self._client_id)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=_RECONNECT_MIN_S, max_delay=_RECONNECT_MAX_S)
        client.max_inflight_messages_set(self._max_inflight)
        self._first.clear()
        self._first_failure = ""
        try:
            client.connect(self._host, self._port)
        except OSError as exc:
            raise RuntimeError(f"MQTT broker {self._host}:{self._port} cannot be reached: "
                               f"{exc}") from exc
        self._client = client
        client.loop_start()
        if not self._first.wait(_CONNECT_TIMEOUT_S) or self._first_failure:
            why = self._first_failure or f"no answer within {_CONNECT_TIMEOUT_S:g} s"
            self._close()
            raise RuntimeError(f"MQTT broker {self._host}:{self._port} did not accept the "
                               f"connection: {why}")

    def publish(self, topic: str, payload: str):
        """Publish at QoS 1, and return paho's ``MQTTMessageInfo``: it reads as published
        once the broker has acknowledged the message (PUBACK)."""
        if self._client is None:
            raise RuntimeError("MqttClient is not connected; call connect() first.")
        return self._client.publish(topic, payload, qos=1)

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties) -> None:
        """The broker answered a connect: the first one, or paho's reconnect after a drop.
        On success every topic is subscribed again, then the session counts as up."""
        if getattr(reason_code, "is_failure", False):
            self._set(False)
            if not self._first.is_set():
                self._first_failure = str(reason_code)
                self._first.set()
            else:
                print(f"vda5050: MQTT broker refused the reconnect: {reason_code}",
                      file=sys.stderr)
            return
        with self._lock:  # with subscribe(): no topic registered meanwhile is missed
            for topic in list(self._subs):
                client.subscribe(topic, qos=1)  # at-least-once; a clean session kept none
            changed, self._up = not self._up, True
        reconnect = self._first.is_set()
        if changed:
            self._tell(True)
        self._first.set()
        if reconnect:
            print("vda5050: MQTT broker session is back; topics subscribed again",
                  file=sys.stderr)

    def _on_disconnect(self, _client, _userdata, _flags, reason_code, _properties) -> None:
        """The broker session dropped (paho reconnects by itself) or :meth:`stop` ended it."""
        was_up = self.connected
        self._set(False)
        if was_up and self._client is not None:
            print(f"vda5050: MQTT broker session lost ({reason_code}); reconnecting",
                  file=sys.stderr)

    def _set(self, up: bool) -> None:
        with self._lock:
            changed, self._up = self._up != up, up
        if changed:
            self._tell(up)

    def _tell(self, up: bool) -> None:
        """Each binding's session handler, outside the lock."""
        for handler in list(self._session_handlers):
            handler(up)

    def _on_message(self, _client, _userdata, msg) -> None:
        handler = self._subs.get(msg.topic)
        if handler is not None:
            handler(msg.topic, msg.payload)

    def stop(self) -> None:
        """Release the caller's use. The last user's stop disconnects and stops paho's network
        thread; an earlier one leaves the session to the others. A no-op with no user, so a
        binding that never connected, or already stopped, can call it safely."""
        with self._use_lock:
            if self._users == 0:
                return
            self._users -= 1
            if self._users:
                return
            self._close()

    @property
    def users(self) -> int:
        """The bindings that connected and haven't stopped (test seam)."""
        with self._use_lock:
            return self._users

    def _close(self) -> None:
        client, self._client = self._client, None
        self._set(False)
        if client is not None:
            client.disconnect()
            client.loop_stop()
