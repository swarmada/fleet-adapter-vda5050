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


class MqttClient:
    """Minimal MQTT client: register topic handlers, connect, publish. The network
    loop runs on paho's own thread once connected."""

    def __init__(self, host: str = "localhost", port: int = 1883,
                 client_id: str = "swarmada-vda5050") -> None:
        self._host = host
        self._port = port
        self._client_id = client_id
        self._client = None
        self._subs: dict[str, _Handler] = {}
        self._lock = threading.Lock()
        self._up = False                                  # the broker accepted us; subscribed
        self._first = threading.Event()                   # the first on_connect has been seen
        self._first_failure = ""                          # ...and why it failed, if it did
        self._session_handler: Callable[[bool], None] | None = None

    def subscribe(self, topic: str, handler: _Handler) -> None:
        """Register a handler for a topic; applied on the next :meth:`connect`, and again
        on every reconnect."""
        self._subs[topic] = handler

    def on_session(self, handler: Callable[[bool], None]) -> None:
        """Call ``handler(True)`` each time the broker session comes up (subscriptions
        sent), and ``handler(False)`` each time it goes down. Called on paho's thread."""
        self._session_handler = handler

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
        live (``RobotBinding.start``)."""
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
            self.stop()
            raise RuntimeError(f"MQTT broker {self._host}:{self._port} did not accept the "
                               f"connection: {why}")

    def publish(self, topic: str, payload: str) -> None:
        if self._client is None:
            raise RuntimeError("MqttClient is not connected; call connect() first.")
        self._client.publish(topic, payload, qos=1)

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
        for topic in list(self._subs):
            client.subscribe(topic, qos=1)  # at-least-once; a clean session kept none
        reconnect = self._first.is_set()
        self._set(True)
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
        handler = self._session_handler
        if changed and handler is not None:
            handler(up)

    def _on_message(self, _client, _userdata, msg) -> None:
        handler = self._subs.get(msg.topic)
        if handler is not None:
            handler(msg.topic, msg.payload)

    def stop(self) -> None:
        """Disconnect and stop paho's network thread. Idempotent; a no-op before connect."""
        client, self._client = self._client, None
        self._set(False)
        if client is not None:
            client.disconnect()
            client.loop_stop()
