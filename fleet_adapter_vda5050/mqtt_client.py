# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Thin MQTT transport for the VDA5050 comms profile.

Wraps ``paho-mqtt`` so the profile and binding stay transport-agnostic and
testable without a broker: ``paho`` is an optional dependency imported lazily in
:meth:`connect`, and the message parsing in ``profiles/`` needs neither ``paho``
nor a live broker. Subscriptions registered before :meth:`connect` are applied on
connect, so the binding can wire its handlers at construction time.

VDA5050 runs over MQTT at QoS 1 (at-least-once); the binding dedups duplicate
deliveries on the message ``headerId`` (sub-items 3–4).
"""

from __future__ import annotations

from collections.abc import Callable

_Handler = Callable[[str, bytes], None]


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

    def subscribe(self, topic: str, handler: _Handler) -> None:
        """Register a handler for a topic; applied on the next :meth:`connect`."""
        self._subs[topic] = handler

    @property
    def subscriptions(self) -> tuple[str, ...]:
        """Registered topics (test seam — assertable without a broker)."""
        return tuple(self._subs)

    def connect(self) -> None:  # pragma: no cover — requires paho + a broker
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError(
                "MqttClient.connect() requires paho-mqtt and an MQTT broker. Use "
                "--comms simulated for a $0 run, or install paho-mqtt and point "
                "--mqtt-host at your VDA5050 broker."
            ) from exc
        client = mqtt.Client(client_id=self._client_id)
        client.on_message = self._on_message
        client.connect(self._host, self._port)
        for topic in self._subs:
            client.subscribe(topic, qos=1)  # at-least-once
        self._client = client
        client.loop_start()

    def publish(self, topic: str, payload: str) -> None:  # pragma: no cover — broker
        if self._client is None:
            raise RuntimeError("MqttClient is not connected; call connect() first.")
        self._client.publish(topic, payload, qos=1)

    def _on_message(self, _client, _userdata, msg) -> None:  # pragma: no cover — broker
        handler = self._subs.get(msg.topic)
        if handler is not None:
            handler(msg.topic, msg.payload)

    def stop(self) -> None:  # pragma: no cover — broker
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None
