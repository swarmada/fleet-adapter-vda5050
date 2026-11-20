# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Make the adapter package importable under pytest without an editable install
(handy for a $0 run; a real deployment `pip install -e .` makes this a no-op)."""

import pathlib
import sys

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ── broker-free "the transport is up" fixture ────────────────────────────────

class PublishInfo:
    """Stands in for paho's ``MQTTMessageInfo`` (paho-mqtt 2.1) without importing paho, which
    the test run doesn't install: ``is_published()`` is True once the broker has acknowledged
    the publish (QoS 1 PUBACK), and raises ``RuntimeError`` when the publish failed (paho's
    ``rc > 0``, such as MQTT_ERR_NO_CONN during a broker drop), as paho's does."""

    def __init__(self, published: bool = False, failed: bool = False) -> None:
        self._published, self._failed = published, failed

    def set_published(self) -> None:
        self._published = True

    def is_published(self) -> bool:
        if self._failed:
            raise RuntimeError("Message publish failed: The client is not currently connected.")
        return self._published


def mark_transport_live(binding):
    """Make a broker-free ``Vda5050Binding`` present as though ``start()`` had run and
    the robot had reported ``ONLINE``, and return it.

    Needed because ``tick()`` now REFUSES to tick a binding whose transport was never
    brought up: an un-started binding answers every accessor with a default
    and confirms nothing, and before this it was indistinguishable from a healthy one.
    That refusal is right in production and wrong for a unit-test fixture standing in for
    a live robot — and some of those fixtures reach ``tick()`` indirectly, via
    ``swarmada_sdk.safety.confirm_estop``, whose polling loop ticks.

    Sets BOTH flags the binding answers liveness questions from, so the fixture is
    consistent across all of them:

    * ``_started`` — what ``tick()`` discriminates on, and what gates publishing.
    * ``_connection`` — the robot's own VDA5050 ``connection`` state, the other half of
      ``transport_alive()``.

    Setting only one would be worse than not having this helper. A binding that ticks
    silently while ``transport_alive()`` says False is a state no real binding can be in,
    and a fixture in it would let a test pass for a reason production could never
    reproduce.

    The third piece is a recording stand-in for paho's client, because ``_started`` is
    also what turns ``self._mqtt.publish`` on: without it these tests would exercise a
    live-transport binding whose every publish raised. It records ``(topic, payload)``
    on ``binding.published`` so a test CAN assert what went to the broker, and answers
    the two calls ``MqttClient.stop`` makes. No parsing, no network, no paho.
    """
    from fleet_adapter_vda5050 import profiles

    published: list[tuple[str, str]] = []

    class _RecordingClient:
        """Stands in for paho's client inside the real MqttClient. Each publish is
        acknowledged at once, as a broker acknowledges a QoS 1 publish."""

        @staticmethod
        def publish(topic, payload, qos=1):
            published.append((topic, payload))
            return PublishInfo(published=True)

        @staticmethod
        def subscribe(topic, qos=1) -> None: ...

        @staticmethod
        def loop_stop() -> None: ...

        @staticmethod
        def disconnect() -> None: ...

    binding._mqtt._client = _RecordingClient()
    binding._mqtt._up = True  # the broker session is up, as after on_connect
    binding._mqtt._users = 1  # this binding counts as its user, as after start()
    binding._mqtt_held = True
    binding._started = True
    binding._connection = profiles.CONN_ONLINE
    binding.published = published
    return binding


# ── "the robot is registered on this session" fixture ───────────────────────

def mark_registered(adapter):
    """Put ``adapter`` in the state an ACCEPTED RegisterAck leaves it in, and return it.

    Work is accepted only for a robot whose registration was accepted on the current
    ControlStream session (C13.2), so a unit test that assigns work to an adapter it built
    by hand has to say the robot is registered. This sets that one flag and nothing else:
    an accepted RegisterAck also reconciles the action state and re-sends a retained
    outcome, which tests of those paths drive through ``_handle_control_plane`` themselves.
    The registration paths are tested in test_provisioning.py.
    """
    adapter._registered = True
    adapter._registered_outbox = adapter._outbox  # the session's outbox, as an accepted register records it
    return adapter


# ── "the vehicle reports after the stop" fixture ────────────────────────────

def reply_on_tick(binding, **state):
    """Make ``binding`` receive a new ``state`` on each ``tick()``, as a vehicle sends one
    after a stop, and return it. The SDK's estop wait ticks the binding and confirms a stop
    only from a state counted after the stop was sent (``state_seq``); a state given before
    the stop can no longer confirm it. Each state carries the next headerId and its own
    timestamp, so none is taken for a redelivery. ``state`` is the VDA5050 state's fields
    (``paused=True``, say); the default is a vehicle at rest."""
    import json

    tick = binding.tick
    count = [0]

    def _tick(dt: float) -> None:
        count[0] += 1
        body = {"driving": False, "velocity": {"vx": 0.0, "vy": 0.0, "omega": 0.0}}
        body.update(state)
        body.update(headerId=10_000 + count[0], timestamp=f"tick-{count[0]}")
        binding.on_state(json.dumps(body))
        tick(dt)

    binding.tick = _tick
    return binding
