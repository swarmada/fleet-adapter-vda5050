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
        """Stands in for paho's client inside the real MqttClient."""

        @staticmethod
        def publish(topic, payload, qos=1) -> None:
            published.append((topic, payload))

        @staticmethod
        def loop_stop() -> None: ...

        @staticmethod
        def disconnect() -> None: ...

    binding._mqtt._client = _RecordingClient()
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
    return adapter
