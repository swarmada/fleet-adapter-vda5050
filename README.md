# VDA5050 Fleet Adapter

A **safety-complete** reference [Swarmada](https://swarmada.io) Fleet Adapter for
the **VDA5050 class** of AMRs — any robot that speaks the public
[VDA5050](https://github.com/VDA5050/VDA5050) interface (`order`, `state`,
`connection`, `instantActions`) over MQTT/JSON. It is a neutral, standard-based
reference module per [ADR-0005](../../../docs/adr/0005-reference-adapter-policy.md);
it lives in its own repository and is tracked in the Swarmada adapter registry.

This adapter covers the **VDA5050 CLASS** — no per-vendor dialects. Vendor quirks
belong in a vendor-specific adapter repo.

## Pluggable comms profiles

The robot-side wire dialect is a **pluggable comms profile** behind one
`RobotBinding` (`fleet_adapter_vda5050/profiles/`). VDA5050 is the first profile.
**ISO 21423** is a planned follow-on: once the standard publishes, it registers as
a second profile in `profiles.get_profile()` and slots in behind the same seam
**without touching the core adapter** — the adapter and `Vda5050Binding` are written
against the `CommsProfile` Protocol, not against VDA5050 specifics.

```
adapter.py ──drives──▶ RobotBinding ──wraps──▶ MqttClient + CommsProfile
                                                          ▲
                                          vda5050 (now) ──┤── iso21423 (later)
```

### The binding lifecycle is part of the contract

`RobotBinding` declares `start()` / `stop()` / `transport_alive()`, and the adapter drives
them unconditionally from `bring_up_robot()` — no `hasattr` probe, no `--comms` branch. A
binding with nothing to bring up implements the first two as **documented no-ops** (see
`SimulatedRobot`), which is two visible lines; an *absent* lifecycle is invisible, and that
is what shipped: `--comms vda5050` constructed `Vda5050Binding` and never called `start()`,
so it subscribed to no topic, connected to no broker, published no orders — and nothing
raised. `pose()` answered the origin, `battery_percent()` answered 0 and `is_stopped()`
answered False forever, which can never confirm a stop.

`transport_alive()` is the other half. It answers True only while the broker session is up
**and** the robot's own VDA5050 `connection` topic says `ONLINE`; the adapter asks it before
every telemetry publish and reports `ROBOT_PHASE_OFFLINE` with `position` and `battery`
**omitted** rather than defaulted when the answer is False — proto3 message-field presence
is what makes "unknown" distinguishable from "0" on the wire (C6.2).

If you implement `RobotBinding` against your own fleet API: `start()` MUST raise when it
cannot connect (returning quietly is the defect above), `stop()` MUST be idempotent and safe
when `start()` never ran, and `transport_alive()` MUST mean *"the readings below are
measurements"* — not *"the process is alive"*.

## Running

```bash
# $0, no MQTT broker — passes the conformance handshake (C1) out of the box:
python -m fleet_adapter_vda5050.adapter --endpoint localhost:9090 --comms simulated

# Against a real VDA5050 fleet over MQTT (needs the optional paho-mqtt extra):
pip install -e '.[vda5050]'
python -m fleet_adapter_vda5050.adapter --endpoint localhost:9090 \
    --comms vda5050 --mqtt-host broker.local --mqtt-port 1883 \
    --manufacturer MiR --serial 200-abc123
```

`--manufacturer` / `--serial` set the robot's VDA5050 topic identity
(`<interfaceName>/<version>/<manufacturer>/<serialNumber>/…`); they must match the
robot's own identity. They default to `swarmada` / `amr-1` (the simulator's identity), so
the `--comms simulated` path and existing setups are unaffected.

## Conformance roadmap (C1–C8)

| # | Sub-item | Conformance | Status |
| :- | :------- | :---------- | :----- |
| 1 | MQTT client + VDA5050 `connection`/state handling → handshake | C1 | ✅ done |
| 2 | Telemetry: VDA5050 `state` → protocol telemetry (numeric-only) | C6 | ✅ done |
| 3 | Task: FleetTask → VDA5050 `order`; status from `state`; decline optionals | C2/C7 | ✅ done |
| 4 | Safety: confirmed estop → `startPause`, then `cancelOrder`; fencing; lease self-stop | C3/C4/C5 | ✅ done |
| 5 | Comms-profile seam ready for `iso21423` | — | ✅ (seam in place) |
| 6 | `make conformance` until C1–C8 pass | C1–C8 | ✅ **CONFORMANT** — 14 passed, 0 failed, 2 skipped (`--comms simulated`); 38 unit tests pass. See `../../REGISTRY.md`. |
| 7 | Pause / resume → VDA5050 `startPause` / `stopPause` instantActions | beyond C1–C8 | ✅ done |
| 8 | Scan / capabilities → VDA5050 `factsheet` (`factsheetRequest` + parse) | beyond C1–C8 | ✅ done |
| 9 | Task completion streaming → `state` order progress → `SUCCEEDED` / `FAILED` | beyond C1–C8 | ✅ done |
| 10 | Contract 1.4.0 stop path: stop latch (`latch.py`), `EstopClear` → `EstopClearAck`, leader term, interrupted work, retained outcome until `ActionOutcomeAck` (`retained.py`, `--state-dir`) | contract 1.4.0 | ✅ **CONFORMANT** at 1.4.0 — 50 passed, 0 failed, 1 advisory (a SHOULD), 20 skipped (`--comms simulated`). The simulated vehicle drives to its target at 0.5 m/s, so an order ends. An edge stop sets the local latch; the vehicle's reset (SIGUSR1 to the adapter process, a local command, never a message) lifts it, and work resumes once the EstopClear that ends its set has arrived too. The reference control plane at v0.4 sends no such clear, so there work stays refused after an edge stop |
| 11 | Mutual TLS to the control plane and the edge node: `--tls-ca`, `--tls-cert`, `--tls-key` (and `--tls-server-name`); without the three files the adapter refuses to start; `--insecure` is explicit, for local tests only, and never the default | mutual TLS | ✅ done; the conformance harness CONFORMANT over mutual TLS and with `--insecure`; a plaintext or untrusted client is refused at the handshake |

**Beyond the safety+task subset.** In addition to C1–C8, the adapter now:

- maps Swarmada `pause` / `resume` Commands onto VDA5050's native `startPause` /
  `stopPause` instantActions — `startPause` pauses the active order without aborting
  it (distinct from the `cancelOrder` confirmed-stop safe-hold), and
  `require_stop_before_ack` acks `paused=true` only once the robot is CONFIRMED at
  rest;
- answers the `scan` (`ScanCapabilities`) command with a full `CapabilitiesSnapshot`
  instead of declining it, and discovers a live robot's capabilities from its
  VDA5050 `factsheet` (`request_factsheet` publishes a `factsheetRequest`
  instantAction; `on_factsheet` parses `typeSpecification` + `protocolFeatures`); and
- **streams task completion**: it derives `SUCCEEDED` (order reached its last node) /
  `FAILED` (FATAL error) from the robot's `state` and reports the terminal
  `TaskStatusUpdate` once — echoing the executing fencing token (C6.4) — so the
  control-plane FleetTask lifecycle sees the order finish, not just start.

VDA5050 has no native firmware/model OTA or hardware-verify action, so
`push_firmware` / `model_update` / `verify_*` remain correctly declined (C7.1). This
completes the VDA5050-native function surface — every Swarmada command that maps to a
VDA5050 concept is handled; the rest is honestly declined.

Safety is **never faked**: `is_stopped` is CONFIRMED from the robot's `state`
(velocity ≈ 0 / not driving), never inferred from a timer or from silence
(CONFORMANCE.md C5).

### Stops and pauses on a VDA 5050 vehicle

- **An emergency stop** is `startPause`, then `cancelOrder`. VDA 5050 2.0.0 lets
  `cancelOrder` stop "immediately or on the next node"; `startPause` means "no more AGV
  driving movements - reaching next node is not necessary". The pause halts the vehicle,
  and the cancel ends the order, which a pause alone leaves resumable.
- **After the stop is cleared**, the first order is preceded by a `stopPause`, and the
  vehicle drives again. On a contract 1.4.0 connection the stop latch refuses work until
  the clear; below 1.4.0 the next order lifts the pause.
- **Any other stop** (a cancel, a lapsed lease, the reconcile at registration) is
  `cancelOrder`. It is not sent when the vehicle's own `state` says there is nothing to
  cancel: at rest, no node left, no action that has not ended, and no order the adapter
  sent still outstanding. An emergency stop always sends it.
- **A pause at the vehicle.** When the vehicle reports `paused` and the adapter did not
  pause it (by its button, or by an earlier adapter process), work is refused
  `ROBOT_UNAVAILABLE` with "paused at the vehicle". The adapter never sends the
  `stopPause` that would release a pause it did not set.
- **The remaining case.** A vehicle does not report who paused it. A pause button pressed
  while the adapter's own estop pause is in force looks like that pause, and the
  `stopPause` before the first order after the clear releases it. VDA 5050 2.0.0 says
  `stopPause` "can also restart vehicles that were stopped with a hardware button that
  triggered startPause (if configured)". Whoever clears the stop clears it for the
  vehicle as it stands.

## Development

```bash
pip install -e '.[dev]'
make test          # safety-wiring + comms-profile tests
make conformance   # drive against the published Swarmada harness (C1–C8)
```

## License

Apache-2.0. See [LICENSE](LICENSE).
