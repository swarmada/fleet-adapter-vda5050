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

The same holds while the vehicle is online:

- **Where the position comes from.** It's the `agvPosition` of the latest `state` or
  `visualization` that carried one. A vehicle may send `visualization` more often than its
  `state` (vda-5050-lib: every second when asked), so the map shows it moving between states.
  `visualization` feeds this position only; stops, orders and tasks read the `state`. A
  `visualization` older than the position already reported, by the vehicle's own timestamps,
  isn't applied: MQTT keeps each topic in order, not the two together.
- **An unknown position is left out.** When the vehicle hasn't reported a position, or reports
  `positionInitialized: false` or a reading that isn't a finite number, no `position` is sent:
  never the origin. A message with no `agvPosition` leaves the last position as it was. When
  the vehicle leaves `ONLINE`, its position and battery count as unknown until it reports
  again. The edge node's `PositionFrame` follows the same rules, stamped with the adapter's
  clock.
- **The rest of the reading.** The heading (`yaw`) is sent from `theta` when the vehicle gives
  one, and left out otherwise. The `floor` is 0, since VDA 5050 has no floors. The position is
  in the map frame the vehicle reports, sent as the zone frame; the `mapId` isn't sent.
- **The battery** is left out while the `state` has no charge reading, never sent as 0%.
- **How often.** A frame goes out every `telemetry_interval_seconds` the control plane sets.
  For a map that moves each second, set the Robot's `telemetryIntervalSeconds` to 1, and have
  the vehicle send `visualization` every second.

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

The image includes paho-mqtt 2.1.0, so it runs `--comms vda5050` as it is; the Dockerfile shows
the run line. The broker session works like this:

- **Start-up.** The adapter starts only once the broker has accepted its connection and
  every topic is subscribed. A broker it can't reach, or one that refuses or doesn't answer
  within 10 s, stops it.
- **A drop.** When the session drops, paho reconnects by itself, 1 s after the drop and then
  at most 10 s apart. Each reconnect subscribes every topic again. While the session is
  down, the vehicle is reported `OFFLINE`, with no position and no battery. Its connection
  state, position and battery stay unknown until the broker delivers them again.
- **A vehicle that doesn't say `ONLINE` again.** Some vehicles publish their retained
  `ONLINE` only when they start; vda-5050-lib does. After a broker restart or a network blip,
  the retained message is then still their last will, `CONNECTIONBROKEN`, while they're
  connected and sending states. So after a drop, or while the connection reads
  `CONNECTIONBROKEN` or hasn't been received, a fresh `state` restores the vehicle as
  connected.
- **When that connection lapses.** A connection restored this way lapses after 60 s without a
  new `state` (`--state-lapse-seconds`): twice the 30 s VDA 5050 allows between states.
- **What still decides.** An `ONLINE` from the vehicle restores it with no lapse. A later
  `OFFLINE` or `CONNECTIONBROKEN` takes it down at once, and a vehicle that said `OFFLINE`
  itself isn't restored by its states.
- **What "not connected" does.** New work is refused `ROBOT_UNAVAILABLE`, naming the reason
  ("the vehicle is not connected: …"), so the control plane can schedule it again; an order now
  would go to a vehicle that isn't reading it. The same applies while the broker session is
  down. A repeat delivery of the work the vehicle already holds is still accepted, and nothing
  starts again. Nothing is sent to the vehicle because of it: no stop and no cancel.
- **No TLS, no login.** The MQTT connection is plain TCP, so keep the broker on a network
  that only the adapters and vehicles reach.

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
- **streams task completion**: it derives `SUCCEEDED` (order reached its last node, read
  from a `nodeStates` list: a `state` without one isn't taken as arrived) /
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

### When an emergency stop is confirmed

An emergency stop is answered `STOPPED` only from a `state` the vehicle sent after the
stop, which shows it at rest and paused:

- **After the stop.** The adapter counts the states it receives. A state counted before the
  stop was sent can't confirm it.
- **At rest and paused.** The state shows the vehicle not driving, with its velocity within
  sensor noise, and paused: `paused: true`, or the stop's `startPause` reported `FINISHED`.
  VDA 5050 2.0.0 says a finished `startPause` means the vehicle "stands still" and reports
  `paused: true`. A `startPause` that `FAILED` confirms nothing by itself. A vehicle already
  paused refuses a second one and still reports `paused: true`, so a second stop is
  confirmed.
- **Or paused, then nothing left to drive.** A vehicle can leave pause mode when the stop's
  `cancelOrder` ends its order, and vda-5050-lib's virtual vehicle does. It also drops an
  ended instant action from its `state` after a few publishes. So the stop is also
  confirmed when a counted `state` that lists the stop's `startPause` as ended has shown
  the vehicle paused, and the latest one shows it at rest, with no node and no action left,
  reporting the last order the adapter sent, in `AUTOMATIC` or `SEMIAUTOMATIC` mode. A
  `state` that doesn't name the `startPause` may have been sent before the vehicle took it.
  A `state` that leaves out `nodeStates` or `actionStates` says nothing about what is left,
  so it doesn't confirm the stop this way. In `MANUAL`, `SERVICE` or `TEACHIN` a person can
  move the vehicle without an order, so only `paused: true` or the `startPause` finished
  confirms the stop there.
- **Within 2 s.** The adapter waits 2 s for that state, then answers `FAILED`. VDA 5050
  lets a vehicle send its `state` on "relevant events" or "at the latest every 30s", so
  the vehicle must report the pause, or its `startPause` finished, within 2 s of the stop.
  vda-5050-lib's virtual vehicle reports it on its next tick, within 200 ms at its default
  5 Hz.
- **Each state once.** MQTT QoS 1 can deliver a message twice. A `state` with the
  `headerId` and `timestamp` of one of the last 64 is dropped: not stored, not counted. A
  vehicle that restarts begins its `headerId`s again, with new timestamps, so its states
  still count. The first drop is logged as a warning: a vehicle that sends one `headerId`
  and `timestamp` for two different states loses the second.
- **Its own `startPause`.** Each `actionId` carries the same token unique to the adapter
  process as an `orderId` does. A vehicle lists an ended action for a while, so a `startPause`
  `FINISHED` from an earlier adapter process can't be taken for this one's.
- **Without a `headerId` or `timestamp`.** A `state` that lacks an integer `headerId` or a
  `timestamp` can't be told from a repeat. It updates the vehicle's telemetry but isn't
  counted, so it never confirms a stop. The adapter logs this once, as an error.

A pause for Swarmada (`require_stop_before_ack`) is checked once, right after its
`startPause`, against the same rule. A vehicle at rest is answered `paused=false` until it
has reported the pause.

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
  sent still outstanding. A `state` that leaves out `nodeStates` or `actionStates` never
  says that, so the cancel is sent. An emergency stop always sends it.
- **The reconcile at registration.** The control plane's reply names the action it holds for
  the robot, or none. An action the adapter runs is kept only when the reply names it under the
  token it was assigned with. The same action under a newer token is stopped and reported under
  that token. An action the reply doesn't name is stopped and reported `STOPPED` once a
  `state` sent after the stop shows the vehicle at rest. If none does within 2 s, or new work
  is taken first, it's reported `FAILED` with the reason. When the reply names another action,
  the end cause is `SUPERSEDED`, naming that action; when it names none, no cause is stated.
  An action the reply names `UNKNOWN` (the control plane couldn't read it) is ended the same
  way, with no cause. If its stop isn't confirmed, nobody can establish what the run achieved:
  it's reported `FAILED`, and the reason says the outcome is unknown and the run may have done
  its work. Read that reason before running the work again. (The 0.4.1 control plane drops an
  `UNKNOWN` report, so `FAILED` is the report that ends the action.)
  The report goes on the session that is current when it's sent, and only if that session's
  registration is accepted; otherwise it is dropped and logged. Sent in the moment between one
  session ending and the next one starting, it is lost.
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

### A new order while the vehicle has one

A VDA 5050 vehicle rejects an order with a new `orderId` while an order is active, unless
the new order extends the active order's base. So when work replaces work in place (a
higher-priority action preempting a running one), the adapter does not send the new order
at once:

- It sends `cancelOrder` and holds the new order. The vehicle "may still have an order"
  when its last `state` lists a node or an action that has not ended, leaves out
  `nodeStates` or `actionStates`, or does not yet report the last order the adapter sent.
- The held order goes out once the vehicle reports that cancel ended (`FINISHED`, or
  refused with a `noOrderToCancel` error naming it), with both lists reported, no node and
  no action left, and not driving. It starts from where the vehicle stopped. Only the cancel's own end counts:
  MQTT keeps one client's messages in order per topic only, and an order that overtook its
  cancel would be ended by it.
- A cancel from a stop is waited on while the vehicle lists it as running, or before any
  `state` since it was sent. Otherwise the preemption sends its own.
- A second order while one is held replaces it. A stop or an emergency stop drops it.
- After an emergency stop, the `stopPause` goes out with the held order, not before. While
  a Swarmada pause is in force, the order goes out with a `startPause` instead, and only a
  Swarmada resume lifts the pause. Some vehicles leave pause mode on a `cancelOrder`.
- If the cancel is not reported ended within 60 seconds, the order is dropped and its
  action ends `FAILED`, with the reason in its message. A vehicle that does not list
  instant actions in its `state` ends here.
- When an action ends `FAILED`, the adapter stops the vehicle: a held order is dropped,
  and what is left of the order before it is cancelled. No `cancelOrder` is sent when the
  vehicle shows nothing to cancel.

### An order the vehicle rejects

A VDA 5050 vehicle that rejects an order keeps reporting its previous `orderId`, with a
warning until it accepts a new order (VDA 5050 2.0.0 6.6.4).

- The adapter counts the last order it sent as rejected when an error in the vehicle's
  `state` names that order's `orderId` in its references and the vehicle does not report
  that order as its own. The error type is not matched. Errors are read as vda-5050-lib's
  master controller reads them: an error about another topic is skipped, and so is one about
  an action, unless it is an `orderError`.
- The action ends `FAILED` with `EXECUTION_FAILURE` and the vehicle's reason in its message.
  The reason is the vehicle's own `errorType` and `errorDescription`, with control characters
  removed and cut to length. Every `FAILED` from the vehicle's state carries
  `EXECUTION_FAILURE`, including a FATAL error and a preemption that timed out.
- A rejected order is not waited on: the next order goes out at once. The stop after the
  `FAILED` still sends its `cancelOrder`. VDA 5050 lets any error name an order, so a
  rejection is read from the vehicle's errors, not known; the price is a `noOrderToCancel`
  warning.
- **An order taken after it was stopped.** If the vehicle reports a stop's `cancelOrder`
  ended and then runs the order the adapter stopped, the adapter sends one more
  `cancelOrder`. That happens when the cancel overtakes the order, or when a rejected order
  is delivered again and taken. A `state` that leaves out `nodeStates` or `actionStates`
  counts as still running it.
- **A `state` without `nodeStates` or `actionStates`.** VDA 5050 2.0.0 requires both lists.
  Without one, what the vehicle has left to do is unknown, never taken as nothing. Every stop
  then sends its `cancelOrder`, an order waits behind a cancel for a `state` with both
  lists, and no task is marked done. The adapter logs this once, as an error. Likewise an
  action listed with no `actionStatus`, or one other than VDA 5050's, counts as not
  ended: only `FINISHED` and `FAILED` end it.
- Each `orderId` carries a token unique to the adapter process. A rejection the vehicle still
  reports from an earlier process can then not be taken for one of this process's orders.
  An `orderId` is the robot id, 16 characters, and the order's number. An `actionId` carries
  the token too: the robot id, 12 characters, and the action's number. It's 4 characters
  shorter at the same number, so a vehicle that takes the adapter's orders takes its stops.
  `idLen` bounds both. When the vehicle's factsheet sets a shorter
  `protocolLimits.maxStringLens.idLen`, the adapter logs an error naming the limit and the
  longer id's length, once for each limit: the vehicle would reject the orders, or instant
  actions, that carry it. A shorter robot id fixes it.
- **Not detected:** a rejection whose error names only the `headerId`. Header ids restart
  with the adapter process, so an old error could be taken for a new order. That action ends
  at its lease or the control plane's timeout.
- The adapter never sends an order update. Every order has a new `orderId` and
  `orderUpdateId` 0, so no rule for a rejected update is needed.

### A command for another robot

The adapter serves one vehicle, under the robot id it registers. Every Command and every
SafetyStream message names a robot by that id.

- **A command for another id, or for none,** is refused before anything runs. Nothing reaches
  the vehicle, the stop latch, the fencing mark, the lease or the current action. Each command
  gets the refusal its result already has: an assignment is refused `ROBOT_UNAVAILABLE`, and a
  renewal, a cancel, a pause, a resume or a zone-rule push is not done.
- **An emergency stop or a stop's fence for another id** is answered `FAILED` on the
  SafetyStream, never `STOPPED`. The adapter cannot confirm that robot is at rest, and nothing
  is latched. Whether this vehicle stops depends on the registration:
  - **Once a registration is accepted** on the current session, the adapter serves its robot,
    and a stop for another id does not stop this vehicle. **This `FAILED` means this vehicle
    was not stopped.**
  - **While no registration is accepted**, the adapter serves no robot, and the likeliest cause
    is an id mismatch: the adapter's `--robot-id` is not the Robot's `swarmada.io/robot-id`, so
    its registration was refused and every stop for that Robot arrives under the other id. The
    vehicle is then stopped as well, and the stop is still answered `FAILED`. The log says "no
    accepted registration; vehicle stopped".
- **A clear for another id** lifts nothing and is not acknowledged.
- A capabilities scan, and a command this adapter declines anyway, are answered as for any
  robot. A scan names this adapter's own robot.
- The leader term is kept for the served robot only. A command for another id neither raises
  it nor is refused by it.
- Each one is logged at error level, naming the id received and the id served, so an id
  mismatch with the control plane shows.
- **Existing gap:** the adapter sends no `CommandResult` receipt for a stop's fence
  (`Command.estop`), for its own robot or another. The stop's answer is the `EstopAck` on the
  SafetyStream, which the control plane reads.

### Routes along lanes

Without a route graph, an order is one straight edge from the vehicle's position to the goal.
VDA 5050 leaves the route to the fleet master, and here the adapter is the master. With
`--route-graph FILE`, an order follows lanes:

```bash
python -m fleet_adapter_vda5050.adapter --endpoint localhost:9090 --comms vda5050 \
    --mqtt-host broker.local --route-graph examples/warehouse-lanes.yaml
```

- **The file** is YAML. `nodes` maps a node id to `[x, y]`, in metres, in the map frame the
  vehicle reports its position in. `edges` lists undirected `[a, b]` pairs, each weighted by
  its length. An optional `map_id` names the map the lanes are drawn on. When it's given and
  the vehicle reports another `mapId`, the adapter logs an error, once for each pair of maps;
  orders still go out. Other keys are ignored.
- **The order** is one released base: the start node at the vehicle's position, each lane node
  on the shortest path, and the goal, with an edge between each pair. A lane node keeps its id
  and names no heading; the goal keeps the assignment's heading. A lane node within 0.05 m of
  the start or the goal is not sent, so no edge has zero length.
- **The goal must be a lane node.** With a graph loaded, a goal more than 0.05 m from every lane
  node is refused `INVALID_ACTION`, naming the nearest node and its distance. Off the lanes,
  the last leg could cross a rack.
- **The start.** The route is worked out when the order is sent. A vehicle between two nodes,
  after a stop or an order replaced in place, enters the graph by the edge it is on, at
  whichever end makes the route shorter. A vehicle more than 0.5 m from every edge goes
  straight to the nearest node, and the adapter logs an error naming the distance.
- **The position must be known.** While the vehicle has reported no initialised position, work
  is refused `ROBOT_UNAVAILABLE`: the order would start nowhere on the lanes.
- **The graph is checked at start-up.** The adapter refuses to start on any of these: a node id
  given twice, an edge naming an unknown node, a self-loop, a duplicate edge, no edge at all, a
  coordinate that is not a finite number within 10^6 m, two nodes closer than 0.05 m, a node
  named `start` or `goal` (the order's own ends), or a graph that is not connected.
- **idLen.** When the vehicle's factsheet arrives, a node id longer than its `idLen` is logged
  as an error, as an over-long orderId or actionId is.
- **No traffic control.** Each vehicle is routed alone. Two vehicles on the same lanes can meet
  head-on, and nothing keeps a vehicle off a lane another robot is using.
- **`examples/warehouse-lanes.yaml`** is the warehouse world's lane graph, generated from the
  warehouse layout. `examples/ks-aisles.yaml` is a graph for the open ks floor, which has no
  racks; the tests use it.

On a floor shared with other robots, the setup must provide:

- **One frame.** The zones, the goals, the lane graph and the vehicles' reported positions all
  in the same map frame.
- **Start poses on lane nodes**, and not on the nodes the other robots start on.
- **Goals at lane nodes.** A goal anywhere else is refused, as above.
- **No operating constraints in the vehicles' zones.** This adapter honours none, and refuses
  an assignment that carries one `OPERATING_CONSTRAINT_UNMET`.

**Admission.** A vehicle with no Robot yet needs no other adapter to admit it first. Its
registration is refused `NOT_ADMITTED`; the adapter then announces the robot (`DiscoverRobot`)
and registers again each provisioning interval until an operator admits it. The interval is
half the `DiscoverAck`'s TTL, at most 300 s, or 60 s without one, so the robot registers up to
one interval after it is admitted. Until then, work for it can't reach the adapter. When a
registration is accepted, the adapter logs `REGISTERED robot=<id>` with the contract version and
the telemetry interval.

## Development

```bash
pip install -e '.[dev]'
make test          # safety-wiring + comms-profile tests
make conformance   # drive against the published Swarmada harness (C1–C8)
```

## License

Apache-2.0. See [LICENSE](LICENSE).
