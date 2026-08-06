# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0

"""Simulator-scenario presets for the VDA5050 simulated comms profile.

Vendored from the in-tree ``adapters/scenarios`` package (same YAML format, same
``--scenario`` surface) so ``--comms simulated`` tells the SAME story by name as the
in-tree simulator and the ROS 2 binding. Proto-free; the binding maps the engine's
output onto the wire protocol.
"""

from fleet_adapter_vda5050.scenarios.loader import (
    HW_DEGRADED,
    HW_FAILED,
    HW_HEALTHY,
    HardwareFaultOverrides,
    HardwareState,
    Scenario,
    ScenarioEngine,
    available_presets,
    load_scenario,
)

__all__ = [
    "HW_DEGRADED",
    "HW_FAILED",
    "HW_HEALTHY",
    "HardwareFaultOverrides",
    "HardwareState",
    "Scenario",
    "ScenarioEngine",
    "available_presets",
    "load_scenario",
]
