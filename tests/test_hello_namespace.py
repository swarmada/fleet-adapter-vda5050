# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
"""The hello's namespace: with mutual TLS the control plane binds it to the client certificate and refuses any
other name, so a hello that always said "default" could not connect from any other namespace."""
from fleet_adapter.v1 import fleet_adapter_pb2 as pb
from fleet_adapter.v1 import fleet_adapter_pb2_grpc as pb_grpc
from fleet_adapter_vda5050.adapter import fleet_adapter_vda5050Adapter, main  # noqa: F401
from fleet_adapter_vda5050.robot import SimulatedRobot


def hello(**kw):
    adapter = fleet_adapter_vda5050Adapter(pb, pb_grpc, SimulatedRobot(), "r1", **kw)
    return next(adapter._control_requests()).hello


def test_the_default_hello_names_no_namespace():
    assert hello().namespace == ""


def test_a_named_namespace_is_declared():
    assert hello(namespace="kit-demo").namespace == "kit-demo"
