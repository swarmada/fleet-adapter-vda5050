# Copyright 2026 The Swarmada Authors <maintainers@swarmada.io>
# SPDX-License-Identifier: Apache-2.0
#
# The VDA5050 Fleet Adapter image, serving the simulated vehicle (--comms simulated).
#
#   docker build --build-context swarmada=<Swarmada checkout> -t fleet-adapter-vda5050:dev .
#
# The shared safety primitives (swarmada-sdk) are not on PyPI: they come from a Swarmada checkout,
# passed as the named build context `swarmada`. The fleet_adapter.v1 stubs are this repository's
# own (fleet_adapter/v1, contract 1.4.0). The third-party versions are the ones the adapter's tests
# and conformance run used, pinned with hashes in requirements.txt.
#
# Run, with the adapter's mutual-TLS files mounted at /tls (ca.crt: the CA the control plane's
# certificate is signed by; tls.crt and tls.key: this adapter's client certificate and key) and a
# writable directory for the retained outcome (contract 1.4.0: the last action's terminal outcome
# is kept until the control plane acknowledges it, across a restart):
#
#   docker run -v <tls>:/tls:ro -v <data>:/data fleet-adapter-vda5050:dev \
#     --endpoint <control plane>:9090 --robot-id <robot>
#
# Without the three files the adapter refuses to start. --insecure (no TLS) exists for local tests
# only and is never the default. Nothing in the image holds a site value or a key.

# The base image, pinned by digest: the multi-arch index of python:3.12-slim as read from Docker Hub
# on 2026-10-15. Moving it is a deliberate change: read the new index digest
# (docker buildx imagetools inspect python:3.12-slim) and rebuild. Both stages use it.
#
# Build stage: the SDK and the adapter are built into wheels with the pinned setuptools
# (build-requirements.txt) and no build isolation, so the build fetches nothing unpinned. Only the
# two wheels leave this stage.
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1 AS build
RUN --mount=type=bind,source=build-requirements.txt,target=/tmp/build-requirements.txt \
    pip install --no-cache-dir --no-compile --require-hashes -r /tmp/build-requirements.txt
COPY --from=swarmada sdk/python /tmp/swarmada-sdk
COPY pyproject.toml LICENSE README.md /tmp/adapter/
COPY fleet_adapter_vda5050/ /tmp/adapter/fleet_adapter_vda5050/
RUN pip wheel --no-cache-dir --no-build-isolation --no-deps -w /wheels /tmp/swarmada-sdk /tmp/adapter

# The image. Every third-party package is installed from requirements.txt with --require-hashes:
# wheels only, each one pinned by hash. --no-compile: no bytecode is written, so the root
# filesystem can be mounted read-only. The requirement file and the wheels are bind-mounted, so
# neither is left in a layer, and no pip cache is kept.
FROM python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
# Set before pip runs: --no-compile covers the packages installed, not pip's own modules and the
# stdlib modules pip imports while it runs.
ENV PYTHONDONTWRITEBYTECODE=1
RUN --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir --no-compile --require-hashes -r /tmp/requirements.txt \
    && rm -rf /root/.cache

WORKDIR /app
COPY fleet_adapter/v1/fleet_adapter_pb2.py fleet_adapter/v1/fleet_adapter_pb2_grpc.py \
     /app/proto/fleet_adapter/v1/
RUN --mount=type=bind,from=build,source=/wheels,target=/tmp/wheels \
    pip install --no-cache-dir --no-compile --no-deps /tmp/wheels/*.whl \
    && rm -rf /root/.cache \
    && mkdir -p /data \
    && chown 65532:65532 /data \
    && chmod -R a+rX /app

# Unbuffered, so the log shows stops, clears and refusals as they happen.
ENV PYTHONPATH=/app/proto \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# It needs no privileges: it only dials out over gRPC.
USER 65532:65532
VOLUME ["/data"]
ENTRYPOINT ["fleet-adapter-vda5050"]
CMD ["--comms", "simulated", "--state-dir", "/data", \
     "--tls-ca", "/tls/ca.crt", "--tls-cert", "/tls/tls.crt", "--tls-key", "/tls/tls.key"]
