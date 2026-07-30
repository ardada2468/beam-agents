# Python SDK harness for the effectively-once end-to-end semantics gate.
#
# Two reasons this is built rather than pulled:
#
# 1. Protobuf major versions. This repo's committed `_pb2.py` bindings are 6.x
#    gencode, but `apache/beam_python3.11_sdk:2.72.0` ships protobuf runtime
#    5.29.5, and protobuf requires gencode and runtime to share a major version
#    -- so `import beam_agents._protos` fails outright in the stock image with
#    `VersionError: Detected mismatched Protobuf Gencode/Runtime major
#    versions`. Beam's own requirement is `protobuf<7.0.0`, so pinning 6.33.6
#    satisfies both sides; Beam's generated protos were verified to import
#    under it.
#
# 2. Worker restart cost. Staging `beam_agents` per job (`--extra_package`)
#    makes every SDK worker start pay a pip install. This gate kills workers on
#    purpose and expects them back promptly, so a network-dependent install on
#    the recovery path would be a flakiness source -- exactly what design R1
#    forbids. Baking the dependencies in makes worker restart a process spawn.
#
# Built by `docker compose -f docker/compose.yaml build beam-sdk-harness`, which
# `make compose-up` performs as part of bringing the stack up.

FROM apache/beam_python3.11_sdk:2.72.0@sha256:9f42fcb45dd6831662830c36f107be4e60036170fb344d5bf55c3cdf5c46448e

# `--no-deps` for beam_agents itself: apache-beam is already present at exactly
# the matching version, and letting pip resolve `apache-beam[gcp]>=2.60` would
# fight the image's own install. The runtime deps beam_agents actually needs at
# import time are named explicitly instead. `aiokafka` is for the e2e gate's
# outbox DoFn, which publishes intents to Kafka from inside this container.
COPY pyproject.toml README.md /src/
COPY src /src/src
# `langgraph`/`langchain-core` mirror the `langgraph` extra: the adapter
# conformance matrix's Flink leg runs the LangGraph adapter's cells inside
# this harness, so the framework must be importable here (the LangGraph e2e
# cells would otherwise fail worker-side instead of skipping host-side).
RUN pip install --no-cache-dir "protobuf==6.33.6" "httpx[http2]" "pydantic>=2" "aiokafka" \
    "langgraph>=1.0,<2" "langchain-core>=1.0,<2" \
 && pip install --no-cache-dir --no-deps /src \
 && python -c "import apache_beam; import beam_agents.core.transform; \
import beam_agents._protos; import langgraph; print('sdk harness ready', apache_beam.__version__)"

# The e2e gate's pipeline-side DoFns (spool source, outbox producer, test
# agent) live under tests/semantics/_e2e — deliberately NOT in the beam_agents
# package (the spec forbids src/ changes for this gate). Beam pickles DoFns by
# module reference, so the harness must be able to import them; putting the
# tests tree on PYTHONPATH is what makes that possible.
COPY tests /app/tests
ENV PYTHONPATH=/app
