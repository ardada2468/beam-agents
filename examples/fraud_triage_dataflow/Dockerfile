# The fraud-triage Flex Template image: template launcher AND SDK harness.
#
# One image serves both roles, and the base image is why. Inspecting
# gcr.io/dataflow-templates-base/python311-template-launcher-base's own config
# blob shows it *is* the Beam Python 3.11 SDK harness image — it installs
# apache-beam[gcp] from /opt/apache/beam/tars/ and keeps
# ENTRYPOINT ["/opt/apache/beam/boot"] — with
# /opt/google/dataflow/python_template_launcher copied in on top. Dataflow
# overrides the entrypoint with the launcher when it runs the launcher
# container; worker containers exec the Beam boot binary that is already there.
# So there is no multi-stage split, no second protobuf-pinning surface, and no
# class of "launcher resolved, worker didn't" failures the nightly gate could
# not distinguish (design D2).
#
# The two traps docker/sdk-harness.Dockerfile documents apply here verbatim:
#
# 1. Protobuf major versions. This repo's committed `_pb2.py` bindings are 6.x
#    gencode; stock Beam images ship a 5.x runtime, and protobuf requires
#    gencode and runtime to share a major version, so `import beam_agents._protos`
#    fails outright without the pin below. Beam's own requirement is
#    `protobuf<7.0.0`, so 6.33.6 satisfies both sides.
# 2. Worker restart cost. Staging `beam_agents` per job (`--extra_package`)
#    makes every SDK worker start pay a pip install over the network. Baking the
#    code in makes a worker restart a process spawn.
#
# Build (see the nightly `dataflow` job, and README.md for the local form):
#
#   docker build -f examples/fraud_triage_dataflow/Dockerfile \
#     --build-arg TEMPLATE_IMAGE=<registry>/fraud-flex:<git-sha> \
#     -t <registry>/fraud-flex:<git-sha> .
#
# The build context is the repository root.

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base@sha256:35e7280471e19e0dfa4399944978569f5f4adee51d6d10191d613295cae8b75d

# The image's own published URI. It cannot be discovered from inside the
# container, and the launcher forwards it as the job's `--sdk_container_image`
# so workers run this exact image rather than a stock Beam one.
ARG TEMPLATE_IMAGE=""
ENV BEAM_AGENTS_TEMPLATE_IMAGE=${TEMPLATE_IMAGE}

# Third-party dependencies FIRST, before any repo files are copied: this layer
# depends only on the pins on this line, so a `src/`-only change reuses it from
# cache instead of re-resolving everything. The runtime deps beam_agents needs
# at import time are named explicitly, because it is installed with `--no-deps`
# below. `google-cloud-secret-manager` is installed here and only here: worker-
# side credential resolution is the template's concern, not the library's, so it
# is deliberately not a `beam-agents` dependency (proposal, Impact).
RUN pip install --no-cache-dir \
    "protobuf==6.33.6" \
    "httpx[http2]" \
    "pydantic>=2" \
    "google-cloud-secret-manager"

COPY pyproject.toml README.md /src/
COPY src /src/src
# `--no-deps` for beam_agents itself: apache-beam[gcp] is already present at
# exactly the version this image's Beam boot binary matches, and letting pip
# resolve `apache-beam[gcp]>=2.60` would fight the image's own install. Its real
# import-time deps were installed in the cached layer above.
RUN pip install --no-cache-dir --no-deps /src

# The example and its launcher are repo-root code outside the wheel. Beam
# pickles by module reference, so `examples.fraud_triage` and
# `examples.fraud_triage_dataflow.launch` must be importable under the same
# names on the launcher and on every worker — which is what putting /template
# on PYTHONPATH buys.
COPY examples /template/examples
ENV PYTHONPATH=/template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/examples/fraud_triage_dataflow/main.py

# Fail at build, not at nightly launch: every import the launcher and the
# workers make on the happy path, plus a plan built from a complete parameter
# set. A broken reference, a protobuf mismatch, or a missing dependency stops
# the build here and nothing is pushed.
RUN python -c "\
import apache_beam; \
import beam_agents._protos; \
import beam_agents.core.transform; \
import beam_agents.yaml._config; \
import examples.fraud_triage; \
from examples.fraud_triage_dataflow import launch; \
plan = launch.build_launch_plan([ \
    '--input_topic=pubsub://selfcheck-project/events', \
    '--approvals_topic=pubsub://selfcheck-project/approvals', \
    '--output_topic=pubsub://selfcheck-project/decisions', \
    '--intents_topic=pubsub://selfcheck-project/intents', \
    '--model=examples.fraud_triage:make_provider']); \
assert plan.agent_config.intents_to == 'pubsub://selfcheck-project/intents'; \
print('fraud-triage flex template ready', apache_beam.__version__)"
