"""Pipeline assembly and submission for the e2e gate.

Module-level encode functions only: the publisher DoFns pickle by module
reference into the beam-sdk-harness container. Submission blocks (attached
client), so the gate runs it on a daemon thread and ends it by cancelling the
job through the Flink REST API.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from beam_agents._protos import ToolIntent
from beam_agents.core.dofn import ActivationError
from tests.semantics._e2e.agent import build_run_agent
from tests.semantics._e2e.outbox import publish_intents, publish_tagged
from tests.semantics._e2e.spool import read_spool
from tests.semantics._e2e.stack import ARTIFACT_ENDPOINT, JOB_ENDPOINT, RunConfig

# Manufactured duplicate-delivery rate on the intents topic (spec: duplicate
# sink writes must never produce a second execution).
DUPLICATE_FRACTION = 0.05

CHECKPOINT_INTERVAL_MS = 5_000


def encode_output(value: bytes) -> tuple[bytes, bytes]:
    # Terminal outputs embed their entity key ("result|<key>|…"); the message
    # key is constant because .output elements are plain bytes by contract.
    return b"out", bytes(value)


def encode_error(error: ActivationError) -> tuple[bytes, bytes]:
    return error.entity_key, error.reason.encode() + b"|" + error.detail.encode()


def key_intent(intent: ToolIntent) -> tuple[bytes, ToolIntent]:
    return intent.entity_key, intent


def run_pipeline(config: RunConfig, *, job_name: str) -> None:
    """Build and run the gate pipeline; blocks until the job ends."""
    options = PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={JOB_ENDPOINT}",
            f"--artifact_endpoint={ARTIFACT_ENDPOINT}",
            "--environment_type=EXTERNAL",
            "--environment_config=localhost:50000",
            "--parallelism=2",
            f"--checkpointing_interval={CHECKPOINT_INTERVAL_MS}",
            f"--job_name={job_name}",
        ]
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        events = read_spool(pipeline, config.container_spool)
        out = build_run_agent(events)
        keyed_intents = out.intents | "KeyIntents" >> beam.Map(key_intent)
        publish_intents(
            keyed_intents,
            config.intents_topic,
            duplicate_fraction=DUPLICATE_FRACTION,
            seed=config.seed,
        )
        publish_tagged(out.output, config.output_topic, encode_output)
        publish_tagged(out.errors, config.errors_topic, encode_error)
