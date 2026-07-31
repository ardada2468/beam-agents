"""The conformance Spark pipeline: one job per adapter, scenarios multiplexed
as per-scenario key prefixes — the Flink leg's shape, submitted to a Beam
**Spark** job server instead (change `promote-spark-runner`, design D2).

Everything the two legs can share is imported from
``tests/conformance/_flink/pipeline.py`` by module reference rather than
copied: the adapter rule builders, the scenario key encoding, and the three
tagged-output encoders. What is *not* shared is the scenario set — the spark
leg publishes and scripts only ``SPARK_SCENARIOS``, so a scenario declared
``Skip`` on spark never reaches the job — and the pipeline options, which name
the Spark job server and its checkpoint directory.

Same worker-side rules as the Flink leg: module-level functions and classes
picklable as plain names, no closures in the DoFn graph, ingest through the
replayable segment spool, egress through the at-least-once outbox publisher.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from beam_agents._protos import AgentEnvelope
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.model.fake import FakeLLM
from beam_agents.tools import ToolRegistry
from tests.conformance._flink.pipeline import (
    RULE_BUILDERS,
    encode_error,
    encode_intent,
    encode_output,
    scenario_from_key,
)
from tests.conformance._spec import (
    BIG_TTL_MS,
    SCENARIOS_BY_NAME,
    SPARK,
    SPARK_SCENARIOS,
    tool_for,
)
from tests.semantics._e2e.outbox import publish_tagged
from tests.semantics._e2e.spool import read_spool

if TYPE_CHECKING:
    from beam_agents.core.agent import Agent, Outcome
    from beam_agents.core.context import ActivationContext

#: Host-published endpoints of the spark overlay's job server
#: (``docker/compose.spark.yaml``). Deliberately distinct from the Flink job
#: server's 18099/18098 so both stacks can be up at once.
SPARK_JOB_ENDPOINT = "localhost:28099"
SPARK_ARTIFACT_ENDPOINT = "localhost:28098"

#: Streaming state durability for the Spark runner, on the artifact-staging
#: volume the job server and the spark worker pool both mount. The Flink leg's
#: equivalent is ``--checkpointing_interval``; Spark takes a directory.
SPARK_CHECKPOINT_DIR = "/tmp/beam-artifact-staging/spark-checkpoints"


def merged_provider(adapter_name: str) -> FakeLLM:
    """Every spark-runnable scenario's rules in one script: matchers are scoped
    by each scenario's unique ``model_id``, so multiplexing cannot
    cross-match."""
    rules = []
    for spec in SPARK_SCENARIOS:
        rules.extend(RULE_BUILDERS[adapter_name](spec.variant_for(SPARK)))
    return FakeLLM(rules)


class MergedSparkRegistry(ToolRegistry):
    """Every spark-runnable scenario's tools, pickled as a rebuild recipe.

    Same reason as the Flink leg's ``MergedConformanceRegistry``: a registry
    pickled by value carries its tools' pydantic argument models, whose
    compiled ``SchemaSerializer``s do not unpickle under the sdk-harness
    container's different pydantic-core. Rebuilding from the module-level
    specs worker-side sidesteps object-graph pickling entirely.
    """

    def __init__(self) -> None:
        super().__init__()
        seen: set[str] = set()
        for spec in SPARK_SCENARIOS:
            for tool_def in spec.tools:
                if tool_def.name not in seen:
                    seen.add(tool_def.name)
                    self.register(tool_for(tool_def.name))

    def __reduce__(self) -> tuple[type[MergedSparkRegistry], tuple[()]]:
        return (MergedSparkRegistry, ())


def merged_registry() -> ToolRegistry:
    return MergedSparkRegistry()


class SparkDispatchAgent:
    """One adapter's agent for the whole multiplexed job: routes each key to
    the per-scenario agent, built lazily worker-side from the registry
    factories (picklable as the adapter name alone).

    Distinct from ``FlinkDispatchAgent`` rather than shared: the two differ in
    the leg they build their scenario variant for, and a shared class would
    have to carry the leg through ``__reduce__`` — a pickled-state change to
    the Flink leg for the spark leg's benefit.
    """

    def __init__(self, adapter_name: str) -> None:
        self._adapter_name = adapter_name
        self._agents: dict[str, Agent] = {}

    def __reduce__(self) -> tuple[type[SparkDispatchAgent], tuple[str]]:
        return (SparkDispatchAgent, (self._adapter_name,))

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        scenario = scenario_from_key(ctx.entity_key)
        agent = self._agents.get(scenario)
        if agent is None:
            # Imported lazily: the registry pulls in the pytest-side factories,
            # which the container has on PYTHONPATH but this module should not
            # bind at import time.
            from tests.conformance._registry import ADAPTERS_BY_NAME

            spec = SCENARIOS_BY_NAME[scenario].variant_for(SPARK)
            agent = ADAPTERS_BY_NAME[self._adapter_name].build_agent(spec)
            self._agents[scenario] = agent
        return await agent(ctx)


def run_conformance_pipeline(
    adapter_name: str,
    *,
    container_spool: str,
    intents_topic: str,
    output_topic: str,
    errors_topic: str,
    job_name: str,
) -> None:
    """Build and run one adapter's spark conformance job; blocks until it ends."""
    options = PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={SPARK_JOB_ENDPOINT}",
            f"--artifact_endpoint={SPARK_ARTIFACT_ENDPOINT}",
            # EXTERNAL: the spark-scoped worker pool lives in the job server's
            # network namespace (the overlay's `network_mode: service:` bind),
            # so the executors reach it at localhost:50000 — the same
            # load-bearing pattern the Flink leg uses against the TaskManager.
            "--environment_type=EXTERNAL",
            "--environment_config=localhost:50000",
            "--parallelism=2",
            f"--checkpoint_dir={SPARK_CHECKPOINT_DIR}",
            f"--job_name={job_name}",
        ]
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        events = read_spool(pipeline, container_spool)
        keyed = events | "KeyByEntity" >> beam.WithKeys(lambda e: e.entity_key).with_output_types(
            tuple[bytes, AgentEnvelope]
        )
        out = keyed | RunAgent(
            SparkDispatchAgent(adapter_name),
            config=AgentConfig(
                provider_factory=functools.partial(merged_provider, adapter_name),
                ttl_ms=BIG_TTL_MS,
                tool_registry=merged_registry(),
            ),
        )
        publish_tagged(out.intents, intents_topic, encode_intent)
        publish_tagged(out.output, output_topic, encode_output)
        publish_tagged(out.errors, errors_topic, encode_error)
