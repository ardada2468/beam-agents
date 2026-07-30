"""The conformance Flink pipeline: one job per adapter, scenarios multiplexed
as per-scenario key prefixes (design D5).

One Flink submission per adapter, not per cell: per-submission cost (jobserver
artifact staging, TaskManager classloader churn) dominates, and the stack's
freshness machinery exists precisely because submissions degrade it. Scenario
isolation comes from the entity key — ``<scenario>|<run_id>`` — exactly the
e2e gate's key-population pattern.

Everything here runs inside the beam-sdk-harness container by module
reference: module-level functions and classes picklable as plain names, no
closures in the DoFn graph, ingest through the replayable segment spool
(cross-language Kafka IO cannot run on this stack; see
``tests/semantics/test_effectively_once_e2e.py``), egress through the
at-least-once outbox publisher.
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
from tests.conformance._adapters.adk import adk_rules
from tests.conformance._adapters.langgraph import langgraph_rules
from tests.conformance._adapters.reference import reference_rules
from tests.conformance._spec import (
    BIG_TTL_MS,
    FLINK_SCENARIOS,
    SCENARIOS_BY_NAME,
    tool_for,
)
from tests.semantics._e2e.outbox import publish_tagged
from tests.semantics._e2e.spool import read_spool
from tests.semantics._flink_stack import ARTIFACT_ENDPOINT, JOB_ENDPOINT

if TYPE_CHECKING:
    from beam_agents._protos import ToolIntent
    from beam_agents.core.agent import Agent, Outcome
    from beam_agents.core.context import ActivationContext
    from beam_agents.core.dofn import ActivationError

CHECKPOINT_INTERVAL_MS = 5_000

# Rule builders by adapter name: kept as a plain mapping (not registry entries)
# so this module never imports pytest-side machinery into the container.
_RULE_BUILDERS = {
    "reference": reference_rules,
    "langgraph": langgraph_rules,
    "adk": adk_rules,
}


def scenario_key(scenario_name: str, run_id: str) -> bytes:
    """The per-scenario entity key: the scenario is the key prefix, so every
    assertion (and every failure message) can name its cell."""
    return f"{scenario_name}|{run_id}".encode()


def scenario_from_key(entity_key: bytes) -> str:
    return entity_key.decode().split("|", 1)[0]


def merged_provider(adapter_name: str) -> FakeLLM:
    """Every Flink-runnable scenario's rules in one script: matchers are
    scoped by each scenario's unique ``model_id``, so multiplexing cannot
    cross-match."""
    rules = []
    for spec in FLINK_SCENARIOS:
        rules.extend(_RULE_BUILDERS[adapter_name](spec.flink_variant()))
    return FakeLLM(rules)


class MergedConformanceRegistry(ToolRegistry):
    """Every Flink-runnable scenario's tools, pickled as a rebuild recipe.

    A registry pickled by value carries its tools' pydantic argument models,
    whose compiled ``SchemaSerializer``s do not unpickle under the sdk-harness
    container's different pydantic-core (host 2.13/2.46 vs the image's
    langchain-capped 2.12/2.41 — workers crashloop with
    ``SchemaSerializer.__new__() takes from 1 to 2 positional arguments``).
    Rebuilding from the module-level specs worker-side sidesteps object-graph
    pickling entirely, matching the leg's everything-by-name convention.
    """

    def __init__(self) -> None:
        super().__init__()
        seen: set[str] = set()
        for spec in FLINK_SCENARIOS:
            for tool_def in spec.tools:
                if tool_def.name not in seen:
                    seen.add(tool_def.name)
                    self.register(tool_for(tool_def.name))

    def __reduce__(self) -> tuple[type[MergedConformanceRegistry], tuple[()]]:
        return (MergedConformanceRegistry, ())


def merged_registry() -> ToolRegistry:
    return MergedConformanceRegistry()


class FlinkDispatchAgent:
    """One adapter's agent for the whole multiplexed job: routes each key to
    the per-scenario agent, built lazily worker-side from the registry
    factories (picklable as the adapter name alone)."""

    def __init__(self, adapter_name: str) -> None:
        self._adapter_name = adapter_name
        self._agents: dict[str, Agent] = {}

    def __reduce__(self) -> tuple[type[FlinkDispatchAgent], tuple[str]]:
        return (FlinkDispatchAgent, (self._adapter_name,))

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        scenario = scenario_from_key(ctx.entity_key)
        agent = self._agents.get(scenario)
        if agent is None:
            # Imported lazily: the registry pulls in the pytest-side factories,
            # which the container has on PYTHONPATH but this module should not
            # bind at import time.
            from tests.conformance._registry import ADAPTERS_BY_NAME

            spec = SCENARIOS_BY_NAME[scenario].flink_variant()
            agent = ADAPTERS_BY_NAME[self._adapter_name].build_agent(spec)
            self._agents[scenario] = agent
        return await agent(ctx)


def encode_output(value: bytes) -> tuple[bytes, bytes]:
    # Terminal outputs are attributed by content (every scenario's expected
    # terminal is distinct); the message key is constant because `.output`
    # elements are plain bytes by contract.
    return b"out", bytes(value)


def encode_intent(intent: ToolIntent) -> tuple[bytes, bytes]:
    return intent.entity_key, intent.SerializeToString()


def encode_error(error: ActivationError) -> tuple[bytes, bytes]:
    return error.entity_key, error.reason.encode() + b"|" + error.detail.encode()


def run_conformance_pipeline(
    adapter_name: str,
    *,
    container_spool: str,
    intents_topic: str,
    output_topic: str,
    errors_topic: str,
    job_name: str,
) -> None:
    """Build and run one adapter's conformance job; blocks until the job ends."""
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
        events = read_spool(pipeline, container_spool)
        keyed = events | "KeyByEntity" >> beam.WithKeys(lambda e: e.entity_key).with_output_types(
            tuple[bytes, AgentEnvelope]
        )
        out = keyed | RunAgent(
            FlinkDispatchAgent(adapter_name),
            config=AgentConfig(
                provider_factory=functools.partial(merged_provider, adapter_name),
                ttl_ms=BIG_TTL_MS,
                tool_registry=merged_registry(),
            ),
        )
        publish_tagged(out.intents, intents_topic, encode_intent)
        publish_tagged(out.output, output_topic, encode_output)
        publish_tagged(out.errors, errors_topic, encode_error)
