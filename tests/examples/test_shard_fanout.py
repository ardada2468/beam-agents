"""The hot-key fan-out pipeline from `docs/sharding.md`, exercised.

`docs/sharding.md` claims that a memory-free agent behind `ShardKeys(n=4)`
spreads one hot logical key across several physical shard keys, and that
`unshard_key` reassembles the logical entity's full output set downstream.
This test holds that claim: the block between the markers below is the doc's
example, copied verbatim.

Changing one without the other is a defect: the doc is the contract this test
holds the runtime to. Keep them in sync.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_agents import AgentConfig, RunAgent, RunAgentOutputs, ShardKeys, unshard_key
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

# --- begin docs/sharding.md example (keep in sync) -----------------------------

# One logical entity, hot enough that per-key serialization is the bottleneck.
HOT_KEY = b"hot-entity"
VERDICT = b"scored"


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(VERDICT))])


async def stateless_scorer(ctx: ActivationContext) -> Complete:
    """Memory-free by construction: no `ctx.memory` read or write, no ordering
    assumption, no `ctx.act(...)` against a logically-keyed approval channel.
    Those three absences are the whole precondition for sharding this agent.
    """
    response = await ctx.call_model(
        LlmRequest(
            model_id="fake-model",
            messages=[ctx.event.decode()],
            tools_schema=None,
            sampling_params=None,
        )
    )
    # Carry the physical key on the output so the regroup below has something
    # to unshard; a real pipeline would key its sink the same way.
    return Complete(output=ctx.entity_key + b"|" + ctx.event + b"|" + response.response)


def split_output(payload: bytes) -> tuple[bytes, bytes]:
    physical_key, _, rest = payload.partition(b"|")
    return physical_key, rest


def build(pipeline: beam.Pipeline, events: list[AgentEnvelope]) -> RunAgentOutputs:
    """Key by entity, fan the hot key across four shards, then run the agent.

    `ShardKeys` goes on the events branch only — after `WithKeys`, before any
    `Flatten` with tool-results or approvals, which already carry the physical
    key from `ToolIntent.entity_key`.
    """
    keyed = (
        pipeline
        | "Events" >> beam.Create(events)
        | "KeyByEntity"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        | "Shard" >> ShardKeys(4)
    )
    return keyed | RunAgent(stateless_scorer, config=AgentConfig(provider_factory=make_provider))


def regroup(outputs: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
    """Reassemble the logical entity downstream: ordinary Beam, no runtime help."""
    return (
        outputs
        | "SplitKey" >> beam.Map(split_output)
        | "Unshard" >> beam.MapTuple(lambda key, rest: (unshard_key(key), rest))
    )


# --- end docs/sharding.md example ----------------------------------------------


EVENTS = [
    AgentEnvelope(entity_key=HOT_KEY, event_time_ms=1_000 + i, external_event=f"event-{i}".encode())
    for i in range(8)
]
# Pinned assignment for these payloads at n = 4 (SHA-256 of the payload, first
# eight digest bytes big-endian, modulo 4): 3, 1, 2, 1, 3, 1, 1, 3. Three of
# the four shards are used — the documented hash-skew caveat, visible in the
# doc's own example rather than hidden by a hand-picked payload set.
EXPECTED_SHARDS = {b"hot-entity#1", b"hot-entity#2", b"hot-entity#3"}


# --- Requirement: the throughput math's worked example is held by a test -------


def test_the_documented_fan_out_example_runs_as_written() -> None:
    # Scenario: The documented fan-out example runs as written — the outputs
    # span multiple physical shard keys for the one logical key.
    def spans_shards(actual: list[tuple[bytes, bytes]]) -> None:
        assert len(actual) == len(EVENTS)
        assert {key for key, _ in actual} == EXPECTED_SHARDS

    with BeamTestPipeline() as p:
        outputs = build(p, EVENTS)
        assert_that(outputs.output | "Split" >> beam.Map(split_output), spans_shards)


def test_regrouping_reassembles_the_logical_keys_full_output_set() -> None:
    # Scenario: The documented fan-out example runs as written — the regroup
    # half. `unshard_key` puts every shard's output back under the one logical
    # key, and nothing is lost or duplicated in the fan-out.
    expected = [(HOT_KEY, f"event-{i}".encode() + b"|" + VERDICT) for i in range(8)]
    with BeamTestPipeline() as p:
        outputs = build(p, EVENTS)
        assert_that(regroup(outputs.output), equal_to(expected))
