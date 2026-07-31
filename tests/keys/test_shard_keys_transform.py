"""`ShardKeys` over a keyed envelope stream, and the failure it is fenced from.

Two requirements land here. The first is the transform's own contract: the KV
key and `AgentEnvelope.entity_key` must leave in agreement (a split brain would
put state under `entity#3` while the envelope claims `entity`), non-KV input
must be refused at pipeline-construction time, and the default hash assignment
must be stable under reprocessing.

The second is the memory-free-only safety contract (design D2). The runtime
cannot detect a memory-carrying agent behind `ShardKeys`, so the documented
failure mode is pinned here as observable behavior instead: the same four
events, for one logical entity, accumulate one shared working-memory ring
unsharded and two independent, divergent rings behind `ShardKeys(n=2)`.
"""

from __future__ import annotations

from collections.abc import Iterable

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_agents import AgentConfig, RunAgent, ShardKeys, unshard_key
from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.fake import FakeLLM, match_any, respond_with

LOGICAL_KEY = b"hot-entity"
# Pinned goldens, computed from the spec's derivation (SHA-256 of the payload,
# first eight digest bytes big-endian, modulo n) rather than read back out of
# the transform: `int.from_bytes(sha256(p).digest()[:8]) % 4`.
SPREAD_PAYLOADS = (b"evt-a", b"evt-b", b"hello", b"payload-49")
SPREAD_KEYS_N4 = [b"hot-entity#1", b"hot-entity#0", b"hot-entity#2", b"hot-entity#0"]
# Sixteen identical payloads all hash to one shard — the skew case.
SKEW_KEY_N4 = b"hot-entity#0"
# Payloads chosen so the n=2 assignment splits them: `e1`/`e2` -> shard 0,
# `e0`/`e3` -> shard 1 (from the same derivation).
MEMORY_EVENTS = (b"e0", b"e1", b"e2", b"e3")
SHARD0_KEY = b"hot-entity#0"
SHARD1_KEY = b"hot-entity#1"
SHARD0_EVENTS = (b"e1", b"e2")
SHARD1_EVENTS = (b"e0", b"e3")


def _envelopes(payloads: Iterable[bytes], key: bytes = LOGICAL_KEY) -> list[AgentEnvelope]:
    return [
        AgentEnvelope(entity_key=key, event_time_ms=1_000 + i, external_event=payload)
        for i, payload in enumerate(payloads)
    ]


def _keyed(
    pipeline: beam.Pipeline, payloads: Iterable[bytes], label: str
) -> beam.pvalue.PCollection:
    return (
        pipeline
        | f"create-{label}" >> beam.Create(_envelopes(payloads))
        | f"key-{label}"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )


def _pairs(element: tuple[bytes, AgentEnvelope]) -> tuple[bytes, bytes]:
    """(KV key, envelope's own entity_key) — the two that must never disagree."""
    key, envelope = element
    return key, envelope.entity_key


# --- Requirement: `ShardKeys` rewrites the physical key consistently ----------


def test_kv_key_and_envelope_key_agree_after_sharding() -> None:
    # Scenario: KV key and envelope key agree after sharding. Every output's
    # KV key equals its envelope's own `entity_key`, and both match the pinned
    # physical key for that payload.
    expected = [(key, key) for key in SPREAD_KEYS_N4]
    with BeamTestPipeline() as p:
        sharded = _keyed(p, SPREAD_PAYLOADS, "agree") | ShardKeys(4)
        assert_that(sharded | "pairs" >> beam.Map(_pairs), equal_to(expected))


def test_every_sharded_key_carries_an_in_range_suffix_and_unshards() -> None:
    # Scenario: KV key and envelope key agree after sharding — the suffix and
    # round-trip half, asserted on the collected output.
    def check(actual: list[tuple[bytes, bytes]]) -> None:
        assert len(actual) == len(SPREAD_PAYLOADS)
        for key, envelope_key in actual:
            assert key == envelope_key
            logical, _, digits = key.rpartition(b"#")
            assert logical == LOGICAL_KEY
            assert 0 <= int(digits) < 4
            assert unshard_key(key) == LOGICAL_KEY

    with BeamTestPipeline() as p:
        sharded = _keyed(p, SPREAD_PAYLOADS, "suffix") | ShardKeys(4)
        assert_that(sharded | "pairs" >> beam.Map(_pairs), check)


def test_non_kv_input_is_rejected_at_construction_time() -> None:
    # Scenario: Non-KV input is rejected at construction time — a definite
    # non-pair element type, refused before any element is processed.
    with BeamTestPipeline() as p:
        bare = p | "bare" >> beam.Create([b"not-a-kv"]).with_output_types(bytes)
        with pytest.raises(ValueError, match="ShardKeys requires a PCollection"):
            bare | ShardKeys(4)


def test_hash_assignment_is_stable_under_reprocessing() -> None:
    # Scenario: Hash assignment is stable under reprocessing — two runs over
    # the same elements produce element-for-element identical physical keys.
    # This is the retry-determinism property `intent_id` and the replay cache
    # both rest on, expressed at the granularity a test can observe: two
    # separately built and separately executed pipelines, both pinned to the
    # same golden assignment.
    for label in ("first", "second"):
        with BeamTestPipeline() as p:
            sharded = _keyed(p, SPREAD_PAYLOADS, label) | f"shard-{label}" >> ShardKeys(4)
            assert_that(
                sharded | f"keys-{label}" >> beam.Map(lambda kv: kv[0]),
                equal_to(SPREAD_KEYS_N4),
                label=f"assert-{label}",
            )


def test_round_robin_spreads_identical_payloads_across_shards() -> None:
    # Scenario: Round-robin requires an explicit opt-in that carries its
    # caveat — the behavioural half. Hash assignment cannot serve the skew
    # case: sixteen identical payloads all hash to one shard, while the
    # opt-in round-robin counter spreads them.
    identical = (b"same",) * 16

    def spread(actual: list[bytes]) -> None:
        assert len(actual) == 16
        assert len(set(actual)) > 1, f"round_robin did not spread: {set(actual)}"
        for key in actual:
            assert unshard_key(key) == LOGICAL_KEY

    def collapsed(actual: list[bytes]) -> None:
        assert set(actual) == {SKEW_KEY_N4}

    with BeamTestPipeline() as p:
        by_hash = _keyed(p, identical, "skew-hash") | "hash" >> ShardKeys(4)
        by_rr = _keyed(p, identical, "skew-rr") | "rr" >> ShardKeys(4, assignment="round_robin")
        assert_that(by_hash | "hash-keys" >> beam.Map(lambda kv: kv[0]), collapsed, label="hash")
        assert_that(by_rr | "rr-keys" >> beam.Map(lambda kv: kv[0]), spread, label="rr")


@pytest.mark.parametrize("n", [0, -1])
def test_an_invalid_shard_count_is_rejected_at_construction(n: int) -> None:
    # Scenario: A non-positive shard count is rejected — the transform-
    # construction half, before a pipeline is even wired.
    with pytest.raises(ValueError, match="shard count"):
        ShardKeys(n)


def test_an_unknown_assignment_mode_is_rejected_at_construction() -> None:
    # The `Assignment` literal catches this statically; the runtime check is
    # the backstop for callers who are not type-checked, so the deliberately
    # invalid literal is silenced here rather than removed.
    with pytest.raises(ValueError, match="assignment"):
        ShardKeys(4, assignment="random")  # type: ignore[arg-type]


def test_a_non_event_envelope_still_spreads_rather_than_collapsing() -> None:
    # Design D4 says tool results and approvals must never reach `ShardKeys` —
    # they already carry the physical key. They have no `external_event` to
    # hash, so this pins the fallback: hash the envelope's deterministic
    # serialization, which stays deterministic and does not put every such
    # element on one shard (which a `b""` payload would).
    results = [
        AgentEnvelope(
            entity_key=LOGICAL_KEY,
            event_time_ms=1_000 + i,
            tool_result=ToolResult(intent_id=f"intent-{i}", entity_key=LOGICAL_KEY, seq=i),
        )
        for i in range(12)
    ]

    def spread(actual: list[bytes]) -> None:
        assert len(actual) == 12
        assert len(set(actual)) > 1, f"non-event envelopes collapsed: {set(actual)}"
        for key in actual:
            assert unshard_key(key) == LOGICAL_KEY

    with BeamTestPipeline() as p:
        sharded = (
            p
            | "create-results" >> beam.Create(results)
            | "key-results"
            >> beam.WithKeys(lambda env: env.entity_key).with_output_types(
                tuple[bytes, AgentEnvelope]
            )
            | ShardKeys(4)
        )
        assert_that(sharded | "result-keys" >> beam.Map(lambda kv: kv[0]), spread)


# --- Requirement: the memory-free-only safety contract is explicit ------------


def _provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"ok"))])


async def remembering_agent(ctx: ActivationContext) -> Complete:
    """Appends every event to a per-key ring — exactly what must not be sharded."""
    ctx.memory.append("seen", ctx.event)
    ring = b",".join(ctx.memory.ring("seen"))
    return Complete(output=ctx.entity_key + b"|" + ring)


def _rings(outputs: list[bytes]) -> dict[bytes, set[bytes]]:
    """Widest ring observed per physical key: the last activation's view."""
    widest: dict[bytes, set[bytes]] = {}
    for line in outputs:
        key, _, ring = line.partition(b"|")
        contents = set(ring.split(b",")) if ring else set()
        if len(contents) > len(widest.get(key, set())):
            widest[key] = contents
    return widest


def test_sharding_a_memory_carrying_agent_splits_its_memory() -> None:
    # Scenario: Sharding a memory-carrying agent splits its memory. The same
    # four events, one logical entity: unsharded they accumulate into one ring;
    # behind ShardKeys(n=2) each physical key holds its own independent ring,
    # neither containing the other's writes. This is the documented reason the
    # utility is restricted to memory-free agents (design D2) — pinned as
    # behavior, since the runtime performs no detection.
    config = AgentConfig(provider_factory=_provider)

    def whole(actual: list[bytes]) -> None:
        assert _rings(actual) == {LOGICAL_KEY: set(MEMORY_EVENTS)}

    def split(actual: list[bytes]) -> None:
        rings = _rings(actual)
        assert rings == {
            SHARD0_KEY: set(SHARD0_EVENTS),
            SHARD1_KEY: set(SHARD1_EVENTS),
        }
        # Neither shard ever saw the whole entity's history.
        for contents in rings.values():
            assert not set(MEMORY_EVENTS) <= contents

    with BeamTestPipeline() as p:
        unsharded = _keyed(p, MEMORY_EVENTS, "whole") | RunAgent(remembering_agent, config=config)
        assert_that(unsharded.output, whole, label="unsharded")

    with BeamTestPipeline() as p:
        sharded = (
            _keyed(p, MEMORY_EVENTS, "split")
            | "shard" >> ShardKeys(2)
            | RunAgent(remembering_agent, config=config)
        )
        assert_that(sharded.output, split, label="sharded")
