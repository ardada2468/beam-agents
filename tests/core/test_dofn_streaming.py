"""Streaming (TestStream) tests for ordered lifecycle behavior.

Covers the scenarios that need deterministic element ordering and timer control:
per-key ordering, exactly-once SEQ, atomic commit under failure, timeout with no
state mutation, resume, TTL wipe/re-arm, and HITL fail-closed.
"""

from __future__ import annotations

from collections.abc import Iterator

import apache_beam as beam
import pytest
from apache_beam.coders.typecoders import registry as coder_registry
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.agent import intent_id_for
from beam_agents.core.dofn import HITL_TIMEOUT_OUTPUT, REASON_ORPHANED
from beam_agents.core.transform import AgentConfig, RunAgent
from tests.core._dofn_helpers import (
    append_agent,
    conditional_append_agent,
    keyed,
    make_pong_provider,
    make_slow_provider,
    seq_agent,
    suspend_then_complete_agent,
    timeout_or_append_agent,
)

# Large event-time TTL so working-memory GC never fires mid-stream unless a test
# deliberately shrinks it.
_BIG_TTL_MS = 1_000_000_000


@pytest.fixture(autouse=True)
def _restore_coder_registry() -> Iterator[None]:
    """Snapshot and restore the global coder registry around every test.

    ``RunAgent.expand`` calls ``register_coders()``, which mutates the process-
    global registry; restoring keeps that registration from leaking into later
    tests that assert import alone registers nothing.
    """
    saved = dict(coder_registry._coders)
    try:
        yield
    finally:
        coder_registry._coders = saved


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


def _result(
    key: bytes, intent_id: str, payload: bytes, t_ms: int
) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.tool_result.intent_id = intent_id
    env.tool_result.entity_key = key
    env.tool_result.payload = payload
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


# --- Requirement: monotonic SEQ incremented exactly once -----------------------


def test_seq_increments_once_per_committed_activation() -> None:
    # Scenario: one increment per committed activation (three events -> 0,1,2).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"b", 2000)])
        .add_elements([_event(b"k", b"c", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS)
        )
        assert_that(out.output, equal_to([b"0", b"1", b"2"]))


# --- Requirement: per-key ordering preserved -----------------------------------


def test_per_key_ordering_and_memory_persistence() -> None:
    # Scenario: elements commit in arrival order (single key).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"b", 2000)])
        .add_elements([_event(b"k", b"c", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # The terminal "a,b,c" is only reachable if appends applied in order.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1", b"a,b,c#2"]))


def test_interleaved_keys_preserve_per_key_order() -> None:
    # Scenario: interleaved event streams across keys via TestStream.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k1", b"a", 1000)])
        .add_elements([_event(b"k2", b"x", 1100)])
        .add_elements([_event(b"k1", b"b", 1200)])
        .add_elements([_event(b"k2", b"y", 1300)])
        .add_elements([_event(b"k1", b"c", 1400)])
        .add_elements([_event(b"k2", b"z", 1500)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(
            out.output,
            equal_to([b"a#0", b"a,b#1", b"a,b,c#2", b"x#0", b"x,y#1", b"x,y,z#2"]),
        )


# --- Requirement: atomic staged commit -----------------------------------------


def test_failed_activation_commits_nothing() -> None:
    # Scenario: failed activation commits nothing (no memory, no SEQ advance).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"FAIL", 2000)])
        .add_elements([_event(b"k", b"b", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            conditional_append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # "b" lands on the pre-failure ring (a,b) with seq 1, proving FAIL neither
        # persisted its scratch write nor advanced SEQ.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1"]))


# --- Requirement: activation timeout, no state mutation ------------------------


def test_timeout_routes_to_errors_without_mutating_state() -> None:
    # Scenario: activation timeout cancels and mutates no state; the next element
    # for the key runs on the same healthy loop and reads pre-timeout state.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"SLOW", 2000)])
        .add_elements([_event(b"k", b"b", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            timeout_or_append_agent,
            config=AgentConfig(
                provider_factory=make_slow_provider,
                activation_timeout_s=0.3,
                ttl_ms=_BIG_TTL_MS,
            ),
        )
        # "a" -> a#0; SLOW times out (no commit); "b" -> a,b#1 (seq not advanced
        # by the timeout, memory intact).
        assert_that(out.output, equal_to([b"a#0", b"a,b#1"]))


# --- Requirement: tool-result resumes the continuation -------------------------


def test_suspend_then_tool_result_resumes() -> None:
    # Scenario: tool-result resumes the matching continuation. The intent ID is
    # deterministic (key,seq,step) = (b"k", 0, 0), so the resume can target it.
    intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 1000)])
        .add_elements([_result(b"k", intent_id, b"done", 2000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            suspend_then_complete_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # Suspend emits an intent (no main output); resume completes to the echo.
        assert_that(out.output, equal_to([b"resumed:done"]), label="resumed")
        tool_names = out.intents | "tool-names" >> beam.Map(lambda i: i.tool_name)
        assert_that(tool_names, equal_to(["http.post"]), label="intent")


# --- Requirement: TTL timer wipes all state and re-arms per element -------------


def test_ttl_fire_wipes_state_and_resets_seq() -> None:
    # Scenario: TTL fire wipes every spec; the key recovers with seq 0.
    # Watermark advances are in SECONDS; event times/TTL marks are in ms. TTL for
    # "a" fires at 1100ms = 1.1s, so advancing the watermark to 1.5s trips it.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])  # arms TTL at 1.1s
        .advance_watermark_to(1.5)  # fires TTL -> wipes memory + seq
        .add_elements([_event(b"k", b"b", 2000)])  # fresh: empty ring, seq 0
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=100)
        )
        # Post-wipe "b" starts a fresh ring at seq 0 (b"b#0"), not b"a,b#1".
        assert_that(out.output, equal_to([b"a#0", b"b#0"]))


def test_new_element_rearms_ttl_and_supersedes_old_mark() -> None:
    # Scenario: a new element re-arms the TTL timer; the earlier mark does not fire.
    # Watermark advances are in SECONDS; TTL marks are in ms. "a" arms 1.1s, "b"
    # re-arms 1.15s. Advancing to 1.12s is past the superseded 1.1s mark but
    # before the live 1.15s mark, so no wipe occurs.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])  # arms TTL at 1.1s
        .add_elements([_event(b"k", b"b", 1050)])  # re-arms TTL at 1.15s
        .advance_watermark_to(1.12)  # past the superseded 1.1s mark, before 1.15s
        .add_elements([_event(b"k", b"c", 1200)])  # state intact -> a,b,c
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=100)
        )
        # No wipe occurred at 1120: "c" sees the accumulated ring and seq 2.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1", b"a,b,c#2"]))


# --- Requirement: HITL timer fails closed --------------------------------------


def test_hitl_timeout_fires_fallback_and_orphans_late_result() -> None:
    # Scenario: HITL timeout triggers fallback and clears the continuation; a
    # late result is orphaned. Suspend deadline = event_time(0) + timeout(1000ms).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, arms HITL at 1000ms
        .advance_processing_time(5)  # -> fires the real-time HITL timer
        .add_elements([_result(b"k", "late", b"late", 100)])  # continuation gone
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            suspend_then_complete_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([HITL_TIMEOUT_OUTPUT]), label="fallback")
        reasons = out.errors | "reasons" >> beam.Map(lambda e: e.reason)
        assert_that(reasons, equal_to([REASON_ORPHANED]), label="orphaned")
