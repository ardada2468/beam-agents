"""Unit tests for the async activation driver (``run_activation``).

Beam-free coverage of the loop step: staged blobs, outcome handling, the
replay-cache zero-extra-provider-call invariant on retry, continuation assembly,
and error propagation (which leaves nothing to commit).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

import beam_agents.core.loop as loop_module
from beam_agents._protos import AgentEnvelope, LlmCacheBlob, MemoryBlob, ToolResult, TraceEvent
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.core.loop import DEFAULT_HITL_TIMEOUT_MS, run_activation
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL, DEFAULT_INTENT_TTL_MS
from beam_agents.observability.metrics import ActivationTally
from tests.core._dofn_helpers import (
    append_agent,
    make_pong_provider,
    model_agent,
    raising_agent,
    seq_agent,
    suspend_then_act_again_agent,
    suspend_then_complete_agent,
)


def _scripted_clock(*readings_ns: int) -> Callable[[], int]:
    """Monotonic-clock double returning `readings_ns` in order, then raising."""
    remaining = iter(readings_ns)
    return lambda: next(remaining)


async def test_completed_activation_stages_output_and_seq() -> None:
    result = await run_activation(
        seq_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )
    assert result.status == "completed"
    assert result.seq == 5
    assert result.outputs == [b"5"]
    assert result.continuation is None
    assert result.hitl_deadline_ms is None
    assert result.intents == []
    assert len(result.traces) == 2
    start, end = result.traces
    assert start.entity_key == end.entity_key == b"k"
    assert start.seq == end.seq == 5
    assert start.step_index == end.step_index == 0
    assert start.event_type == TraceEvent.ACTIVATION_START
    assert end.event_type == TraceEvent.ACTIVATION_END
    assert start.start_ms == start.end_ms == 1000
    assert end.start_ms == end.end_ms == 1000


async def test_memory_write_is_staged_into_blob() -> None:
    result = await run_activation(
        append_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        event=b"a",
    )
    keys = {entry.key for entry in result.memory_blob.entries}
    assert keys == {"log"}
    assert result.outputs == [b"a#0"]


async def test_retry_incurs_zero_extra_provider_calls() -> None:
    # Scenario: replay of a retried bundle incurs zero extra provider calls.
    provider = make_pong_provider()
    first = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
    )
    assert provider.call_count == 1
    assert first.outputs == [b"pong"]
    assert len(first.cache_blob.entries) == 1

    # A retried bundle re-runs the SAME activation (same seq) against the cache
    # committed by the first run: the provider is not called again.
    replay = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=provider,
        memory_blob=None,
        cache_blob=first.cache_blob,
    )
    assert provider.call_count == 1
    assert replay.outputs == [b"pong"]


async def test_suspend_builds_continuation_and_intents() -> None:
    result = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )
    assert result.status == "suspended"
    assert result.seq == 3
    assert result.outputs == []
    assert isinstance(result.memory_blob, MemoryBlob)
    assert isinstance(result.cache_blob, LlmCacheBlob)
    assert len(result.intents) == 1
    intent = result.intents[0]
    assert intent.seq == 3
    assert result.continuation is not None
    assert list(result.continuation.pending_intent_ids) == [intent.intent_id]
    assert result.continuation.seq == 3
    assert result.continuation.state_schema_version == 1
    assert result.continuation.step_index == 1
    assert result.continuation.adapter == "test"
    assert result.continuation.snapshot == b"waiting"
    assert result.continuation.suspended_at_ms == 1000
    assert result.continuation.deadline_ms == 2000
    assert result.hitl_deadline_ms == 1000 + 1000
    assert len(result.traces) == 2
    assert result.traces[0].event_type == TraceEvent.ACTIVATION_START
    assert result.traces[1].event_type == TraceEvent.ACTIVATION_END
    assert result.traces[1].step_index == 1


async def test_resume_uses_continuation_seq_and_completes() -> None:
    resume = ToolResult(intent_id="i", entity_key=b"k", seq=3, payload=b"done")
    result = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=2000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        resume_result=resume,
    )
    assert result.status == "completed"
    assert result.outputs == [b"resumed:done"]


async def test_agent_error_propagates_and_stages_nothing() -> None:
    # The exception surfaces; the caller commits nothing, so no ActivationResult
    # (and therefore no staged blob) escapes.
    with pytest.raises(RuntimeError, match="agent blew up"):
        await run_activation(
            raising_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )


async def test_loop_forwards_every_activation_context_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    context = object()
    memory_blob = MemoryBlob(state_schema_version=1)
    cache_blob = LlmCacheBlob(state_schema_version=1)
    provider = object()
    resume_result = ToolResult(intent_id="intent-1")
    resume_approval = AgentEnvelope.Approval(intent_id="intent-1", approved=True)
    compactor = object()
    monotonic_ns = _scripted_clock()
    tool_registry = object()
    tool_runner = object()

    class FakeContext:
        def __init__(self) -> None:
            self.step_index = 0
            self.staged_intents: list[object] = []
            self.staged_traces: list[TraceEvent] = []

        def stage_trace(self, event: TraceEvent) -> None:
            self.staged_traces.append(event)

        def memory_blob(self) -> MemoryBlob:
            return memory_blob

        def cache_blob(self) -> LlmCacheBlob:
            return cache_blob

        def tally(self) -> ActivationTally:
            return ActivationTally()

    fake_context = FakeContext()

    def construct_context(**kwargs: object) -> FakeContext:
        captured.update(kwargs)
        return fake_context

    async def agent(received: object) -> Complete:
        assert received is fake_context
        assert received is not context
        return Complete(b"done")

    monkeypatch.setattr(loop_module, "ActivationContext", construct_context)

    result = await run_activation(
        agent,  # type: ignore[arg-type]
        entity_key=b"key",
        seq=4,
        now_ms=123,
        provider=provider,  # type: ignore[arg-type]
        memory_blob=memory_blob,
        cache_blob=cache_blob,
        event=b"event",
        resume_result=resume_result,
        resume_approval=resume_approval,
        snapshot=b"snapshot",
        compactor=compactor,  # type: ignore[arg-type]
        monotonic_ns=monotonic_ns,
        tool_registry=tool_registry,  # type: ignore[arg-type]
        tool_runner=tool_runner,  # type: ignore[arg-type]
    )

    # The measurement clock is forwarded like every other injected dependency,
    # so the DoFn's clock times the whole activation, model calls included.
    assert captured == {
        "entity_key": b"key",
        "seq": 4,
        "now_ms": 123,
        "provider": provider,
        "memory_blob": memory_blob,
        "cache_blob": cache_blob,
        "event": b"event",
        "resume_result": resume_result,
        "resume_approval": resume_approval,
        "snapshot": b"snapshot",
        "compactor": compactor,
        "step_index": 0,
        "intent_ttl_ms": DEFAULT_INTENT_TTL_MS,
        "approval_channel": DEFAULT_APPROVAL_CHANNEL,
        "monotonic_ns": monotonic_ns,
        "tool_registry": tool_registry,
        "tool_runner": tool_runner,
    }
    assert result.memory_blob is memory_blob
    assert result.cache_blob is cache_blob


async def test_loop_forwards_default_event_and_snapshot() -> None:
    async def agent(ctx: ActivationContext) -> Complete:
        assert ctx.event == b""
        assert ctx.snapshot == b""
        return Complete()

    result = await run_activation(
        agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert result.outputs == []


async def test_suspend_without_timeout_uses_configured_default() -> None:
    async def agent(_ctx: object) -> Suspend:
        return Suspend(snapshot=b"state", adapter="adapter")

    result = await run_activation(
        agent,  # type: ignore[arg-type]
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        default_hitl_timeout_ms=321,
    )

    assert result.hitl_deadline_ms == 1321
    assert result.continuation is not None
    assert result.continuation.deadline_ms == 1321
    assert DEFAULT_HITL_TIMEOUT_MS == 86_400_000


async def test_short_intent_expiry_shortens_the_suspension_deadline() -> None:
    # Scenario: A short intent TTL shortens the suspension deadline.
    # Waiting 24h for a result the effector will refuse after 60s is a
    # fail-open stall: the deadline is the earliest moment nothing can arrive.
    async def agent(ctx: ActivationContext) -> Suspend:
        ctx.act("http.post", "{}", ttl_ms=60_000)
        return Suspend(snapshot=b"state", adapter="adapter", timeout_ms=86_400_000)

    result = await run_activation(
        agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert result.hitl_deadline_ms == 61_000
    assert result.continuation is not None
    assert result.continuation.deadline_ms == 61_000


async def test_deadline_uses_the_earliest_expiry_across_staged_intents() -> None:
    async def agent(ctx: ActivationContext) -> Suspend:
        ctx.act("slow", "{}", ttl_ms=90_000)
        ctx.act("fast", "{}", ttl_ms=30_000)
        return Suspend(snapshot=b"state", adapter="adapter", timeout_ms=86_400_000)

    result = await run_activation(
        agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert result.hitl_deadline_ms == 31_000


async def test_suspension_with_no_intents_uses_its_timeout() -> None:
    # Scenario: A suspension with no intents uses its timeout.
    async def agent(_ctx: object) -> Suspend:
        return Suspend(snapshot=b"state", adapter="adapter", timeout_ms=5_000)

    result = await run_activation(
        agent,  # type: ignore[arg-type]
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert result.hitl_deadline_ms == 6_000


async def test_timeout_wins_when_it_is_the_earlier_bound() -> None:
    async def agent(ctx: ActivationContext) -> Suspend:
        ctx.act("http.post", "{}", ttl_ms=90_000)
        return Suspend(snapshot=b"state", adapter="adapter", timeout_ms=5_000)

    result = await run_activation(
        agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert result.hitl_deadline_ms == 6_000


# --- Requirement: the activation result carries the metric tally -------------


async def test_a_completed_activation_carries_its_tally() -> None:
    provider = make_pong_provider()
    result = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=provider,
        memory_blob=None,
        cache_blob=None,
        monotonic_ns=_scripted_clock(0, 4_000_000),
    )

    assert result.status == "completed"
    assert result.tally.llm_calls == 1
    assert result.tally.llm_ms == [4]
    assert result.tally.iterations == 1


async def test_a_suspended_activation_carries_its_tally() -> None:
    result = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert result.status == "suspended"
    # The staged intent consumed exactly one step.
    assert result.tally.iterations == 1
    assert result.tally.llm_calls == 0


async def test_a_resumed_activation_reports_only_its_own_iterations() -> None:
    resume = ToolResult(intent_id="i", entity_key=b"k", seq=3, payload=b"done")
    result = await run_activation(
        suspend_then_act_again_agent,
        entity_key=b"k",
        seq=3,
        now_ms=2000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        resume_result=resume,
        step_index=1,
    )

    assert result.status == "completed"
    assert result.tally.iterations == 1


async def test_the_tally_does_not_change_the_committed_blobs() -> None:
    # Scenario: The tally never reaches keyed state, at the driver's boundary:
    # the blobs the DoFn commits are what they would be with no measurement.
    first = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        monotonic_ns=_scripted_clock(0, 1_000_000),
    )
    second = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        monotonic_ns=_scripted_clock(0, 900_000_000),
    )

    assert first.tally.llm_ms != second.tally.llm_ms
    assert first.memory_blob.SerializeToString(
        deterministic=True
    ) == second.memory_blob.SerializeToString(deterministic=True)
    assert first.cache_blob.SerializeToString(
        deterministic=True
    ) == second.cache_blob.SerializeToString(deterministic=True)


async def test_non_outcome_error_includes_the_returned_value() -> None:
    async def agent(_ctx: object) -> str:
        return "invalid"

    with pytest.raises(
        TypeError,
        match=r"^agent returned a non-Outcome value: 'invalid'$",
    ):
        await run_activation(
            agent,  # type: ignore[arg-type]
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )
