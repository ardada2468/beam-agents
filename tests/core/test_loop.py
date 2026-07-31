"""Unit tests for the async activation driver (``run_activation``).

Beam-free coverage of the loop step: staged blobs, outcome handling, the
replay-cache zero-extra-provider-call invariant on retry, continuation assembly,
and error propagation (which leaves nothing to commit).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import beam_agents.core.loop as loop_module
from beam_agents._protos import AgentEnvelope, LlmCacheBlob, MemoryBlob, ToolResult, TraceEvent
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.batching import TRACE_BATCH_SIZE, TRACE_BATCH_TRIGGER, TRIGGER_SIZE
from beam_agents.core.context import ActivationContext
from beam_agents.core.loop import (
    DEFAULT_HITL_TIMEOUT_MS,
    ActivationFailed,
    FailureContext,
    run_activation,
)
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL, DEFAULT_INTENT_TTL_MS
from beam_agents.model import BudgetExceeded
from beam_agents.observability import (
    ROLE_ACTIVATION,
    ActivationTrace,
    span_id_for,
    trace_id_for,
)
from beam_agents.observability.metrics import ActivationTally
from tests.core._context_helpers import decode_len_based
from tests.core._dofn_helpers import (
    append_agent,
    batch_join_agent,
    make_pong_provider,
    model_agent,
    raising_agent,
    request,
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


# --- Requirement: Activation span with start, end, and outcome ---------------


async def test_the_initial_activation_span_is_the_trace_root() -> None:
    # Scenario: The initial activation span is the trace root.
    # Scenario: A completing activation brackets its work.
    result = await run_activation(
        seq_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    start, end = result.traces
    assert start.trace_id == trace_id_for(b"k", 5)
    assert start.span_id == span_id_for(b"k", 5, ROLE_ACTIVATION, 0)
    assert start.parent_span_id == b""
    assert end.span_id == start.span_id
    assert end.attributes["beam_agents.activation.status"] == "completed"
    assert start.attributes["beam_agents.activation.kind"] == "start"


async def test_a_resumed_attempt_shares_the_trace_and_parents_to_the_root() -> None:
    # Scenario: A resume shares the suspended activation's trace.
    # Scenario: A resumed attempt is a child of the initial attempt.
    # Scenario: A resumed activation is labelled as a resume.
    suspended = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )
    assert suspended.continuation is not None

    resumed = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=suspended.continuation.seq,
        now_ms=2000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        resume_result=ToolResult(intent_id="i", entity_key=b"k", seq=3, payload=b"done"),
        snapshot=suspended.continuation.snapshot,
        step_index=suspended.continuation.step_index,
    )

    root_start = suspended.traces[0]
    resumed_start = resumed.traces[0]
    assert resumed_start.trace_id == root_start.trace_id
    assert resumed_start.span_id != root_start.span_id
    assert resumed_start.parent_span_id == root_start.span_id
    assert resumed_start.attributes["beam_agents.activation.kind"] == "resume"


async def test_a_suspension_records_its_deadline_and_adapter() -> None:
    # Scenario: A suspension records its deadline and adapter.
    result = await run_activation(
        suspend_then_complete_agent,
        entity_key=b"k",
        seq=3,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    (suspended,) = [e for e in result.traces if e.event_type == TraceEvent.SUSPENDED]
    assert suspended.attributes["beam_agents.deadline_ms"] == "2000"
    assert suspended.attributes["beam_agents.adapter"] == "test"
    assert suspended.attributes["beam_agents.pending_intent_ids"] == result.intents[0].intent_id
    # A child of the activation span, not a second root.
    assert suspended.parent_span_id == result.traces[0].span_id
    # The step the activation reached, so the suspension orders after the
    # intent it is waiting on rather than collapsing to the start.
    assert suspended.step_index == 1
    assert result.traces[-1].step_index == 1


async def test_a_completing_activation_ends_at_the_step_it_reached() -> None:
    # `model_agent` consumes one step, so a zeroed step_index on the terminal
    # event would misorder it against the LLM_CALL it follows.
    result = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    end = result.traces[-1]
    assert end.event_type == TraceEvent.ACTIVATION_END
    assert end.step_index == 1


async def test_the_provider_decode_reaches_the_activation_context() -> None:
    # Scenario: A cache-hit call reports the stored response's real token
    # counts -- through the driver, which is the path the DoFn actually uses.
    # Without the decode threaded through, the counts would be silently absent.
    result = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        decode=decode_len_based,
    )

    (llm_call,) = [e for e in result.traces if e.event_type == TraceEvent.LLM_CALL]
    assert llm_call.attributes["gen_ai.usage.input_tokens"] == str(len(b"pong"))
    assert llm_call.attributes["beam_agents.billed"] == "true"


async def test_without_a_decode_usage_is_absent_rather_than_zero() -> None:
    result = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    (llm_call,) = [e for e in result.traces if e.event_type == TraceEvent.LLM_CALL]
    assert "gen_ai.usage.input_tokens" not in llm_call.attributes


async def test_every_event_in_one_activation_shares_the_trace() -> None:
    # Scenario: Traces flow through a pipeline end to end (loop-level half).
    result = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=5,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    assert {event.trace_id for event in result.traces} == {trace_id_for(b"k", 5)}
    assert TraceEvent.LLM_CALL in {event.event_type for event in result.traces}


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
    # Start, the intent the agent staged, the suspension, then the end.
    assert [event.event_type for event in result.traces] == [
        TraceEvent.ACTIVATION_START,
        TraceEvent.INTENT_EMITTED,
        TraceEvent.SUSPENDED,
        TraceEvent.ACTIVATION_END,
    ]
    assert result.traces[-1].step_index == 1
    assert result.traces[-1].attributes["beam_agents.activation.status"] == "suspended"


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
    # The failure surfaces (wrapped, with the original attached as the cause);
    # the caller commits nothing, so no ActivationResult (and therefore no
    # staged blob) escapes.
    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            raising_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == "agent blew up"


# --- Requirement: agent failures carry their position out (add-failure-context)


async def test_an_agent_failure_is_wrapped_with_its_position() -> None:
    # Scenario: A raising activation is traced with its error type and failure
    # position (loop-level half): one provider-reached call (step 0), one
    # staged intent (step 1), then the raise — so the cursor reads 2 and the
    # last staged event is the intent's.
    async def agent(ctx: ActivationContext) -> Complete:
        await ctx.call_model(request())
        ctx.act("http.post", '{"url":"x"}', ttl_ms=60_000)
        raise RuntimeError("agent blew up")

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )

    failure = excinfo.value
    assert isinstance(failure.__cause__, RuntimeError)
    assert str(failure.__cause__) == "agent blew up"
    assert failure.context == FailureContext(
        step_index=2, last_event="INTENT_EMITTED", staged_intents=1, llm_calls=1
    )
    # The wrapper's own message names the position: it is what a log shows if
    # the exception ever escapes the DoFn's handling.
    assert str(failure) == "activation failed at step 2 after INTENT_EMITTED"


async def test_a_cache_hit_does_not_count_toward_the_failure_llm_calls() -> None:
    # `llm_calls` is provider-reached calls, matching the metrics vocabulary: a
    # replayed activation that fails after a cache hit reports zero, though the
    # hit still consumed a step and staged its LLM_CALL trace.
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

    async def agent(ctx: ActivationContext) -> Complete:
        await ctx.call_model(request())
        raise RuntimeError("after the hit")

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=provider,
            memory_blob=None,
            cache_blob=first.cache_blob,
        )

    assert provider.call_count == 1
    assert excinfo.value.context == FailureContext(
        step_index=1, last_event="LLM_CALL", staged_intents=0, llm_calls=0
    )


async def test_a_budget_trip_propagates_as_an_activation_failure() -> None:
    # Scenario: The budget kill produces both enriched records (the loop half).
    # `BudgetExceeded` is an ordinary agent-path raise, so the existing wrap
    # delivers it as `ActivationFailed` with its position -- no new machinery,
    # and the cause is preserved for the DoFn's reason dispatch.
    async def agent(ctx: ActivationContext) -> Complete:
        await ctx.call_model(request())
        ctx.act("http.post", '{"url":"x"}', ttl_ms=60_000)
        await ctx.call_model(request("second"))
        return Complete(output=b"unreachable")

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
            decode=decode_len_based,
            max_tokens_per_activation=10,
        )

    failure = excinfo.value
    assert isinstance(failure.__cause__, BudgetExceeded)
    # b"pong" decodes to 2 * 4 = 8 tokens; the second call crosses 10 at 16.
    assert failure.__cause__.limit == 10
    assert failure.__cause__.consumed == 16
    assert failure.context == FailureContext(
        step_index=3, last_event="LLM_CALL", staged_intents=1, llm_calls=2
    )


async def test_an_activation_within_its_budget_completes_normally() -> None:
    # The knob only fails what crosses it: an agent that stays inside its bound
    # commits exactly what it would with no budget configured.
    result = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        decode=decode_len_based,
        max_tokens_per_activation=1_000,
    )

    assert result.status == "completed"
    assert result.outputs == [b"pong"]


async def test_an_unbudgeted_run_activation_is_unchanged() -> None:
    # Scenario: Unset means unlimited. The parameter defaults to `None`, so
    # every historical call site still builds and behaves identically.
    unbudgeted = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        decode=decode_len_based,
    )
    budgeted = await run_activation(
        model_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        decode=decode_len_based,
        max_tokens_per_activation=1_000,
    )

    assert unbudgeted.memory_blob.SerializeToString(
        deterministic=True
    ) == budgeted.memory_blob.SerializeToString(deterministic=True)
    assert unbudgeted.cache_blob.SerializeToString(
        deterministic=True
    ) == budgeted.cache_blob.SerializeToString(deterministic=True)


async def test_a_budgeted_suspension_persists_an_unchanged_continuation() -> None:
    # Scenario: The continuation is unchanged by budgeting. The meter is
    # worker-local and per-attempt: no `Continuation` field, no state-schema
    # implication, no golden blob moved.
    async def agent(ctx: ActivationContext) -> Suspend:
        await ctx.call_model(request())
        ctx.act("http.post", '{"url":"x"}', ttl_ms=60_000)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=1_000)

    async def run(limit: int | None) -> bytes:
        result = await run_activation(
            agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
            decode=decode_len_based,
            max_tokens_per_activation=limit,
        )
        assert result.continuation is not None
        return result.continuation.SerializeToString(deterministic=True)

    assert await run(None) == await run(10_000)


async def test_cancellation_is_never_wrapped() -> None:
    # Scenario: Cancellation is never wrapped. `CancelledError` is how the
    # bridge's timeout cancellation completes; wrapping it would corrupt the
    # cancellation semantics, so only `Exception` is caught.
    async def agent(_ctx: object) -> Complete:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_activation(
            agent,  # type: ignore[arg-type]
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )


async def test_a_failure_before_the_start_event_reports_an_empty_last_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The wrap window opens before ACTIVATION_START is staged: a failure in
    # that sliver (here: the trace surface itself raising) has no last event,
    # and the context reports "" rather than a fabricated kind.
    class NoTraceContext:
        def __init__(self, **_kwargs: object) -> None:
            self.step_index = 0
            self.staged_intents: list[object] = []
            self.staged_traces: list[TraceEvent] = []
            # No long-term store is configured, so the commit-tail flush is a
            # no-op: nothing staged, nothing to flush.
            self.staged_upserts: tuple[object, ...] = ()
            self.longterm_store: object | None = None

        @property
        def trace(self) -> ActivationTrace:
            raise RuntimeError("trace surface unavailable")

        def tally(self) -> ActivationTally:
            return ActivationTally()

    monkeypatch.setattr(loop_module, "ActivationContext", NoTraceContext)

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            seq_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )

    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert excinfo.value.context == FailureContext(
        step_index=0, last_event="", staged_intents=0, llm_calls=0
    )


async def test_a_failure_before_any_staging_reports_the_activation_start() -> None:
    # The driver stages ACTIVATION_START before the agent runs, so even an
    # agent that raises immediately has a position: step 0, after the start.
    async def agent(_ctx: object) -> Complete:
        raise ValueError("early")

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            agent,  # type: ignore[arg-type]
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )

    assert isinstance(excinfo.value.__cause__, ValueError)
    assert excinfo.value.context == FailureContext(
        step_index=0, last_event="ACTIVATION_START", staged_intents=0, llm_calls=0
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
            # Not a batch entry: this scenario forwards the per-event inputs,
            # so the driver must stamp no batch attributes on the trace.
            self.is_batch = False
            self.trace = ActivationTrace(entity_key=b"key", seq=4, now_ms=123)
            self.staged_intents: list[object] = []
            self.staged_traces: list[TraceEvent] = []
            # No long-term store is configured, so the commit-tail flush is a
            # no-op: nothing staged, nothing to flush.
            self.staged_upserts: tuple[object, ...] = ()
            self.longterm_store: object | None = None

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
        "events": None,
        "resume_result": resume_result,
        "resume_approval": resume_approval,
        "snapshot": b"snapshot",
        "compactor": compactor,
        "step_index": 0,
        "intent_ttl_ms": DEFAULT_INTENT_TTL_MS,
        "approval_channel": DEFAULT_APPROVAL_CHANNEL,
        "decode": None,
        "monotonic_ns": monotonic_ns,
        "tool_registry": tool_registry,
        "tool_runner": tool_runner,
        # Forwarded like every other injected dependency; `None` here, so the
        # context builds no long-term handle and the commit tail flushes nothing.
        "longterm_store": None,
        # `None` is unlimited: an unconfigured pipeline builds no meter and
        # behaves byte-identically to the pre-budget runtime.
        "max_tokens_per_activation": None,
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
    # The defensive TypeError is raised inside the wrap window (everything
    # after context construction), so it arrives wrapped like an agent raise.
    async def agent(_ctx: object) -> str:
        return "invalid"

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            agent,  # type: ignore[arg-type]
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
        )
    cause = excinfo.value.__cause__
    assert isinstance(cause, TypeError)
    assert str(cause) == "agent returned a non-Outcome value: 'invalid'"


# --- Requirement: Batch activations are batch-visible with ctx.event as a list


async def test_a_batch_entry_reaches_the_agent_as_a_list_in_arrival_order() -> None:
    # Scenario: The agent receives the batch as a list in arrival order. The
    # driver is the seam the DoFn's flush path enters through, so the batch has
    # to survive it unchanged and in order.
    result = await run_activation(
        batch_join_agent,
        entity_key=b"k",
        seq=2,
        now_ms=3000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        events=[b"a", b"b", b"c"],
    )

    assert result.status == "completed"
    assert result.outputs == [b"a|b|c#2"]


async def test_a_batch_activation_stamps_its_size_and_trigger_on_the_trace() -> None:
    # Scenario: One activation per flush ... the flush activation's trace SHALL
    # carry `beam_agents.batch.size` and `beam_agents.batch.trigger`, so a trace
    # consumer can tell a batch decision from a per-event one.
    result = await run_activation(
        batch_join_agent,
        entity_key=b"k",
        seq=0,
        now_ms=3000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        events=[b"a", b"b"],
        batch_trigger=TRIGGER_SIZE,
    )

    start = result.traces[0]
    assert start.event_type == TraceEvent.ACTIVATION_START
    assert start.attributes[TRACE_BATCH_SIZE] == "2"
    assert start.attributes[TRACE_BATCH_TRIGGER] == TRIGGER_SIZE


async def test_a_per_event_activation_carries_no_batch_attributes() -> None:
    # Under `NONE` nothing about batching appears anywhere: no attribute, no
    # empty string, nothing for a trace consumer to have to ignore.
    result = await run_activation(
        seq_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )

    start = result.traces[0]
    assert TRACE_BATCH_SIZE not in start.attributes
    assert TRACE_BATCH_TRIGGER not in start.attributes
