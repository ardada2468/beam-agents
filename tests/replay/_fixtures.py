"""Replay fixtures: an original activation, its trace, and a post-hoc snapshot.

The replay CLI's inputs are a `StateSnapshot`, a framed `TraceEvent` stream, and
the triggering `AgentEnvelope`. These helpers produce all three the way the
runtime does — by running `run_activation` and taking the committed blobs — so
the unit suites replay against real bytes rather than hand-built ones.

The agents here are module-level (importable by `module:attribute`, which is how
the CLI loads them) and picklable, so the semantics gate can run the same agent
inside a `TestPipeline`.

Not collected by pytest (module name doesn't match ``test_*``).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from beam_agents._protos import (
    AgentEnvelope,
    LlmCacheBlob,
    MemoryBlob,
    StateSnapshot,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import REASON_ERROR, _error_trace
from beam_agents.core.loop import ActivationFailed, ActivationResult, run_activation
from beam_agents.core.snapshot import build_snapshot
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

KEY = b"entity-1"
NOW_MS = 1_700_000_000_000
SEQ = 3
TTL_MS = 60_000
REQUEST_ID = "req-1"


def make_provider() -> FakeLLM:
    """Provider answering every request with ``b"pong"``, no latency."""
    return FakeLLM([(match_any(), respond_with(b"pong"))])


def request(text: str = "hello") -> LlmRequest:
    return LlmRequest(model_id="m", messages=[text], tools_schema=None, sampling_params=None)


# -- agents -------------------------------------------------------------------


async def exact_replay_agent(ctx: ActivationContext) -> Complete:
    """The exactly-replayable shape (design D2): read the event, call the model,
    emit an intent, and write memory only *after* every call.

    Nothing it reads from memory is anything it wrote, so the post-commit
    snapshot's memory blob feeds the same cache keys the original computed.
    """
    response = await ctx.call_model(request())
    ctx.act("http.post", '{"url":"x"}', ttl_ms=TTL_MS)
    ctx.memory.append("log", ctx.single_event, max_items=64)
    return Complete(output=response.response + b":" + ctx.single_event)


async def failing_agent(ctx: ActivationContext) -> Complete:
    """Raise in agent logic before any provider-reached call (design D2 row 2)."""
    raise RuntimeError("agent blew up")


async def suspending_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Suspend with an intent on the first activation; complete on resume.

    The resume makes no provider-reached call, so it replays exactly against a
    snapshot exported *while the key was suspended* — the one pre-image a
    post-hoc export can hand a resume (design D2, "pending resume" row).
    """
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=600_000)
    assert ctx.resume_result is not None
    return Complete(output=b"resumed:" + ctx.resume_result.payload)


# -- original attempts --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Original:
    """One original activation: what it staged, and what an export would see."""

    result: ActivationResult
    traces: list[TraceEvent]
    snapshot: StateSnapshot
    envelope: AgentEnvelope


def _snapshot_of(
    result: ActivationResult,
    *,
    seq_counter: int,
    snapshot_at_ms: int,
    memory_blob: MemoryBlob | None = None,
) -> StateSnapshot:
    return build_snapshot(
        entity_key=KEY,
        seq=seq_counter,
        snapshot_at_ms=snapshot_at_ms,
        request_id=REQUEST_ID,
        memory_blob=memory_blob if memory_blob is not None else result.memory_blob,
        cache_blob=result.cache_blob,
        continuation=result.continuation,
        pending=result.intents,
    )


def run_original(
    agent: object = exact_replay_agent,
    *,
    event: bytes = b"go",
    seq: int = SEQ,
    now_ms: int = NOW_MS,
    memory_blob: MemoryBlob | None = None,
    cache_blob: LlmCacheBlob | None = None,
) -> Original:
    """Run one activation and package it as the replay CLI's three inputs."""
    result = asyncio.run(
        run_activation(
            agent,  # type: ignore[arg-type]
            entity_key=KEY,
            seq=seq,
            now_ms=now_ms,
            provider=make_provider(),
            memory_blob=memory_blob,
            cache_blob=cache_blob,
            event=event,
        )
    )
    # A committed activation increments SEQ, so a snapshot taken after it reads
    # the next counter value; the target seq comes from the trace, not here.
    snapshot = _snapshot_of(result, seq_counter=seq + 1, snapshot_at_ms=now_ms + 1_000)
    envelope = AgentEnvelope(entity_key=KEY, event_time_ms=now_ms, external_event=event)
    return Original(result, list(result.traces), snapshot, envelope)


def run_original_resume(
    *, seq: int = SEQ, now_ms: int = NOW_MS, resume_ms: int | None = None
) -> Original:
    """Suspend, then resume — the snapshot is taken while suspended.

    The snapshot therefore carries the live `Continuation` and pending intent,
    and the resume's own trace events are what replay must reproduce.
    """
    resume_at_ms = resume_ms if resume_ms is not None else now_ms + 10_000
    suspended = asyncio.run(
        run_activation(
            suspending_agent,
            entity_key=KEY,
            seq=seq,
            now_ms=now_ms,
            provider=make_provider(),
            memory_blob=None,
            cache_blob=None,
            event=b"go",
        )
    )
    assert suspended.continuation is not None
    intent_id = suspended.intents[0].intent_id
    resume_result = ToolResult(
        intent_id=intent_id, entity_key=KEY, seq=seq, status=ToolResult.OK, payload=b"done"
    )
    resumed = asyncio.run(
        run_activation(
            suspending_agent,
            entity_key=KEY,
            seq=seq,
            now_ms=resume_at_ms,
            provider=make_provider(),
            memory_blob=suspended.memory_blob,
            cache_blob=suspended.cache_blob,
            resume_result=resume_result,
            snapshot=suspended.continuation.snapshot,
            step_index=suspended.continuation.step_index,
        )
    )
    # The export is issued while the key is suspended: the snapshot carries the
    # suspension's blobs, its live `Continuation`, and its pending intent —
    # which is exactly the resume's own pre-image.
    snapshot = _snapshot_of(suspended, seq_counter=seq, snapshot_at_ms=now_ms + 1_000)
    envelope = AgentEnvelope(entity_key=KEY, event_time_ms=resume_at_ms, tool_result=resume_result)
    return Original(resumed, list(resumed.traces), snapshot, envelope)


def run_original_failure(*, seq: int = SEQ, now_ms: int = NOW_MS) -> Original:
    """Run an activation that raises, and build the record the DoFn emits.

    A failed activation commits nothing, so its staged traces are discarded and
    `.traces` carries only the synthesized `ERROR` event — built here through
    the DoFn's own `_error_trace`, so the fixture cannot drift from production.
    """
    try:
        asyncio.run(
            run_activation(
                failing_agent,
                entity_key=KEY,
                seq=seq,
                now_ms=now_ms,
                provider=make_provider(),
                memory_blob=None,
                cache_blob=None,
                event=b"go",
            )
        )
    except ActivationFailed as failed:
        cause = failed.__cause__ if failed.__cause__ is not None else failed
        error_event = _error_trace(
            KEY,
            seq,
            now_ms,
            REASON_ERROR,
            error_type=type(cause).__name__,
            failure=failed.context,
        ).value
    else:  # pragma: no cover - the agent always raises
        raise AssertionError("failing_agent did not fail")

    # Invariant 1: the failed attempt committed nothing, so a snapshot taken
    # after it is the exact pre-image the replay needs.
    snapshot = build_snapshot(
        entity_key=KEY,
        seq=seq,
        snapshot_at_ms=now_ms + 1_000,
        request_id=REQUEST_ID,
        memory_blob=None,
        cache_blob=None,
        continuation=None,
        pending=(),
    )
    envelope = AgentEnvelope(entity_key=KEY, event_time_ms=now_ms, external_event=b"go")
    empty = ActivationResult(
        status="completed",
        seq=seq,
        memory_blob=MemoryBlob(),
        cache_blob=LlmCacheBlob(),
        intents=[],
        traces=[],
        outputs=[],
        continuation=None,
        hitl_deadline_ms=None,
    )
    return Original(empty, [error_event], snapshot, envelope)
