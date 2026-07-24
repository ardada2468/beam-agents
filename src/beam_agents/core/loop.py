"""The async activation driver: run one agent activation, stage its effects.

This is pure with respect to Beam state — it never reads or writes keyed state or
a wall clock. It builds an :class:`~beam_agents.core.context.ActivationContext`
from the loaded state blobs, runs the agent, and returns an
:class:`ActivationResult` holding the staged blobs, intents, traces, and outcome.
The stateful DoFn submits :func:`run_activation` to the async bridge and, on
success, commits the result atomically. An exception propagates out unchanged so
the DoFn can route the element to ``.errors`` having committed nothing.

Importing this module has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from beam_agents._protos import (
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext

if TYPE_CHECKING:
    from beam_agents.core.agent import Agent
    from beam_agents.memory.facade import Compactor
    from beam_agents.model.client import LLMClient

# Default HITL deadline when a Suspend omits an explicit timeout_ms (24h). Real
# approvals are wall-clock bound; the value only sets when the fail-closed HITL
# timer fires, never how the activation runs.
DEFAULT_HITL_TIMEOUT_MS = 86_400_000


@dataclass(frozen=True, slots=True)
class ActivationResult:
    """The staged, all-or-nothing result of one activation.

    Every field is applied to Beam state in a fixed order by the DoFn commit; a
    failed activation never produces one of these (the exception propagates
    instead), so there is nothing to partially apply.
    """

    status: Literal["completed", "suspended"]
    seq: int
    memory_blob: MemoryBlob
    cache_blob: LlmCacheBlob
    intents: list[ToolIntent]
    traces: list[TraceEvent]
    outputs: list[bytes]
    continuation: Continuation | None
    hitl_deadline_ms: int | None


async def run_activation(
    agent: Agent,
    *,
    entity_key: bytes,
    seq: int,
    now_ms: int,
    provider: LLMClient,
    memory_blob: MemoryBlob | None,
    cache_blob: LlmCacheBlob | None,
    event: bytes = b"",
    resume_result: ToolResult | None = None,
    resume_approval: object | None = None,
    snapshot: bytes = b"",
    compactor: Compactor | None = None,
    default_hitl_timeout_ms: int = DEFAULT_HITL_TIMEOUT_MS,
) -> ActivationResult:
    """Run one activation to a terminal :class:`ActivationResult`.

    Raises whatever the agent raises (or a provider error); the caller commits
    nothing on failure, preserving the atomic-commit invariant.
    """
    ctx = ActivationContext(
        entity_key=entity_key,
        seq=seq,
        now_ms=now_ms,
        provider=provider,
        memory_blob=memory_blob,
        cache_blob=cache_blob,
        event=event,
        resume_result=resume_result,
        resume_approval=resume_approval,
        snapshot=snapshot,
        compactor=compactor,
    )

    ctx.stage_trace(
        TraceEvent(
            entity_key=entity_key,
            seq=seq,
            event_type=TraceEvent.ACTIVATION_START,
            start_ms=now_ms,
            end_ms=now_ms,
        )
    )

    outcome = await agent(ctx)

    ctx.stage_trace(
        TraceEvent(
            entity_key=entity_key,
            seq=seq,
            step_index=ctx.step_index,
            event_type=TraceEvent.ACTIVATION_END,
            start_ms=now_ms,
            end_ms=now_ms,
        )
    )

    if isinstance(outcome, Suspend):
        deadline_ms = now_ms + (
            outcome.timeout_ms if outcome.timeout_ms is not None else default_hitl_timeout_ms
        )
        continuation = ctx.build_continuation(
            snapshot=outcome.snapshot,
            adapter=outcome.adapter,
            deadline_ms=deadline_ms,
        )
        return ActivationResult(
            status="suspended",
            seq=seq,
            memory_blob=ctx.memory_blob(),
            cache_blob=ctx.cache_blob(),
            intents=list(ctx.staged_intents),
            traces=list(ctx.staged_traces),
            outputs=[],
            continuation=continuation,
            hitl_deadline_ms=deadline_ms,
        )

    if not isinstance(outcome, Complete):  # pragma: no cover - defensive
        raise TypeError(f"agent returned a non-Outcome value: {outcome!r}")

    outputs = [outcome.output] if outcome.output else []
    return ActivationResult(
        status="completed",
        seq=seq,
        memory_blob=ctx.memory_blob(),
        cache_blob=ctx.cache_blob(),
        intents=list(ctx.staged_intents),
        traces=list(ctx.staged_traces),
        outputs=outputs,
        continuation=None,
        hitl_deadline_ms=None,
    )
