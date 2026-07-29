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
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.hitl import (
    DEFAULT_APPROVAL_CHANNEL,
    DEFAULT_HITL_TIMEOUT_MS,
    DEFAULT_INTENT_TTL_MS,
)

if TYPE_CHECKING:
    from beam_agents.core.agent import Agent
    from beam_agents.memory.facade import Compactor
    from beam_agents.model.client import LLMClient
    from beam_agents.model.facade import Decode

# `DEFAULT_HITL_TIMEOUT_MS` is re-exported from `hitl` (its home, alongside the
# rest of the HITL policy defaults) so the historical import site keeps working.
__all__ = ["DEFAULT_HITL_TIMEOUT_MS", "ActivationResult", "run_activation"]


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
    resume_approval: AgentEnvelope.Approval | None = None,
    snapshot: bytes = b"",
    compactor: Compactor | None = None,
    default_hitl_timeout_ms: int = DEFAULT_HITL_TIMEOUT_MS,
    step_index: int = 0,
    intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS,
    approval_channel: str = DEFAULT_APPROVAL_CHANNEL,
    decode: Decode | None = None,
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
        step_index=step_index,
        intent_ttl_ms=intent_ttl_ms,
        approval_channel=approval_channel,
        decode=decode,
    )

    # The attempt's activation span, taken from the context rather than rebuilt:
    # a second `ActivationTrace` with the same inputs would be a second source of
    # truth for identity, free to drift from the one the child events use. A
    # resume shares the suspended activation's `seq`, so it recomputes the same
    # trace ID and hangs its own span under the initial attempt's (design D2).
    trace = ctx.trace
    ctx.stage_trace(trace.activation_start())

    outcome = await agent(ctx)

    if isinstance(outcome, Suspend):
        timeout_deadline_ms = now_ms + (
            outcome.timeout_ms if outcome.timeout_ms is not None else default_hitl_timeout_ms
        )
        # The earliest of the suspension timeout and the staged intents'
        # expiries. Past an intent's expiry the effector refuses it, so no
        # result can ever arrive; waiting out a longer timeout would be a
        # fail-open stall. Both layers then agree on one moment after which
        # nothing is resumable.
        deadline_ms = min(
            [timeout_deadline_ms] + [intent.expires_at_ms for intent in ctx.staged_intents]
        )
        continuation = ctx.build_continuation(
            snapshot=outcome.snapshot,
            adapter=outcome.adapter,
            deadline_ms=deadline_ms,
        )
        # What the suspension is waiting on, and until when: the state an
        # operator most wants to see, so it gets its own event rather than
        # living only in an ACTIVATION_END attribute (design D6).
        ctx.stage_trace(
            trace.suspended(
                step_index=ctx.step_index,
                deadline_ms=deadline_ms,
                adapter=outcome.adapter,
                pending_intent_ids=tuple(continuation.pending_intent_ids),
            )
        )
        ctx.stage_trace(trace.activation_end(status="suspended", step_index=ctx.step_index))
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

    ctx.stage_trace(trace.activation_end(status="completed", step_index=ctx.step_index))
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
