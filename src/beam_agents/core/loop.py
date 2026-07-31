"""The async activation driver: run one agent activation, stage its effects.

This is pure with respect to Beam state — it never reads or writes keyed state or
a wall clock. It builds an :class:`~beam_agents.core.context.ActivationContext`
from the loaded state blobs, runs the agent, and returns an
:class:`ActivationResult` holding the staged blobs, intents, traces, and outcome.
The stateful DoFn submits :func:`run_activation` to the async bridge and, on
success, commits the result atomically. An agent failure propagates as
:class:`ActivationFailed` — the original exception attached as the cause, plus a
:class:`FailureContext` naming where the activation was — so the DoFn can route
the element to ``.errors`` having committed nothing, without losing the
position. ``CancelledError`` and other ``BaseException``s pass through
unwrapped.

Importing this module has no side effects.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
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
from beam_agents.core.batching import TRACE_BATCH_SIZE, TRACE_BATCH_TRIGGER
from beam_agents.core.context import ActivationContext, MonotonicNs
from beam_agents.hitl import (
    DEFAULT_APPROVAL_CHANNEL,
    DEFAULT_HITL_TIMEOUT_MS,
    DEFAULT_INTENT_TTL_MS,
)
from beam_agents.observability.metrics import ActivationTally
from beam_agents.observability.traces import (
    FAILURE_LAST_EVENT,
    FAILURE_LLM_CALLS,
    FAILURE_STAGED_INTENTS,
    FAILURE_STEP,
)

if TYPE_CHECKING:
    from beam_agents.core.agent import Agent
    from beam_agents.memory.compaction import Summarizer
    from beam_agents.memory.facade import Compactor
    from beam_agents.memory.stores import MemoryRecord, MemoryStore
    from beam_agents.model.client import LLMClient
    from beam_agents.model.facade import Decode
    from beam_agents.tools.registry import ToolRegistry
    from beam_agents.tools.runner import ToolRunner

# `DEFAULT_HITL_TIMEOUT_MS` is re-exported from `hitl` (its home, alongside the
# rest of the HITL policy defaults) so the historical import site keeps working.
__all__ = [
    "DEFAULT_HITL_TIMEOUT_MS",
    "ActivationFailed",
    "ActivationResult",
    "FailureContext",
    "LongtermFlushFailed",
    "run_activation",
]


class LongtermFlushFailed(Exception):
    """The commit-tail long-term flush raised; the activation fails closed.

    Raised ``from`` the store's own error and immediately re-wrapped as
    :class:`ActivationFailed`, so the DoFn routes the element to ``.errors``
    having committed nothing. Safe to fail here: the next attempt re-stages
    byte-identical upserts and the store's seq guard absorbs whatever a
    partially-applied flush already wrote.
    """

    def __init__(self, key: str) -> None:
        super().__init__(f"long-term flush failed for key {key!r}")
        self.key = key


@dataclass(frozen=True, slots=True)
class FailureContext:
    """Where a failed activation was when its agent raised.

    Four scalars, each a pure function of the activation's deterministic walk —
    cursor advances, staged lists, the tally's call count — never a clock or the
    discarded payloads, so a replayed bundle that fails identically reports a
    byte-identical position. This is position metadata *about* the rolled-back
    context, never its contents: no staged event, intent, output, or blob rides
    along (correctness invariant 1 is untouched).
    """

    #: The intent-step cursor at failure.
    step_index: int
    #: ``EventType`` name of the last staged trace event, ``""`` if none.
    last_event: str
    #: Count of staged intents.
    staged_intents: int
    #: Provider-reached model calls (a cache hit is not a call).
    llm_calls: int

    def trace_attributes(self) -> dict[str, str]:
        """The `beam_agents.failure.*` attributes for the ERROR trace event."""
        return {
            FAILURE_STEP: str(self.step_index),
            FAILURE_LAST_EVENT: self.last_event,
            FAILURE_STAGED_INTENTS: str(self.staged_intents),
            FAILURE_LLM_CALLS: str(self.llm_calls),
        }

    def detail_suffix(self) -> str:
        """The dead-letter detail tail, appended after the cause's ``repr``.

        Built from the same fields as :meth:`trace_attributes`, so the two
        records cannot disagree about the position.
        """
        return f" failed_at_step={self.step_index} after={self.last_event}"


class ActivationFailed(Exception):
    """An agent raise wrapped with its failure position, raised ``from`` it.

    Runtime-internal: agents never see it (they are inside the wrap), and the
    stateful DoFn consumes it immediately — reading :attr:`context` and
    ``__cause__`` — to build the ``activation_error`` dead letter and ERROR
    trace event. Wraps ``Exception`` only; ``CancelledError`` and other
    ``BaseException``s pass through untouched so the bridge's cancellation
    semantics are unchanged.
    """

    def __init__(self, context: FailureContext) -> None:
        super().__init__(
            f"activation failed at step {context.step_index} after {context.last_event}"
        )
        self.context = context


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
    #: Worker-local counts and durations for the metric recorder. Not an effect:
    #: it is never applied to Beam state, and the DoFn reads it on the Beam
    #: thread, where a metric update actually lands. Defaulted so historical
    #: construction sites keep building.
    tally: ActivationTally = field(default_factory=ActivationTally)
    #: Long-term upserts this activation staged, already flushed through the
    #: store by the time the DoFn sees the result (the commit-tail flush runs
    #: inside `run_activation`). Reported so the DoFn can count them and tests
    #: can assert byte-identity across a replay. Empty without a store.
    upserts: list[MemoryRecord] = field(default_factory=list)


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
    events: list[bytes] | None = None,
    batch_trigger: str = "",
    resume_result: ToolResult | None = None,
    resume_approval: AgentEnvelope.Approval | None = None,
    snapshot: bytes = b"",
    compactor: Compactor | None = None,
    summarizer: Summarizer | None = None,
    default_hitl_timeout_ms: int = DEFAULT_HITL_TIMEOUT_MS,
    step_index: int = 0,
    intent_ttl_ms: int = DEFAULT_INTENT_TTL_MS,
    approval_channel: str = DEFAULT_APPROVAL_CHANNEL,
    decode: Decode | None = None,
    monotonic_ns: MonotonicNs = time.monotonic_ns,
    tool_registry: ToolRegistry | None = None,
    tool_runner: ToolRunner | None = None,
    longterm_store: MemoryStore | None = None,
    max_tokens_per_activation: int | None = None,
) -> ActivationResult:
    """Run one activation to a terminal :class:`ActivationResult`.

    An agent raise (or a provider error) surfaces as :class:`ActivationFailed`,
    raised ``from`` the original and carrying a :class:`FailureContext` naming
    where the activation was; the caller commits nothing on failure, preserving
    the atomic-commit invariant. Only ``Exception`` is wrapped — cancellation
    and other ``BaseException``s propagate untouched.

    ``events`` is the adaptive-batching entry point: a flush passes the whole
    buffer's payloads and the agent sees them as ``ctx.event: list[bytes]``.
    Everything downstream of that is unchanged — one activation, one ``seq``,
    the same staged effects, the same commit — because a flush *is* one
    activation (design D4). ``batch_trigger`` names which trigger produced it
    and rides only the trace.

    ``max_tokens_per_activation`` bounds this attempt's token consumption; an
    uncaught :class:`~beam_agents.model.facade.BudgetExceeded` is an ordinary
    agent-path raise and rides the failure wrap below like any other, so the
    DoFn routes it with the position metadata already attached.
    """
    ctx = ActivationContext(
        entity_key=entity_key,
        seq=seq,
        now_ms=now_ms,
        provider=provider,
        memory_blob=memory_blob,
        cache_blob=cache_blob,
        event=event,
        events=events,
        resume_result=resume_result,
        resume_approval=resume_approval,
        snapshot=snapshot,
        compactor=compactor,
        step_index=step_index,
        intent_ttl_ms=intent_ttl_ms,
        approval_channel=approval_channel,
        decode=decode,
        monotonic_ns=monotonic_ns,
        tool_registry=tool_registry,
        tool_runner=tool_runner,
        longterm_store=longterm_store,
        max_tokens_per_activation=max_tokens_per_activation,
    )

    # Everything after context construction runs inside the failure wrap:
    # `Exception` only (never `CancelledError` or any other `BaseException`,
    # which would corrupt the bridge's cancellation semantics), and the context
    # exists before the `try`, so the position is always readable in the
    # `except`. Failures before this point reach the DoFn un-enriched via its
    # generic fallback — correct, since there is no position to report.
    try:
        # The attempt's activation span, taken from the context rather than
        # rebuilt: a second `ActivationTrace` with the same inputs would be a
        # second source of truth for identity, free to drift from the one the
        # child events use. A resume shares the suspended activation's `seq`,
        # so it recomputes the same trace ID and hangs its own span under the
        # initial attempt's (design D2).
        trace = ctx.trace
        start_event = trace.activation_start()
        if ctx.is_batch:
            # Stamped here rather than plumbed through `ActivationTrace`: the
            # batch is this driver's own entry shape, and the attributes are two
            # scalars derived from it. A per-event activation carries neither
            # key at all, so nothing downstream has to ignore an empty one.
            start_event.attributes[TRACE_BATCH_SIZE] = str(len(ctx.events))
            start_event.attributes[TRACE_BATCH_TRIGGER] = batch_trigger
        ctx.stage_trace(start_event)

        outcome = await agent(ctx)

        # Tier-2 compaction, at the one point where it can be both deterministic
        # and atomic (memory-compaction design D1/D2): after the agent's outcome
        # exists, before that outcome is folded into a `Continuation` or an
        # `ActivationResult`, and inside this failure wrap — so a summarizer
        # raise fails the activation closed and a half-summarized blob can never
        # commit. Running it before `build_continuation` is what makes the
        # persisted `step_index` include the summarizer's `call_model` advances,
        # so a resume cannot re-mint an intent ID the suspension consumed.
        #
        # The trigger is a pure function of staged memory — no clock, no
        # sampling — so a replayed bundle makes the identical run/don't-run
        # decision, and the summarizer's own model calls ride `ctx.call_model`'s
        # cache-first path (correctness invariant 3).
        if summarizer is not None and ctx.memory.size_bytes >= summarizer.trigger_bytes:
            await summarizer.compact(ctx)

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
            upserts = await _flush_longterm(ctx)
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
                tally=ctx.tally(),
                upserts=upserts,
            )

        if not isinstance(outcome, Complete):  # pragma: no cover - defensive
            raise TypeError(f"agent returned a non-Outcome value: {outcome!r}")

        ctx.stage_trace(trace.activation_end(status="completed", step_index=ctx.step_index))
        outputs = [outcome.output] if outcome.output else []
        upserts = await _flush_longterm(ctx)
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
            tally=ctx.tally(),
            upserts=upserts,
        )
    except Exception as exc:
        # Reached by an agent raise AND by a flush failure: both are activation
        # failures, both leave the caller committing nothing. A failed agent
        # never got as far as the flush, so a failed activation flushes nothing.
        staged = ctx.staged_traces
        raise ActivationFailed(
            FailureContext(
                step_index=ctx.step_index,
                last_event=(TraceEvent.EventType.Name(staged[-1].event_type) if staged else ""),
                staged_intents=len(ctx.staged_intents),
                llm_calls=ctx.tally().llm_calls,
            )
        ) from exc


async def _flush_longterm(ctx: ActivationContext) -> list[MemoryRecord]:
    """Flush the activation's staged long-term upserts, in staging order.

    The commit tail: reached only after the agent returned successfully and
    before the caller commits the bundle-atomic effects, so a failed or
    timed-out activation flushes nothing (correctness invariant 1's reading of
    the sanctioned invariant-5 exception). Still on the bridge loop, inside
    ``activation_timeout``.

    The flush is outside the Beam transaction, so the case to reason about is
    "flush succeeded, commit failed, bundle retries": the retry re-runs
    deterministically, re-stages byte-identical upserts, and the store's
    ``seq >= stored_seq`` guard turns the re-flush into an identical overwrite
    — the rows converge however many times the bundle retries. A flush failure
    fails the activation closed for the same reason: the next attempt re-stages
    identical upserts and the guard absorbs whatever already landed.
    """
    upserts = list(ctx.staged_upserts)
    if not upserts:
        return []
    store = ctx.longterm_store
    assert store is not None, "staged upserts without a configured store"
    for record in upserts:
        try:
            await store.save(record)
        except Exception as exc:
            raise LongtermFlushFailed(record.key) from exc
    return upserts
