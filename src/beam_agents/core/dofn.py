"""The keyed, stateful ``_AgentDoFn`` — where every correctness invariant lands.

One activation runs per input :class:`~beam_agents._protos.AgentEnvelope`, keyed
by ``entity_key``. Beam serializes elements per key, so working memory is race-
free by construction. The DoFn:

- declares five state specs (``MEMORY``, ``CONTINUATION``, ``LLM_CACHE``,
  ``PENDING``, ``SEQ``) and two timers (``TTL_TIMER`` watermark, ``HITL_TIMER``
  real-time), all protobuf-coded, never pickle;
- routes each element by payload variant (event → start; tool_result/approval →
  resume; unmatched → ``orphaned_result`` on ``.errors``);
- runs the activation on the async bridge bounded by ``activation_timeout``,
  cancelling and routing to ``.errors`` with zero state mutation on timeout;
- stages all effects and commits them in a fixed order only on success,
  incrementing ``SEQ`` exactly once per committed activation;
- garbage-collects all state when ``TTL_TIMER`` fires and fails HITL closed.

Constructing the state-spec coders here has no global side effects (they are
plain ``Coder`` instances, not registry mutations); importing this module is
side-effect-free.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import apache_beam as beam
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    BagStateSpec,
    CombiningValueStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
)
from beam_agents.core.agent import FallbackContext, intent_id_for
from beam_agents.core.bridge import ActivationTimeout, AsyncBridge
from beam_agents.core.coders import DeterministicProtoCoder
from beam_agents.core.loop import ActivationResult, run_activation
from beam_agents.hitl import (
    HITL_TIMEOUT_OUTPUT,
    REASON_HITL_TIMEOUT,
    Deny,
    Drop,
    Escalate,
    HitlPolicy,
    Route,
    intent_expired,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.agent import Agent
    from beam_agents.model.client import LLMClient

# State/timer handles are injected by Beam's StateParam/TimerParam machinery
# at call time; they are dynamic runtime objects Beam does not statically type,
# so we annotate the parameters as Any.
_State = Any
_Timer = Any

# Default per-activation wall-budget and working-memory TTL. Both are transform
# -level with per-construction override; agents can shorten HITL via Suspend.
_DEFAULT_ACTIVATION_TIMEOUT_S = 30.0
_DEFAULT_TTL_MS = 3_600_000  # 1 hour of event time

# Error reasons routed to the .errors output. `HITL_TIMEOUT_OUTPUT` and
# `REASON_HITL_TIMEOUT` live in `hitl` (with the policy that produces them) and
# are re-exported here, where callers have always imported them from.
REASON_TIMEOUT = "activation_timeout"
REASON_ERROR = "activation_error"
REASON_ORPHANED = "orphaned_result"

# Details distinguishing the four ways a resume can fail admission, carried on
# the `orphaned_result` record so triage does not have to re-derive them.
DETAIL_NO_CONTINUATION = "no_continuation"
DETAIL_UNKNOWN_INTENT = "unknown_intent"
DETAIL_DEADLINE_PASSED = "deadline_passed"
DETAIL_INTENT_EXPIRED = "intent_expired"

__all__ = [
    "HITL_TIMEOUT_OUTPUT",
    "REASON_ERROR",
    "REASON_HITL_TIMEOUT",
    "REASON_ORPHANED",
    "REASON_TIMEOUT",
    "ActivationError",
]


@dataclass(frozen=True)
class ActivationError:
    """Dead-letter record for the ``.errors`` output. Carries the key and reason
    so a downstream sink can triage without re-deriving context.
    """

    entity_key: bytes
    reason: str
    detail: str = ""


def _ms_timestamp(ms: int) -> Timestamp:
    """Beam ``Timestamp`` from unix-epoch milliseconds."""
    return Timestamp(micros=ms * 1000)


class _SumCombineFn(beam.CombineFn):
    """Integer-accumulator sum combiner for the ``SEQ`` monotonic counter.

    A raw ``sum`` callable makes Beam accumulate a *list* of inputs, which the
    ``VarIntCoder`` cannot encode; an explicit ``CombineFn`` keeps the
    accumulator an ``int`` so the combining-value state round-trips through the
    varint coder. A fresh key reads the ``0`` identity.
    """

    def create_accumulator(self) -> int:
        return 0

    def add_input(self, accumulator: int, element: int) -> int:
        return accumulator + element

    def merge_accumulators(self, accumulators: Iterable[int]) -> int:
        return sum(accumulators)

    def extract_output(self, accumulator: int) -> int:
        return accumulator


class _AgentDoFn(beam.DoFn):
    """Stateful activation engine for one keyed agent stream."""

    MEMORY = ReadModifyWriteStateSpec("memory", DeterministicProtoCoder(MemoryBlob))
    CONTINUATION = ReadModifyWriteStateSpec("continuation", DeterministicProtoCoder(Continuation))
    LLM_CACHE = ReadModifyWriteStateSpec("llm_cache", DeterministicProtoCoder(LlmCacheBlob))
    PENDING = BagStateSpec("pending", DeterministicProtoCoder(ToolIntent))
    SEQ = CombiningValueStateSpec("seq", beam.coders.VarIntCoder(), _SumCombineFn())

    TTL_TIMER = TimerSpec("ttl", TimeDomain.WATERMARK)
    HITL_TIMER = TimerSpec("hitl", TimeDomain.REAL_TIME)

    def __init__(
        self,
        agent: Agent,
        *,
        provider_factory: Callable[[], LLMClient],
        activation_timeout_s: float = _DEFAULT_ACTIVATION_TIMEOUT_S,
        ttl_ms: int = _DEFAULT_TTL_MS,
        cancel_grace_s: float = 5.0,
        hitl_policy: HitlPolicy | None = None,
    ) -> None:
        self._agent = agent
        self._provider_factory = provider_factory
        self._activation_timeout_s = activation_timeout_s
        self._ttl_ms = ttl_ms
        self._cancel_grace_s = cancel_grace_s
        self._hitl_policy = hitl_policy if hitl_policy is not None else HitlPolicy()
        self._bridge: AsyncBridge | None = None
        self._provider: LLMClient | None = None

    # -- lifecycle: one bridge thread + provider per DoFn instance -------------

    def setup(self) -> None:
        self._bridge = AsyncBridge(cancel_grace_s=self._cancel_grace_s)
        self._bridge.start()
        self._provider = self._provider_factory()

    def teardown(self) -> None:
        if self._bridge is not None:
            self._bridge.stop()
            self._bridge = None
        self._provider = None

    # -- element processing ---------------------------------------------------

    def process(
        self,
        element: tuple[bytes, AgentEnvelope],
        memory: _State = beam.DoFn.StateParam(MEMORY),
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        llm_cache: _State = beam.DoFn.StateParam(LLM_CACHE),
        pending: _State = beam.DoFn.StateParam(PENDING),
        seq: _State = beam.DoFn.StateParam(SEQ),
        ttl_timer: _Timer = beam.DoFn.TimerParam(TTL_TIMER),
        hitl_timer: _Timer = beam.DoFn.TimerParam(HITL_TIMER),
    ) -> Iterator[object]:
        key, envelope = element
        now_ms = envelope.event_time_ms
        variant = envelope.WhichOneof("payload")

        if variant == "tool_result":
            resume = envelope.tool_result
            intent_id = resume.intent_id
            approval = None
        elif variant == "approval":
            resume = None
            intent_id = envelope.approval.intent_id
            approval = envelope.approval
        else:
            # external_event or empty payload: a fresh activation.
            yield from self._start(
                key,
                now_ms,
                envelope.external_event,
                memory,
                continuation,
                llm_cache,
                pending,
                seq,
                ttl_timer,
                hitl_timer,
            )
            return

        yield from self._resume(
            key,
            now_ms,
            intent_id,
            resume,
            approval,
            memory,
            continuation,
            llm_cache,
            pending,
            seq,
            ttl_timer,
            hitl_timer,
        )

    def _start(
        self,
        key: bytes,
        now_ms: int,
        event: bytes,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
    ) -> Iterator[object]:
        current_seq = seq.read()
        memory_blob = memory.read()
        cache_blob = llm_cache.read()

        try:
            result = self._activate(
                key=key,
                seq=current_seq,
                now_ms=now_ms,
                memory_blob=memory_blob,
                cache_blob=cache_blob,
                event=event,
            )
        except ActivationTimeout:
            yield _error(key, REASON_TIMEOUT)
            return
        except Exception as exc:
            # Any activation failure fails closed: route to errors, commit nothing.
            yield _error(key, REASON_ERROR, repr(exc))
            return

        yield from self._commit(
            result, now_ms, memory, continuation, llm_cache, pending, seq, ttl_timer, hitl_timer
        )

    def _resume(
        self,
        key: bytes,
        now_ms: int,
        intent_id: str,
        resume_result: ToolResult | None,
        approval: AgentEnvelope.Approval | None,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
    ) -> Iterator[object]:
        cont = continuation.read()
        pending_intents = [] if cont is None else list(pending.read())
        detail = _admission_failure(cont, pending_intents, intent_id, now_ms)
        if detail is not None:
            # No live, unexpired continuation matches: orphaned. Mutate nothing.
            yield _error(key, REASON_ORPHANED, f"{detail}:{intent_id}")
            return
        # `_admission_failure` returns DETAIL_NO_CONTINUATION for a missing one.
        assert cont is not None

        memory_blob = memory.read()
        cache_blob = llm_cache.read()
        try:
            result = self._activate(
                key=key,
                seq=cont.seq,
                now_ms=now_ms,
                memory_blob=memory_blob,
                cache_blob=cache_blob,
                resume_result=resume_result,
                resume_approval=approval,
                snapshot=cont.snapshot,
                step_index=cont.step_index,
            )
        except ActivationTimeout:
            yield _error(key, REASON_TIMEOUT)
            return
        except Exception as exc:
            # Any activation failure fails closed: route to errors, commit nothing.
            yield _error(key, REASON_ERROR, repr(exc))
            return

        yield from self._commit(
            result, now_ms, memory, continuation, llm_cache, pending, seq, ttl_timer, hitl_timer
        )

    def _activate(
        self,
        *,
        key: bytes,
        seq: int,
        now_ms: int,
        memory_blob: MemoryBlob | None,
        cache_blob: LlmCacheBlob | None,
        event: bytes = b"",
        resume_result: ToolResult | None = None,
        resume_approval: AgentEnvelope.Approval | None = None,
        snapshot: bytes = b"",
        step_index: int = 0,
    ) -> ActivationResult:
        assert self._bridge is not None and self._provider is not None, "setup() not called"
        provider = self._provider
        policy = self._hitl_policy
        return self._bridge.run(
            lambda: run_activation(
                self._agent,
                entity_key=key,
                seq=seq,
                now_ms=now_ms,
                provider=provider,
                memory_blob=memory_blob,
                cache_blob=cache_blob,
                event=event,
                resume_result=resume_result,
                resume_approval=resume_approval,
                snapshot=snapshot,
                step_index=step_index,
                default_hitl_timeout_ms=policy.timeout_ms,
                intent_ttl_ms=policy.intent_ttl_ms,
                approval_channel=policy.approval_channel,
            ),
            self._activation_timeout_s,
        )

    def _commit(
        self,
        result: ActivationResult,
        now_ms: int,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
    ) -> Iterator[object]:
        # Fixed commit order (design D3): MEMORY, LLM_CACHE, CONTINUATION,
        # PENDING, SEQ, timers, emits. Reached only on activation success.
        memory.write(result.memory_blob)
        llm_cache.write(result.cache_blob)

        if result.continuation is not None:
            continuation.write(result.continuation)
        else:
            continuation.clear()

        pending.clear()
        for intent in result.intents:
            pending.add(intent)

        # Exactly one increment per committed activation, here and nowhere else.
        seq.add(1)

        # Re-arm working-memory GC on every committed element (event-time). A
        # re-set to a later time supersedes the prior mark, so a live key is
        # never GC'd by a stale earlier timer.
        ttl_timer.set(_ms_timestamp(now_ms + self._ttl_ms))
        if result.status == "suspended" and result.hitl_deadline_ms is not None:
            hitl_timer.set(_ms_timestamp(result.hitl_deadline_ms))
        else:
            hitl_timer.clear()

        yield from result.outputs
        for intent in result.intents:
            yield beam.pvalue.TaggedOutput("intents", intent)
        for trace in result.traces:
            yield beam.pvalue.TaggedOutput("traces", trace)

    # -- timers ---------------------------------------------------------------

    @on_timer(TTL_TIMER)
    def on_ttl(
        self,
        memory: _State = beam.DoFn.StateParam(MEMORY),
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        llm_cache: _State = beam.DoFn.StateParam(LLM_CACHE),
        pending: _State = beam.DoFn.StateParam(PENDING),
        seq: _State = beam.DoFn.StateParam(SEQ),
    ) -> None:
        # Working memory is event-time garbage: wipe every spec so an idle key
        # leaves zero residue. No emit, no SEQ change.
        memory.clear()
        continuation.clear()
        llm_cache.clear()
        pending.clear()
        seq.clear()

    @on_timer(HITL_TIMER)
    def on_hitl(
        self,
        key: bytes = beam.DoFn.KeyParam,  # type: ignore[assignment]
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore[assignment]
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        pending: _State = beam.DoFn.StateParam(PENDING),
        hitl_timer: _Timer = beam.DoFn.TimerParam(HITL_TIMER),
    ) -> Iterator[object]:
        cont = continuation.read()
        fired_at_ms = timestamp.micros // 1000
        if cont is None or fired_at_ms < cont.deadline_ms:
            # Stale handle: either the suspension was already resolved, or this
            # delivery belongs to one superseded by a later suspension whose
            # deadline has not arrived. Mutate nothing, emit nothing — a
            # fail-closed mechanism that can be tricked into killing a *live*
            # continuation is not fail-closed.
            return

        route, detail = self._route_timeout(key, cont, fired_at_ms)

        if isinstance(route, Escalate):
            yield self._escalate(key, cont, fired_at_ms, route, continuation, pending, hitl_timer)
            return

        # Deny/Drop end the suspension: clear the dangling continuation and its
        # pending intents. SEQ is unchanged (this is not a committed activation).
        continuation.clear()
        pending.clear()
        hitl_timer.clear()
        if isinstance(route, Deny):
            yield route.output
        else:
            yield _error(key, route.reason, detail)

    def _route_timeout(self, key: bytes, cont: Continuation, fired_at_ms: int) -> tuple[Route, str]:
        """Ask the policy what to do, failing closed on its own failure.

        The policy is pure by contract, but it is user code running inside a
        timer callback: letting it raise would fail the bundle and, on a
        permanently-raising policy, wedge the key in a retry loop it can never
        leave. A raise becomes a `Drop` to `.errors`.
        """
        fallback = FallbackContext(
            entity_key=key,
            seq=cont.seq,
            snapshot=cont.snapshot,
            kind="timer",
            deadline_ms=cont.deadline_ms,
            fired_at_ms=fired_at_ms,
            pending_intent_ids=tuple(cont.pending_intent_ids),
        )
        detail = f"seq={cont.seq}"
        try:
            route = self._hitl_policy.on_timeout(fallback)
        except Exception as exc:
            return Drop(REASON_HITL_TIMEOUT), f"{detail} policy_error={exc!r}"
        if isinstance(route, Escalate) and cont.escalations >= self._hitl_policy.max_escalations:
            # The bound is reached: end the wait rather than escalating forever.
            return Deny(HITL_TIMEOUT_OUTPUT), detail
        return route, detail

    def _escalate(
        self,
        key: bytes,
        cont: Continuation,
        fired_at_ms: int,
        route: Escalate,
        continuation: _State,
        pending: _State,
        hitl_timer: _Timer,
    ) -> beam.pvalue.TaggedOutput:
        """Ask again, louder: stage an approval intent and extend the deadline.

        The intent consumes the continuation's next free step index, so its ID
        is a pure function of persisted state (a retried timer bundle re-mints
        it byte-identically and the effector dedups) and cannot collide with
        another escalation or with a later resumed activation, which seeds its
        step index from the same cursor.
        """
        deadline_ms = fired_at_ms + route.timeout_ms
        intent = ToolIntent(
            intent_id=intent_id_for(key, cont.seq, cont.step_index),
            entity_key=key,
            seq=cont.seq,
            step_index=cont.step_index,
            tool_name=route.tool_name,
            args_json=route.args_json,
            created_at_ms=fired_at_ms,
            expires_at_ms=deadline_ms,
            kind=ToolIntent.APPROVAL,
        )
        escalated = Continuation()
        escalated.CopyFrom(cont)
        escalated.step_index = cont.step_index + 1
        escalated.deadline_ms = deadline_ms
        escalated.escalations = cont.escalations + 1
        escalated.pending_intent_ids.append(intent.intent_id)

        continuation.write(escalated)
        # The earlier intents stay pending: escalating adds a channel, it does
        # not withdraw the original request. Each remains bounded by its own
        # expires_at_ms, which the resume admission check enforces.
        pending.add(intent)
        hitl_timer.set(_ms_timestamp(deadline_ms))
        return beam.pvalue.TaggedOutput("intents", intent)


def _admission_failure(
    cont: Continuation | None,
    pending_intents: Iterable[ToolIntent],
    intent_id: str,
    now_ms: int,
) -> str | None:
    """Why a resume is refused, or ``None`` when it may resume the activation.

    Fail-closed layer 1 (correctness invariant 6). A resume is admitted only
    against a continuation that exists, pended this ``intent_id``, has not
    passed its deadline, and whose matching pending intent has not expired. A
    non-positive deadline or expiry counts as **passed**: the runtime always
    writes positive ones, so a zero means a corrupt blob, and "do not resume"
    is the safe reading.

    Pure: the DoFn passes ``now_ms`` from the element's event time and never
    reads a wall clock, so a replayed bundle makes the same decision.
    """
    if cont is None:
        return DETAIL_NO_CONTINUATION
    if intent_id not in set(cont.pending_intent_ids):
        return DETAIL_UNKNOWN_INTENT
    if cont.deadline_ms <= 0 or now_ms >= cont.deadline_ms:
        return DETAIL_DEADLINE_PASSED
    for intent in pending_intents:
        if intent.intent_id == intent_id and intent_expired(intent, now_ms):
            return DETAIL_INTENT_EXPIRED
    return None


def _error(entity_key: bytes, reason: str, detail: str = "") -> beam.pvalue.TaggedOutput:
    return beam.pvalue.TaggedOutput("errors", ActivationError(entity_key, reason, detail))
