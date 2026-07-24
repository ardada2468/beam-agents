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
from beam_agents.core.bridge import ActivationTimeout, AsyncBridge
from beam_agents.core.coders import DeterministicProtoCoder
from beam_agents.core.loop import ActivationResult, run_activation

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

# Emitted on the main output when an approval/result never arrives and HITL_TIMER
# fires the fail-closed fallback (correctness invariant 6).
HITL_TIMEOUT_OUTPUT = b"__hitl_timeout__"

# Error reasons routed to the .errors output.
REASON_TIMEOUT = "activation_timeout"
REASON_ERROR = "activation_error"
REASON_ORPHANED = "orphaned_result"


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
    ) -> None:
        self._agent = agent
        self._provider_factory = provider_factory
        self._activation_timeout_s = activation_timeout_s
        self._ttl_ms = ttl_ms
        self._cancel_grace_s = cancel_grace_s
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
        approval: object | None,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
    ) -> Iterator[object]:
        cont = continuation.read()
        if cont is None or intent_id not in set(cont.pending_intent_ids):
            # No live, unexpired continuation matches: orphaned. Mutate nothing.
            yield _error(key, REASON_ORPHANED, intent_id)
            return

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
        resume_approval: object | None = None,
        snapshot: bytes = b"",
    ) -> ActivationResult:
        assert self._bridge is not None and self._provider is not None, "setup() not called"
        provider = self._provider
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
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        pending: _State = beam.DoFn.StateParam(PENDING),
    ) -> Iterator[object]:
        cont = continuation.read()
        if cont is None:
            return
        # Fail closed: run the fallback, clear the dangling continuation and its
        # pending intents. SEQ is unchanged (this is not a committed activation).
        continuation.clear()
        pending.clear()
        yield HITL_TIMEOUT_OUTPUT


def _error(entity_key: bytes, reason: str, detail: str = "") -> beam.pvalue.TaggedOutput:
    return beam.pvalue.TaggedOutput("errors", ActivationError(entity_key, reason, detail))
