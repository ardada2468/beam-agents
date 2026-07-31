"""The keyed, stateful ``_AgentDoFn`` — where every correctness invariant lands.

One activation runs per input :class:`~beam_agents._protos.AgentEnvelope`, keyed
by ``entity_key``. Beam serializes elements per key, so working memory is race-
free by construction. The DoFn:

- declares five state specs (``MEMORY``, ``CONTINUATION``, ``LLM_CACHE``,
  ``PENDING``, ``SEQ``) and two timers (``TTL_TIMER`` watermark, ``HITL_TIMER``
  real-time), all protobuf-coded, never pickle;
- passes every ``MEMORY``/``CONTINUATION``/``LLM_CACHE`` read through
  :func:`~beam_agents.core.migration.migrate_to_current` before interpreting
  any field — the seven read sites in ``_start``, ``_resume``, ``on_ttl``, and
  ``on_hitl``. Migration is lazy and writes nothing at read time: the migrated
  view reaches durable state only through the next successful commit's writes,
  which stamp ``CURRENT_STATE_SCHEMA_VERSION``. A blob from a *newer* schema
  version raises out of the bundle uncaught (never dead-letters), wedging the
  key with zero mutation until the binary is rolled forward;
- routes each element by payload variant (event → start; tool_result/approval →
  resume; unmatched → ``orphaned_result`` on ``.errors``);
- runs the activation on the async bridge bounded by ``activation_timeout``,
  cancelling and routing to ``.errors`` with zero state mutation on timeout;
- stages all effects and commits them in a fixed order only on success,
  incrementing ``SEQ`` exactly once per committed activation;
- garbage-collects all state when ``TTL_TIMER`` fires and fails HITL closed;
- records the runtime metrics (``beam_agents.runtime``) on the Beam thread, from
  the tally the activation staged — a metric update made from the bridge thread
  would be discarded by Beam with no error.

Constructing the state-spec coders here has no global side effects (they are
plain ``Coder`` instances, not registry mutations); importing this module is
side-effect-free.
"""

from __future__ import annotations

import time
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
from beam_agents.core.context import MonotonicNs
from beam_agents.core.loop import (
    ActivationFailed,
    ActivationResult,
    FailureContext,
    run_activation,
)
from beam_agents.core.migration import migrate_to_current
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
from beam_agents.memory.compaction import ExpiringMemory
from beam_agents.memory.stores import MemoryStore, build_memory_store, parse_memory_store_uri
from beam_agents.observability import ROLE_TIMER, ActivationTrace
from beam_agents.observability.metrics import (
    COUNTER_ACTIVATIONS,
    COUNTER_AGENT_ERRORS,
    COUNTER_INTENTS_EMITTED,
    COUNTER_LLM_CALLS,
    COUNTER_LONGTERM_UPSERTS,
    COUNTER_ORPHANED_RESULTS,
    COUNTER_SUSPENSIONS,
    COUNTER_TOOL_CALLS,
    DISTRIBUTION_ACTIVATION_MS,
    DISTRIBUTION_ITERATIONS,
    DISTRIBUTION_LLM_MS,
    DISTRIBUTION_MEMORY_BYTES,
    DISTRIBUTION_OVERHEAD_MS,
    DISTRIBUTION_TOKENS,
    MetricsSink,
    RuntimeMetrics,
)
from beam_agents.tools.registry import ToolRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.core.agent import Agent
    from beam_agents.memory.compaction import ExpireHook, Summarizer
    from beam_agents.memory.facade import Compactor
    from beam_agents.model.client import LLMClient
    from beam_agents.model.facade import Decode

# State/timer handles are injected by Beam's StateParam/TimerParam machinery
# at call time; they are dynamic runtime objects Beam does not statically type,
# so we annotate the parameters as Any.
_State = Any
_Timer = Any

# Default per-activation wall-budget and working-memory TTL. Both are transform
# -level with per-construction override; agents can shorten HITL via Suspend.
_DEFAULT_ACTIVATION_TIMEOUT_S = 30.0
_DEFAULT_TTL_MS = 3_600_000  # 1 hour of event time

_NS_PER_MS = 1_000_000

# Error reasons routed to the .errors output. `HITL_TIMEOUT_OUTPUT` and
# `REASON_HITL_TIMEOUT` live in `hitl` (with the policy that produces them) and
# are re-exported here, where callers have always imported them from.
REASON_TIMEOUT = "activation_timeout"
REASON_ERROR = "activation_error"
REASON_ORPHANED = "orphaned_result"
# Working-memory GC reached a key whose suspension was still awaiting an answer.
# `_commit` arms `TTL_TIMER` past the suspension's deadline precisely so this
# cannot happen, but the two timers read different clocks (watermark vs. wall),
# so a backlog replay can still cross the event-time mark first. The suspension
# is unrecoverable at that point; this reason makes the loss observable.
REASON_TTL_WIPED_SUSPENSION = "ttl_wiped_suspension"
# An intent that could not be serialized for the outbox. Not produced by this
# DoFn -- `WriteIntents` dead-letters it downstream and `RunAgent` maps it onto
# the same `ActivationError` shape, so one schema covers the whole errors sink.
REASON_INTENT_DEAD_LETTER = "intent_dead_letter"

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
    "REASON_INTENT_DEAD_LETTER",
    "REASON_ORPHANED",
    "REASON_TIMEOUT",
    "REASON_TTL_WIPED_SUSPENSION",
    "ActivationError",
]


@dataclass(frozen=True)
class ActivationError:
    """Dead-letter record for the ``.errors`` output. Carries the key, reason,
    and when it happened, so a downstream sink can triage without re-deriving
    context.

    ``event_time_ms`` is always a replay-deterministic time — the element's
    event time on the element path, a timer's scheduled firing time on the
    timer paths — never a wall-clock reading. The errors sink encodes it into
    every published record, so a retried bundle that walks the same failure
    path must produce a byte-identical one. It defaults to ``0`` so a record
    built by a caller that has no timestamp is still constructible; every
    emission site inside this DoFn supplies one.
    """

    entity_key: bytes
    reason: str
    detail: str = ""
    event_time_ms: int = 0


async def _build_store(scheme: str, parts: tuple[str, ...]) -> MemoryStore:
    """Construct the long-term store on the bridge loop.

    A coroutine because backend constructors bind async clients to the running
    loop; ``build_memory_store`` itself is synchronous and import-lazy.
    """
    return build_memory_store(scheme, parts)


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
        decode: Decode | None = None,
        tool_registry: ToolRegistry | None = None,
        metrics: MetricsSink | None = None,
        monotonic_ns: MonotonicNs | None = None,
        longterm_memory: str | None = None,
        compactor: Compactor | None = None,
        summarizer: Summarizer | None = None,
        on_expire: ExpireHook | None = None,
    ) -> None:
        self._agent = agent
        self._provider_factory = provider_factory
        self._activation_timeout_s = activation_timeout_s
        self._ttl_ms = ttl_ms
        self._cancel_grace_s = cancel_grace_s
        self._hitl_policy = hitl_policy if hitl_policy is not None else HitlPolicy()
        # The provider's response decoder. Without it a model call's token
        # counts are unknown, and the trace omits them rather than reporting
        # zeros that would be summed as real (design D4).
        self._decode = decode
        # The read-only tools `ctx.run_tool` executes inline; empty by default,
        # so an unconfigured pipeline refuses every inline call by name.
        self._tool_registry = tool_registry if tool_registry is not None else ToolRegistry()
        # `metrics`/`monotonic_ns` are test seams, not user configuration: there
        # is no `AgentConfig` knob for either, and metrics are always published.
        # Built here rather than in `setup()` so the timer callbacks (which a
        # unit test drives without a bundle) always have a recorder.
        self._metrics: MetricsSink = metrics if metrics is not None else RuntimeMetrics()
        self._monotonic_ns: MonotonicNs = (
            monotonic_ns if monotonic_ns is not None else time.monotonic_ns
        )
        # The long-term store's URI, already grammar-validated by `AgentConfig`.
        # `None` means the tier is off: no store is constructed and
        # `ctx.memory.longterm` raises actionably.
        self._longterm_memory = longterm_memory
        # Tier-1 compaction: handed to the activation's `Memory`, which invokes
        # it at the soft-cap crossing and before hard-cap rejection. `None`
        # (only reachable by explicit `AgentConfig.compactor=None`, since the
        # config defaults to `DropOldestCompactor`) restores strict-overflow
        # semantics: an over-cap write raises `MemoryOverflow` and dead-letters.
        self._compactor = compactor
        # Tier-2 compaction: invoked by the loop driver inside the activation,
        # so its model calls are replay-cached. Opt-in; `None` means no
        # summarization pass and no behavior change at all.
        self._summarizer = summarizer
        # The TTL demotion hook. `None` (the default) is today's wipe-only
        # behavior, byte for byte. `AgentConfig` refuses to set it without a
        # configured long-term store, so a non-None hook here implies one.
        self._on_expire = on_expire
        self._bridge: AsyncBridge | None = None
        self._provider: LLMClient | None = None
        self._longterm_store: MemoryStore | None = None

    # -- lifecycle: one bridge thread + provider + store per DoFn instance -----

    def setup(self) -> None:
        self._bridge = AsyncBridge(cancel_grace_s=self._cancel_grace_s)
        self._bridge.start()
        self._provider = self._provider_factory()
        if self._longterm_memory is not None:
            # One client per DoFn instance, built on the bridge loop and shared
            # across keys — the sanctioned kind of worker-local sharing (like
            # the httpx pools), not cross-key mutable state (design D6).
            scheme, parts = parse_memory_store_uri(self._longterm_memory)
            self._longterm_store = self._bridge.run(
                lambda: _build_store(scheme, parts), self._activation_timeout_s
            )

    def teardown(self) -> None:
        store = self._longterm_store
        if store is not None and self._bridge is not None:
            self._bridge.run(store.close, self._activation_timeout_s)
        self._longterm_store = None
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
        # Lazy schema migration on first read (state-migration): before any
        # field is interpreted, never writing — the migrated view reaches
        # state only through `_commit`. A future-version blob raises here,
        # uncaught, failing the bundle with nothing mutated or emitted.
        memory_blob = migrate_to_current(memory.read())
        cache_blob = migrate_to_current(llm_cache.read())

        try:
            result, activation_ms = self._activate(
                key=key,
                seq=current_seq,
                now_ms=now_ms,
                memory_blob=memory_blob,
                cache_blob=cache_blob,
                event=event,
            )
        except ActivationTimeout:
            yield self._dead_letter(key, REASON_TIMEOUT, event_time_ms=now_ms)
            yield _error_trace(key, current_seq, now_ms, REASON_TIMEOUT)
            return
        except ActivationFailed as failed:
            # The agent raised inside the wrap: both records carry the failure
            # position. Ordered ahead of the generic fallback, which would
            # otherwise name the wrapper and drop the enrichment.
            yield from self._failed_activation(key, current_seq, now_ms, failed)
            return
        except Exception as exc:
            # Any activation failure fails closed: route to errors, commit nothing.
            yield self._dead_letter(key, REASON_ERROR, repr(exc), event_time_ms=now_ms)
            yield _error_trace(
                key, current_seq, now_ms, REASON_ERROR, error_type=type(exc).__name__
            )
            return

        yield from self._commit(
            result,
            now_ms,
            activation_ms,
            memory,
            continuation,
            llm_cache,
            pending,
            seq,
            ttl_timer,
            hitl_timer,
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
        # Migrated before the admission check: a resume must be admitted (or
        # refused) against the current schema's reading of `deadline_ms` and
        # `pending_intent_ids`, never an old layout's.
        cont = migrate_to_current(continuation.read())
        pending_intents = [] if cont is None else list(pending.read())
        detail = _admission_failure(cont, pending_intents, intent_id, now_ms)
        if detail is not None:
            # No live, unexpired continuation matches: orphaned. Mutate nothing.
            # No agent runs, so there is no duration to sample either.
            yield self._dead_letter(
                key, REASON_ORPHANED, f"{detail}:{intent_id}", event_time_ms=now_ms
            )
            # Scope the trace to the continuation's activation when there is
            # one; a genuinely orphaned result has no activation to belong to,
            # so the key's current seq is the closest true scope.
            yield _error_trace(
                key,
                cont.seq if cont is not None else seq.read(),
                now_ms,
                REASON_ORPHANED,
                error_type=detail,
            )
            return
        # `_admission_failure` returns DETAIL_NO_CONTINUATION for a missing one.
        assert cont is not None

        memory_blob = migrate_to_current(memory.read())
        cache_blob = migrate_to_current(llm_cache.read())
        try:
            result, activation_ms = self._activate(
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
            yield self._dead_letter(key, REASON_TIMEOUT, event_time_ms=now_ms)
            yield _error_trace(key, cont.seq, now_ms, REASON_TIMEOUT)
            return
        except ActivationFailed as failed:
            # The agent raised inside the wrap: both records carry the failure
            # position. Ordered ahead of the generic fallback, which would
            # otherwise name the wrapper and drop the enrichment.
            yield from self._failed_activation(key, cont.seq, now_ms, failed)
            return
        except Exception as exc:
            # Any activation failure fails closed: route to errors, commit nothing.
            yield self._dead_letter(key, REASON_ERROR, repr(exc), event_time_ms=now_ms)
            yield _error_trace(key, cont.seq, now_ms, REASON_ERROR, error_type=type(exc).__name__)
            return

        yield from self._commit(
            result,
            now_ms,
            activation_ms,
            memory,
            continuation,
            llm_cache,
            pending,
            seq,
            ttl_timer,
            hitl_timer,
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
    ) -> tuple[ActivationResult, int]:
        assert self._bridge is not None and self._provider is not None, "setup() not called"
        provider = self._provider
        longterm_store = self._longterm_store
        policy = self._hitl_policy
        decode = self._decode
        monotonic_ns = self._monotonic_ns
        # `activation_ms` brackets the bounded bridge submission -- what the
        # caller actually waits for, submission overhead included -- and is
        # sampled in a `finally` so all three exits are covered by one site: a
        # commit, an `ActivationTimeout`, and any other failure. The timeout tail
        # is the most interesting part of the distribution, so it must not be
        # the exit that skips the sample. The clock is read on the Beam thread;
        # inside the activation it is the same injected callable, forwarded so
        # one clock times the whole element.
        started_ns = monotonic_ns()
        try:
            result = self._bridge.run(
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
                    compactor=self._compactor,
                    summarizer=self._summarizer,
                    default_hitl_timeout_ms=policy.timeout_ms,
                    intent_ttl_ms=policy.intent_ttl_ms,
                    approval_channel=policy.approval_channel,
                    decode=decode,
                    monotonic_ns=monotonic_ns,
                    tool_registry=self._tool_registry,
                    longterm_store=longterm_store,
                ),
                self._activation_timeout_s,
            )
        finally:
            # The elapsed reading is taken in the finally so all three exits --
            # commit, ActivationTimeout, any other failure -- share the one
            # sample site; on success it is also what `overhead_ms` subtracts
            # the tally's call durations from.
            elapsed_ms = (monotonic_ns() - started_ns) // _NS_PER_MS
            self._metrics.observe(DISTRIBUTION_ACTIVATION_MS, elapsed_ms)
        return result, elapsed_ms

    def _commit(
        self,
        result: ActivationResult,
        now_ms: int,
        activation_ms: int,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
    ) -> Iterator[object]:
        # Fixed commit order (design D3, extended by add-runtime-metrics):
        # MEMORY, LLM_CACHE, CONTINUATION, PENDING, SEQ, timers, metrics, emits.
        # Reached only on activation success.
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
        # `activations` counts the same event; it is recorded below with the
        # rest of the metrics so the commit keeps one metrics step, not two.
        seq.add(1)

        # Re-arm working-memory GC on every committed element (event-time). A
        # re-set to a later time supersedes the prior mark, so a live key is
        # never GC'd by a stale earlier timer.
        #
        # A *suspending* activation measures the mark from its deadline rather
        # than from the activation clock: the memory a resume will read has to
        # outlive the wait, and a TTL fire that beat `HITL_TIMER` would clear the
        # continuation and leave the later HITL fire reading a stale handle --
        # swallowing the timeout entirely, on no output at all. `max` guards the
        # deadline being at or before the clock: `Suspend.timeout_ms` and
        # `act(ttl_ms=...)` are agent-supplied and unvalidated, so a non-positive
        # one must not pull the mark into the past.
        ttl_from_ms = now_ms
        if result.status == "suspended" and result.hitl_deadline_ms is not None:
            ttl_from_ms = max(now_ms, result.hitl_deadline_ms)
            hitl_timer.set(_ms_timestamp(result.hitl_deadline_ms))
        else:
            hitl_timer.clear()
        ttl_timer.set(_ms_timestamp(ttl_from_ms + self._ttl_ms))

        # Recorded before the yields, not after: `_commit` is a generator, and
        # recording placed after them would be contingent on how the consumer
        # drains it. Beam always drains fully, but the ordering must not depend
        # on that.
        self._record_commit(result, activation_ms)

        yield from result.outputs
        for intent in result.intents:
            yield beam.pvalue.TaggedOutput("intents", intent)
        for trace in result.traces:
            yield beam.pvalue.TaggedOutput("traces", trace)

    def _record_commit(self, result: ActivationResult, activation_ms: int) -> None:
        """Record one committed activation's metrics, on the Beam thread.

        Everything here comes from the staged `ActivationResult`: the counts and
        durations were accumulated on the async bridge thread, where Beam's
        thread-local state sampler does not exist and an update would have been
        discarded with no error.

        A failed or refused activation never reaches this method, so the
        commit-path metrics obey the same all-or-nothing rule the state
        mutations do.
        """
        metrics = self._metrics
        tally = result.tally
        metrics.incr(COUNTER_ACTIVATIONS)
        if result.status == "suspended":
            metrics.incr(COUNTER_SUSPENSIONS)
        if result.intents:
            # Keeps `intents_emitted` equal to the element count on `.intents`;
            # `_escalate` counts the one intent it mints outside this path.
            metrics.incr(COUNTER_INTENTS_EMITTED, len(result.intents))
        if result.upserts:
            # The flush already happened (commit tail, inside the activation);
            # counting here keeps it on the committed path, so a discarded
            # attempt's flush is not counted twice with its retry's.
            metrics.incr(COUNTER_LONGTERM_UPSERTS, len(result.upserts))
        if tally.llm_calls:
            metrics.incr(COUNTER_LLM_CALLS, tally.llm_calls)
        if tally.tool_calls:
            metrics.incr(COUNTER_TOOL_CALLS, tally.tool_calls)
        metrics.observe(DISTRIBUTION_MEMORY_BYTES, result.memory_blob.total_value_bytes)
        metrics.observe(DISTRIBUTION_ITERATIONS, tally.iterations)
        if tally.usage_observed:
            # Only when a provider response was actually decoded: a zero sample
            # from a path that never decodes would deflate the distribution.
            metrics.observe(DISTRIBUTION_TOKENS, tally.total_tokens)
        for duration_ms in tally.llm_ms:
            metrics.observe(DISTRIBUTION_LLM_MS, duration_ms)
        # The release-gate figure: the activation's wall time with its model
        # and inline-tool time subtracted out. Clamped -- an agent that awaits
        # calls concurrently can make the subtrahend exceed the wall time.
        metrics.observe(
            DISTRIBUTION_OVERHEAD_MS,
            max(0, activation_ms - sum(tally.llm_ms) - sum(tally.tool_ms)),
        )

    # -- timers ---------------------------------------------------------------

    @on_timer(TTL_TIMER)
    def on_ttl(
        self,
        key: bytes = beam.DoFn.KeyParam,  # type: ignore[assignment]
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore[assignment]
        memory: _State = beam.DoFn.StateParam(MEMORY),
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        llm_cache: _State = beam.DoFn.StateParam(LLM_CACHE),
        pending: _State = beam.DoFn.StateParam(PENDING),
        seq: _State = beam.DoFn.StateParam(SEQ),
    ) -> Iterator[object]:
        # A live continuation here means working-memory GC reached a key that is
        # still waiting. `_commit` arms this timer past the suspension deadline
        # so it should be unreachable -- but the mark is compared against the
        # watermark while the deadline is compared against the wall clock, and a
        # backlog replay can cross the event-time mark while real time is still
        # short of the deadline. The suspension cannot be rescued (the memory it
        # would resume against is genuinely event-time garbage); dead-letter it
        # so it is not lost in silence.
        #
        # Migrated before `seq`/`deadline_ms` are read — and a future-version
        # continuation raises *before the wipe*: GC must not destroy state a
        # newer binary wrote and this one cannot read.
        cont = migrate_to_current(continuation.read())
        if cont is not None:
            yield self._dead_letter(
                key,
                REASON_TTL_WIPED_SUSPENSION,
                f"seq={cont.seq},deadline_ms={cont.deadline_ms}",
                event_time_ms=timestamp.micros // 1000,
            )
            yield _error_trace(
                key,
                cont.seq,
                timestamp.micros // 1000,
                REASON_TTL_WIPED_SUSPENSION,
                role=ROLE_TIMER,
            )

        # Demote before destroying, when the hook is configured. Expiry is the
        # one moment where the runtime performs an external write outside an
        # activation -- the idempotent `(entity_key, seq)`-keyed upsert
        # correctness invariant 5 carves out -- because a watermark timer has no
        # activation context to stage through (design D4). A flush failure
        # raises out of this callback, failing the timer bundle so the runner
        # retries it against state this method has deliberately not yet wiped.
        if self._on_expire is not None:
            self._flush_expiring(key, memory, seq, timestamp.micros // 1000)

        # Working memory is event-time garbage: wipe every spec so an idle key
        # leaves zero residue. Unconditional -- reporting the loss above does not
        # rescue the key. No SEQ change beyond the wipe.
        memory.clear()
        continuation.clear()
        llm_cache.clear()
        pending.clear()
        seq.clear()

    def _flush_expiring(self, key: bytes, memory: _State, seq: _State, expired_at_ms: int) -> None:
        """Upsert the expiring key's final ``MemoryBlob`` to the long-term tier.

        Migrated before the blob is interpreted, like every other state read; a
        future-version blob raises here, *before* the wipe, so GC can never
        destroy state a newer binary wrote and this one cannot read.

        An empty (or absent) working memory is wiped with no store call: there
        is nothing to demote, and a round trip per idle expiry would be pure
        cost. Everything the hook is handed is replay-stable -- committed state
        plus the timer's scheduled firing time, never a wall clock -- so a
        retried timer bundle produces a byte-identical upsert the store's
        equal-seq guard collapses onto one row.
        """
        hook = self._on_expire
        assert hook is not None
        blob = migrate_to_current(memory.read())
        if blob is None or not blob.entries:
            return
        store = self._longterm_store
        if store is None or self._bridge is None:
            raise RuntimeError(
                "AgentConfig.on_expire is set but no long-term store is available; "
                "set AgentConfig.longterm_memory to a store URI"
            )
        expiry = ExpiringMemory(
            entity_key=key, seq=seq.read(), blob=blob, expired_at_ms=expired_at_ms
        )
        # Bounded on the bridge the DoFn already runs for its lifetime; a wedged
        # store surfaces as a failed timer bundle rather than a stalled worker.
        self._bridge.run(lambda: hook(store, expiry), self._activation_timeout_s)

    @on_timer(HITL_TIMER)
    def on_hitl(
        self,
        key: bytes = beam.DoFn.KeyParam,  # type: ignore[assignment]
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore[assignment]
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        pending: _State = beam.DoFn.StateParam(PENDING),
        hitl_timer: _Timer = beam.DoFn.TimerParam(HITL_TIMER),
        ttl_timer: _Timer = beam.DoFn.TimerParam(TTL_TIMER),
    ) -> Iterator[object]:
        # Migrated before the stale-handle comparison: a fail-closed mechanism
        # that misreads an old layout's `deadline_ms` is not fail-closed, and a
        # future-version continuation must raise rather than be routed on.
        cont = migrate_to_current(continuation.read())
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
            yield from self._escalate(
                key, cont, fired_at_ms, route, continuation, pending, hitl_timer, ttl_timer
            )
            return

        # Deny/Drop end the suspension: clear the dangling continuation and its
        # pending intents. The working-memory mark is left alone -- the wait is
        # over, so the mark `_commit` armed is already correct and letting it
        # fire later is the GC we want. SEQ is unchanged (this is not a
        # committed activation).
        continuation.clear()
        pending.clear()
        hitl_timer.clear()
        if isinstance(route, Deny):
            # An ordinary output, not a dead letter: nothing to count.
            yield route.output
        else:
            yield self._dead_letter(key, route.reason, detail, event_time_ms=fired_at_ms)
        # Both routes end the wait without an answer; the trace records it
        # either way, in the suspended activation's own trace.
        yield _error_trace(key, cont.seq, fired_at_ms, REASON_HITL_TIMEOUT, role=ROLE_TIMER)

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
        ttl_timer: _Timer,
    ) -> Iterator[beam.pvalue.TaggedOutput]:
        """Ask again, louder: stage an approval intent and extend the deadline.

        The intent consumes the continuation's next free step index, so its ID
        is a pure function of persisted state (a retried timer bundle re-mints
        it byte-identically and the effector dedups) and cannot collide with
        another escalation or with a later resumed activation, which seeds its
        step index from the same cursor.
        """
        deadline_ms = fired_at_ms + route.timeout_ms
        # Minted outside any activation, so the trace is rebuilt from the
        # continuation's scope — the same `(key, seq)` the suspended activation
        # traced under, which is what puts the escalation in its trace. No
        # `is_resume`: it only decides an *activation*-role span's parent, and
        # this route emits a child event, never an activation bracket.
        trace = ActivationTrace(
            entity_key=key,
            seq=cont.seq,
            now_ms=fired_at_ms,
            entry_step_index=cont.step_index,
        )
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
            trace_id=trace.trace_id,
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
        # Carry the working-memory mark forward with the deadline. Escalation
        # walks the deadline past the mark `_commit` sized against the *original*
        # suspension, so without this a bounded escalation chain would outlive
        # working memory and be GC'd mid-wait -- the same preemption `_commit`
        # guards against, reintroduced one route later.
        ttl_timer.set(_ms_timestamp(deadline_ms + self._ttl_ms))
        # This intent reaches `.intents` without passing through `_commit`, so
        # it is counted here or `intents_emitted` stops matching the output.
        self._metrics.incr(COUNTER_INTENTS_EMITTED)
        yield beam.pvalue.TaggedOutput("intents", intent)
        yield beam.pvalue.TaggedOutput(
            "traces",
            trace.intent_emitted(
                step_index=cont.step_index,
                intent_id=intent.intent_id,
                tool_name=intent.tool_name,
                intent_kind=ToolIntent.Kind.Name(intent.kind),
                expires_at_ms=deadline_ms,
            ),
        )

    def _failed_activation(
        self, key: bytes, seq: int, now_ms: int, failed: ActivationFailed
    ) -> Iterator[beam.pvalue.TaggedOutput]:
        """The enriched `activation_error` route: both records from one context.

        The dead-letter detail keeps leading with the *original* exception's
        ``repr`` — the cause, not the wrapper — so existing triage habits and
        prefix-matching consumers keep working; the position suffix and the
        trace attributes are built from the same :class:`FailureContext`, so
        the two records cannot disagree. Still counted through `_dead_letter`,
        the single chokepoint.
        """
        cause = failed.__cause__ if failed.__cause__ is not None else failed
        context = failed.context
        yield self._dead_letter(
            key, REASON_ERROR, f"{cause!r}{context.detail_suffix()}", event_time_ms=now_ms
        )
        yield _error_trace(
            key,
            seq,
            now_ms,
            REASON_ERROR,
            error_type=type(cause).__name__,
            failure=context,
        )

    # -- metrics --------------------------------------------------------------

    def _dead_letter(
        self, key: bytes, reason: str, detail: str = "", *, event_time_ms: int
    ) -> beam.pvalue.TaggedOutput:
        """Build a dead-letter record and count it. The single chokepoint.

        Every `.errors` emission in this DoFn goes through here, including the
        two timer callbacks, so `agent_errors + orphaned_results` equals the
        element count on `.errors` by construction. A new emission site that
        called the pure `_error` builder directly would be a visibly different
        call, not an invisible omission.

        `event_time_ms` is keyword-only and has no default for the same reason:
        every caller must name the deterministic time it is stamping (the
        element's event time, or the timer's firing time), and a site that
        forgets is a `TypeError` at import-time-adjacent test collection rather
        than a record that silently reads epoch zero downstream.
        """
        if reason == REASON_ORPHANED:
            self._metrics.incr(COUNTER_ORPHANED_RESULTS)
        else:
            self._metrics.incr(COUNTER_AGENT_ERRORS)
        return _error(key, reason, detail, event_time_ms=event_time_ms)


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


def _error(
    entity_key: bytes, reason: str, detail: str = "", *, event_time_ms: int = 0
) -> beam.pvalue.TaggedOutput:
    return beam.pvalue.TaggedOutput(
        "errors", ActivationError(entity_key, reason, detail, event_time_ms)
    )


def _error_trace(
    entity_key: bytes,
    seq: int,
    now_ms: int,
    reason: str,
    *,
    error_type: str = "",
    role: str | None = None,
    failure: FailureContext | None = None,
) -> beam.pvalue.TaggedOutput:
    """An `ERROR` trace event for a failure route, synthesized here (design D5).

    Built from what the DoFn already holds — key, seq, clock, reason, and on
    the `activation_error` route the failure-position scalars — and never from
    the failed activation's staged *effects*, which stay discarded. Emitting it
    mutates no state: a trace is an output record, not keyed state, so
    correctness invariant 1 is untouched. It lands in the same trace as the
    activation's committed events, or stands alone as a one-event trace when
    nothing committed.
    """
    trace = ActivationTrace(entity_key=entity_key, seq=seq, now_ms=now_ms)
    return beam.pvalue.TaggedOutput(
        "traces", trace.error(reason=reason, error_type=error_type, role=role, failure=failure)
    )
