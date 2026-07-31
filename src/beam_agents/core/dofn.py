"""The keyed, stateful ``_AgentDoFn`` — where every correctness invariant lands.

One activation runs per input :class:`~beam_agents._protos.AgentEnvelope`, keyed
by ``entity_key``. Beam serializes elements per key, so working memory is race-
free by construction. The DoFn:

- declares six state specs (``MEMORY``, ``CONTINUATION``, ``LLM_CACHE``,
  ``PENDING``, ``SEQ``, ``BATCH``) and three timers (``TTL_TIMER`` watermark,
  ``HITL_TIMER`` real-time, ``FLUSH_TIMER`` real-time), all protobuf-coded,
  never pickle. ``BATCH``/``FLUSH_TIMER`` belong to adaptive batching and are
  read, written, and armed only under ``BatchPolicy.ADAPTIVE``; under the
  default ``NONE`` they are declared and inert, and every path below is the
  pre-batching one;
- passes every ``MEMORY``/``CONTINUATION``/``LLM_CACHE`` read through
  :func:`~beam_agents.core.migration.migrate_to_current` before interpreting
  any field — the seven read sites in ``_start``, ``_resume``, ``on_ttl``, and
  ``on_hitl``. Migration is lazy and writes nothing at read time: the migrated
  view reaches durable state only through the next successful commit's writes,
  which stamp ``CURRENT_STATE_SCHEMA_VERSION``. A blob from a *newer* schema
  version raises out of the bundle uncaught (never dead-letters), wedging the
  key with zero mutation until the binary is rolled forward;
- routes each element by payload variant (event → start, or → the ``BATCH``
  buffer under ``ADAPTIVE``, where a size or ``FLUSH_TIMER`` trigger later runs
  the whole buffer as one activation; tool_result/approval → resume, never
  buffered; export_request → a read-only ``StateSnapshot`` on ``.snapshots``,
  mutating nothing; unmatched → ``orphaned_result`` on ``.errors``);
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
from beam_agents.core.batching import (
    TRIGGER_SIZE,
    TRIGGER_TIMER,
    BatchSettings,
    buffer_is_full,
    should_flush_on_size,
    should_flush_on_timer,
)
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
from beam_agents.core.snapshot import build_snapshot
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
from beam_agents.model.facade import BudgetExceeded
from beam_agents.observability import ROLE_TIMER, ActivationTrace
from beam_agents.observability.metrics import (
    COUNTER_ACTIVATIONS,
    COUNTER_AGENT_ERRORS,
    COUNTER_BATCH_FLUSHES_SIZE,
    COUNTER_BATCH_FLUSHES_TIMER,
    COUNTER_EVENTS_BUFFERED,
    COUNTER_INTENTS_EMITTED,
    COUNTER_LLM_CALLS,
    COUNTER_LONGTERM_UPSERTS,
    COUNTER_ORPHANED_RESULTS,
    COUNTER_SUSPENSIONS,
    COUNTER_TOOL_CALLS,
    DISTRIBUTION_ACTIVATION_MS,
    DISTRIBUTION_BATCH_SIZE,
    DISTRIBUTION_COMPLETION_TOKENS,
    DISTRIBUTION_ITERATIONS,
    DISTRIBUTION_LLM_MS,
    DISTRIBUTION_MEMORY_BYTES,
    DISTRIBUTION_OVERHEAD_MS,
    DISTRIBUTION_PROMPT_TOKENS,
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
# Working-memory GC reached a key whose `BATCH` buffer still held un-flushed
# events. `max_wait_ms` is orders of magnitude inside `ttl_ms`, so this means a
# stalled pipeline or a backlog watermark jump -- the same clock-skew corner as
# `ttl_wiped_suspension`. One record per wiped envelope, so the loss is
# element-granular and replayable rather than silent.
REASON_TTL_WIPED_BATCH = "ttl_wiped_batch"
# An `event` element that arrived at a key whose buffer already held
# `max_buffered_events`. Dropping is explicit, counted, and triageable; growing
# keyed state silently toward the 1 MiB cap is none of those.
REASON_BATCH_OVERFLOW = "batch_buffer_overflow"
# An activation crossed `max_tokens_per_activation`. Distinct from
# `activation_error` because the triage is different: not "the agent is broken"
# but "this activation was too expensive", which is a budget question, not a
# stack-trace one. The activation commits nothing, exactly as any other failure.
REASON_BUDGET_EXCEEDED = "budget_exceeded"
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
    "DETAIL_DEADLINE_PASSED",
    "DETAIL_INTENT_EXPIRED",
    "DETAIL_NO_CONTINUATION",
    "DETAIL_UNKNOWN_INTENT",
    "HITL_TIMEOUT_OUTPUT",
    "REASON_BATCH_OVERFLOW",
    "REASON_BUDGET_EXCEEDED",
    "REASON_ERROR",
    "REASON_HITL_TIMEOUT",
    "REASON_INTENT_DEAD_LETTER",
    "REASON_ORPHANED",
    "REASON_TIMEOUT",
    "REASON_TTL_WIPED_BATCH",
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
    # The adaptive-batching buffer. Declared unconditionally alongside the other
    # five -- new state IDs start empty, so declaring it is `--update`-compatible
    # -- but read and written only under `BatchPolicy.ADAPTIVE`. Whole envelopes,
    # not payload bytes: each event's `event_time_ms` is what the batch clock is
    # a maximum over, and storing the existing wire message costs no schema
    # change and no `state_schema_version` bump.
    BATCH = BagStateSpec("batch", DeterministicProtoCoder(AgentEnvelope))

    TTL_TIMER = TimerSpec("ttl", TimeDomain.WATERMARK)
    HITL_TIMER = TimerSpec("hitl", TimeDomain.REAL_TIME)
    # The `max_wait_ms` bound. REAL_TIME, not WATERMARK, on purpose: during a
    # backlog replay event time lags wall time by hours, so an event-time mark
    # would land in the past and every element would flush a batch of one --
    # processing time is what makes batching keep working during catch-up.
    FLUSH_TIMER = TimerSpec("flush", TimeDomain.REAL_TIME)

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
        batch: BatchSettings | None = None,
        time_fn: Callable[[], float] | None = None,
        max_tokens_per_activation: int | None = None,
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
        # The resolved batching bounds, or `None` for `BatchPolicy.NONE`. This
        # one field is the whole policy switch: `None` means the element path,
        # `BATCH`, and `FLUSH_TIMER` behave exactly as they did before adaptive
        # batching existed.
        self._batch = batch
        # Wall clock, read at exactly one site: arming `FLUSH_TIMER`. Injected
        # like `metrics`/`monotonic_ns` (a test seam, not an `AgentConfig`
        # knob), and safe for the same reason the monotonic clock is -- the
        # reading decides only *when the timer fires*, never a staged effect, an
        # intent ID, a cache key, a deadline, or an output byte. Replay
        # determinism is scoped to bundle retry, and a retried bundle's batch
        # composition comes from committed bag state, not from this.
        self._time_fn: Callable[[], float] = time_fn if time_fn is not None else time.time
        # The per-attempt token bound, forwarded to each `run_activation`.
        # `None` is unlimited; `AgentConfig` has already refused to pair a set
        # value with a missing `decode`.
        self._max_tokens_per_activation = max_tokens_per_activation
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
        batch: _State = beam.DoFn.StateParam(BATCH),
        flush_timer: _Timer = beam.DoFn.TimerParam(FLUSH_TIMER),
    ) -> Iterator[object]:
        key, envelope = element
        now_ms = envelope.event_time_ms
        variant = envelope.WhichOneof("payload")

        if variant == "export_request":
            # Read-only, ahead of every other route: no activation, no state
            # write, no SEQ increment, no timer. Beam serializes elements per
            # key, so the five reads below observe a consistent point in this
            # key's history — after every activation committed before the
            # request, before every one that follows.
            yield beam.pvalue.TaggedOutput(
                "snapshots",
                build_snapshot(
                    entity_key=key,
                    seq=seq.read(),
                    snapshot_at_ms=now_ms,
                    request_id=envelope.export_request.request_id,
                    # Deliberately NOT migrated: a snapshot records what is
                    # committed, and interpreting older bytes belongs to the
                    # loader (`beam_agents.replay`), which runs the same
                    # migrations this DoFn applies lazily.
                    memory_blob=memory.read(),
                    cache_blob=llm_cache.read(),
                    continuation=continuation.read(),
                    pending=pending.read(),
                ),
            )
            return

        if variant == "tool_result":
            resume = envelope.tool_result
            intent_id = resume.intent_id
            approval = None
        elif variant == "approval":
            resume = None
            intent_id = envelope.approval.intent_id
            approval = envelope.approval
        else:
            # external_event or empty payload. Under `NONE` it starts a fresh
            # activation, unchanged; under `ADAPTIVE` it joins the key's buffer
            # and only a flush trigger activates anything (design D2).
            if self._batch is not None:
                yield from self._buffer(
                    key,
                    envelope,
                    memory,
                    continuation,
                    llm_cache,
                    pending,
                    seq,
                    ttl_timer,
                    hitl_timer,
                    batch,
                    flush_timer,
                )
                return
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

        # A `tool_result`/`approval` never buffers under either policy: it
        # answers one specific suspension, and delaying it would spend that
        # suspension's own `deadline_ms` doing nothing -- a self-inflicted HITL
        # timeout.
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
            batch,
            flush_timer,
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

    # -- adaptive batching: buffer, then flush -------------------------------

    def _buffer(
        self,
        key: bytes,
        envelope: AgentEnvelope,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
        batch: _State,
        flush_timer: _Timer,
    ) -> Iterator[object]:
        """Append one ``event`` element to the key's buffer, and flush if due.

        Commits only the buffering effects — the bag append, a ``TTL_TIMER``
        re-arm (a buffered element is still a processed element), and a
        ``FLUSH_TIMER`` arming on the empty-to-non-empty transition. No
        activation runs, so there is no ``SEQ`` increment, no ``activations``
        count, no trace, and nothing on any output.
        """
        settings = self._batch
        assert settings is not None, "_buffer reached under BatchPolicy.NONE"
        buffered = list(batch.read())
        if buffer_is_full(len(buffered), settings.max_buffered_events):
            # The cap binds only while a suspension defers flushing; past it,
            # shed the event explicitly rather than grow state without bound.
            yield self._dead_letter(
                key,
                REASON_BATCH_OVERFLOW,
                f"buffered={len(buffered)},cap={settings.max_buffered_events}",
                event_time_ms=envelope.event_time_ms,
            )
            return

        batch.add(envelope)
        buffered.append(envelope)
        self._metrics.incr(COUNTER_EVENTS_BUFFERED)
        ttl_timer.set(_ms_timestamp(envelope.event_time_ms + self._ttl_ms))
        if len(buffered) == 1:
            # Armed from the *first* buffered event and never re-armed: a
            # re-arm per element would let a steady trickle starve the flush
            # forever, which is the one thing `max_wait_ms` exists to prevent.
            flush_timer.set(_ms_timestamp(self._wall_now_ms() + settings.max_wait_ms))

        # Migrated before `cont` decides anything, like every other keyed-state
        # read: a deferral decision made against an old layout is not a
        # deferral decision.
        cont = migrate_to_current(continuation.read())
        if should_flush_on_size(
            len(buffered), settings.max_batch_size, continuation_live=cont is not None
        ):
            yield from self._flush(
                key,
                buffered,
                TRIGGER_SIZE,
                memory,
                continuation,
                llm_cache,
                pending,
                seq,
                ttl_timer,
                hitl_timer,
                batch,
                flush_timer,
            )

    def _flush(
        self,
        key: bytes,
        envelopes: list[AgentEnvelope],
        trigger: str,
        memory: _State,
        continuation: _State,
        llm_cache: _State,
        pending: _State,
        seq: _State,
        ttl_timer: _Timer,
        hitl_timer: _Timer,
        batch: _State,
        flush_timer: _Timer,
    ) -> Iterator[object]:
        """Run the whole buffer as exactly one activation.

        Identical to ``_start`` in everything that matters to the correctness
        invariants — one ``SEQ``, intent IDs from ``(key, seq, step_index)``,
        the replay cache keyed by the same ``(key, seq)`` scope, the same
        fixed-order commit — and different in exactly two: the agent sees a
        ``list[bytes]``, and the activation clock is ``max(event_time_ms)``
        over the buffer, a pure function of its contents (so a retried bundle
        re-reads the same bag and reproduces it).
        """
        now_ms = max(envelope.event_time_ms for envelope in envelopes)
        current_seq = seq.read()
        memory_blob = migrate_to_current(memory.read())
        cache_blob = migrate_to_current(llm_cache.read())
        batch_detail = f"batch_size={len(envelopes)},trigger={trigger}"

        try:
            result, activation_ms = self._activate(
                key=key,
                seq=current_seq,
                now_ms=now_ms,
                memory_blob=memory_blob,
                cache_blob=cache_blob,
                events=[envelope.external_event for envelope in envelopes],
                batch_trigger=trigger,
            )
        except ActivationTimeout:
            yield from self._failed_flush(
                key, now_ms, envelopes, REASON_TIMEOUT, batch_detail, batch, flush_timer
            )
            yield _error_trace(key, current_seq, now_ms, REASON_TIMEOUT)
            return
        except ActivationFailed as failed:
            cause = failed.__cause__ if failed.__cause__ is not None else failed
            context = failed.context
            yield from self._failed_flush(
                key,
                now_ms,
                envelopes,
                REASON_ERROR,
                f"{cause!r}{context.detail_suffix()} {batch_detail}",
                batch,
                flush_timer,
            )
            yield _error_trace(
                key,
                current_seq,
                now_ms,
                REASON_ERROR,
                error_type=type(cause).__name__,
                failure=context,
            )
            return
        except Exception as exc:
            yield from self._failed_flush(
                key,
                now_ms,
                envelopes,
                REASON_ERROR,
                f"{exc!r} {batch_detail}",
                batch,
                flush_timer,
            )
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
            batch=batch,
            flush_timer=flush_timer,
            flush_size=len(envelopes),
            flush_trigger=trigger,
        )

    def _failed_flush(
        self,
        key: bytes,
        now_ms: int,
        envelopes: list[AgentEnvelope],
        reason: str,
        detail: str,
        batch: _State,
        flush_timer: _Timer,
    ) -> Iterator[beam.pvalue.TaggedOutput]:
        """Dead-letter every buffered envelope and consume the buffer.

        The activation's own staged effects are discarded by the atomic-commit
        rule, exactly as on the per-event path. What is different is that a
        flush's *inputs* live in keyed state, so "commit nothing" alone would
        re-flush the same poison batch on every later trigger, forever. Clearing
        the buffer here is therefore a deliberate fail-closed cleanup — the same
        class of mutation ``on_hitl`` performs when it clears a dangling
        continuation — and it leaves ``SEQ`` and every other spec untouched.
        The records stay per-envelope, so triage and replay remain
        element-granular even though the failure was batch-granular.
        """
        for _envelope in envelopes:
            yield self._dead_letter(key, reason, detail, event_time_ms=now_ms)
        batch.clear()
        flush_timer.clear()

    def _wall_now_ms(self) -> int:
        """Wall-clock epoch milliseconds, from the injected clock."""
        return int(self._time_fn() * 1000)

    def _rearm_flush(self, batch: _State | None, flush_timer: _Timer | None) -> None:
        """Re-arm ``FLUSH_TIMER`` to fire promptly over a deferred buffer.

        Called by the two paths that end a suspension — a resume that commits
        without re-suspending, and ``on_hitl``'s ``Deny``/``Drop`` routes. While
        the continuation was live both flush triggers deferred, so without this
        the deferred batch would wait out a mark that has long since fired (or
        was never armed at all) and only TTL GC would ever reach it. Routing it
        through the timer rather than flushing inline keeps the invariant of one
        activation per ``process()``/timer call.

        ``Escalate`` does not call this: it keeps the suspension live, so
        flushing would run straight into the continuation it would overwrite.
        """
        if self._batch is None or batch is None or flush_timer is None:
            return
        if batch.read():
            flush_timer.set(_ms_timestamp(self._wall_now_ms()))

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
        batch: _State | None = None,
        flush_timer: _Timer | None = None,
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
            # A resume that completes ends the suspension, which is what
            # unblocks a deferred buffer (design D6). Handed the buffer's
            # handles under `ADAPTIVE`; both are `None` under `NONE`.
            batch=batch,
            flush_timer=flush_timer,
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
        events: list[bytes] | None = None,
        batch_trigger: str = "",
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
                    events=events,
                    batch_trigger=batch_trigger,
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
                    max_tokens_per_activation=self._max_tokens_per_activation,
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
        *,
        batch: _State | None = None,
        flush_timer: _Timer | None = None,
        flush_size: int | None = None,
        flush_trigger: str = "",
    ) -> Iterator[object]:
        # Fixed commit order (design D3, extended by add-runtime-metrics and
        # add-adaptive-batching): MEMORY, LLM_CACHE, CONTINUATION, PENDING,
        # BATCH, SEQ, timers, metrics, emits. Reached only on activation
        # success. The batch parameters are keyword-only and default to the
        # per-event shape, so every pre-existing call site is unchanged: under
        # `BatchPolicy.NONE` nothing below touches `BATCH` or `FLUSH_TIMER`.
        memory.write(result.memory_blob)
        llm_cache.write(result.cache_blob)

        if result.continuation is not None:
            continuation.write(result.continuation)
        else:
            continuation.clear()

        pending.clear()
        for intent in result.intents:
            pending.add(intent)

        if flush_size is not None:
            # This activation's inputs are keyed state, so consuming them is
            # part of the same all-or-nothing commit as the effects they
            # produced: the buffer clears exactly when the flush commits.
            assert batch is not None, "a flush commit needs its buffer handle"
            batch.clear()

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

        if flush_size is not None:
            # The buffer this flush consumed must not be flushed again by the
            # mark its first element armed.
            assert flush_timer is not None, "a flush commit needs its timer handle"
            flush_timer.clear()
        elif result.continuation is None:
            # A resume that completed: the suspension is over, so a buffer that
            # deferred behind it flushes promptly, in its own callback.
            self._rearm_flush(batch, flush_timer)

        # Recorded before the yields, not after: `_commit` is a generator, and
        # recording placed after them would be contingent on how the consumer
        # drains it. Beam always drains fully, but the ordering must not depend
        # on that.
        self._record_commit(result, activation_ms, flush_size, flush_trigger)

        yield from result.outputs
        for intent in result.intents:
            yield beam.pvalue.TaggedOutput("intents", intent)
        for trace in result.traces:
            yield beam.pvalue.TaggedOutput("traces", trace)

    def _record_commit(
        self,
        result: ActivationResult,
        activation_ms: int,
        flush_size: int | None = None,
        flush_trigger: str = "",
    ) -> None:
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
        if flush_size is not None:
            # Counted here, on the committed path, so `batch_flushes_size +
            # batch_flushes_timer` equals the committed flushes and the
            # `batch_size` sample count -- and so a failed flush (visible
            # through `agent_errors`) never inflates them.
            metrics.incr(
                COUNTER_BATCH_FLUSHES_SIZE
                if flush_trigger == TRIGGER_SIZE
                else COUNTER_BATCH_FLUSHES_TIMER
            )
            metrics.observe(DISTRIBUTION_BATCH_SIZE, flush_size)
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
            # from a path that never decodes would deflate the distribution. The
            # cost pair rides the same guard, so all three sample counts mean
            # "activations with known usage" and stay directly comparable.
            metrics.observe(DISTRIBUTION_TOKENS, tally.total_tokens)
            metrics.observe(DISTRIBUTION_PROMPT_TOKENS, tally.prompt_tokens)
            metrics.observe(DISTRIBUTION_COMPLETION_TOKENS, tally.completion_tokens)
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
        batch: _State = beam.DoFn.StateParam(BATCH),
        flush_timer: _Timer = beam.DoFn.TimerParam(FLUSH_TIMER),
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
        #
        # Ordered before the buffered-event dead letters below on purpose: a
        # raising flush must abort this callback *before* anything is emitted,
        # or the retry would re-emit the dead letters it already yielded.
        if self._on_expire is not None:
            self._flush_expiring(key, memory, seq, timestamp.micros // 1000)

        if self._batch is not None:
            # Buffered events are lost with everything else, and losing them
            # silently would be indistinguishable from never having received
            # them. One record per envelope keeps the loss element-granular, so
            # a replay from the errors sink is possible.
            buffered = list(batch.read())
            for index in range(len(buffered)):
                yield self._dead_letter(
                    key,
                    REASON_TTL_WIPED_BATCH,
                    f"buffered={len(buffered)},index={index}",
                    event_time_ms=timestamp.micros // 1000,
                )

        # Working memory is event-time garbage: wipe every spec so an idle key
        # leaves zero residue. Unconditional -- reporting the loss above does not
        # rescue the key. No SEQ change beyond the wipe.
        memory.clear()
        continuation.clear()
        llm_cache.clear()
        pending.clear()
        seq.clear()
        if self._batch is not None:
            # Guarded, not unconditional: under `BatchPolicy.NONE` this spec is
            # never read or written, and clearing it would be a write.
            batch.clear()
            flush_timer.clear()

    @on_timer(FLUSH_TIMER)
    def on_flush(
        self,
        key: bytes = beam.DoFn.KeyParam,  # type: ignore[assignment]
        timestamp: Timestamp = beam.DoFn.TimestampParam,  # type: ignore[assignment]
        memory: _State = beam.DoFn.StateParam(MEMORY),
        continuation: _State = beam.DoFn.StateParam(CONTINUATION),
        llm_cache: _State = beam.DoFn.StateParam(LLM_CACHE),
        pending: _State = beam.DoFn.StateParam(PENDING),
        seq: _State = beam.DoFn.StateParam(SEQ),
        ttl_timer: _Timer = beam.DoFn.TimerParam(TTL_TIMER),
        hitl_timer: _Timer = beam.DoFn.TimerParam(HITL_TIMER),
        batch: _State = beam.DoFn.StateParam(BATCH),
        flush_timer: _Timer = beam.DoFn.TimerParam(FLUSH_TIMER),
    ) -> Iterator[object]:
        """``max_wait_ms`` elapsed: flush the buffer, or decline to.

        Two declines, both silent and both mutating nothing. An empty buffer
        means the mark is stale — a size flush consumed the batch and cleared
        the timer, but the delivery arrived anyway — and re-processing a
        consumed batch is exactly what the clear was for. A live continuation
        means deferral: the buffer stays intact, and the path that resolves the
        suspension re-arms this timer (design D6).

        Unreachable under `BatchPolicy.NONE`, which never arms this timer; the
        guard makes that explicit rather than relying on it.
        """
        if self._batch is None:
            return
        buffered = list(batch.read())
        cont = migrate_to_current(continuation.read())
        if not should_flush_on_timer(len(buffered), continuation_live=cont is not None):
            return
        yield from self._flush(
            key,
            buffered,
            TRIGGER_TIMER,
            memory,
            continuation,
            llm_cache,
            pending,
            seq,
            ttl_timer,
            hitl_timer,
            batch,
            flush_timer,
        )

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
        batch: _State = beam.DoFn.StateParam(BATCH),
        flush_timer: _Timer = beam.DoFn.TimerParam(FLUSH_TIMER),
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
        # The wait is over, so a buffer that deferred behind this suspension is
        # free to flush -- the `Escalate` route above returned before reaching
        # here precisely because its wait is *not* over.
        self._rearm_flush(batch, flush_timer)
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

        The reason is selected from the cause's class: a tripped token budget is
        `budget_exceeded`, everything else stays `activation_error`
        byte-identically. Dispatching here rather than catching `BudgetExceeded`
        in `_start`/`_resume` keeps one handler building both records — a second
        catch clause would be a second place for them to drift apart.
        """
        cause = failed.__cause__ if failed.__cause__ is not None else failed
        context = failed.context
        reason = REASON_BUDGET_EXCEEDED if isinstance(cause, BudgetExceeded) else REASON_ERROR
        yield self._dead_letter(
            key, reason, f"{cause!r}{context.detail_suffix()}", event_time_ms=now_ms
        )
        yield _error_trace(
            key,
            seq,
            now_ms,
            reason,
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
