"""Adaptive batching driven with fake state/timer handles, no runner.

Same rationale as `test_dofn_metrics.py`: driving `process` and the timer
callbacks directly keeps the buffering branch, both flush paths, the deferral
guards, and the failure/overflow/TTL routes inside the mutation gate's test
selection, which the `TestStream` pipeline suites (deselected under mutmut)
cannot do. What a fake handle cannot show — that a REAL_TIME `FLUSH_TIMER`
actually fires `max_wait_ms` later — is `test_dofn_pipeline.py`'s job.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import AgentEnvelope, Continuation, LlmCacheBlob, MemoryBlob, ToolIntent
from beam_agents._protos import TraceEvent as TraceEventProto
from beam_agents.core.agent import intent_id_for
from beam_agents.core.batching import (
    TRACE_BATCH_SIZE,
    TRACE_BATCH_TRIGGER,
    TRIGGER_SIZE,
    TRIGGER_TIMER,
    BatchSettings,
)
from beam_agents.core.dofn import (
    REASON_BATCH_OVERFLOW,
    REASON_ERROR,
    REASON_TIMEOUT,
    REASON_TTL_WIPED_BATCH,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.core.loop import ActivationResult
from beam_agents.hitl import HITL_TIMEOUT_OUTPUT, HitlPolicy
from beam_agents.memory import Memory
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key
from beam_agents.observability import trace_id_for
from beam_agents.observability.metrics import (
    COUNTER_ACTIVATIONS,
    COUNTER_AGENT_ERRORS,
    COUNTER_BATCH_FLUSHES_SIZE,
    COUNTER_BATCH_FLUSHES_TIMER,
    COUNTER_EVENTS_BUFFERED,
    DISTRIBUTION_BATCH_SIZE,
)
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeTimer, FakeValue, RecordingMetrics
from tests.core._dofn_helpers import (
    batch_intent_agent,
    batch_join_agent,
    batch_prior_agent,
    batch_suspend_agent,
    escalate_once,
    make_briefly_slow_provider,
    make_pong_provider,
    model_agent,
    raising_agent,
    request,
)

_KEY = b"k"
_TTL_MS = 3_600_000
# Small enough that a test can reach the threshold in three elements, and a
# buffer cap with room for deferral above it.
_SETTINGS = BatchSettings(max_batch_size=3, max_wait_ms=500, max_buffered_events=6)
_WALL_S = 10.0
# The one request `model_agent` issues, restated here so a test can compute the
# replay-cache key a flush will look up.
_REQUEST = request()


def _event(payload: bytes, t_ms: int = 1_000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=t_ms, external_event=payload)


def _tool_result(intent_id: str, t_ms: int = 1_500) -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=t_ms)
    envelope.tool_result.intent_id = intent_id
    envelope.tool_result.entity_key = _KEY
    envelope.tool_result.payload = b"done"
    return envelope


def _main(emitted: list[Any]) -> list[Any]:
    return [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)]


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


@dataclass
class _Handles:
    """One key's state and timer doubles, in `process()` keyword order."""

    memory: FakeValue = field(default_factory=lambda: FakeValue(MemoryBlob()))
    continuation: FakeValue = field(default_factory=lambda: FakeValue(None))
    llm_cache: FakeValue = field(default_factory=lambda: FakeValue(LlmCacheBlob()))
    pending: FakeBag = field(default_factory=FakeBag)
    seq: FakeSum = field(default_factory=FakeSum)
    ttl_timer: FakeTimer = field(default_factory=FakeTimer)
    hitl_timer: FakeTimer = field(default_factory=FakeTimer)
    batch: FakeBag = field(default_factory=FakeBag)
    flush_timer: FakeTimer = field(default_factory=FakeTimer)

    def kwargs(self) -> dict[str, Any]:
        return {
            "memory": self.memory,
            "continuation": self.continuation,
            "llm_cache": self.llm_cache,
            "pending": self.pending,
            "seq": self.seq,
            "ttl_timer": self.ttl_timer,
            "hitl_timer": self.hitl_timer,
            "batch": self.batch,
            "flush_timer": self.flush_timer,
        }

    def payloads(self) -> list[bytes]:
        return [envelope.external_event for envelope in self.batch.items]


class _Driver:
    """An `ADAPTIVE` DoFn plus one key's handles, driven element by element."""

    def __init__(
        self,
        agent: Any = batch_join_agent,
        *,
        settings: BatchSettings = _SETTINGS,
        handles: _Handles | None = None,
        hitl_policy: HitlPolicy | None = None,
        wall_s: float = _WALL_S,
        provider_factory: Any = make_pong_provider,
        activation_timeout_s: float = 30.0,
        cancel_grace_s: float = 5.0,
        setup: bool = True,
    ) -> None:
        self.metrics = RecordingMetrics()
        self.handles = handles if handles is not None else _Handles()
        self.dofn = _AgentDoFn(
            agent,
            provider_factory=provider_factory,
            activation_timeout_s=activation_timeout_s,
            cancel_grace_s=cancel_grace_s,
            ttl_ms=_TTL_MS,
            hitl_policy=hitl_policy,
            metrics=self.metrics,
            batch=settings,
            time_fn=lambda: wall_s,
        )
        # `setup=False` leaves the bridge and provider unbuilt, which is what
        # presents `_activate`'s wiring assertion to the flush path -- the only
        # failure that is neither an `ActivationTimeout` nor an
        # `ActivationFailed`, and so the only one that reaches `_flush`'s
        # general-exception route.
        self._setup = setup
        if setup:
            self.dofn.setup()

    def process(self, envelope: AgentEnvelope) -> list[Any]:
        return list(self.dofn.process((_KEY, envelope), **self.handles.kwargs()))

    def fire_flush(self, fired_at_ms: int = 2_000) -> list[Any]:
        return list(
            self.dofn.on_flush(
                key=_KEY,
                timestamp=Timestamp(micros=fired_at_ms * 1000),
                **self.handles.kwargs(),
            )
        )

    def fire_hitl(self, fired_at_ms: int = 9_000) -> list[Any]:
        handles = self.handles
        return list(
            self.dofn.on_hitl(
                key=_KEY,
                timestamp=Timestamp(micros=fired_at_ms * 1000),
                continuation=handles.continuation,
                pending=handles.pending,
                hitl_timer=handles.hitl_timer,
                ttl_timer=handles.ttl_timer,
                batch=handles.batch,
                flush_timer=handles.flush_timer,
            )
        )

    def fire_ttl(self, fired_at_ms: int = 12_000) -> list[Any]:
        handles = self.handles
        return list(
            self.dofn.on_ttl(
                key=_KEY,
                timestamp=Timestamp(micros=fired_at_ms * 1000),
                memory=handles.memory,
                continuation=handles.continuation,
                llm_cache=handles.llm_cache,
                pending=handles.pending,
                seq=handles.seq,
                batch=handles.batch,
                flush_timer=handles.flush_timer,
            )
        )

    def close(self) -> None:
        if self._setup:
            self.dofn.teardown()


_DriverFactory = Callable[..., _Driver]


@pytest.fixture
def driver() -> Iterator[Callable[..., _Driver]]:
    """Factory for `_Driver`s, torn down (bridge thread included) at test end."""
    made: list[_Driver] = []

    def build(*args: Any, **kwargs: Any) -> _Driver:
        instance = _Driver(*args, **kwargs)
        made.append(instance)
        return instance

    yield build
    for instance in made:
        instance.close()


# --- Requirement: ADAPTIVE opt-in buffers instead of activating ---------------


def test_adaptive_opt_in_buffers_instead_of_activating(driver: _DriverFactory) -> None:
    # Scenario: ADAPTIVE opt-in buffers instead of activating. The envelope is
    # appended, nothing is emitted on any output, and no activation ran -- so
    # SEQ is untouched and no commit-path metric was recorded.
    run = driver()

    emitted = run.process(_event(b"a"))

    assert emitted == []
    assert run.handles.payloads() == [b"a"]
    assert run.handles.seq.value == 0
    assert run.metrics.counters[COUNTER_EVENTS_BUFFERED] == 1
    assert COUNTER_ACTIVATIONS not in run.metrics.counters
    # A buffered element is still a processed element: it re-arms working-memory
    # GC like any other.
    assert run.handles.ttl_timer.set_to == Timestamp(micros=(1_000 + _TTL_MS) * 1000)


def test_the_wait_is_armed_once_from_the_first_buffered_event(driver: _DriverFactory) -> None:
    # Scenario: The wait is measured from the first buffered event. Re-arming
    # per element would let a steady trickle starve the flush forever, so the
    # arming happens on the empty-to-non-empty transition and nowhere else.
    run = driver()

    run.process(_event(b"a"))
    run.process(_event(b"b"))

    assert run.handles.flush_timer.marks == [
        Timestamp(micros=(int(_WALL_S * 1000) + _SETTINGS.max_wait_ms) * 1000)
    ]
    assert run.handles.payloads() == [b"a", b"b"]
    assert run.handles.seq.value == 0


def test_an_event_joining_a_buffer_left_by_an_earlier_bundle_arms_nothing(
    driver: _DriverFactory,
) -> None:
    # The same scenario across a bundle boundary, which is where the arming
    # rule is actually load-bearing. `BATCH` is committed keyed state, so a new
    # bundle can find the buffer already non-empty with its `FLUSH_TIMER` mark
    # already set and no reading of the clock in this DoFn's history. The
    # empty-to-non-empty transition is the only arming point, so appending to
    # an existing buffer must arm nothing at all -- and unlike the two-events-
    # in-one-bundle case above, which cannot distinguish "armed on the first"
    # from "armed on every later one" under a fixed clock, this one has an
    # observable difference: zero marks against one.
    handles = _Handles(batch=FakeBag([_event(b"earlier")]))
    run = driver(handles=handles)

    run.process(_event(b"b"))

    assert handles.flush_timer.marks == []
    assert handles.payloads() == [b"earlier", b"b"]
    assert handles.seq.value == 0


def test_buffering_under_the_none_policy_names_the_wiring_bug() -> None:
    # `_buffer`'s guard. `process()` routes to it only when batch settings
    # exist, so `BatchPolicy.NONE` can never reach it -- which is exactly what
    # the assertion says, and why the message has to name the policy: a future
    # rewiring that routed a NONE pipeline here would otherwise crash on
    # `settings.max_buffered_events` with a bare `AttributeError` that names
    # nothing.
    dofn = _AgentDoFn(batch_join_agent, provider_factory=make_pong_provider, batch=None)
    handles = _Handles()

    with pytest.raises(AssertionError) as exc_info:
        list(dofn._buffer(_KEY, _event(b"a"), **handles.kwargs()))

    assert str(exc_info.value) == "_buffer reached under BatchPolicy.NONE"
    assert handles.batch.items == []


# --- Requirement: reaching the size threshold flushes the buffer --------------


def test_size_threshold_flush_runs_one_activation_over_the_whole_buffer(
    driver: _DriverFactory,
) -> None:
    # Scenario: Size-threshold flush runs one activation over the whole buffer.
    run = driver()

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    emitted = run.process(_event(b"c"))

    # One activation over all three, in arrival order, at seq 0.
    assert _main(emitted) == [b"a|b|c#0"]
    assert run.handles.seq.value == 1
    assert run.metrics.counters[COUNTER_ACTIVATIONS] == 1
    assert run.metrics.counters[COUNTER_BATCH_FLUSHES_SIZE] == 1
    assert COUNTER_BATCH_FLUSHES_TIMER not in run.metrics.counters
    assert run.metrics.samples[DISTRIBUTION_BATCH_SIZE] == [3]
    # The buffer is consumed atomically with the commit.
    assert run.handles.batch.items == []
    assert run.handles.batch.cleared is True


def test_a_size_flush_disarms_the_pending_flush_timer(driver: _DriverFactory) -> None:
    # Scenario: A size flush disarms the pending flush timer -- otherwise the
    # mark armed by the buffer's first event would fire over the consumed
    # buffer later.
    run = driver()

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    run.process(_event(b"c"))

    assert run.handles.flush_timer.cleared is True


def test_a_flush_runs_against_the_keys_committed_working_memory(
    driver: _DriverFactory,
) -> None:
    # A flush is an activation like any other, so it reads the key's committed
    # `MEMORY` before running and writes the result back. Every batching test
    # above starts from an empty blob, where "read the committed memory" and
    # "start from nothing" are the same picture; this one seeds the key so they
    # are not. Without the read a batch would silently reason from a blank
    # working memory -- the failure mode a keyed agent runtime exists to
    # prevent, and one no output shape reveals on its own.
    seeded = Memory(now_ms=1_000)
    seeded.set("prior", b"seed")
    handles = _Handles(memory=FakeValue(seeded.to_blob()))
    run = driver(batch_prior_agent, handles=handles)

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    emitted = run.process(_event(b"c"))

    assert _main(emitted) == [b"seed/a|b|c"]
    # ...and the committed blob carries the batch's own write forward.
    committed = Memory(handles.memory.value, now_ms=1_000)
    assert committed.get("prior") == b"seed"
    assert committed.get("last_batch") == b"a|b|c"


def test_a_flush_resolves_its_model_call_from_the_keys_replay_cache(
    driver: _DriverFactory,
) -> None:
    # Scenario: A retried flush bundle replays deterministically -- "resolves
    # the call from the replay cache with zero extra provider calls". The cache
    # is keyed by `(entity_key, seq)`, which a flush shares with the attempt
    # that populated it, so a re-run flush must reach the cached bytes. A flush
    # that read no cache would look identical apart from the provider call it
    # made, which is the entire correctness claim.
    cache = ReplayCache(None, now_ms=1_000)
    cache.put(
        compute_cache_key(
            _REQUEST.model_id,
            _REQUEST.messages,
            _REQUEST.tools_schema,
            _REQUEST.sampling_params,
            _KEY,
            0,
        ),
        b"from-cache",
    )
    provider = make_pong_provider()
    handles = _Handles(llm_cache=FakeValue(cache.to_blob()))
    run = driver(model_agent, handles=handles, provider_factory=lambda: provider)

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    emitted = run.process(_event(b"c"))

    assert _main(emitted) == [b"from-cache"]
    assert provider.call_count == 0


def test_a_flushs_activation_trace_names_the_batch_it_ran_over(
    driver: _DriverFactory,
) -> None:
    # Scenario: One activation per flush ... "the flush activation's trace SHALL
    # carry `beam_agents.batch.size` and `beam_agents.batch.trigger`". Asserted
    # here, at the DoFn, and not only at the loop driver: the trigger is decided
    # by whichever of the two flush paths fired and has to survive the hand-off
    # through `_flush` and `_activate` to reach the trace. A trigger dropped in
    # transit leaves a trace that still says "this was a batch" while silently
    # losing which threshold assembled it.
    run = driver()

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    size_flush = run.process(_event(b"c"))

    start = _tagged(size_flush, "traces")[0]
    assert start.event_type == TraceEventProto.ACTIVATION_START
    assert start.attributes[TRACE_BATCH_SIZE] == "3"
    assert start.attributes[TRACE_BATCH_TRIGGER] == TRIGGER_SIZE

    run.process(_event(b"d"))
    timer_flush = run.fire_flush()

    start = _tagged(timer_flush, "traces")[0]
    assert start.attributes[TRACE_BATCH_SIZE] == "1"
    assert start.attributes[TRACE_BATCH_TRIGGER] == TRIGGER_TIMER


async def test_the_activation_seam_names_no_trigger_by_default(driver: _DriverFactory) -> None:
    # `_activate` carries `events` and `batch_trigger` as two independent
    # parameters, so a caller can hand it a batch without naming a trigger --
    # `_start` and `_resume` rely on that default to omit both. The unnamed
    # value has to be empty for the same reason `run_activation`'s is: the
    # attribute reaches a trace consumer that partitions flushes by trigger,
    # and a placeholder there would count as a third trigger that never fired.
    run = driver()

    result, _elapsed_ms = run.dofn._activate(
        key=_KEY,
        seq=0,
        now_ms=1_000,
        memory_blob=None,
        cache_blob=None,
        events=[b"a"],
    )

    (start,) = [e for e in result.traces if e.event_type == TraceEventProto.ACTIVATION_START]
    assert start.attributes[TRACE_BATCH_SIZE] == "1"
    assert start.attributes[TRACE_BATCH_TRIGGER] == ""


def test_the_batch_clock_is_the_latest_buffered_event_time(driver: _DriverFactory) -> None:
    # Scenario: The batch clock is the latest buffered event time. `now_ms` is
    # `max(event_time_ms)` over the buffer -- a pure function of its contents,
    # so a retried flush reproduces it -- and intent expiries derive from it.
    run = driver(batch_intent_agent)

    run.process(_event(b"a", t_ms=1_000))
    run.process(_event(b"b", t_ms=3_000))
    emitted = run.process(_event(b"c", t_ms=2_000))

    (intent,) = _tagged(emitted, "intents")
    assert intent.created_at_ms == 3_000
    assert intent.expires_at_ms == 3_000 + 60_000


# --- Requirement: max_wait is honored via a processing-time FLUSH_TIMER -------


def test_a_timer_firing_flushes_the_whole_buffer(driver: _DriverFactory) -> None:
    # The timer trigger's own flush: identical to a size flush, but counted
    # under the trigger that fired it.
    run = driver()
    run.process(_event(b"a"))
    run.process(_event(b"b"))

    emitted = run.fire_flush()

    assert _main(emitted) == [b"a|b#0"]
    assert run.handles.seq.value == 1
    assert run.metrics.counters[COUNTER_BATCH_FLUSHES_TIMER] == 1
    assert COUNTER_BATCH_FLUSHES_SIZE not in run.metrics.counters
    assert run.metrics.samples[DISTRIBUTION_BATCH_SIZE] == [2]
    assert run.handles.batch.items == []


def test_a_stale_flush_firing_over_an_empty_buffer_is_a_no_op(driver: _DriverFactory) -> None:
    # Scenario: A stale flush firing over an empty buffer is a no-op. A cleared
    # timer's mark can still be delivered, so the callback must mutate nothing
    # and emit nothing -- mirroring `on_hitl`'s stale-handle guard.
    run = driver()

    emitted = run.fire_flush()

    assert emitted == []
    assert run.handles.seq.value == 0
    assert run.handles.batch.cleared is False
    assert run.handles.flush_timer.cleared is False
    assert run.metrics.counters == {}


def test_batch_metrics_reconcile_with_batch_behavior(driver: _DriverFactory) -> None:
    # Scenario: Batch metrics reconcile with batch behavior. Asserted on one
    # DoFn instance with one recorder so both triggers are visible in the same
    # totals -- which the classic DirectRunner, reporting one bundle's metric
    # updates per TestStream group, cannot show.
    run = driver()

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    run.process(_event(b"c"))  # size flush of three
    run.process(_event(b"d"))
    run.process(_event(b"e"))
    run.fire_flush()  # timer flush of two

    assert run.metrics.counters[COUNTER_EVENTS_BUFFERED] == 5
    assert run.metrics.counters[COUNTER_BATCH_FLUSHES_SIZE] == 1
    assert run.metrics.counters[COUNTER_BATCH_FLUSHES_TIMER] == 1
    assert run.metrics.samples[DISTRIBUTION_BATCH_SIZE] == [3, 2]
    # A committed flush is one activation regardless of batch size, and the two
    # flush counters partition the committed flushes exactly.
    assert run.metrics.counters[COUNTER_ACTIVATIONS] == 2
    assert run.metrics.counters[COUNTER_BATCH_FLUSHES_SIZE] + run.metrics.counters[
        COUNTER_BATCH_FLUSHES_TIMER
    ] == len(run.metrics.samples[DISTRIBUTION_BATCH_SIZE])
    assert run.handles.seq.value == 2


# --- Requirement: flush triggers defer while a continuation is live ----------


def _live_continuation(seq: int = 0, step_index: int = 1) -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=seq,
        step_index=step_index,
        pending_intent_ids=[intent_id_for(_KEY, seq, 0)],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=9_000,
    )


def test_the_size_trigger_defers_during_a_suspension(driver: _DriverFactory) -> None:
    # Scenario: The size trigger defers during a suspension. A flush that
    # suspended would overwrite the live continuation and orphan its intents,
    # so the buffer keeps absorbing past `max_batch_size` instead.
    handles = _Handles(continuation=FakeValue(_live_continuation()))
    run = driver(handles=handles)

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    emitted = run.process(_event(b"c"))

    assert emitted == []
    assert handles.payloads() == [b"a", b"b", b"c"]
    assert handles.seq.value == 0
    assert handles.continuation.value == _live_continuation()
    assert COUNTER_ACTIVATIONS not in run.metrics.counters


def test_a_timer_firing_during_a_suspension_does_not_overwrite_the_continuation(
    driver: _DriverFactory,
) -> None:
    # Scenario: A timer firing during a suspension does not overwrite the
    # continuation. The callback leaves the buffer intact and runs nothing.
    handles = _Handles(continuation=FakeValue(_live_continuation()))
    run = driver(handles=handles)
    run.process(_event(b"a"))
    run.process(_event(b"b"))

    emitted = run.fire_flush()

    assert emitted == []
    assert handles.payloads() == [b"a", b"b"]
    assert handles.batch.cleared is False
    assert handles.continuation.value == _live_continuation()
    assert handles.seq.value == 0


def test_a_resume_commit_rearms_the_flush_timer_over_a_deferred_buffer(
    driver: _DriverFactory,
) -> None:
    # Scenario: Resolution flushes the deferred buffer promptly. The resume
    # runs its own activation; the deferred batch flushes in its own timer
    # callback, so one `process()` call never runs two activations.
    cont = _live_continuation()
    handles = _Handles(
        continuation=FakeValue(cont),
        pending=FakeBag([ToolIntent(intent_id=cont.pending_intent_ids[0], expires_at_ms=9_000)]),
    )
    run = driver(batch_suspend_agent, handles=handles)
    run.process(_event(b"a"))
    handles.flush_timer.marks.clear()

    emitted = run.process(_tool_result(cont.pending_intent_ids[0]))

    assert _main(emitted) == [b"resumed:waiting#0"]
    # The suspension is over and the buffer is non-empty: the resolving path
    # re-arms the timer to fire promptly, at the resolution's wall clock.
    assert handles.flush_timer.marks == [Timestamp(micros=int(_WALL_S * 1000) * 1000)]
    assert handles.payloads() == [b"a"]


def test_a_hitl_deny_route_rearms_the_flush_over_a_deferred_buffer(
    driver: _DriverFactory,
) -> None:
    # Scenario: Resolution flushes the deferred buffer promptly -- the other
    # resolving path. `Deny`/`Drop` end the wait without a resume, so if they
    # did not re-arm, the deferred buffer would sit until `max_wait_ms` was
    # long past and only TTL GC would ever reach it.
    handles = _Handles(continuation=FakeValue(_live_continuation()))
    run = driver(handles=handles)
    run.process(_event(b"a"))
    handles.flush_timer.marks.clear()

    emitted = run.fire_hitl()

    assert _main(emitted) == [HITL_TIMEOUT_OUTPUT]
    assert handles.continuation.value is None
    assert handles.flush_timer.marks == [Timestamp(micros=int(_WALL_S * 1000) * 1000)]
    assert handles.payloads() == [b"a"]


def test_an_escalation_keeps_the_suspension_live_and_keeps_deferring(
    driver: _DriverFactory,
) -> None:
    # Scenario: Flush triggers defer while a continuation is live -- the
    # `Escalate` clause. The wait is not over, so re-arming here would run a
    # flush straight into the still-live continuation it would overwrite.
    handles = _Handles(continuation=FakeValue(_live_continuation()))
    run = driver(
        handles=handles,
        hitl_policy=HitlPolicy(on_timeout=escalate_once, max_escalations=1),
    )
    run.process(_event(b"a"))
    handles.flush_timer.marks.clear()

    run.fire_hitl()

    assert handles.continuation.value is not None
    assert handles.flush_timer.marks == []
    assert handles.payloads() == [b"a"]


def test_a_resolution_over_an_empty_buffer_arms_nothing(driver: _DriverFactory) -> None:
    # The re-arm is conditional on there being something to flush: a resolved
    # suspension with an empty buffer must not schedule a callback whose only
    # possible outcome is the empty-buffer no-op.
    handles = _Handles(continuation=FakeValue(_live_continuation()))
    run = driver(handles=handles)

    run.fire_hitl()

    assert handles.flush_timer.marks == []


# --- Requirement: batch failure and overflow fail closed ---------------------


def test_a_failed_flush_dead_letters_every_batched_event_and_consumes_the_buffer(
    driver: _DriverFactory,
) -> None:
    # Scenario: A failed flush dead-letters every batched event and consumes the
    # buffer. "Commit nothing" alone would retry the same poison batch on every
    # later trigger forever, so the failure route explicitly consumes it.
    run = driver(raising_agent)

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    emitted = run.process(_event(b"c"))

    # The whole record, per envelope, not a field at a time. `.errors` is a
    # sink an operator triages from: the key says which entity is poisoned,
    # the detail says what raised and how big the batch was, and the event
    # time says when -- and every one of those is derived from a different
    # argument at the call site.
    detail = (
        f"{RuntimeError('agent blew up')!r} failed_at_step=0 after=ACTIVATION_START "
        f"batch_size=3,trigger={TRIGGER_SIZE}"
    )
    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ERROR, detail, 1_000) for _ in range(3)
    ]
    # One activation, so one ERROR trace -- not one per batched envelope.
    assert len(_tagged(emitted, "traces")) == 1
    assert run.metrics.counters[COUNTER_AGENT_ERRORS] == 3
    # The buffer and its timer are consumed; nothing else moved.
    assert run.handles.batch.items == []
    assert run.handles.flush_timer.cleared is True
    assert run.handles.seq.value == 0
    assert run.handles.memory.value == MemoryBlob()
    assert run.handles.llm_cache.value == LlmCacheBlob()
    assert run.handles.continuation.value is None
    assert run.handles.pending.items == []


def test_a_failed_flushs_error_trace_carries_the_batchs_scope_and_position(
    driver: _DriverFactory,
) -> None:
    # The trace half of the same scenario. The dead letters say what happened;
    # the ERROR trace says *where* -- under which key and seq the batch ran, at
    # which clock, what type raised, and how far the activation had walked. It
    # is synthesized entirely from values `_flush` is holding when it catches,
    # so every one of them is an argument that can be dropped or crossed with
    # its neighbour and still produce a plausible-looking event.
    run = driver(raising_agent)

    run.process(_event(b"a"))
    emitted = run.process(_event(b"b", t_ms=3_000))
    emitted += run.process(_event(b"c", t_ms=2_000))

    (trace,) = _tagged(emitted, "traces")
    assert trace.event_type == TraceEventProto.ERROR
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    # The batch's own scope: the key's current seq (the flush never got to
    # increment it) and the batch clock, `max(event_time_ms)`.
    assert trace.trace_id == trace_id_for(_KEY, 0)
    assert trace.start_ms == 3_000
    # The agent's exception type, not the runtime's `ActivationFailed` wrapper.
    assert trace.attributes["error.type"] == "RuntimeError"
    # ...and the failure position, which only the enriched `ActivationFailed`
    # route can supply: nothing was staged before the raise.
    assert trace.attributes["beam_agents.failure.step"] == "0"
    assert trace.attributes["beam_agents.failure.last_event"] == "ACTIVATION_START"
    assert trace.attributes["beam_agents.failure.staged_intents"] == "0"
    assert trace.attributes["beam_agents.failure.llm_calls"] == "0"


def test_a_flush_that_times_out_dead_letters_the_batch_with_no_exception_named(
    driver: _DriverFactory,
) -> None:
    # Scenario: A flush activation that ... exceeds `activation_timeout` ...
    # emits one ActivationError per buffered envelope with reason
    # `activation_timeout`. The timeout route is the flush path's third exit
    # and the only one with no exception to name: the coroutine may still be
    # running, so neither its type nor its failure position is reachable, and
    # absent is the only truthful reading. `make_briefly_slow_provider`
    # outlasts the 50ms budget but finishes in ~300ms, so a flush that failed
    # to apply its timeout would commit here rather than hang.
    run = driver(
        model_agent,
        provider_factory=make_briefly_slow_provider,
        activation_timeout_s=0.05,
        cancel_grace_s=0.5,
    )

    run.process(_event(b"a"))
    run.process(_event(b"b", t_ms=3_000))
    emitted = run.process(_event(b"c", t_ms=2_000))

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_TIMEOUT, f"batch_size=3,trigger={TRIGGER_SIZE}", 3_000)
        for _ in range(3)
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.event_type == TraceEventProto.ERROR
    assert trace.attributes["beam_agents.reason"] == REASON_TIMEOUT
    assert trace.trace_id == trace_id_for(_KEY, 0)
    assert trace.start_ms == 3_000
    assert "error.type" not in trace.attributes
    assert not any(key.startswith("beam_agents.failure.") for key in trace.attributes)
    # Fail-closed, exactly as the raising route: the poison batch is consumed
    # and nothing else moved.
    assert run.handles.batch.items == []
    assert run.handles.flush_timer.cleared is True
    assert run.handles.seq.value == 0
    assert run.handles.memory.value == MemoryBlob()
    assert run.handles.continuation.value is None


def test_a_flush_that_fails_outside_the_activation_wrap_still_fails_closed(
    driver: _DriverFactory,
) -> None:
    # The flush path's general-exception exit. `run_activation` wraps every
    # agent-path raise into `ActivationFailed`, so what reaches this branch is
    # a failure of the runtime *around* the activation -- here the wiring
    # assertion an un-`setup()` DoFn trips before any agent runs. It has to
    # fail closed identically: one dead letter per envelope naming the
    # exception and the batch, one ERROR trace, and a consumed buffer. Without
    # this the whole branch is unexecuted, and an argument dropped from either
    # of its two calls would look exactly like a working one.
    run = driver(setup=False)

    run.process(_event(b"a"))
    run.process(_event(b"b"))
    emitted = run.process(_event(b"c"))

    detail = f"{AssertionError('setup() not called')!r} batch_size=3,trigger={TRIGGER_SIZE}"
    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_ERROR, detail, 1_000) for _ in range(3)
    ]
    (trace,) = _tagged(emitted, "traces")
    assert trace.event_type == TraceEventProto.ERROR
    assert trace.attributes["beam_agents.reason"] == REASON_ERROR
    assert trace.trace_id == trace_id_for(_KEY, 0)
    assert trace.start_ms == 1_000
    assert trace.attributes["error.type"] == "AssertionError"
    # No failure position: there is no `ActivationFailed` to read one from, and
    # this route must not invent one.
    assert not any(key.startswith("beam_agents.failure.") for key in trace.attributes)
    assert run.handles.batch.items == []
    assert run.handles.flush_timer.cleared is True
    assert run.handles.seq.value == 0


def test_a_failed_timer_flush_names_the_timer_trigger(driver: _DriverFactory) -> None:
    # The failure detail carries which trigger fired, so triage can tell a
    # size-threshold poison batch from one the max_wait deadline assembled.
    run = driver(raising_agent)
    run.process(_event(b"a"))

    emitted = run.fire_flush()

    (error,) = _tagged(emitted, "errors")
    assert f"batch_size=1,trigger={TRIGGER_TIMER}" in error.detail
    assert run.handles.batch.items == []
    assert run.handles.flush_timer.cleared is True


# --- The flush commit's own handle contract ----------------------------------


def _flush_result() -> ActivationResult:
    """A committed-flush `ActivationResult` with nothing staged on it."""
    return ActivationResult(
        status="completed",
        seq=0,
        memory_blob=MemoryBlob(state_schema_version=1),
        cache_blob=LlmCacheBlob(state_schema_version=1),
        intents=[],
        traces=[],
        outputs=[],
        continuation=None,
        hitl_deadline_ms=None,
    )


@pytest.mark.parametrize(
    ("missing", "message"),
    [
        ("batch", "a flush commit needs its buffer handle"),
        ("flush_timer", "a flush commit needs its timer handle"),
    ],
)
def test_a_flush_commit_refuses_a_missing_handle_by_name(
    driver: _DriverFactory, missing: str, message: str
) -> None:
    # `_commit`'s two flush guards. `flush_size is not None` *is* the statement
    # "this commit consumes a buffer", so both handles must be there -- the bag
    # to clear and the timer to disarm. Without the guards a caller that passed
    # a size but forgot a handle would raise `AttributeError: 'NoneType' object
    # has no attribute 'clear'` from inside the commit, halfway through a fixed
    # commit order, naming nothing about what it actually needed.
    run = driver()
    handles = run.handles
    kwargs: dict[str, Any] = {"batch": handles.batch, "flush_timer": handles.flush_timer}
    kwargs[missing] = None

    with pytest.raises(AssertionError) as exc_info:
        list(
            run.dofn._commit(
                _flush_result(),
                1_000,
                5,
                handles.memory,
                handles.continuation,
                handles.llm_cache,
                handles.pending,
                handles.seq,
                handles.ttl_timer,
                handles.hitl_timer,
                flush_size=2,
                flush_trigger=TRIGGER_SIZE,
                **kwargs,
            )
        )

    assert str(exc_info.value) == message


@pytest.mark.parametrize("missing", ["batch", "flush_timer"])
def test_the_deferred_buffer_rearm_needs_both_handles_or_does_nothing(
    driver: _DriverFactory, missing: str
) -> None:
    # `_rearm_flush`'s guard is a three-way `or`, and each arm stands for a
    # different caller: no batch settings at all (`BatchPolicy.NONE`), or a
    # resolving path handed only part of the pair. Either half missing means
    # there is nothing to re-arm *with*, so the re-arm is a no-op -- it must
    # not read a bag it does not have, nor set a timer it does not have. A
    # guard that required *both* to be absent would sail past exactly the
    # half-wired case it is there for.
    run = driver()
    batch: Any = FakeBag([_event(b"deferred")]) if missing != "batch" else None
    flush_timer: Any = FakeTimer() if missing != "flush_timer" else None

    run.dofn._rearm_flush(batch, flush_timer)

    if flush_timer is not None:
        assert flush_timer.marks == []
    if batch is not None:
        assert batch.items == [_event(b"deferred")]


def test_overflow_during_deferral_is_explicit(driver: _DriverFactory) -> None:
    # Scenario: Overflow during deferral is explicit. Growing keyed state
    # silently toward the 1 MiB cap is neither counted nor triageable; a
    # dead letter is both.
    handles = _Handles(continuation=FakeValue(_live_continuation()))
    settings = BatchSettings(max_batch_size=3, max_wait_ms=500, max_buffered_events=2)
    run = driver(handles=handles, settings=settings)
    run.process(_event(b"a"))
    run.process(_event(b"b"))

    emitted = run.process(_event(b"overflow", t_ms=4_000))

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_BATCH_OVERFLOW, "buffered=2,cap=2", 4_000)
    ]
    assert handles.payloads() == [b"a", b"b"]
    assert run.metrics.counters[COUNTER_AGENT_ERRORS] == 1
    assert run.metrics.counters[COUNTER_EVENTS_BUFFERED] == 2


# --- Requirement: TTL GC wipes the buffer and reports what it wiped ----------


def test_wiped_buffered_events_are_reported(driver: _DriverFactory) -> None:
    # Scenario: Wiped buffered events are reported. `max_wait_ms` is orders of
    # magnitude inside `ttl_ms`, so a TTL fire over a non-empty buffer means a
    # stalled pipeline or a watermark jump -- observable, not silent.
    run = driver()
    run.process(_event(b"a"))
    run.process(_event(b"b"))

    emitted = run.fire_ttl()

    assert _tagged(emitted, "errors") == [
        ActivationError(_KEY, REASON_TTL_WIPED_BATCH, "buffered=2,index=0", 12_000),
        ActivationError(_KEY, REASON_TTL_WIPED_BATCH, "buffered=2,index=1", 12_000),
    ]
    assert run.handles.batch.cleared is True
    assert run.handles.flush_timer.cleared is True
    assert run.metrics.counters[COUNTER_AGENT_ERRORS] == 2


def test_a_ttl_fire_over_an_empty_buffer_reports_nothing(driver: _DriverFactory) -> None:
    # The overwhelming majority of TTL fires are ordinary idle-key GC; an empty
    # buffer must not manufacture a dead letter.
    run = driver()

    emitted = run.fire_ttl()

    assert emitted == []
    assert run.handles.batch.cleared is True


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
