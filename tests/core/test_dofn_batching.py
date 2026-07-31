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
from beam_agents.core.agent import intent_id_for
from beam_agents.core.batching import TRIGGER_SIZE, TRIGGER_TIMER, BatchSettings
from beam_agents.core.dofn import (
    REASON_BATCH_OVERFLOW,
    REASON_ERROR,
    REASON_TTL_WIPED_BATCH,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.hitl import HITL_TIMEOUT_OUTPUT, HitlPolicy
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
    batch_suspend_agent,
    escalate_once,
    make_pong_provider,
    raising_agent,
)

_KEY = b"k"
_TTL_MS = 3_600_000
# Small enough that a test can reach the threshold in three elements, and a
# buffer cap with room for deferral above it.
_SETTINGS = BatchSettings(max_batch_size=3, max_wait_ms=500, max_buffered_events=6)
_WALL_S = 10.0


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
    ) -> None:
        self.metrics = RecordingMetrics()
        self.handles = handles if handles is not None else _Handles()
        self.dofn = _AgentDoFn(
            agent,
            provider_factory=make_pong_provider,
            ttl_ms=_TTL_MS,
            hitl_policy=hitl_policy,
            metrics=self.metrics,
            batch=settings,
            time_fn=lambda: wall_s,
        )
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

    errors = _tagged(emitted, "errors")
    assert len(errors) == 3
    assert {error.reason for error in errors} == {REASON_ERROR}
    assert all(f"batch_size=3,trigger={TRIGGER_SIZE}" in error.detail for error in errors)
    assert all(error.event_time_ms == 1_000 for error in errors)
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
