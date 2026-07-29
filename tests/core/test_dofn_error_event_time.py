"""`ActivationError.event_time_ms`: where each dead letter's timestamp comes from.

The errors sink encodes this field into every published record, so a wall-clock
reading anywhere here would break replay identity (correctness invariant 2's
argument applied to dead letters): a retried bundle must re-emit byte-identical
records. Every emission site therefore takes its timestamp from the element's
event time or from a timer's scheduled firing time, both of which a replay
reproduces exactly.

Driven with the fake state handles rather than a pipeline, like
`test_dofn_activation` and `test_dofn_ttl`, so these assertions stay inside the
mutation gate's selection.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import AgentEnvelope, Continuation, LlmCacheBlob, MemoryBlob, ToolIntent
from beam_agents.core.agent import Complete, FallbackContext
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import (
    REASON_ERROR,
    REASON_HITL_TIMEOUT,
    REASON_ORPHANED,
    REASON_TTL_WIPED_SUSPENSION,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.hitl import Drop, HitlPolicy, Route
from beam_agents.model.fake import FakeLLM
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeTimer, FakeValue

_KEY = b"k"
# Deliberately unlike any plausible wall clock, so a site that reached for
# `time.time()` cannot coincidentally match.
_EVENT_MS = 1_000
_FIRED_AT_MS = 12_000


async def _raising_agent(ctx: ActivationContext) -> Complete:
    raise RuntimeError("agent blew up")


async def _unused_agent(ctx: ActivationContext) -> Complete:  # pragma: no cover - never invoked
    raise AssertionError("no activation runs on these paths")


def _live_continuation() -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=3,
        step_index=2,
        pending_intent_ids=["intent-1"],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=9_000,
    )


def _errors(emitted: list[Any]) -> list[ActivationError]:
    return [
        e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == "errors"
    ]


def _process(
    envelope: AgentEnvelope, *, agent: Any, continuation: Continuation | None = None
) -> list[Any]:
    """Run one element through `process` with fresh fake handles."""
    dofn = _AgentDoFn(agent, provider_factory=FakeLLM)
    dofn.setup()
    try:
        return list(
            dofn.process(
                (_KEY, envelope),
                memory=FakeValue(MemoryBlob()),
                continuation=FakeValue(continuation),
                llm_cache=FakeValue(LlmCacheBlob()),
                pending=FakeBag([]),
                seq=FakeSum(0),
                ttl_timer=FakeTimer(),
                hitl_timer=FakeTimer(),
            )
        )
    finally:
        dofn.teardown()


def _fire_ttl(cont: Continuation | None) -> list[Any]:
    dofn = _AgentDoFn(_unused_agent, provider_factory=FakeLLM)
    return list(
        dofn.on_ttl(
            key=_KEY,
            timestamp=Timestamp(micros=_FIRED_AT_MS * 1000),
            memory=FakeValue(MemoryBlob()),
            continuation=FakeValue(cont),
            llm_cache=FakeValue(LlmCacheBlob()),
            pending=FakeBag([ToolIntent(intent_id="intent-1")]),
            seq=FakeSum(3),
        )
    )


def _drop(fallback: FallbackContext) -> Route:
    """Timeout route that dead-letters instead of emitting a fallback output."""
    return Drop()


def _fire_hitl(cont: Continuation) -> list[Any]:
    # The drop route is the timeout's `.errors` path; the default `deny` route
    # emits on the main output instead and has no dead letter to timestamp.
    dofn = _AgentDoFn(
        _unused_agent, provider_factory=FakeLLM, hitl_policy=HitlPolicy(on_timeout=_drop)
    )
    return list(
        dofn.on_hitl(
            key=_KEY,
            timestamp=Timestamp(micros=_FIRED_AT_MS * 1000),
            continuation=FakeValue(cont),
            pending=FakeBag([ToolIntent(intent_id="intent-1")]),
            hitl_timer=FakeTimer(),
            ttl_timer=FakeTimer(),
        )
    )


# --- Requirement: ActivationError carries a deterministic event time ----------


def test_a_start_failure_dead_letter_carries_the_elements_event_time() -> None:
    # Scenario: Element-path dead letters carry the element's event time.
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_EVENT_MS, external_event=b"go")

    errors = _errors(_process(envelope, agent=_raising_agent))

    assert [e.reason for e in errors] == [REASON_ERROR]
    assert errors[0].event_time_ms == _EVENT_MS


def test_an_orphaned_result_dead_letter_carries_the_elements_event_time() -> None:
    # Scenario: Element-path dead letters carry the element's event time. The
    # admission path emits without running an activation at all, so it has only
    # the element's own time to carry.
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_EVENT_MS)
    envelope.tool_result.intent_id = "ghost"

    errors = _errors(_process(envelope, agent=_unused_agent))

    assert [e.reason for e in errors] == [REASON_ORPHANED]
    assert errors[0].event_time_ms == _EVENT_MS


def test_a_ttl_wiped_suspension_dead_letter_carries_the_timers_firing_time() -> None:
    # Scenario: Timer-path dead letters carry the scheduled firing time.
    errors = _errors(_fire_ttl(_live_continuation()))

    assert [e.reason for e in errors] == [REASON_TTL_WIPED_SUSPENSION]
    assert errors[0].event_time_ms == _FIRED_AT_MS


def test_a_hitl_timeout_dead_letter_carries_the_timers_firing_time() -> None:
    # Scenario: Timer-path dead letters carry the scheduled firing time.
    errors = _errors(_fire_hitl(_live_continuation()))

    assert [e.reason for e in errors] == [REASON_HITL_TIMEOUT]
    assert errors[0].event_time_ms == _FIRED_AT_MS


def test_replaying_the_same_failure_re_emits_an_identical_record() -> None:
    # Scenario: Replay produces identical records. A bundle retry walks the
    # same path against the same element; nothing in the record may vary
    # between the two runs, or downstream dedup on the errors topic breaks.
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_EVENT_MS, external_event=b"go")

    first = _errors(_process(envelope, agent=_raising_agent))
    second = _errors(_process(envelope, agent=_raising_agent))

    assert first == second
    assert _errors(_fire_ttl(_live_continuation())) == _errors(_fire_ttl(_live_continuation()))
