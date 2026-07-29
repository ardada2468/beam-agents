"""`ActivationTrace` tests for the `trace-events` capability.

Covers: parent/child linkage (including across a suspend/resume boundary),
stamping semantics (fill only what is empty), and the omit-rather-than-zero
usage rule.
"""

from __future__ import annotations

from beam_agents._protos import TraceEvent
from beam_agents.model import TokenUsage
from beam_agents.observability import (
    ACTIVATION_KIND,
    ACTIVATION_STATUS,
    BILLED,
    ROLE_ACTIVATION,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
    ActivationTrace,
    span_id_for,
    trace_id_for,
    usage_attributes,
)

_NOW = 1_700_000_000_000


def _trace(*, entry_step_index: int = 0, is_resume: bool = False) -> ActivationTrace:
    return ActivationTrace(
        entity_key=b"key-1",
        seq=3,
        now_ms=_NOW,
        entry_step_index=entry_step_index,
        is_resume=is_resume,
    )


# --- Requirement: One trace per activation scope, spanning suspension --------


def test_the_initial_activation_span_is_the_trace_root() -> None:
    # Scenario: The initial activation span is the trace root.
    trace = _trace()
    event = trace.activation_start()

    assert event.trace_id == trace_id_for(b"key-1", 3)
    assert event.span_id == span_id_for(b"key-1", 3, ROLE_ACTIVATION, 0)
    assert event.parent_span_id == b""
    assert event.attributes[ACTIVATION_KIND] == "start"
    assert trace.parent_span_id == b""
    assert trace.span_id == event.span_id


def test_a_resumed_attempt_is_a_child_of_the_initial_attempt() -> None:
    # Scenario: A resumed attempt is a child of the initial attempt.
    # Scenario: A resume shares the suspended activation's trace.
    root = _trace()
    resumed = _trace(entry_step_index=2, is_resume=True)

    start = resumed.activation_start()
    assert start.trace_id == root.trace_id
    assert start.span_id != root.span_id
    assert start.parent_span_id == root.span_id
    assert start.attributes[ACTIVATION_KIND] == "resume"


def test_activation_events_share_the_attempts_span_and_use_the_injected_clock() -> None:
    # Scenario: A completing activation brackets its work.
    # Scenario: Activation events use the injected clock.
    trace = _trace()
    start = trace.activation_start()
    end = trace.activation_end(status="completed", step_index=4)

    assert start.span_id == end.span_id
    assert end.attributes[ACTIVATION_STATUS] == "completed"
    for event in (start, end):
        assert event.start_ms == _NOW
        assert event.end_ms == _NOW


def test_a_suspending_activation_reports_suspended_status() -> None:
    # Scenario: A suspending activation reports suspended status.
    trace = _trace()
    end = trace.activation_end(status="suspended", step_index=1)
    assert end.attributes[ACTIVATION_STATUS] == "suspended"


# --- Requirement: Correlation stamped at the staging boundary ----------------


def test_an_uncorrelated_event_is_stamped_on_staging() -> None:
    # Scenario: An uncorrelated event is stamped on staging.
    trace = _trace()
    event = TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=2)

    trace.stamp(event)

    assert event.trace_id == trace.trace_id
    assert event.span_id == span_id_for(b"key-1", 3, "LLM_CALL", 2)
    assert event.parent_span_id == trace.span_id


def test_a_producer_supplied_parent_is_preserved() -> None:
    # Scenario: A producer-supplied parent is preserved.
    trace = _trace()
    event = TraceEvent(
        event_type=TraceEvent.LLM_CALL,
        step_index=2,
        parent_span_id=bytes(range(8)),
    )

    trace.stamp(event)

    assert event.parent_span_id == bytes(range(8))
    assert event.trace_id == trace.trace_id


def test_stamping_is_idempotent() -> None:
    # Re-staging an already-correlated event must not rewrite its identity:
    # the DoFn re-emits committed events, and a second stamp that moved a span
    # would break dedup on (trace_id, span_id, event_type).
    trace = _trace()
    event = trace.stamp(TraceEvent(event_type=TraceEvent.LLM_CALL, step_index=2))
    first = event.SerializeToString(deterministic=True)

    trace.stamp(event)

    assert event.SerializeToString(deterministic=True) == first


# --- Requirement: Token counts are truthful or absent ------------------------


def test_known_usage_is_reported_with_the_billed_flag() -> None:
    attributes = usage_attributes(
        TokenUsage(prompt_tokens=1200, completion_tokens=300, total_tokens=1500), billed=True
    )
    assert attributes[USAGE_INPUT_TOKENS] == "1200"
    assert attributes[USAGE_OUTPUT_TOKENS] == "300"
    assert attributes[BILLED] == "true"


def test_unknown_usage_is_omitted_not_zeroed() -> None:
    # Scenario: Unknown usage is omitted, not zeroed.
    # A "0" here is indistinguishable from a real zero-token call to anything
    # summing the attribute, which is the whole point of omitting it.
    attributes = usage_attributes(None, billed=False)
    assert USAGE_INPUT_TOKENS not in attributes
    assert USAGE_OUTPUT_TOKENS not in attributes
    assert attributes[BILLED] == "false"
