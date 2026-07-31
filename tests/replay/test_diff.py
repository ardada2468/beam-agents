"""Traced-vs-replayed comparison: normalization, divergence, and digests.

Covers the `replay-cli` scenarios "A divergent re-run produces a diff and exit
code 1" (the report half; the exit code is asserted in `test_cli`), "Cache-hit
normalization does not report false divergence", and "Unrepresented fields are
reported, not diffed".
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Sequence
from typing import Any

from beam_agents._protos import TraceEvent
from beam_agents.observability.traces import BILLED, CACHE_HIT, INTENT_ID
from beam_agents.replay.bundle import build_bundle, run_replay
from beam_agents.replay.diff import NORMALIZED_ATTRIBUTES, compare, normalize_event
from tests.replay._fixtures import (
    exact_replay_agent,
    failing_agent,
    run_original,
    run_original_failure,
)


def _bundle(original: Any, **kwargs: Any) -> Any:
    return build_bundle(
        snapshot=original.snapshot,
        traces=original.traces,
        envelope=original.envelope,
        **kwargs,
    )


def _event_of(events: Sequence[TraceEvent], event_type: int) -> TraceEvent:
    matches = [e for e in events if e.event_type == event_type]
    assert matches, f"no {TraceEvent.EventType.Name(event_type)} event"
    return matches[0]


# --- Requirement: traced and replayed outcomes are diffed ---------------------


def test_a_reproduced_run_reports_no_differences() -> None:
    original = run_original()
    bundle = _bundle(original)

    report = compare(bundle, run_replay(bundle, exact_replay_agent))

    assert report.reproduced is True
    assert report.differences == ()


def test_cache_hit_normalization_does_not_report_false_divergence() -> None:
    # Scenario: Cache-hit normalization does not report false divergence.
    original = run_original()
    bundle = _bundle(original)
    outcome = run_replay(bundle, exact_replay_agent)

    traced_call = _event_of(bundle.traced, TraceEvent.LLM_CALL)
    replayed_call = _event_of(outcome.traces, TraceEvent.LLM_CALL)
    # The original reached the provider; the replay served the same call from
    # the snapshot's cache blob. These two attributes legitimately differ...
    assert traced_call.attributes[CACHE_HIT] == "false"
    assert traced_call.attributes[BILLED] == "true"
    assert replayed_call.attributes[CACHE_HIT] == "true"
    assert replayed_call.attributes[BILLED] == "false"
    assert traced_call.SerializeToString(deterministic=True) != replayed_call.SerializeToString(
        deterministic=True
    )

    # ...and only these two: the normalization list is closed, and after it the
    # events are byte-identical.
    assert set(NORMALIZED_ATTRIBUTES) == {CACHE_HIT, BILLED}
    assert normalize_event(traced_call).SerializeToString(deterministic=True) == normalize_event(
        replayed_call
    ).SerializeToString(deterministic=True)
    assert compare(bundle, outcome).reproduced is True


def test_normalization_leaves_every_other_attribute_alone() -> None:
    # A model-name drift must still be caught: normalization must not become a
    # general-purpose "ignore the attributes that differ" escape hatch.
    original = run_original()
    bundle = _bundle(original)
    outcome = run_replay(bundle, exact_replay_agent)
    tampered = TraceEvent()
    tampered.CopyFrom(_event_of(bundle.traced, TraceEvent.LLM_CALL))
    tampered.attributes["gen_ai.request.model"] = "some-other-model"

    assert normalize_event(tampered).SerializeToString(deterministic=True) != normalize_event(
        _event_of(outcome.traces, TraceEvent.LLM_CALL)
    ).SerializeToString(deterministic=True)


def test_a_divergent_intent_id_is_named_with_both_values() -> None:
    # Scenario: A divergent re-run produces a diff and exit code 1.
    original = run_original()
    traced = [TraceEvent() for _ in original.traces]
    for copy, event in zip(traced, original.traces, strict=True):
        copy.CopyFrom(event)
    intent_event = _event_of(traced, TraceEvent.INTENT_EMITTED)
    real_intent_id = intent_event.attributes[INTENT_ID]
    intent_event.attributes[INTENT_ID] = "00000000-0000-5000-8000-000000000000"
    bundle = build_bundle(snapshot=original.snapshot, traces=traced, envelope=original.envelope)

    report = compare(bundle, run_replay(bundle, exact_replay_agent))

    assert report.reproduced is False
    rendered = report.render()
    assert "00000000-0000-5000-8000-000000000000" in rendered
    assert real_intent_id in rendered
    # The first diverging event is identified by position, not just by content.
    positions = [d.kind for d in report.differences]
    assert "trace_event" in positions
    assert "intent" in positions


def test_a_diverging_event_count_is_reported() -> None:
    original = run_original()
    truncated = list(original.traces)[:-1]
    bundle = build_bundle(snapshot=original.snapshot, traces=truncated, envelope=original.envelope)

    report = compare(bundle, run_replay(bundle, exact_replay_agent))

    assert report.reproduced is False
    assert "event count" in report.render()


def test_a_diverging_status_is_reported() -> None:
    original = run_original()
    bundle = _bundle(original)
    outcome = run_replay(bundle, exact_replay_agent)
    diverged = dataclasses.replace(outcome, status="suspended")

    report = compare(bundle, diverged)

    assert report.reproduced is False
    assert "status" in report.render()


def test_unrepresented_fields_are_reported_not_diffed() -> None:
    # Scenario: Unrepresented fields are reported, not diffed. Outputs and the
    # post-activation memory blob have no traced counterpart, so the CLI prints
    # their digests instead of inventing a baseline.
    original = run_original()
    bundle = _bundle(original)
    outcome = run_replay(bundle, exact_replay_agent)

    report = compare(bundle, outcome)
    rendered = report.render()

    assert report.reproduced is True
    output_digest = hashlib.sha256(b"pong:go").hexdigest()
    memory_digest = hashlib.sha256(
        outcome.memory_blob.SerializeToString(deterministic=True)
    ).hexdigest()
    assert output_digest in rendered
    assert memory_digest in rendered
    assert "outputs" in rendered
    assert "memory" in rendered


# --- Requirement: a failed activation replays to its traced failure position ---


def test_a_failed_replay_matches_the_traced_error_event() -> None:
    original = run_original_failure()
    bundle = _bundle(original)

    outcome = run_replay(bundle, failing_agent)
    report = compare(bundle, outcome)

    assert outcome.status == "failed"
    assert outcome.error_type == "RuntimeError"
    assert report.reproduced is True


def test_a_failure_at_a_different_position_diverges() -> None:
    original = run_original_failure()
    error_event = TraceEvent()
    error_event.CopyFrom(original.traces[0])
    error_event.attributes["beam_agents.failure.step"] = "9"
    bundle = build_bundle(
        snapshot=original.snapshot, traces=[error_event], envelope=original.envelope
    )

    report = compare(bundle, run_replay(bundle, failing_agent))

    assert report.reproduced is False
    assert "beam_agents.failure.step" in report.render()
