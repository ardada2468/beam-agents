"""Retry-determinism gate (project.md's `-m semantics` promise).

Verifies: a chaos-forced bundle retry of a resumed activation adds zero
additional real provider calls (the resume's repeated LLM call reads the
`LLM_CACHE` committed at suspend time, both on the discarded failed attempt
and on Beam's own retry), and commits the deterministically-expected
`ToolIntent`.

See openspec/changes/add-retry-determinism-gate/design.md for the empirical
findings behind this shape: Beam's classic (streaming) DirectRunner already
retries a failed bundle with genuine per-key state rollback, so the chaos
helper only needs to fail one targeted commit once; the cache key bakes in
`seq`, so "zero additional calls" is only achievable across a suspend->resume
pair (same seq), not a raw single-shot activation's own retry.
"""

from __future__ import annotations

import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult, TraceEvent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.loop import ActivationResult
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.observability import trace_id_for
from beam_agents.testing.chaos import fail_first_matching_commit
from tests.core._dofn_helpers import keyed, make_pong_provider
from tests.semantics._helpers import suspend_then_recall_agent

pytestmark = pytest.mark.semantics

_ENTITY_KEY = b"k"
_SEQ = 0
# call_model() consumes step_index=0 (the pre-suspend call); act() consumes
# step_index=1. These are pure functions of call order, per agent-context.
_INTENT_STEP_INDEX = 1
_EXPECTED_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, _INTENT_STEP_INDEX)
# Derived from the activation scope alone, so suspend and resume — which
# share `seq` — land in the same trace with nothing carried on the wire.
_EXPECTED_TRACE_ID = trace_id_for(_ENTITY_KEY, _SEQ)


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


def _tool_result(key: bytes, intent_id: str, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.tool_result.intent_id = intent_id
    env.tool_result.entity_key = key
    env.tool_result.payload = b"tool-done"
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


def _is_resume_commit(result: ActivationResult) -> bool:
    # The resume is the only "completed" commit in this scenario; the
    # pre-suspend activation commits with status "suspended".
    return result.status == "completed"


def _assert_zero_additional_calls(traces: object) -> None:
    """The invariant under test: exactly one real provider call (the
    unavoidable pre-suspend miss) and exactly one cache hit (the resume),
    regardless of how many attempts Beam needed to commit the resume.
    """
    llm_events = [t for t in traces if t.event_type == TraceEvent.LLM_CALL]  # type: ignore[attr-defined]
    cache_hits = [e.attributes["beam_agents.cache_hit"] for e in llm_events]
    assert cache_hits.count("false") == 1, (
        f"expected exactly one real provider call, got trace cache_hit values {cache_hits!r}"
    )
    assert cache_hits.count("true") == 1, (
        f"expected exactly one cache hit (the resume), got trace cache_hit values {cache_hits!r}"
    )
    assert len(llm_events) == 2, f"expected exactly two LLM_CALL traces, got {len(llm_events)}"


def _check_output(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"resumed:pong"], f"unexpected output: {items!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


def _check_committed_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    intent = items[0]
    assert intent.intent_id == _EXPECTED_INTENT_ID
    assert intent.seq == _SEQ
    assert intent.step_index == _INTENT_STEP_INDEX
    # The intent's whole encoding is the determinism claim, not just its ID:
    # `trace_id` rides on it now, and a non-deterministic one would make a
    # retried bundle's intents differ byte-for-byte while every field the
    # earlier assertions look at still matched.
    assert intent.trace_id == _EXPECTED_TRACE_ID
    assert intent.SerializeToString(deterministic=True) == _expected_intent_bytes(intent)


def _expected_intent_bytes(intent: object) -> bytes:
    """Re-mint the intent from its own scope and encode it.

    Independent of the committed message: everything here is derived from
    `(entity_key, seq, step_index)` and the agent's own arguments, so a match
    proves the commit was reproducible rather than merely self-consistent.
    """
    expected = ToolIntent(
        intent_id=_EXPECTED_INTENT_ID,
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        step_index=_INTENT_STEP_INDEX,
        tool_name=intent.tool_name,  # type: ignore[attr-defined]
        args_json=intent.args_json,  # type: ignore[attr-defined]
        created_at_ms=intent.created_at_ms,  # type: ignore[attr-defined]
        expires_at_ms=intent.expires_at_ms,  # type: ignore[attr-defined]
        attempt=0,
        kind=intent.kind,  # type: ignore[attr-defined]
        trace_id=_EXPECTED_TRACE_ID,
    )
    return expected.SerializeToString(deterministic=True)


def _check_one_trace_spans_the_suspension(actual: object) -> None:
    """Every event of the suspend/resume cycle shares one trace ID.

    The resume runs under the suspended activation's `seq`, so it recomputes
    the same trace with nothing carried on the wire — including across the
    chaos-forced retry, which re-emits byte-identical events.
    """
    trace_ids = {event.trace_id for event in actual}  # type: ignore[attr-defined]
    assert trace_ids == {_EXPECTED_TRACE_ID}, f"expected one trace, got {trace_ids!r}"


def test_chaos_forced_resume_retry_adds_zero_calls_and_commits_deterministic_intent() -> None:
    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(_ENTITY_KEY, b"go", 1000)])
            .add_elements([_tool_result(_ENTITY_KEY, _EXPECTED_INTENT_ID, 2000)])
            .advance_watermark_to_infinity()
        )
        envs = p | stream
        out = keyed(envs) | RunAgent(
            suspend_then_recall_agent,
            config=AgentConfig(provider_factory=make_pong_provider),
        )
        # `assert_that` matchers must assert (and raise) on the collected
        # values themselves — a closure that copies values out to an
        # outer list does not reliably survive Beam's own serialization
        # of the assertion DoFn, even on the in-process DirectRunner.
        assert_that(out.output, _check_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_committed_intent, label="intents")
        assert_that(out.traces, _assert_zero_additional_calls, label="traces")
        assert_that(out.traces, _check_one_trace_spans_the_suspension, label="trace-id")


def test_assertion_helper_catches_a_broken_cache_first_path() -> None:
    # Negative-path check (task 2.6): a hand-built trace list simulating a
    # regression that re-calls the provider on resume (two "false" cache_hit
    # events instead of one "false" + one "true") must fail the assertion.
    broken_traces = [
        TraceEvent(
            event_type=TraceEvent.LLM_CALL,
            attributes={"beam_agents.cache_hit": "false"},
        ),
        TraceEvent(
            event_type=TraceEvent.LLM_CALL,
            attributes={"beam_agents.cache_hit": "false"},
        ),
    ]
    with pytest.raises(AssertionError):
        _assert_zero_additional_calls(broken_traces)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
