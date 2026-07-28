"""Streaming (TestStream) tests for ordered lifecycle behavior.

Covers the scenarios that need deterministic element ordering and timer control:
per-key ordering, exactly-once SEQ, atomic commit under failure, timeout with no
state mutation, resume, TTL wipe/re-arm, and HITL fail-closed.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from beam_agents.core.agent import intent_id_for
from beam_agents.core.dofn import (
    DETAIL_DEADLINE_PASSED,
    DETAIL_NO_CONTINUATION,
    HITL_TIMEOUT_OUTPUT,
    REASON_ERROR,
    REASON_ORPHANED,
    REASON_TIMEOUT,
)
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.hitl import HitlPolicy
from tests.core._dofn_helpers import (
    append_agent,
    approval_agent,
    conditional_append_agent,
    escalate_once,
    keyed,
    make_pong_provider,
    make_slow_provider,
    seq_agent,
    suspend_then_act_again_agent,
    suspend_then_complete_agent,
    suspend_then_fail_agent,
    timeout_or_append_agent,
)

# Large event-time TTL so working-memory GC never fires mid-stream unless a test
# deliberately shrinks it.
_BIG_TTL_MS = 1_000_000_000


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


def _result(
    key: bytes,
    intent_id: str,
    payload: bytes,
    t_ms: int,
    status: ToolResult.Status = ToolResult.OK,
) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.tool_result.intent_id = intent_id
    env.tool_result.entity_key = key
    env.tool_result.payload = payload
    env.tool_result.status = status
    return TimestampedValue(env, t_ms / 1000)


def _approval(
    key: bytes, intent_id: str, *, approved: bool, t_ms: int
) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.approval.intent_id = intent_id
    env.approval.approved = approved
    env.approval.approver = "alice@example.test"
    env.approval.decided_at_ms = t_ms
    return TimestampedValue(env, t_ms / 1000)


# --- Requirement: monotonic SEQ incremented exactly once -----------------------


def test_seq_increments_once_per_committed_activation() -> None:
    # Scenario: one increment per committed activation (three events -> 0,1,2).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"b", 2000)])
        .add_elements([_event(b"k", b"c", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS)
        )
        assert_that(out.output, equal_to([b"0", b"1", b"2"]))


# --- Requirement: per-key ordering preserved -----------------------------------


def test_per_key_ordering_and_memory_persistence() -> None:
    # Scenario: elements commit in arrival order (single key).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"b", 2000)])
        .add_elements([_event(b"k", b"c", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # The terminal "a,b,c" is only reachable if appends applied in order.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1", b"a,b,c#2"]))


def test_interleaved_keys_preserve_per_key_order() -> None:
    # Scenario: interleaved event streams across keys via TestStream.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k1", b"a", 1000)])
        .add_elements([_event(b"k2", b"x", 1100)])
        .add_elements([_event(b"k1", b"b", 1200)])
        .add_elements([_event(b"k2", b"y", 1300)])
        .add_elements([_event(b"k1", b"c", 1400)])
        .add_elements([_event(b"k2", b"z", 1500)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(
            out.output,
            equal_to([b"a#0", b"a,b#1", b"a,b,c#2", b"x#0", b"x,y#1", b"x,y,z#2"]),
        )


# --- Requirement: atomic staged commit -----------------------------------------


def test_failed_activation_commits_nothing() -> None:
    # Scenario: failed activation commits nothing (no memory, no SEQ advance).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"FAIL", 2000)])
        .add_elements([_event(b"k", b"b", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            conditional_append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # "b" lands on the pre-failure ring (a,b) with seq 1, proving FAIL neither
        # persisted its scratch write nor advanced SEQ.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1"]))
        errors = out.errors | "reasons" >> beam.Map(lambda e: (e.entity_key, e.reason))
        assert_that(errors, equal_to([(b"k", REASON_ERROR)]), label="errors")


# --- Requirement: activation timeout, no state mutation ------------------------


def test_timeout_routes_to_errors_without_mutating_state() -> None:
    # Scenario: activation timeout cancels and mutates no state; the next element
    # for the key runs on the same healthy loop and reads pre-timeout state.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])
        .add_elements([_event(b"k", b"SLOW", 2000)])
        .add_elements([_event(b"k", b"b", 3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            timeout_or_append_agent,
            config=AgentConfig(
                provider_factory=make_slow_provider,
                activation_timeout_s=0.3,
                ttl_ms=_BIG_TTL_MS,
            ),
        )
        # "a" -> a#0; SLOW times out (no commit); "b" -> a,b#1 (seq not advanced
        # by the timeout, memory intact).
        assert_that(out.output, equal_to([b"a#0", b"a,b#1"]))
        errors = out.errors | "reasons" >> beam.Map(lambda e: (e.entity_key, e.reason))
        assert_that(errors, equal_to([(b"k", REASON_TIMEOUT)]), label="errors")


# --- Requirement: resume failure fails closed, same as activation failure -------


def test_resume_failure_routes_to_errors_without_mutating_state() -> None:
    # Scenario: a failing resume fails closed. `_AgentDoFn._resume` has its own
    # except clause independent of `_start`'s (dofn.py), so this exercises a
    # path `test_failed_activation_commits_nothing` (which only fails on
    # `_start`) does not.
    intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 1000)])
        .add_elements([_result(b"k", intent_id, b"done", 1500)])  # inside the deadline
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            suspend_then_fail_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([]), label="no-output")
        errors = out.errors | "reasons" >> beam.Map(lambda e: (e.entity_key, e.reason))
        assert_that(errors, equal_to([(b"k", REASON_ERROR)]), label="errors")


# --- Requirement: tool-result resumes the continuation -------------------------


def test_suspend_then_tool_result_resumes() -> None:
    # Scenario: tool-result resumes the matching continuation. The intent ID is
    # deterministic (key,seq,step) = (b"k", 0, 0), so the resume can target it.
    intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 1000)])
        .add_elements([_result(b"k", intent_id, b"done", 1500)])  # inside the deadline
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            suspend_then_complete_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # Suspend emits an intent (no main output); resume completes to the echo.
        assert_that(out.output, equal_to([b"resumed:done"]), label="resumed")
        tool_names = out.intents | "tool-names" >> beam.Map(lambda i: i.tool_name)
        assert_that(tool_names, equal_to(["http.post"]), label="intent")


# --- Requirement: a resumed activation continues its step index ----------------


def test_intent_staged_on_resume_does_not_collide_with_the_suspended_one() -> None:
    # Scenario: An intent staged on resume does not collide with the suspended
    # activation's intent. Both live in seq 0, so a resumed activation that
    # restarted step_index at 0 would re-mint the suspension's own intent_id
    # and the effector would dedup the second effect away as a duplicate.
    first_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 1000)])
        .add_elements([_result(b"k", first_id, b"done", 1500)])  # inside the deadline
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            suspend_then_act_again_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([b"done"]), label="resumed")
        ids = out.intents | "ids" >> beam.Map(lambda i: (i.step_index, i.intent_id))
        assert_that(
            ids,
            equal_to([(0, first_id), (1, intent_id_for(b"k", 0, 1))]),
            label="distinct-ids",
        )


# --- Requirement: TTL timer wipes all state and re-arms per element -------------


def test_ttl_fire_wipes_state_and_resets_seq() -> None:
    # Scenario: TTL fire wipes every spec; the key recovers with seq 0.
    # Watermark advances are in SECONDS; event times/TTL marks are in ms. TTL for
    # "a" fires at 1100ms = 1.1s, so advancing the watermark to 1.5s trips it.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])  # arms TTL at 1.1s
        .advance_watermark_to(1.5)  # fires TTL -> wipes memory + seq
        .add_elements([_event(b"k", b"b", 2000)])  # fresh: empty ring, seq 0
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=100)
        )
        # Post-wipe "b" starts a fresh ring at seq 0 (b"b#0"), not b"a,b#1".
        assert_that(out.output, equal_to([b"a#0", b"b#0"]))


def test_new_element_rearms_ttl_and_supersedes_old_mark() -> None:
    # Scenario: a new element re-arms the TTL timer; the earlier mark does not fire.
    # Watermark advances are in SECONDS; TTL marks are in ms. "a" arms 1.1s, "b"
    # re-arms 1.15s. Advancing to 1.12s is past the superseded 1.1s mark but
    # before the live 1.15s mark, so no wipe occurs.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"a", 1000)])  # arms TTL at 1.1s
        .add_elements([_event(b"k", b"b", 1050)])  # re-arms TTL at 1.15s
        .advance_watermark_to(1.12)  # past the superseded 1.1s mark, before 1.15s
        .add_elements([_event(b"k", b"c", 1200)])  # state intact -> a,b,c
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=100)
        )
        # No wipe occurred at 1120: "c" sees the accumulated ring and seq 2.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1", b"a,b,c#2"]))


# --- Requirement: HITL timer fails closed --------------------------------------


def test_hitl_timeout_fires_fallback_and_orphans_late_result() -> None:
    # Scenario: HITL timeout triggers fallback and clears the continuation; a
    # late result is orphaned. Suspend deadline = event_time(0) + timeout(1000ms).
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, arms HITL at 1000ms
        .advance_processing_time(5)  # -> fires the real-time HITL timer
        .add_elements([_result(b"k", "late", b"late", 100)])  # continuation gone
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            suspend_then_complete_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([HITL_TIMEOUT_OUTPUT]), label="fallback")
        reasons = out.errors | "reasons" >> beam.Map(lambda e: e.reason)
        assert_that(reasons, equal_to([REASON_ORPHANED]), label="orphaned")


def test_timer_first_then_late_approval_is_orphaned() -> None:
    # Scenario: Timer first, then a late approval is orphaned. The approval
    # variant of the fail-closed ordering: the fallback runs exactly once and
    # the human's late "yes" cannot resurrect the suspension.
    approval_intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, arms HITL at 1000ms
        .advance_processing_time(5)  # -> fires the real-time HITL timer
        .add_elements([_approval(b"k", approval_intent_id, approved=True, t_ms=100)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([HITL_TIMEOUT_OUTPUT]), label="fallback-once")
        details = out.errors | "details" >> beam.Map(lambda e: (e.reason, e.detail))
        assert_that(
            details,
            equal_to([(REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:{approval_intent_id}")]),
            label="orphaned",
        )


def test_approval_after_the_deadline_is_refused_before_the_timer_fires() -> None:
    # Scenario: An approval arriving after the deadline is refused even before
    # the timer fires. Processing time never advances here, so the real-time
    # HITL timer has not fired and the continuation is still stored -- only the
    # deadline check keeps the blocked effect from going through.
    approval_intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, deadline = 1000ms
        .add_elements([_approval(b"k", approval_intent_id, approved=True, t_ms=1500)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([]), label="not-resumed")
        details = out.errors | "details" >> beam.Map(lambda e: (e.reason, e.detail))
        assert_that(
            details,
            equal_to([(REASON_ORPHANED, f"{DETAIL_DEADLINE_PASSED}:{approval_intent_id}")]),
            label="deadline-passed",
        )


def test_in_time_approval_resumes_and_clears_the_timer() -> None:
    # Scenario: An in-time approval resumes the agent and clears the timer.
    # Processing time then advances well past the original deadline: nothing
    # fires, because the commit cleared the armed timer.
    approval_intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, deadline = 1000ms
        .add_elements([_approval(b"k", approval_intent_id, approved=True, t_ms=500)])
        .advance_processing_time(5)  # past the original deadline
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        assert_that(out.output, equal_to([b"approved"]), label="resumed")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_escalated_suspension_is_resumed_by_an_answer_to_the_escalation() -> None:
    # Scenario: An answer to the escalated suspension resumes the agent.
    # The whole loop end to end: suspend -> timer fires -> escalate (new intent,
    # extended deadline, re-armed timer) -> a human answers the escalation
    # before the new deadline -> the activation resumes.
    escalation_intent_id = intent_id_for(b"k", 0, 1)  # next free step after the suspension
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, arms HITL at 1000ms
        .advance_processing_time(2)  # -> fires the timer; escalates to 6000ms
        .add_elements([_approval(b"k", escalation_intent_id, approved=False, t_ms=3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(
                provider_factory=make_pong_provider,
                ttl_ms=_BIG_TTL_MS,
                hitl_policy=HitlPolicy(on_timeout=escalate_once, max_escalations=1),
            ),
        )
        assert_that(out.output, equal_to([b"rejected"]), label="resumed-by-escalation")
        assert_that(out.errors, equal_to([]), label="no-errors")
        channels = out.intents | "channels" >> beam.Map(lambda i: (i.tool_name, i.kind))
        assert_that(
            channels,
            equal_to([("approval", ToolIntent.APPROVAL), ("pager", ToolIntent.APPROVAL)]),
            label="escalation-intent",
        )


# --- Requirement: working-memory GC never preempts a live suspension ----------

# Deliberately *smaller* than the suspensions under test (whose deadline is
# event_time + 1000ms). Every other scenario in this file pins `_BIG_TTL_MS`, so
# the two timers are never in contention -- which is exactly why the preemption
# below went unnoticed. These scenarios invert that.
_SHORT_TTL_MS = 100


def test_a_hitl_window_longer_than_the_memory_ttl_still_reports_its_timeout() -> None:
    # Scenario: A HITL window longer than the memory TTL still reports its
    # timeout. The watermark crosses the suspension's own TTL mark before the
    # real-time HITL timer fires; if working-memory GC is allowed to win, it
    # clears the continuation and the later HITL fire reads a stale handle and
    # returns -- the timeout vanishes with nothing on any output.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, deadline = 1000ms
        .advance_watermark_to(0.5)  # past now+ttl (100ms), before deadline+ttl (1100ms)
        .advance_processing_time(2)  # -> fires the real-time HITL timer
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_SHORT_TTL_MS),
        )
        assert_that(out.output, equal_to([HITL_TIMEOUT_OUTPUT]), label="timeout-reported")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_the_timeout_is_reported_regardless_of_which_clock_advances_first() -> None:
    # Scenario: The timeout is reported regardless of which clock advances
    # first. Mirror of the scenario above with the two advances swapped -- the
    # ordering that happens to work today. Both must produce the same output,
    # or the outcome is a coin flip decided by the runner.
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, deadline = 1000ms
        .advance_processing_time(2)  # -> fires the real-time HITL timer first
        .advance_watermark_to(0.5)
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_SHORT_TTL_MS),
        )
        assert_that(out.output, equal_to([HITL_TIMEOUT_OUTPUT]), label="timeout-reported")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_an_escalation_carries_the_memory_ttl_forward_with_the_deadline() -> None:
    # Scenario: An escalation carries the memory TTL forward with the deadline.
    # The escalation moves the deadline to 6000ms, well past the mark the
    # original suspension armed; the watermark then crosses that original mark.
    # If the escalation does not carry the TTL forward, GC wipes the
    # continuation mid-wait and the answer is orphaned instead of resuming.
    escalation_intent_id = intent_id_for(b"k", 0, 1)  # next free step after the suspension
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])  # suspends, deadline = 1000ms
        .advance_processing_time(2)  # -> fires the timer; escalates to 6000ms
        .advance_watermark_to(2.0)  # past the pre-escalation mark (1100ms)
        .add_elements([_approval(b"k", escalation_intent_id, approved=False, t_ms=3000)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(
                provider_factory=make_pong_provider,
                ttl_ms=_SHORT_TTL_MS,
                hitl_policy=HitlPolicy(on_timeout=escalate_once, max_escalations=1),
            ),
        )
        assert_that(out.output, equal_to([b"rejected"]), label="resumed-by-escalation")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_expired_result_reinjected_into_a_live_suspension_resumes() -> None:
    # Scenario: An EXPIRED result re-injected into a live suspension resumes
    # the agent. The effector refused the intent (layer 2) and published its
    # refusal; that is a legitimate resume, not an orphan, so the agent gets to
    # take its own degraded path.
    approval_intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"k", b"go", 0)])
        .add_elements([_result(b"k", approval_intent_id, b"", 500, status=ToolResult.EXPIRED)])
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        expected = b"result:" + str(ToolResult.EXPIRED).encode()
        assert_that(out.output, equal_to([expected]), label="resumed")
        assert_that(out.errors, equal_to([]), label="no-errors")
