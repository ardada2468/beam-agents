"""HITL fail-closed gate (correctness invariant 6, `-m semantics`).

Verifies, under a chaos-forced retry of the `HITL_TIMER` bundle, that the
timeout path is *effectively once*: the discarded attempt's escalation is
rolled back with its bundle, Beam's retry re-mints a byte-identical intent
from the same persisted state, and the suspension ends up escalated exactly
once. Then the late answer to the *original* request — expired by the time it
arrives — is refused, while the answer to the escalation resumes the agent.

This is the invariant that makes a timer callback safe to mutate state from:
it re-executes on retry, so its effects must be pure functions of persisted
values.
"""

from __future__ import annotations

import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.dofn import DETAIL_NO_CONTINUATION, REASON_ORPHANED
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.hitl import HitlPolicy
from beam_agents.testing.chaos import fail_first_hitl_fire
from tests.core._dofn_helpers import approval_agent, escalate_once, keyed, make_pong_provider

pytestmark = pytest.mark.semantics

_ENTITY_KEY = b"k"
_SEQ = 0
# The suspended activation consumes step 0 (its approval request); the
# escalation consumes the continuation's next free step, 1.
_APPROVAL_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, 0)
_ESCALATION_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, 1)


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


def _approval(intent_id: str, *, approved: bool, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=t_ms)
    env.approval.intent_id = intent_id
    env.approval.approved = approved
    env.approval.decided_at_ms = t_ms
    return TimestampedValue(env, t_ms / 1000)


def _check_escalated_exactly_once(actual: object) -> None:
    """One approval request, one escalation — no duplicate from the retry."""
    intents = list(actual)  # type: ignore[call-overload]
    ids = [i.intent_id for i in intents]
    assert ids == [_APPROVAL_INTENT_ID, _ESCALATION_INTENT_ID], (
        f"expected exactly one approval and one escalation intent, got {ids!r}"
    )
    escalation = intents[1]
    assert escalation.kind == ToolIntent.APPROVAL
    assert escalation.tool_name == "pager"
    assert escalation.step_index == 1


def _check_resumed_by_the_escalation(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [b"approved"], f"unexpected output: {items!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


def test_chaos_forced_timer_retry_escalates_exactly_once_and_resumes() -> None:
    with fail_first_hitl_fire(), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(b"go", 0)])  # suspends; deadline 1000ms
            .advance_processing_time(2)  # fires HITL: attempt 1 dies, retry escalates
            .add_elements([_approval(_ESCALATION_INTENT_ID, approved=True, t_ms=3000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(
                provider_factory=make_pong_provider,
                hitl_policy=HitlPolicy(on_timeout=escalate_once, max_escalations=1),
            ),
        )
        assert_that(out.intents, _check_escalated_exactly_once, label="intents")
        assert_that(out.output, _check_resumed_by_the_escalation, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")


def _check_late_original_answer_is_refused(actual: object) -> None:
    items = [(e.reason, e.detail) for e in actual]  # type: ignore[attr-defined]
    assert items == [(REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:{_APPROVAL_INTENT_ID}")], (
        f"expected the late answer to be orphaned, got {items!r}"
    )


def test_answer_arriving_after_the_final_timeout_is_refused() -> None:
    # The escalation bound is 0 here, so the timer denies instead of escalating
    # and the suspension is over; a human's later "yes" cannot revive it.
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(b"go", 0)])
            .advance_processing_time(2)  # fires HITL -> deny, continuation cleared
            .add_elements([_approval(_APPROVAL_INTENT_ID, approved=True, t_ms=3000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            approval_agent,
            config=AgentConfig(provider_factory=make_pong_provider),
        )
        assert_that(out.errors, _check_late_original_answer_is_refused, label="errors")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
