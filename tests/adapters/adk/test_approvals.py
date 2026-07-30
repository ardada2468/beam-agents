"""Spec: adk-adapter / Requirement: Approval requests map to approval intents.

Scenarios: Approval request suspends with a staged approval intent; Approval
decision resumes the run.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("google.adk")

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL
from tests.adapters._helpers import NOW_MS, make_ctx
from tests.adapters.adk._helpers import call_turn, scripted_adk_agent, text_turn

_SCRIPT = [
    call_turn(("request_approval", {"amount": "5"})),
    text_turn("done-approval"),
]


def _approval(intent_id: str, *, approved: bool = True) -> AgentEnvelope.Approval:
    return AgentEnvelope.Approval(
        intent_id=intent_id,
        approved=approved,
        approver="ops@example.com",
        decided_at_ms=NOW_MS + 5,
    )


async def test_approval_request_suspends_with_a_staged_approval_intent() -> None:
    # Scenario: Approval request suspends with a staged approval intent —
    # exactly one APPROVAL-kind intent on the approval channel, deterministic
    # id, stamped TTL, and the suspension arming the HITL timer.
    agent, model = scripted_adk_agent(_SCRIPT, approval=True, hitl_timeout_ms=1_000)
    ctx = make_ctx(event=b"go", seq=0)

    outcome = await agent(ctx)

    assert isinstance(outcome, Suspend)
    assert outcome.adapter == "adk"
    assert outcome.timeout_ms == 1_000

    assert len(ctx.staged_intents) == 1
    intent = ctx.staged_intents[0]
    assert intent.kind == ToolIntent.APPROVAL
    assert intent.tool_name == DEFAULT_APPROVAL_CHANNEL
    assert intent.intent_id == intent_id_for(ctx.entity_key, 0, 0)
    assert intent.expires_at_ms > intent.created_at_ms, "the approval intent needs a TTL"
    assert json.loads(intent.args_json) == {"amount": "5"}

    # No agent step after the approval request ran: only the requesting turn
    # reached the model.
    assert model.calls == [0]


async def test_approval_decision_resumes_the_run() -> None:
    # Scenario: Approval decision resumes the run — the decision arrives as the
    # approval call's function response and the activation completes.
    agent, model = scripted_adk_agent(_SCRIPT, approval=True, hitl_timeout_ms=1_000)
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)
    intent_id = first_ctx.staged_intents[0].intent_id

    resume_ctx = make_ctx(
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        cache_blob=first_ctx.cache_blob(),
        snapshot=suspended.snapshot,
        resume_approval=_approval(intent_id),
        step_index=first_ctx.step_index,
    )
    resumed = await agent(resume_ctx)

    assert isinstance(resumed, Complete)
    assert resumed.output == b"done-approval"
    assert resume_ctx.staged_intents == []
    assert model.calls[-1] == 1, "the decision must reach the model as a function response"

    # The decision fields ride the function response into the committed session.
    session = json.loads(resume_ctx.memory.get("__adk__/session") or b"{}")
    payloads = [
        part["function_response"]["response"]
        for event in session["events"]
        for part in (event.get("content") or {}).get("parts") or []
        if part.get("function_response")
    ]
    assert payloads[-1] == {
        "approved": True,
        "approver": "ops@example.com",
        "decided_at_ms": NOW_MS + 5,
    }


async def test_a_denial_also_resumes_the_run() -> None:
    agent, _model = scripted_adk_agent(_SCRIPT, approval=True)
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)
    intent_id = first_ctx.staged_intents[0].intent_id

    resume_ctx = make_ctx(
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        cache_blob=first_ctx.cache_blob(),
        snapshot=suspended.snapshot,
        resume_approval=_approval(intent_id, approved=False),
        step_index=first_ctx.step_index,
    )
    resumed = await agent(resume_ctx)

    assert isinstance(resumed, Complete)


async def test_an_approval_for_an_unknown_intent_fails_closed() -> None:
    agent, _model = scripted_adk_agent(_SCRIPT, approval=True)
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)

    resume_ctx = make_ctx(
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        snapshot=suspended.snapshot,
        resume_approval=_approval("never-staged"),
        step_index=first_ctx.step_index,
    )
    with pytest.raises(ValueError, match="unknown intent"):
        await agent(resume_ctx)
