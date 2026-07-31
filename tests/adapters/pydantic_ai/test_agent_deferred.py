"""Spec: pydantic-ai-adapter / Requirement: Side-effect and approval tool calls
map to intents and suspension.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from beam_agents.adapters.pydantic_ai import PydanticAIAgent
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.model.fake import FakeLLM
from beam_agents.tools import ToolRegistry, tool
from tests.adapters.pydantic_ai._helpers import (
    ENTITY_KEY,
    RecognizedModel,
    make_ctx,
    scripted,
    tripwire,
)

EXECUTED: list[str] = []


@tool(side_effect=True)
def charge(amount: str) -> str:
    """Side-effect tool: must never execute inside the pipeline."""
    EXECUTED.append(amount)
    return "charged"


@tool
def notify(target: str) -> str:
    """Read-only tool, gated on human approval in these scenarios."""
    EXECUTED.append(target)
    return f"notified:{target}"


@pytest.fixture(autouse=True)
def _clear() -> None:
    EXECUTED.clear()


def _registry(*tools: object) -> ToolRegistry:
    registry = ToolRegistry()
    for t in tools:
        registry.register(t)  # type: ignore[arg-type]
    return registry


def _build(
    model_id: str, directives: list[bytes], **kwargs: Any
) -> tuple[PydanticAIAgent, FakeLLM]:
    model = RecognizedModel(model_id, tripwire())
    agent = PydanticAIAgent(Agent(model), hitl_timeout_ms=3_600_000, **kwargs)
    return agent, scripted(model_id, directives)


_ACT_CHARGE = b'{"act": {"name": "charge", "args": {"amount": "5"}}}'


async def test_deferred_side_effect_call_suspends_instead_of_executing() -> None:
    # Scenario: Deferred side-effect call suspends instead of executing — the
    # tool body never runs, exactly one ToolIntent with the deterministic
    # intent_id is staged, and the activation returns Suspend.
    agent, provider = _build("m-defer", [_ACT_CHARGE], tools=[charge])
    ctx = make_ctx(provider=provider, seq=0, tool_registry=_registry(charge))

    outcome = await agent(ctx)

    assert isinstance(outcome, Suspend)
    assert outcome.adapter == "pydantic_ai"
    assert outcome.timeout_ms == 3_600_000
    assert EXECUTED == [], "the side-effect tool's body must never execute"
    assert len(ctx.staged_intents) == 1
    intent = ctx.staged_intents[0]
    assert intent.kind == ToolIntent.TOOL
    assert intent.tool_name == "charge"
    # The model call took step 0, so the intent lands at step 1.
    assert intent.intent_id == intent_id_for(ENTITY_KEY, 0, 1)
    assert json.loads(intent.args_json) == {"amount": "5"}


async def test_reinjected_result_resumes_the_run_with_the_deferred_result() -> None:
    # Scenario: Re-injected result resumes the run with the deferred result.
    agent, provider = _build(
        "m-resume", [_ACT_CHARGE, b'{"answer": "done-resume"}'], tools=[charge]
    )
    ctx = make_ctx(provider=provider, seq=0, tool_registry=_registry(charge))
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    intent_id = ctx.staged_intents[0].intent_id

    resume_agent, _ = _build("m-resume", [], tools=[charge])
    resume_ctx = make_ctx(
        provider=provider,
        seq=0,
        memory_blob=ctx.memory_blob(),
        cache_blob=ctx.cache_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        tool_registry=_registry(charge),
        resume_result=ToolResult(
            intent_id=intent_id,
            entity_key=ENTITY_KEY,
            payload=b"ack",
            status=ToolResult.OK,
        ),
    )
    outcome = await resume_agent(resume_ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"done-resume"
    assert resume_ctx.staged_intents == [], "a fully answered resume stages no new intent"
    assert EXECUTED == []
    # The injected payload reached the conversation as the tool call's result.
    history = json.loads(
        next(e.value for e in resume_ctx.memory_blob().entries).decode("utf-8", errors="replace")[
            1:
        ]
    )
    assert any("ack" in json.dumps(message) for message in history)


async def test_parallel_deferred_calls_resume_after_all_results_arrive() -> None:
    # Scenario: Parallel deferred calls resume after all results arrive — the
    # first re-injection re-suspends staging nothing new, the second resumes.
    two_calls = json.dumps(
        {
            "act": [
                {"name": "charge", "args": {"amount": "1"}},
                {"name": "charge", "args": {"amount": "2"}},
            ]
        }
    ).encode()
    # Both results land before the next model turn, so the resumed request
    # carries two tool-return parts: the answering rule sits at index 2.
    agent, provider = _build(
        "m-parallel",
        [two_calls, b'{"answer": "unreachable"}', b'{"answer": "done-parallel"}'],
        tools=[charge],
    )
    ctx = make_ctx(provider=provider, seq=0, tool_registry=_registry(charge))
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    intent_ids = [intent.intent_id for intent in ctx.staged_intents]
    assert len(intent_ids) == 2
    assert intent_ids == [intent_id_for(ENTITY_KEY, 0, 1), intent_id_for(ENTITY_KEY, 0, 2)]

    first_agent, _ = _build("m-parallel", [], tools=[charge])
    first_ctx = make_ctx(
        provider=provider,
        seq=0,
        memory_blob=ctx.memory_blob(),
        cache_blob=ctx.cache_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        tool_registry=_registry(charge),
        resume_result=ToolResult(
            intent_id=intent_ids[0], entity_key=ENTITY_KEY, payload=b"r1", status=ToolResult.OK
        ),
    )
    partial = await first_agent(first_ctx)
    assert isinstance(partial, Suspend), "an unanswered call must keep the activation suspended"
    assert first_ctx.staged_intents == [], "accumulating stages nothing new"

    second_agent, _ = _build("m-parallel", [], tools=[charge])
    second_ctx = make_ctx(
        provider=provider,
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        cache_blob=first_ctx.cache_blob(),
        snapshot=partial.snapshot,
        step_index=first_ctx.step_index,
        tool_registry=_registry(charge),
        resume_result=ToolResult(
            intent_id=intent_ids[1], entity_key=ENTITY_KEY, payload=b"r2", status=ToolResult.OK
        ),
    )
    outcome = await second_agent(second_ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"done-parallel"
    assert EXECUTED == []


async def test_approval_requiring_call_maps_to_an_approval_intent() -> None:
    # Scenario: Approval-requiring call maps to an approval intent — one
    # APPROVAL-kind intent on the approval channel, and the resumed run receives
    # the approved decision for the original tool call.
    agent, provider = _build(
        "m-approve",
        [
            b'{"request_approval": {"name": "notify", "args": {"target": "ops"}}}',
            b'{"answer": "done-approve"}',
        ],
        tools=[notify],
        approval_required=["notify"],
    )
    ctx = make_ctx(provider=provider, seq=0, tool_registry=_registry(notify))
    suspended = await agent(ctx)

    assert isinstance(suspended, Suspend)
    assert len(ctx.staged_intents) == 1
    intent = ctx.staged_intents[0]
    assert intent.kind == ToolIntent.APPROVAL
    assert intent.tool_name == "approval"
    assert intent.intent_id == intent_id_for(ENTITY_KEY, 0, 1)
    assert EXECUTED == [], "the gated tool must not run before approval"

    resume_agent, _ = _build("m-approve", [], tools=[notify], approval_required=["notify"])
    resume_ctx = make_ctx(
        provider=provider,
        seq=0,
        memory_blob=ctx.memory_blob(),
        cache_blob=ctx.cache_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        tool_registry=_registry(notify),
        resume_approval=AgentEnvelope.Approval(
            intent_id=intent.intent_id, approved=True, approver="alice"
        ),
    )
    outcome = await resume_agent(resume_ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"done-approve"
    assert EXECUTED == ["ops"], "an approved gated tool executes on resume"


async def test_denied_approval_resumes_without_executing_the_tool() -> None:
    # The fail-closed half of the approval path: a denial reaches the model as
    # a denial and the gated tool never runs.
    agent, provider = _build(
        "m-deny",
        [
            b'{"request_approval": {"name": "notify", "args": {"target": "ops"}}}',
            b'{"answer": "done-deny"}',
        ],
        tools=[notify],
        approval_required=["notify"],
    )
    ctx = make_ctx(provider=provider, seq=0, tool_registry=_registry(notify))
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    intent_id = ctx.staged_intents[0].intent_id

    resume_agent, _ = _build("m-deny", [], tools=[notify], approval_required=["notify"])
    resume_ctx = make_ctx(
        provider=provider,
        seq=0,
        memory_blob=ctx.memory_blob(),
        cache_blob=ctx.cache_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        tool_registry=_registry(notify),
        resume_approval=AgentEnvelope.Approval(intent_id=intent_id, approved=False),
    )
    outcome = await resume_agent(resume_ctx)

    assert isinstance(outcome, Complete)
    assert EXECUTED == [], "a denied call must not execute the tool"


async def test_bundle_replay_stages_byte_identical_intents() -> None:
    # Scenario: Bundle replay stages byte-identical intents — two executions
    # from identical committed state serialize to the same intent bytes.
    serialized: list[list[bytes]] = []
    for _attempt in range(2):
        agent, provider = _build("m-replay", [_ACT_CHARGE], tools=[charge])
        ctx = make_ctx(provider=provider, seq=4, tool_registry=_registry(charge))
        outcome = await agent(ctx)
        assert isinstance(outcome, Suspend)
        serialized.append(
            [intent.SerializeToString(deterministic=True) for intent in ctx.staged_intents]
        )

    assert serialized[0] == serialized[1]


async def test_unknown_resume_intent_fails_closed() -> None:
    # A result for an intent this suspension never staged must not be silently
    # swallowed: fail the activation so the element routes to errors.
    agent, provider = _build("m-stray", [_ACT_CHARGE], tools=[charge])
    ctx = make_ctx(provider=provider, seq=0, tool_registry=_registry(charge))
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)

    resume_agent, _ = _build("m-stray", [], tools=[charge])
    resume_ctx = make_ctx(
        provider=provider,
        seq=0,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        tool_registry=_registry(charge),
        resume_result=ToolResult(intent_id="not-a-real-intent", status=ToolResult.OK),
    )
    with pytest.raises(ValueError, match="not-a-real-intent"):
        await resume_agent(resume_ctx)
