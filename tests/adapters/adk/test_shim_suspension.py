"""Spec: adk-adapter / Requirement: Side-effect tools suspend via long-running
function calls.

Scenarios: Side-effect tool call suspends instead of executing; ToolResult
resumes the run as a function response; Parallel side-effect calls resume after
all results arrive; Bundle replay stages byte-identical intents.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("google.adk")

from beam_agents._protos import ToolResult
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.tools.errors import SideEffectToolError
from tests.adapters._helpers import ENTITY_KEY, make_ctx
from tests.adapters.adk._helpers import call_turn, scripted_adk_agent, text_turn
from tests.conformance._spec import EXECUTED_SIDE_EFFECTS, charge, lookup_a

_SCRIPT = [call_turn(("charge", {"amount": "5"})), text_turn("done-suspension")]


@pytest.fixture(autouse=True)
def _clear_side_effects() -> None:
    EXECUTED_SIDE_EFFECTS.clear()


def _tool_result(intent_id: str, payload: bytes = b"ack") -> ToolResult:
    return ToolResult(
        intent_id=intent_id, entity_key=ENTITY_KEY, payload=payload, status=ToolResult.OK
    )


async def test_side_effect_tool_call_suspends_instead_of_executing() -> None:
    # Scenario: Side-effect tool call suspends instead of executing.
    agent, _model = scripted_adk_agent(_SCRIPT, [charge], hitl_timeout_ms=60_000)
    ctx = make_ctx(event=b"go", seq=0)

    outcome = await agent(ctx)

    assert isinstance(outcome, Suspend)
    assert outcome.adapter == "adk"
    assert outcome.timeout_ms == 60_000
    assert EXECUTED_SIDE_EFFECTS == [], "the side-effect tool's body executed in-pipeline"

    assert len(ctx.staged_intents) == 1
    intent = ctx.staged_intents[0]
    assert intent.tool_name == "charge"
    assert json.loads(intent.args_json) == {"amount": "5"}
    assert intent.intent_id == intent_id_for(ENTITY_KEY, 0, 0)

    snapshot = json.loads(outcome.snapshot)
    entry = snapshot["pending"][intent.intent_id]
    assert entry["kind"] == "tool"
    assert entry["tool_name"] == "charge"
    assert entry["function_call_id"]


async def test_calling_the_side_effect_tool_directly_still_raises() -> None:
    # Invariant 5's backstop: the shim is the sanctioned detour, and a
    # mis-wired agent cannot bypass it.
    with pytest.raises(SideEffectToolError):
        charge(amount="5")


async def test_tool_result_resumes_the_run_as_a_function_response() -> None:
    # Scenario: ToolResult resumes the run as a function response.
    agent, model = scripted_adk_agent(_SCRIPT, [charge])
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)
    intent_id = first_ctx.staged_intents[0].intent_id

    resume_ctx = make_ctx(
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        cache_blob=first_ctx.cache_blob(),
        snapshot=suspended.snapshot,
        resume_result=_tool_result(intent_id),
        step_index=first_ctx.step_index,
    )
    resumed = await agent(resume_ctx)

    assert isinstance(resumed, Complete)
    assert resumed.output == b"done-suspension"
    assert resume_ctx.staged_intents == [], "a resume must stage no new intents"
    # The post-resume turn was selected by the function response's presence,
    # which is only true if the result reached the model as one.
    assert model.calls[-1] == 1
    assert EXECUTED_SIDE_EFFECTS == []


async def test_a_result_for_an_unknown_intent_fails_closed() -> None:
    agent, _model = scripted_adk_agent(_SCRIPT, [charge])
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)

    resume_ctx = make_ctx(
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        snapshot=suspended.snapshot,
        resume_result=_tool_result("not-an-intent-this-suspension-staged"),
        step_index=first_ctx.step_index,
    )
    with pytest.raises(ValueError, match="unknown intent"):
        await agent(resume_ctx)


async def test_parallel_side_effect_calls_resume_after_all_results_arrive() -> None:
    # Scenario: Parallel side-effect calls resume after all results arrive.
    agent, _model = scripted_adk_agent(
        [
            call_turn(
                ("charge", {"amount": "5"}),
                ("charge", {"amount": "7"}),
            ),
            text_turn("done-parallel"),
        ],
        [charge],
    )
    first_ctx = make_ctx(event=b"go", seq=0)
    suspended = await agent(first_ctx)
    assert isinstance(suspended, Suspend)
    assert len(first_ctx.staged_intents) == 2
    amounts = [json.loads(i.args_json)["amount"] for i in first_ctx.staged_intents]
    assert amounts == ["5", "7"], "intents must follow the model's call order"
    first_id, second_id = (i.intent_id for i in first_ctx.staged_intents)

    # First re-injection: accumulate, re-suspend, stage nothing new.
    partial_ctx = make_ctx(
        seq=0,
        memory_blob=first_ctx.memory_blob(),
        cache_blob=first_ctx.cache_blob(),
        snapshot=suspended.snapshot,
        resume_result=_tool_result(first_id, b"ack-5"),
        step_index=first_ctx.step_index,
    )
    partial = await agent(partial_ctx)
    assert isinstance(partial, Suspend)
    assert partial_ctx.staged_intents == []
    assert json.loads(partial.snapshot)["results"].keys() == {first_id}

    # Second re-injection: both responses present, the run resumes.
    final_ctx = make_ctx(
        seq=0,
        memory_blob=partial_ctx.memory_blob(),
        cache_blob=partial_ctx.cache_blob(),
        snapshot=partial.snapshot,
        resume_result=_tool_result(second_id, b"ack-7"),
        step_index=partial_ctx.step_index,
    )
    final = await agent(final_ctx)

    assert isinstance(final, Complete)
    assert final.output == b"done-parallel"
    assert EXECUTED_SIDE_EFFECTS == []


async def test_bundle_replay_stages_byte_identical_intents() -> None:
    # Scenario: Bundle replay stages byte-identical intents — two executions
    # from identical committed state produce identical intent bytes.
    first_agent, _ = scripted_adk_agent(_SCRIPT, [charge])
    replay_agent, _ = scripted_adk_agent(_SCRIPT, [charge])

    first_ctx = make_ctx(event=b"go", seq=4)
    await first_agent(first_ctx)
    replay_ctx = make_ctx(event=b"go", seq=4)
    await replay_agent(replay_ctx)

    first_bytes = [i.SerializeToString(deterministic=True) for i in first_ctx.staged_intents]
    replay_bytes = [i.SerializeToString(deterministic=True) for i in replay_ctx.staged_intents]
    assert first_bytes == replay_bytes
    assert [i.intent_id for i in first_ctx.staged_intents] == [intent_id_for(ENTITY_KEY, 4, 0)]


async def test_mixed_inline_and_side_effect_calls_in_one_turn() -> None:
    # A read-only call executes inline while the side-effect call in the same
    # turn only suspends: one suspension, one intent, one execution.
    agent, _model = scripted_adk_agent(
        [
            call_turn(
                ("lookup_a", {"customer_id": "aa"}),
                ("charge", {"amount": "5"}),
            ),
            text_turn("done-mixed"),
        ],
        [lookup_a, charge],
    )
    ctx = make_ctx(event=b"go", seq=0)

    outcome = await agent(ctx)

    assert isinstance(outcome, Suspend)
    assert [i.tool_name for i in ctx.staged_intents] == ["charge"]
    assert EXECUTED_SIDE_EFFECTS == []
