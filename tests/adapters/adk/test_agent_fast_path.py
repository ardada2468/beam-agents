"""Spec: adk-adapter / Requirement: AdkAgent runs an ADK agent as an activation.

Scenario: Fast-path run completes in one activation.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("google.adk")

from google.adk.agents import LlmAgent

from beam_agents.adapters.adk.session import RESERVED_NAMESPACE
from beam_agents.core.agent import Complete
from tests.adapters._helpers import make_ctx
from tests.adapters.adk._helpers import call_turn, scripted_adk_agent, text_turn
from tests.conformance._spec import lookup_a, lookup_b


async def test_fast_path_run_completes_in_one_activation() -> None:
    # Scenario: Fast-path run completes in one activation — the model plus
    # read-only shim tools finish inside one element, and the committed working
    # memory carries the session under the reserved namespace.
    agent, _model = scripted_adk_agent(
        [
            call_turn(("lookup_a", {"customer_id": "aa"})),
            call_turn(("lookup_b", {"customer_id": "bb"})),
            text_turn("done-fast-path"),
        ],
        [lookup_a, lookup_b],
    )
    ctx = make_ctx(event=b"go", seq=3)

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"done-fast-path"
    assert not ctx.staged_intents, "the fast path must stage no intents"

    entries = {entry.key for entry in ctx.memory_blob().entries}
    session_keys = [key for key in entries if key.startswith(RESERVED_NAMESPACE)]
    assert session_keys == [RESERVED_NAMESPACE + "session"], entries


async def test_the_adapter_does_not_restructure_the_user_agent() -> None:
    # The adapter builds a per-activation Runner around the user's agent; the
    # agent's structure (sub-agents, tools, model) must be untouched and carry
    # no adapter state. ADK's own Runner normalizes an unset root `mode` to
    # "chat" (verified against stock ADK with no adapter involved), so that one
    # field is excluded — see the tasks.md Revision.
    agent, _model = scripted_adk_agent([text_turn("ok")])
    inner = agent._agent
    assert isinstance(inner, LlmAgent)
    excluded = {"model", "mode"}
    before = inner.model_dump(exclude=excluded)
    tools_before = list(inner.tools)

    outcome = await agent(make_ctx(event=b"go"))

    assert isinstance(outcome, Complete)
    assert inner.model_dump(exclude=excluded) == before
    assert list(inner.tools) == tools_before
    assert inner.sub_agents == []


async def test_read_only_tool_executes_inline_with_its_value_reaching_the_model() -> None:
    # Scenario: Read-only tool executes inline — the tool runs inside the
    # activation with validated arguments, its value is delivered as that
    # call's function response, and nothing is staged.
    agent, model = scripted_adk_agent(
        [call_turn(("lookup_a", {"customer_id": "aa"})), text_turn("after-tool")],
        [lookup_a],
    )
    ctx = make_ctx(event=b"go")

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"after-tool"
    assert ctx.staged_intents == []
    # The second model turn was selected by the presence of a function
    # response, which only exists because the tool executed inline.
    assert model.calls == [0, 1]
    session = json.loads(ctx.memory.get(RESERVED_NAMESPACE + "session") or b"{}")
    responses = [
        part["function_response"]
        for event in session["events"]
        for part in (event.get("content") or {}).get("parts") or []
        if part.get("function_response")
    ]
    assert responses and responses[0]["response"] == {"result": "AA"}
