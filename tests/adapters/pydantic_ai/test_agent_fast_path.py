"""Spec: pydantic-ai-adapter / Requirements: PydanticAIAgent runs a Pydantic AI
agent as an activation; Read-only tools execute inline through the runtime tool
path; Run usage accumulates into the activation tally.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent

from beam_agents._protos import TraceEvent
from beam_agents.adapters.pydantic_ai import PydanticAIAgent
from beam_agents.adapters.pydantic_ai.history import _RESERVED_NAMESPACE
from beam_agents.core.agent import Complete
from beam_agents.tools import ToolRegistry, tool
from tests.adapters.pydantic_ai._helpers import (
    RecognizedModel,
    make_ctx,
    scripted,
    tripwire,
)

EXECUTED: list[str] = []


@tool
def lookup(customer_id: str) -> str:
    """Read-only conformance tool: uppercases its argument."""
    EXECUTED.append(customer_id)
    return customer_id.upper()


@pytest.fixture(autouse=True)
def _clear() -> None:
    EXECUTED.clear()


def _registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(lookup)
    return registry


async def test_fast_path_completes_and_commits_history() -> None:
    # Scenario: Fast-path run completes in one activation — Complete carries the
    # encoded run output and the committed working memory holds the message
    # history under the reserved namespace.
    model = RecognizedModel("m-fast", tripwire())
    provider = scripted("m-fast", [b'{"answer": "done-fast"}'])
    agent = PydanticAIAgent(Agent(model))
    ctx = make_ctx(provider=provider)

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"done-fast"
    keys = [entry.key for entry in ctx.memory_blob().entries]
    assert keys, "the activation must persist the message history"
    assert all(key.startswith(_RESERVED_NAMESPACE) for key in keys), keys


async def test_read_only_tool_runs_inline_with_a_trace_event() -> None:
    # Scenario: Read-only tool runs inline with a trace event — the tool
    # executes inside the activation, its result reaches the conversation, a
    # TOOL_CALL trace event is staged, and no intent is staged.
    model = RecognizedModel("m-inline", tripwire())
    provider = scripted(
        "m-inline",
        [
            b'{"run_tool": {"name": "lookup", "args": {"customer_id": "aa"}}}',
            b'{"answer": "done-inline"}',
        ],
    )
    agent = PydanticAIAgent(Agent(model), tools=[lookup])
    ctx = make_ctx(provider=provider, tool_registry=_registry())

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    assert outcome.output == b"done-inline"
    assert EXECUTED == ["aa"], "the read-only tool must execute inline"
    assert ctx.staged_intents == [], "an inline tool call stages no intent"
    tool_traces = [
        event
        for event in ctx.staged_traces
        if event.event_type == TraceEvent.TOOL_CALL
        and event.attributes.get("beam_agents.tool_name") == "lookup"
    ]
    assert len(tool_traces) == 1, [(e.event_type, dict(e.attributes)) for e in ctx.staged_traces]
    assert ctx.tally().tool_calls == 1


async def test_completed_run_reports_usage_in_the_tally() -> None:
    # Scenario: Completed run reports usage in the tally.
    model = RecognizedModel("m-usage", tripwire())
    provider = scripted("m-usage", [b'{"answer": "done-usage"}'])
    agent = PydanticAIAgent(Agent(model))
    ctx = make_ctx(provider=provider)

    await agent(ctx)

    tally = ctx.tally()
    assert tally.usage_observed is True
    assert tally.total_tokens > 0
