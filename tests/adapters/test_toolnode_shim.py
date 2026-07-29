"""Spec: langgraph-adapter / Requirement: ToolNode shim converts side-effect
tools to suspension.

The "model" in these graphs is a scripted node emitting AIMessages with tool
calls — the shim's contract is about tool calls, not about how they were
produced, and the transport hook has its own tests.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("langgraph")

from typing import Annotated

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.adapters.langgraph import BeamToolNode, LangGraphAgent
from beam_agents.core.agent import Complete, Suspend
from beam_agents.tools.registry import tool
from tests.adapters._helpers import make_ctx

SENT: list[str] = []


@tool(side_effect=True)
def send_alert(message: str) -> str:
    """Page a human (side effect: executes only in the effector)."""
    SENT.append(message)
    return "sent"


@tool(side_effect=True)
def open_ticket(title: str) -> str:
    """Open an ops ticket (side effect)."""
    SENT.append(title)
    return "opened"


@tool
def lookup(word: str) -> str:
    """Reverse a word (read-only)."""
    return word[::-1]


class _MsgState(TypedDict):
    messages: Annotated[list, add_messages]


def _scripted_graph(tool_calls: list[dict], node_runs: list[str]) -> StateGraph:
    """model -> tools -> model: the first model turn requests `tool_calls`,
    the second summarizes the ToolMessages and ends."""
    graph: StateGraph = StateGraph(_MsgState)
    shim = BeamToolNode([send_alert, open_ticket, lookup])

    def model(state: _MsgState) -> _MsgState:
        node_runs.append("model")
        last = state["messages"][-1]
        if isinstance(last, HumanMessage):
            return {"messages": [AIMessage(content="", tool_calls=tool_calls)]}
        tool_messages = [m for m in state["messages"] if isinstance(m, ToolMessage)]
        summary = "|".join(str(m.content) for m in tool_messages)
        return {"messages": [AIMessage(content=f"done:{summary}")]}

    def route(state: _MsgState) -> str:
        last = state["messages"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "tools"
        return END

    graph.add_node("model", model)
    graph.add_node("tools", shim)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", route, {"tools": "tools", END: END})
    graph.add_edge("tools", "model")
    return graph


def _event(text: str) -> bytes:
    return json.dumps({"messages": [{"role": "user", "content": text}]}).encode()


async def test_side_effect_tool_call_suspends_instead_of_executing() -> None:
    # Scenario: Side-effect tool call suspends instead of executing.
    SENT.clear()
    node_runs: list[str] = []
    calls = [{"name": "send_alert", "args": {"message": "fire"}, "id": "call-1"}]
    agent = LangGraphAgent(_scripted_graph(calls, node_runs))
    ctx = make_ctx(event=_event("alert please"), seq=3)

    outcome = await agent(ctx)

    assert isinstance(outcome, Suspend)
    assert SENT == [], "the side-effect callable must not run inside the pipeline"
    intents = ctx.staged_intents
    assert len(intents) == 1
    assert intents[0].tool_name == "send_alert"
    assert intents[0].kind == ToolIntent.TOOL
    assert json.loads(intents[0].args_json) == {"message": "fire"}


async def test_read_only_tool_executes_inline() -> None:
    # Scenario: Read-only tool executes inline — completes in one activation,
    # ToolMessage carries the value, no intent staged.
    node_runs: list[str] = []
    calls = [{"name": "lookup", "args": {"word": "stream"}, "id": "call-1"}]
    agent = LangGraphAgent(_scripted_graph(calls, node_runs))
    ctx = make_ctx(event=_event("reverse it"), seq=3)

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    assert b"done:maerts" in outcome.output
    assert ctx.staged_intents == []


async def test_tool_result_resumes_as_tool_message() -> None:
    # Scenario: ToolResult resumes the graph as a ToolMessage with the
    # original tool_call_id and the result payload as content.
    SENT.clear()
    calls = [{"name": "send_alert", "args": {"message": "fire"}, "id": "call-1"}]
    agent = LangGraphAgent(_scripted_graph(calls, []))
    ctx = make_ctx(event=_event("alert please"), seq=3)
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    intent_id = ctx.staged_intents[0].intent_id

    resume_ctx = make_ctx(
        seq=3,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_result=ToolResult(
            intent_id=intent_id,
            status=ToolResult.OK,
            payload=b"alert-ack-42",
            completed_at_ms=1_700_000_100_000,
        ),
    )
    outcome = await LangGraphAgent(_scripted_graph(calls, []))(resume_ctx)

    assert isinstance(outcome, Complete)
    assert b"done:alert-ack-42" in outcome.output
    assert SENT == []


async def test_parallel_side_effect_calls_resume_after_all_results() -> None:
    # Scenario: Parallel side-effect calls resume after all results arrive —
    # the first re-injection re-suspends without running the graph or staging
    # new intents; the second resumes with both ToolMessages present.
    SENT.clear()
    calls = [
        {"name": "send_alert", "args": {"message": "fire"}, "id": "call-a"},
        {"name": "open_ticket", "args": {"title": "sev1"}, "id": "call-b"},
    ]
    node_runs: list[str] = []
    agent = LangGraphAgent(_scripted_graph(calls, node_runs))
    ctx = make_ctx(event=_event("both"), seq=4)
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    assert len(ctx.staged_intents) == 2
    by_name = {intent.tool_name: intent.intent_id for intent in ctx.staged_intents}
    runs_after_suspend = list(node_runs)

    first_ctx = make_ctx(
        seq=4,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_result=ToolResult(
            intent_id=by_name["send_alert"], status=ToolResult.OK, payload=b"sent"
        ),
    )
    partial = await agent(first_ctx)
    assert isinstance(partial, Suspend), "one of two results must re-suspend"
    assert first_ctx.staged_intents == [], "re-suspending must not stage new intents"
    assert node_runs == runs_after_suspend, "the graph must not run on a partial resume"

    second_ctx = make_ctx(
        seq=4,
        memory_blob=first_ctx.memory_blob(),
        snapshot=partial.snapshot,
        step_index=first_ctx.step_index,
        resume_result=ToolResult(
            intent_id=by_name["open_ticket"], status=ToolResult.OK, payload=b"opened"
        ),
    )
    outcome = await agent(second_ctx)

    assert isinstance(outcome, Complete)
    assert b"sent" in outcome.output and b"opened" in outcome.output
    assert SENT == []


async def test_mixed_calls_execute_read_only_inline_and_suspend_the_rest() -> None:
    # Read-only calls in the same model turn run inline; only the side-effect
    # call suspends, and after its result arrives all ToolMessages are present.
    calls = [
        {"name": "lookup", "args": {"word": "ab"}, "id": "call-r"},
        {"name": "send_alert", "args": {"message": "hi"}, "id": "call-s"},
    ]
    agent = LangGraphAgent(_scripted_graph(calls, []))
    ctx = make_ctx(event=_event("mixed"), seq=5)
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    assert [intent.tool_name for intent in ctx.staged_intents] == ["send_alert"]

    resume_ctx = make_ctx(
        seq=5,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_result=ToolResult(
            intent_id=ctx.staged_intents[0].intent_id, status=ToolResult.OK, payload=b"ok"
        ),
    )
    outcome = await agent(resume_ctx)
    assert isinstance(outcome, Complete)
    assert b"done:ba|ok" in outcome.output


async def test_adoption_by_retagging_only() -> None:
    # Scenario: Adoption by re-tagging only — a standard model->tools->model
    # graph whose tools are runtime-tagged and whose tool node is the shim
    # compiles and runs its side effects through staged intents, end to end.
    SENT.clear()
    calls = [{"name": "send_alert", "args": {"message": "adopt"}, "id": "call-1"}]
    compiled = _scripted_graph(calls, []).compile()  # user compiles their own graph
    agent = LangGraphAgent(compiled)
    ctx = make_ctx(event=_event("go"), seq=6)

    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    assert [intent.tool_name for intent in ctx.staged_intents] == ["send_alert"]

    resume_ctx = make_ctx(
        seq=6,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_result=ToolResult(
            intent_id=ctx.staged_intents[0].intent_id, status=ToolResult.OK, payload=b"done"
        ),
    )
    outcome = await agent(resume_ctx)
    assert isinstance(outcome, Complete)
    assert SENT == []
