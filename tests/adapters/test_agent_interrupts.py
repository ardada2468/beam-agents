"""Spec: langgraph-adapter / Requirement: Graph interrupts map to approval
intents and resume via Command.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("langgraph")

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import TypedDict

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.adapters.langgraph import LangGraphAgent
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from tests.adapters._helpers import ENTITY_KEY, make_ctx


class _State(TypedDict, total=False):
    n: int
    approved: str


def _build_gated_graph(executed: list[str]) -> StateGraph:
    graph: StateGraph = StateGraph(_State)

    def step_a(state: _State) -> _State:
        executed.append("a")
        return {"n": state.get("n", 0) + 1}

    def gate(state: _State) -> _State:
        executed.append("gate")
        decision = interrupt({"question": "proceed?", "n": state["n"]})
        return {"approved": json.dumps(decision, sort_keys=True)}

    def step_b(state: _State) -> _State:
        executed.append("b")
        return {"n": state["n"] + 10}

    graph.add_node("a", step_a)
    graph.add_node("gate", gate)
    graph.add_node("b", step_b)
    graph.add_edge(START, "a")
    graph.add_edge("a", "gate")
    graph.add_edge("gate", "b")
    graph.add_edge("b", END)
    return graph


async def test_interrupt_suspends_with_staged_approval_intent() -> None:
    # Scenario: Interrupt suspends with a staged approval intent — exactly one
    # APPROVAL intent on the approval channel, deterministic step-indexed id,
    # and nothing after the interrupting node has run.
    executed: list[str] = []
    agent = LangGraphAgent(_build_gated_graph(executed))
    ctx = make_ctx(event=json.dumps({"n": 0}).encode(), seq=7)

    outcome = await agent(ctx)

    assert isinstance(outcome, Suspend)
    assert outcome.adapter == "langgraph"
    assert executed == ["a", "gate"], "the node after the interrupt must not have executed"

    intents = ctx.staged_intents
    assert len(intents) == 1
    intent = intents[0]
    assert intent.kind == ToolIntent.APPROVAL
    assert intent.tool_name == "approval"
    assert intent.intent_id == intent_id_for(ENTITY_KEY, 7, 0)
    args = json.loads(intent.args_json)
    assert args["question"] == "proceed?"


async def test_bundle_replay_stages_byte_identical_intents() -> None:
    # Scenario: Bundle replay stages byte-identical intents — two executions
    # from identical committed state serialize to the same intent bytes.
    serialized: list[list[bytes]] = []
    snapshots: list[bytes] = []
    for _attempt in range(2):
        agent = LangGraphAgent(_build_gated_graph([]))
        ctx = make_ctx(event=json.dumps({"n": 0}).encode(), seq=7)
        result = await agent(ctx)
        assert isinstance(result, Suspend)
        snapshots.append(result.snapshot)
        serialized.append(
            [intent.SerializeToString(deterministic=True) for intent in ctx.staged_intents]
        )

    assert serialized[0] == serialized[1]
    # The snapshot need not be byte-identical: interrupt ids derive from
    # LangGraph's time-based checkpoint ids, and the snapshot commits
    # atomically WITH the checkpoint it references, so the pair is always
    # mutually consistent. What must match across replays is the intent
    # correlation: same pending intent ids, same kinds.
    decoded = [json.loads(s) for s in snapshots]
    assert sorted(decoded[0]["pending"]) == sorted(decoded[1]["pending"])
    assert [e["kind"] for e in decoded[0]["pending"].values()] == [
        e["kind"] for e in decoded[1]["pending"].values()
    ]


async def test_approval_resumes_the_graph_with_command() -> None:
    # Scenario: Approval resumes the graph with Command — the approval payload
    # reaches the interrupted node's `interrupt()` return value, completed
    # nodes do not re-execute, and the activation completes.
    executed: list[str] = []
    agent = LangGraphAgent(_build_gated_graph(executed))
    ctx = make_ctx(event=json.dumps({"n": 0}).encode(), seq=7)
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)
    intent_id = ctx.staged_intents[0].intent_id

    # Fresh agent + fresh memory objects: the resume may land on another worker.
    resume_executed: list[str] = []
    resume_agent = LangGraphAgent(_build_gated_graph(resume_executed))
    resume_ctx = make_ctx(
        seq=7,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_approval=AgentEnvelope.Approval(
            intent_id=intent_id,
            approved=True,
            approver="alice",
            decided_at_ms=1_700_000_100_000,
        ),
    )
    outcome = await resume_agent(resume_ctx)

    assert isinstance(outcome, Complete)
    final = json.loads(outcome.output)
    assert final["n"] == 11
    decision = json.loads(final["approved"])
    assert decision["approved"] is True
    assert decision["approver"] == "alice"
    # Completed nodes stay completed; the interrupted node re-runs from its
    # start (LangGraph's documented semantics).
    assert resume_executed == ["gate", "b"]
    # A resume that answers everything stages no new intents.
    assert resume_ctx.staged_intents == []


async def test_unknown_resume_intent_fails_closed() -> None:
    # A result for an intent this suspension never staged must not be silently
    # swallowed: fail the activation so the element routes to errors.
    agent = LangGraphAgent(_build_gated_graph([]))
    ctx = make_ctx(event=json.dumps({"n": 0}).encode(), seq=7)
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)

    stray = AgentEnvelope.Approval(intent_id="not-a-real-intent", approved=True)
    resume_ctx = make_ctx(
        seq=7,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_approval=stray,
    )
    with pytest.raises(ValueError, match="not-a-real-intent"):
        await LangGraphAgent(_build_gated_graph([]))(resume_ctx)
