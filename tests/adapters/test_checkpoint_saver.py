"""Spec: langgraph-adapter / Requirement: Checkpoints commit atomically with the
bundle — driven through a real compiled graph over the `Memory` facade, never
through the saver's internals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

pytest.importorskip("langgraph")

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from typing_extensions import TypedDict

from beam_agents._protos import MemoryBlob
from beam_agents.adapters.langgraph import BeamCheckpointSaver
from beam_agents.memory.facade import HARD_CAP_BYTES, Memory, MemoryOverflow

if TYPE_CHECKING:
    from langgraph.graph.state import CompiledStateGraph

_NOW_MS = 1_700_000_000_000
_THREAD: RunnableConfig = {"configurable": {"thread_id": "test-key"}}

_RESERVED_PREFIX = "__langgraph__/"


class _State(TypedDict, total=False):
    n: int
    approved: str
    payload: bytes


def _committed_bytes(memory: Memory) -> bytes:
    return memory.to_blob().SerializeToString(deterministic=True)


def _build_graph(
    memory: Memory,
    executed: list[str],
    *,
    with_gate: bool = False,
    raise_in_b: bool = False,
) -> CompiledStateGraph:
    """A three-node linear graph (a -> [gate ->] b) over a fresh saver."""
    graph: StateGraph = StateGraph(_State)

    def step_a(state: _State) -> _State:
        executed.append("a")
        return {"n": state.get("n", 0) + 1}

    def step_gate(state: _State) -> _State:
        executed.append("gate")
        answer = interrupt({"question": "proceed?"})
        return {"approved": str(answer)}

    def step_b(state: _State) -> _State:
        executed.append("b")
        if raise_in_b:
            raise RuntimeError("node b exploded")
        return {"n": state["n"] + 10}

    graph.add_node("a", step_a)
    graph.add_node("b", step_b)
    graph.add_edge(START, "a")
    if with_gate:
        graph.add_node("gate", step_gate)
        graph.add_edge("a", "gate")
        graph.add_edge("gate", "b")
    else:
        graph.add_edge("a", "b")
    graph.add_edge("b", END)
    return graph.compile(checkpointer=BeamCheckpointSaver(memory))


async def test_latest_only_retention() -> None:
    # Scenario: Latest-only retention — after a multi-superstep run the
    # reserved namespace holds exactly one checkpoint tuple: the latest.
    memory = Memory(None, now_ms=_NOW_MS)
    executed: list[str] = []
    graph = _build_graph(memory, executed)
    result = await graph.ainvoke({"n": 0}, _THREAD)
    assert result["n"] == 11
    assert executed == ["a", "b"]

    saver = BeamCheckpointSaver(memory)
    tuples = [t async for t in saver.alist(_THREAD)]
    assert len(tuples) == 1
    latest = await saver.aget_tuple(_THREAD)
    assert latest is not None
    assert (
        latest.config["configurable"]["checkpoint_id"]
        == (tuples[0].config["configurable"]["checkpoint_id"])
    )
    assert latest.checkpoint["channel_values"]["n"] == 11
    # Latest-only means no parent chain to walk.
    assert latest.parent_config is None


async def test_checkpoint_lives_under_reserved_namespace() -> None:
    # Scenario: Fast-path graph completes in one activation (checkpoint half):
    # every key the graph run added to working memory is namespaced.
    memory = Memory(None, now_ms=_NOW_MS)
    graph = _build_graph(memory, [])
    await graph.ainvoke({"n": 0}, _THREAD)

    keys = [entry.key for entry in memory.to_blob().entries]
    assert keys, "graph run must persist a checkpoint"
    assert all(key.startswith(_RESERVED_PREFIX) for key in keys), keys


async def test_failed_activation_leaves_no_partial_checkpoint() -> None:
    # Scenario: Failed activation leaves no partial checkpoint — the saver
    # writes only through the staged facade, so the pre-activation blob is
    # byte-identical after a mid-graph failure.
    committed = MemoryBlob(state_schema_version=1)
    before = committed.SerializeToString(deterministic=True)

    memory = Memory(committed, now_ms=_NOW_MS)
    executed: list[str] = []
    graph = _build_graph(memory, executed, raise_in_b=True)
    with pytest.raises(RuntimeError, match="node b exploded"):
        await graph.ainvoke({"n": 0}, _THREAD)

    assert executed == ["a", "b"], "node a must have run (and checkpointed) before the failure"
    assert committed.SerializeToString(deterministic=True) == before


async def test_worker_failover_resumes_mid_graph() -> None:
    # Scenario: Worker failover resumes mid-graph — rebuild memory, saver, and
    # graph from the committed blob bytes (a fresh DoFn instance) and resume:
    # superstep-complete nodes do not re-execute.
    memory = Memory(None, now_ms=_NOW_MS)
    executed: list[str] = []
    graph = _build_graph(memory, executed, with_gate=True)
    first = await graph.ainvoke({"n": 0}, _THREAD)
    assert "__interrupt__" in first
    assert executed == ["a", "gate"]

    committed = MemoryBlob()
    committed.ParseFromString(_committed_bytes(memory))

    fresh_memory = Memory(committed, now_ms=_NOW_MS + 1_000)
    fresh_executed: list[str] = []
    fresh_graph = _build_graph(fresh_memory, fresh_executed, with_gate=True)
    result = await fresh_graph.ainvoke(Command(resume="yes"), _THREAD)

    assert result["n"] == 11
    assert result["approved"] == "yes"
    # The interrupted node re-executes from its start (LangGraph's documented
    # resume semantics); the node completed before it does NOT.
    assert fresh_executed == ["gate", "b"]


async def test_oversized_checkpoint_raises_memory_overflow() -> None:
    # Scenario (cap behavior, design D2): a checkpoint that would push working
    # memory past the hard cap fails the activation cleanly — MemoryOverflow
    # propagates, nothing executes downstream, no partial state.
    memory = Memory(None, now_ms=_NOW_MS)
    graph: StateGraph = StateGraph(_State)

    def step_bloat(state: _State) -> _State:
        return {"payload": b"x" * (HARD_CAP_BYTES + 1)}

    graph.add_node("bloat", step_bloat)
    graph.add_edge(START, "bloat")
    graph.add_edge("bloat", END)
    compiled = graph.compile(checkpointer=BeamCheckpointSaver(memory))

    with pytest.raises(MemoryOverflow):
        await compiled.ainvoke({"n": 0}, _THREAD)
