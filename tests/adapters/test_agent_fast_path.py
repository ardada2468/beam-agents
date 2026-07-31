"""Spec: langgraph-adapter / Requirement: LangGraphAgent runs a compiled graph
as an activation — the fast-path scenario.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("langgraph")

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from beam_agents.adapters.langgraph import LangGraphAgent
from beam_agents.adapters.langgraph.checkpoint import _RESERVED_NAMESPACE
from beam_agents.core.agent import Complete
from beam_agents.tools.registry import tool
from tests.adapters._helpers import make_ctx


@tool
def reverse(word: str) -> str:
    """Reverse a word (read-only)."""
    return word[::-1]


class _State(TypedDict, total=False):
    word: str
    reversed: str
    n: int


def _build_graph() -> StateGraph:
    graph: StateGraph = StateGraph(_State)

    def enrich(state: _State) -> _State:
        # Read-only runtime tools stay directly callable inside nodes.
        return {"reversed": str(reverse(word=state["word"]))}

    def count(state: _State) -> _State:
        return {"n": len(state["reversed"])}

    graph.add_node("enrich", enrich)
    graph.add_node("count", count)
    graph.add_edge(START, "enrich")
    graph.add_edge("enrich", "count")
    graph.add_edge("count", END)
    return graph


async def test_fast_path_completes_with_final_output_and_checkpoint() -> None:
    # Scenario: Fast-path graph completes in one activation — Complete carries
    # the final output and the committed working memory holds the latest
    # checkpoint under the reserved namespace.
    agent = LangGraphAgent(_build_graph())
    ctx = make_ctx(event=json.dumps({"word": "stream"}).encode())

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    final = json.loads(outcome.output)
    assert final["reversed"] == "maerts"
    assert final["n"] == 6

    keys = [entry.key for entry in ctx.memory_blob().entries]
    assert keys, "the activation must persist a checkpoint"
    assert all(key.startswith(_RESERVED_NAMESPACE) for key in keys), keys


async def test_accepts_an_already_compiled_graph() -> None:
    # Adoption path: an existing graph compiled by the user (no checkpointer)
    # runs identically — the adapter injects its saver without mutating the
    # user's object.
    compiled = _build_graph().compile()
    agent = LangGraphAgent(compiled)
    ctx = make_ctx(event=json.dumps({"word": "ok"}).encode())

    outcome = await agent(ctx)

    assert isinstance(outcome, Complete)
    assert json.loads(outcome.output)["reversed"] == "ko"
    assert compiled.checkpointer is None, "the user's compiled graph must stay untouched"


async def test_two_activations_share_one_thread_per_key() -> None:
    # The second activation on the same key sees the first's committed
    # checkpoint (same thread), so graph state accumulates per key.
    agent = LangGraphAgent(_build_graph())
    first_ctx = make_ctx(event=json.dumps({"word": "one"}).encode(), seq=1)
    await agent(first_ctx)

    second_ctx = make_ctx(
        event=json.dumps({"word": "four"}).encode(),
        seq=2,
        memory_blob=first_ctx.memory_blob(),
    )
    outcome = await agent(second_ctx)

    assert isinstance(outcome, Complete)
    assert json.loads(outcome.output)["reversed"] == "ruof"
