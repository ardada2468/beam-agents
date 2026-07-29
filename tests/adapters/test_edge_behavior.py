"""Edge and failure branches of the adapter surfaces: construction validation,
latest-only lookups for superseded checkpoints, transport recognition shapes,
and fail-closed resume admission. Each is spec-relevant behavior a partner
integration will hit — not coverage filler.
"""

from __future__ import annotations

import json

import httpx
import pytest

pytest.importorskip("langgraph")

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import TypedDict

from beam_agents._protos import ToolResult
from beam_agents.adapters.langgraph import BeamCheckpointSaver, BeamToolNode
from beam_agents.adapters.langgraph import transport as transport_mod
from beam_agents.adapters.langgraph.agent import LangGraphAgent
from beam_agents.adapters.langgraph.transport import (
    _ReplayTransport,
    find_async_client,
    install_transport,
)
from beam_agents.core.agent import Suspend
from beam_agents.memory.facade import Memory
from beam_agents.tools.errors import ToolError, ToolNotFoundError
from beam_agents.tools.registry import tool
from tests.adapters._helpers import make_ctx

_NOW_MS = 1_700_000_000_000
_THREAD: RunnableConfig = {"configurable": {"thread_id": "t"}}


@tool
def echo(text: str) -> str:
    """Echo (read-only)."""
    return text


# -- BeamToolNode construction / input validation -------------------------------


def test_toolnode_rejects_non_runtime_tools() -> None:
    def plain(text: str) -> str:
        return text

    with pytest.raises(ToolError, match="runtime Tool"):
        BeamToolNode([plain])  # type: ignore[list-item]


def test_toolnode_unknown_tool_name_fails_closed() -> None:
    node = BeamToolNode([echo])
    message = AIMessage(content="", tool_calls=[{"name": "ghost", "args": {}, "id": "c1"}])
    with pytest.raises(ToolNotFoundError):
        node({"messages": [message]})


def test_toolnode_missing_tool_call_id_fails_closed() -> None:
    node = BeamToolNode([echo])
    message = AIMessage(
        content="", tool_calls=[{"name": "echo", "args": {"text": "x"}, "id": None}]
    )
    with pytest.raises(ToolError, match="tool_call_id"):
        node({"messages": [message]})


def test_toolnode_requires_a_trailing_ai_message() -> None:
    node = BeamToolNode([echo])
    with pytest.raises(ToolError, match="AIMessage"):
        node({"messages": [HumanMessage(content="hello")]})


# -- BeamCheckpointSaver latest-only lookups ------------------------------------


def _saver_with_one_checkpoint() -> BeamCheckpointSaver:
    memory = Memory(None, now_ms=_NOW_MS)
    saver = BeamCheckpointSaver(memory)
    saver.put(
        _THREAD,
        {"id": "ckpt-1", "channel_values": {"n": 1}, "channel_versions": {}},  # type: ignore[typeddict-item]
        {"step": 0},
        {},
    )
    return saver


def test_get_tuple_for_a_superseded_checkpoint_returns_none() -> None:
    saver = _saver_with_one_checkpoint()
    stale: RunnableConfig = {
        "configurable": {"thread_id": "t", "checkpoint_id": "ckpt-0-superseded"}
    }
    assert saver.get_tuple(stale) is None
    assert saver.get_tuple(_THREAD) is not None


def test_list_edge_arguments_yield_nothing() -> None:
    saver = _saver_with_one_checkpoint()
    assert list(saver.list(None)) == []
    assert list(saver.list(_THREAD, before=_THREAD)) == []
    assert list(saver.list(_THREAD, limit=0)) == []
    assert len(list(saver.list(_THREAD))) == 1


# -- transport recognition shapes -----------------------------------------------


def test_find_async_client_on_a_bare_client_attribute() -> None:
    class _Model:
        def __init__(self) -> None:
            self.async_client = httpx.AsyncClient(
                transport=httpx.MockTransport(lambda r: httpx.Response(200))
            )

    model = _Model()
    assert find_async_client(model) is model.async_client


def test_install_transport_is_idempotent() -> None:
    class _Model:
        def __init__(self) -> None:
            self.async_client = httpx.AsyncClient(
                transport=httpx.MockTransport(lambda r: httpx.Response(200))
            )

    model = _Model()
    assert install_transport(model) is True
    first = model.async_client._transport
    assert install_transport(model) is True
    assert model.async_client._transport is first, "a second install must not re-wrap"
    assert isinstance(first, _ReplayTransport)


async def test_transport_passes_through_outside_an_activation() -> None:
    # Outside an activation (no context set) the wrapped original transport
    # serves the request untouched.
    hits: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        hits.append(request)
        return httpx.Response(200, json={"content": "direct"})

    class _Model:
        def __init__(self) -> None:
            self.async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    model = _Model()
    install_transport(model)
    assert transport_mod._current_activation.get() is None
    response = await model.async_client.post("https://provider.example/v1/chat", json={})
    assert response.json() == {"content": "direct"}
    assert len(hits) == 1


# -- fail-closed resume admission for tool results ------------------------------


async def test_unknown_tool_result_intent_fails_closed() -> None:
    # Mirror of the unknown-approval case: a ToolResult for an intent this
    # suspension never staged must fail the activation, not be dropped.
    class _State(TypedDict, total=False):
        n: int

    graph = StateGraph(_State)

    def gate(state: _State) -> _State:
        return {"n": int(str(interrupt({"q": "?"})) == "yes")}

    graph.add_node("gate", gate)
    graph.add_edge(START, "gate")
    graph.add_edge("gate", END)

    agent = LangGraphAgent(graph)
    ctx = make_ctx(event=json.dumps({"n": 0}).encode(), seq=2)
    suspended = await agent(ctx)
    assert isinstance(suspended, Suspend)

    resume_ctx = make_ctx(
        seq=2,
        memory_blob=ctx.memory_blob(),
        snapshot=suspended.snapshot,
        step_index=ctx.step_index,
        resume_result=ToolResult(intent_id="stray-intent", status=ToolResult.OK, payload=b"x"),
    )
    with pytest.raises(ValueError, match="stray-intent"):
        await LangGraphAgent(graph)(resume_ctx)
