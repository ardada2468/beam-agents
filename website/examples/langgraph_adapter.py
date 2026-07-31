"""Adopt an existing LangGraph graph without editing its topology.

# requires-extra: langgraph

The runtime is not an agent-authoring framework — authoring belongs to
LangGraph and friends. Adopting an existing graph takes three changes, none of
them to the graph's shape:

1. Re-declare side-effectful tools with the runtime decorator:
   `@tool(side_effect=True)`.
2. Swap LangGraph's prebuilt `ToolNode` for `BeamToolNode(tools)`.
3. Wrap the graph: `RunAgent(LangGraphAgent(graph, chat_models=[model]))`.

What the graph gains is what LangGraph alone does not provide: durable keyed
checkpoints, side effects behind deduplicated intents, replay-cached model
calls, and per-key serialization.

Two caveats worth knowing before you rely on this, both documented by the
adapter itself. Checkpoints persist latest-only inside working memory and the
1 MiB per-key cap applies, so long message histories must be trimmed or
summarized on the LangGraph side. And an interrupted node re-runs *from its
start* on resume (LangGraph's own semantics), so keep pre-interrupt node code
idempotent.

Install the extra:  uv pip install 'beam-agents[langgraph]'
Run it:             python website/examples/langgraph_adapter.py
"""

from __future__ import annotations

import json
from typing import Annotated, Any

import apache_beam as beam
import httpx
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from beam_agents import AgentConfig, RunAgent
from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.adapters.langgraph import BeamToolNode, LangGraphAgent
from beam_agents.core.agent import intent_id_for
from beam_agents.model.fake import FakeLLM, match_any, match_contains, respond_with
from beam_agents.tools import tool

ENTITY_KEY = b"incident-7"
# The model call consumes step 0; the tool shim's side-effect intent is step 1.
EXPECTED_INTENT_ID = intent_id_for(ENTITY_KEY, 0, 1)


# region: tool
@tool(side_effect=True)
def page_oncall(message: str) -> str:
    """Page the on-call engineer.

    `side_effect=True` is the whole declaration. Calling this directly from an
    agent raises; the runtime turns it into a `ToolIntent` and the effector is
    what actually executes it, exactly once per intent id.
    """
    return f"paged: {message}"


# endregion: tool


class GraphState(TypedDict):
    messages: Annotated[list, add_messages]


def make_provider() -> FakeLLM:
    """Turn 1 asks for the tool; turn 2 (a tool message is present) finishes."""
    return FakeLLM(
        [
            (
                match_contains("'role': 'tool'"),
                respond_with(json.dumps({"content": "incident acknowledged"}).encode()),
            ),
            (
                match_any(),
                respond_with(
                    json.dumps(
                        {
                            "tool_call": {
                                "name": "page_oncall",
                                "args": {"message": "disk pressure"},
                                "id": "call-1",
                            }
                        }
                    ).encode()
                ),
            ),
        ]
    )


class _SdkClient:
    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._client = httpx.AsyncClient(transport=transport)


class _ChatModel:
    """Stands in for a LangChain chat model.

    Recognized httpx-backed chat models are served through the runtime's
    replay-cached `LLMClient`; the transport below is a tripwire proving the
    model's own transport is never reached.
    """

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self.root_async_client = _SdkClient(transport)


def _tripwire(request: httpx.Request) -> httpx.Response:
    raise AssertionError("the chat model's own transport must never be reached")


def _to_wire(messages: list[Any]) -> list[dict[str, str]]:
    wire: list[dict[str, str]] = []
    for message in messages:
        if isinstance(message, HumanMessage):
            wire.append({"role": "user", "content": str(message.content)})
        elif isinstance(message, ToolMessage):
            wire.append({"role": "tool", "content": str(message.content)})
        elif isinstance(message, AIMessage):
            wire.append({"role": "assistant", "content": str(message.content)})
    return wire


def encode_output(state: object) -> bytes:
    """Emit just the final assistant message on `.output`.

    The adapter's default encoder serializes the whole terminal state as JSON,
    which for a message graph includes LangChain's per-message UUIDs — fine for
    a debugging tap, awkward for a downstream consumer. `encode_output` is the
    hook for deciding what the pipeline actually publishes.
    """
    assert isinstance(state, dict)
    return str(state["messages"][-1].content).encode()


# region: graph
def build_agent() -> LangGraphAgent:
    """An ordinary model/tools graph, wrapped rather than rewritten."""
    model = _ChatModel(httpx.MockTransport(_tripwire))
    graph: StateGraph = StateGraph(GraphState)

    async def call_model(state: GraphState) -> GraphState:
        response = await model.root_async_client._client.post(
            "https://provider.example/v1/chat",
            json={"model": "demo", "messages": _to_wire(state["messages"]), "temperature": 0},
        )
        data = response.json()
        if "tool_call" in data:
            return {"messages": [AIMessage(content="", tool_calls=[data["tool_call"]])]}
        return {"messages": [AIMessage(content=data["content"])]}

    def route(state: GraphState) -> str:
        last = state["messages"][-1]
        return "tools" if isinstance(last, AIMessage) and last.tool_calls else END

    graph.add_node("model", call_model)
    # The only topology-adjacent change: BeamToolNode in place of ToolNode.
    graph.add_node("tools", BeamToolNode([page_oncall]))
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", route, {"tools": "tools", END: END})
    graph.add_edge("tools", "model")

    return LangGraphAgent(graph, chat_models=[model], encode_output=encode_output)


# endregion: graph


_AGENT: LangGraphAgent | None = None


async def langgraph_agent(ctx: Any) -> Any:
    """Worker-side lazy singleton, so the DoFn pickles by reference."""
    global _AGENT  # noqa: PLW0603 - worker-local singleton
    if _AGENT is None:
        _AGENT = build_agent()
    return await _AGENT(ctx)


def _event(t_ms: int) -> TimestampedValue:
    env = AgentEnvelope(
        entity_key=ENTITY_KEY,
        event_time_ms=t_ms,
        external_event=json.dumps(
            {"messages": [{"role": "user", "content": "disk is filling up"}]}
        ).encode(),
    )
    return TimestampedValue(env, t_ms / 1000)


def _tool_result(t_ms: int) -> TimestampedValue:
    env = AgentEnvelope(entity_key=ENTITY_KEY, event_time_ms=t_ms)
    env.tool_result.intent_id = EXPECTED_INTENT_ID
    env.tool_result.entity_key = ENTITY_KEY
    env.tool_result.payload = json.dumps("paged: disk pressure").encode()
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


def main() -> None:
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(1_000)])
        .add_elements([_tool_result(1_500)])
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        keyed = (
            pipeline
            | stream
            | "Key"
            >> beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        )
        outputs = keyed | "Agent" >> RunAgent(
            langgraph_agent,
            config=AgentConfig(provider_factory=make_provider, ttl_ms=1_000_000_000),
        )

        names = outputs.intents | "IntentNames" >> beam.Map(
            lambda intent: (intent.tool_name, intent.intent_id)
        )
        assert_that(names, equal_to([("page_oncall", EXPECTED_INTENT_ID)]), label="intents")

        assert_that(outputs.output, equal_to([b"incident acknowledged"]), label="output")

    print("langgraph_adapter: ok")


if __name__ == "__main__":
    main()
