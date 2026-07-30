"""The LangGraph conformance factory: each ``ScenarioSpec`` as a ``StateGraph``.

Follows the shape of ``tests/adapters/test_e2e_pipeline.py``: a message-state
graph whose model node posts provider-shaped JSON through a chat-model object
the adapter's transport hook instruments (so every model call rides the
runtime's cache-first ``ctx.call_model`` path), ``BeamToolNode`` for tools
(read-only inline, side effects via the batched interrupt), and a plain
``interrupt(...)`` node for approvals. The scripted responses are the shared
directive vocabulary from ``_spec.turn_response`` — the same bytes the
reference provider serves.

LangGraph is imported lazily inside the build functions (never at module
level): this module must import cleanly in environments without the extra,
where the adapter's cells report as clean skips. Graphs are built worker-side
(the module-level factory is referenced by name; nothing holding an httpx
client is ever pickled).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

# httpx is a core beam-agents dependency (unlike the LangGraph distributions,
# which stay lazily imported below).
import httpx

from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import Behavior, FakeLLM, Matcher, respond_with
from tests.conformance._spec import SCENARIOS_BY_NAME, ScenarioSpec, tool_for, turn_response

if TYPE_CHECKING:
    from beam_agents.adapters.langgraph import LangGraphAgent

_PROVIDER_URL = "https://provider.example/v1/chat"
_APPROVE_MARKER = '{"approve"'


class _SdkClient:
    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._client = httpx.AsyncClient(transport=transport)


class _ChatModel:
    """Minimal chat-model layout the transport hook recognizes
    (``root_async_client._client`` is an ``httpx.AsyncClient``)."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self.root_async_client = _SdkClient(transport)


def _tripwire(request: httpx.Request) -> httpx.Response:
    raise AssertionError("the chat model's own transport must never be reached")


def _to_wire(messages: list[Any]) -> list[dict[str, str]]:
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    wire = []
    for message in messages:
        if isinstance(message, HumanMessage):
            wire.append({"role": "user", "content": str(message.content)})
        elif isinstance(message, ToolMessage):
            wire.append({"role": "tool", "content": str(message.content)})
        elif isinstance(message, AIMessage):
            wire.append({"role": "assistant", "content": str(message.content)})
    return wire


def decode_event(event: bytes) -> object:
    """Raw event bytes -> the graph's message-state input."""
    from langchain_core.messages import HumanMessage

    return {"messages": [HumanMessage(content=event.decode())]}


def encode_output(scenario: str, result: object) -> bytes:
    """Reconstruct the canonical terminal-output shape from the final graph
    state — the same segments the reference agent emits, so both adapters
    produce byte-identical terminals for the same conversation."""
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    spec = SCENARIOS_BY_NAME[scenario]
    messages = result["messages"]  # type: ignore[index]
    readonly = {t.name for t in spec.tools if not t.side_effect}
    segments: list[str] = []
    if spec.uses_memory:
        segments.append(f"seen={sum(isinstance(m, HumanMessage) for m in messages)}")
    tool_values = [
        str(m.content) for m in messages if isinstance(m, ToolMessage) and m.name in readonly
    ]
    if tool_values:
        segments.append(",".join(tool_values))
    side_values = [
        str(m.content) for m in messages if isinstance(m, ToolMessage) and m.name not in readonly
    ]
    if side_values:
        segments.append(f"resumed:{side_values[-1]}")
    last = messages[-1]
    if isinstance(last, AIMessage) and last.content:
        segments.append(str(last.content))
    return "|".join(segments).encode()


class _EncodeOutput:
    """Picklable ``encode_output`` bound to one scenario (module-level class)."""

    def __init__(self, scenario: str) -> None:
        self._scenario = scenario

    def __call__(self, result: object) -> bytes:
        return encode_output(self._scenario, result)


def build_langgraph_agent(spec: ScenarioSpec) -> LangGraphAgent:
    """Translate ``spec`` into a StateGraph + BeamToolNode + transport-routed
    model, wrapped as a fresh ``LangGraphAgent``. Imports LangGraph — call
    only where the extra is installed (worker-side, behind importorskip)."""
    from typing import Annotated

    import httpx
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import add_messages
    from langgraph.types import interrupt
    from typing_extensions import TypedDict

    from beam_agents.adapters.langgraph import BeamToolNode, LangGraphAgent

    # Functional TypedDict form: this module has `from __future__ import
    # annotations`, so a class-form annotation would be a string LangGraph's
    # get_type_hints cannot resolve against function-local names.
    _MsgState = TypedDict("_MsgState", {"messages": Annotated[list, add_messages]})  # noqa: UP013 - class syntax would stringify the annotation (future annotations) and break LangGraph's get_type_hints

    model = _ChatModel(httpx.MockTransport(_tripwire))

    # Node/branch callables carry `Any` hints: with `from __future__ import
    # annotations` a `_MsgState` hint is a string LangGraph's schema inference
    # cannot resolve against function-local names.
    async def call_model(state: Any) -> Any:
        response = await model.root_async_client._client.post(
            _PROVIDER_URL,
            json={
                "model": spec.model_id,
                "messages": _to_wire(state["messages"]),
                "temperature": 0,
            },
        )
        directive = response.json()
        if "answer" in directive:
            return {"messages": [AIMessage(content=directive["answer"])]}
        call = directive.get("run_tool") or directive.get("act")
        if call is not None:
            call_id = f"call-{len(state['messages'])}"
            return {
                "messages": [
                    AIMessage(
                        content="",
                        tool_calls=[{"name": call["name"], "args": call["args"], "id": call_id}],
                    )
                ]
            }
        approval = directive["request_approval"]
        return {
            "messages": [
                AIMessage(content=json.dumps({"approve": approval["args"]}, sort_keys=True))
            ]
        }

    def approve(state: Any) -> Any:
        payload = json.loads(str(state["messages"][-1].content))["approve"]
        decision = interrupt(payload)
        verdict = "approved" if decision.get("approved") else "denied"
        return {"messages": [AIMessage(content=f"resumed:{verdict}")]}

    def route(state: Any) -> str:
        last = state["messages"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "tools"
        if isinstance(last, AIMessage) and str(last.content).startswith(_APPROVE_MARKER):
            return "approve"
        return END

    graph: StateGraph = StateGraph(_MsgState)
    graph.add_node("model", call_model)
    graph.add_node("approve", approve)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", route, {"tools": "tools", "approve": "approve", END: END})
    graph.add_edge("approve", END)
    graph.add_node("tools", BeamToolNode([tool_for(t.name) for t in spec.tools]))
    if spec.turns and spec.turns[-1].directive == "answer":
        # The conversation continues after the tools: hand the results back to
        # the model for the script's final turn.
        graph.add_edge("tools", "model")
    else:
        # The script ends at the side effect: the re-injected result's
        # ToolMessage is the terminal state.
        graph.add_edge("tools", END)

    return LangGraphAgent(
        graph,
        decode_event=decode_event,
        encode_output=_EncodeOutput(spec.name),
        hitl_timeout_ms=spec.hitl_timeout_ms,
        chat_models=[model],
    )


# -- the scripted provider --------------------------------------------------------


def _match_turn(model_id: str, tool_messages: int) -> Matcher:
    """Turn *i* of a transport-routed conversation carries exactly *i*
    tool-role messages (every completed prior turn contributed one)."""

    def matcher(request: LlmRequest) -> bool:
        if request.model_id != model_id or not isinstance(request.messages, list):
            return False
        count = sum(1 for m in request.messages if isinstance(m, dict) and m.get("role") == "tool")
        return count == tool_messages

    return matcher


def langgraph_rules(spec: ScenarioSpec) -> list[tuple[Matcher, Behavior]]:
    """One FakeLLM rule per scripted turn, scoped by the scenario's model_id."""
    return [
        (_match_turn(spec.model_id, index), respond_with(turn_response(turn)))
        for index, turn in enumerate(spec.turns)
    ]


def build_langgraph_provider(spec: ScenarioSpec) -> FakeLLM:
    return FakeLLM(langgraph_rules(spec))
