"""End-to-end adapter gate on the real runtime: an existing-style LangGraph
graph, adopted by re-tagging its tool and swapping the tool node, runs
suspend -> re-inject -> resume through `RunAgent` on a streaming TestPipeline,
with chaos-forced bundle retries at both commits.

Everything the pipeline references is module-level: the DirectRunner pickles
the DoFn, and module-level functions travel by reference. The LangGraphAgent
itself (which holds an httpx client) is built lazily, worker-side, on first
activation — the same shape a real deployment needs.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

pytest.importorskip("langgraph")

from typing import Annotated

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult, TraceEvent
from beam_agents.adapters.langgraph import BeamToolNode, LangGraphAgent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.loop import ActivationResult
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.model.fake import FakeLLM, match_any, match_contains, respond_with
from beam_agents.testing.chaos import fail_first_matching_commit
from beam_agents.tools.registry import tool
from tests.core._dofn_helpers import keyed

_ENTITY_KEY = b"k"
_SEQ = 0
# Step usage in the first activation: the transport-routed model call (turn 1)
# consumes step 0; the shim's side-effect intent consumes step 1.
_EXPECTED_INTENT_ID = intent_id_for(_ENTITY_KEY, _SEQ, 1)

_EXECUTED_ALERTS: list[str] = []


@tool(side_effect=True)
def send_alert(message: str) -> str:
    """Page a human (side effect: executes only in the effector)."""
    _EXECUTED_ALERTS.append(message)
    return "sent"


def make_scripted_provider() -> FakeLLM:
    """Turn 1 (no tool message yet): request the side-effect tool call.
    Turn 2 (a tool message present): finish."""
    return FakeLLM(
        [
            (
                match_contains("'role': 'tool'"),
                respond_with(json.dumps({"content": "done"}).encode()),
            ),
            (
                match_any(),
                respond_with(
                    json.dumps(
                        {
                            "tool_call": {
                                "name": "send_alert",
                                "args": {"message": "fire"},
                                "id": "call-1",
                            }
                        }
                    ).encode()
                ),
            ),
        ]
    )


class _MsgState(TypedDict):
    messages: Annotated[list, add_messages]


class _SdkClient:
    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._client = httpx.AsyncClient(transport=transport)


class _ChatModel:
    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self.root_async_client = _SdkClient(transport)


def _tripwire(request: httpx.Request) -> httpx.Response:
    raise AssertionError("the chat model's own transport must never be reached")


def _to_wire(messages: list[Any]) -> list[dict[str, str]]:
    wire = []
    for message in messages:
        if isinstance(message, HumanMessage):
            wire.append({"role": "user", "content": str(message.content)})
        elif isinstance(message, ToolMessage):
            wire.append({"role": "tool", "content": str(message.content)})
        elif isinstance(message, AIMessage):
            wire.append({"role": "assistant", "content": str(message.content)})
    return wire


def _build_agent() -> LangGraphAgent:
    model = _ChatModel(httpx.MockTransport(_tripwire))
    graph: StateGraph = StateGraph(_MsgState)
    shim = BeamToolNode([send_alert])

    async def call_model(state: _MsgState) -> _MsgState:
        response = await model.root_async_client._client.post(
            "https://provider.example/v1/chat",
            json={"model": "e2e", "messages": _to_wire(state["messages"]), "temperature": 0},
        )
        data = response.json()
        if "tool_call" in data:
            return {"messages": [AIMessage(content="", tool_calls=[data["tool_call"]])]}
        return {"messages": [AIMessage(content=data["content"])]}

    def route(state: _MsgState) -> str:
        last = state["messages"][-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "tools"
        return END

    graph.add_node("model", call_model)
    graph.add_node("tools", shim)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", route, {"tools": "tools", END: END})
    graph.add_edge("tools", "model")
    return LangGraphAgent(graph, chat_models=[model])


_AGENT: LangGraphAgent | None = None


async def e2e_langgraph_agent(ctx: Any) -> Any:
    """Worker-side lazy singleton around the adapter (picklable by reference)."""
    global _AGENT  # noqa: PLW0603 - worker-local singleton, same pattern as providers
    if _AGENT is None:
        _AGENT = _build_agent()
    return await _AGENT(ctx)


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _event(t_ms: int) -> TimestampedValue:
    env = AgentEnvelope(
        entity_key=_ENTITY_KEY,
        event_time_ms=t_ms,
        external_event=json.dumps(
            {"messages": [{"role": "user", "content": "alert please"}]}
        ).encode(),
    )
    return TimestampedValue(env, t_ms / 1000)


def _tool_result(t_ms: int) -> TimestampedValue:
    env = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=t_ms)
    env.tool_result.intent_id = _EXPECTED_INTENT_ID
    env.tool_result.entity_key = _ENTITY_KEY
    env.tool_result.payload = b"alert-ack"
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


def _is_suspend_commit(result: ActivationResult) -> bool:
    return result.status == "suspended"


def _is_resume_commit(result: ActivationResult) -> bool:
    return result.status == "completed"


def _check_output(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one output, got {items!r}"
    assert b"done" in items[0], f"unexpected output: {items[0]!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


def _check_committed_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    intent = items[0]
    assert intent.intent_id == _EXPECTED_INTENT_ID
    assert intent.tool_name == "send_alert"
    assert json.loads(intent.args_json) == {"message": "fire"}
    # Byte-identity: re-mint the intent from its scope; a match proves the
    # (chaos-retried) commit was reproducible, not merely self-consistent.
    expected = ToolIntent(
        intent_id=_EXPECTED_INTENT_ID,
        entity_key=_ENTITY_KEY,
        seq=_SEQ,
        step_index=1,
        tool_name=intent.tool_name,
        args_json=intent.args_json,
        created_at_ms=intent.created_at_ms,
        expires_at_ms=intent.expires_at_ms,
        attempt=0,
        kind=ToolIntent.TOOL,
        trace_id=intent.trace_id,
    )
    assert intent.SerializeToString(deterministic=True) == expected.SerializeToString(
        deterministic=True
    )


def _check_turn_one_committed_once(actual: object) -> None:
    """Two committed LLM_CALL events total — one per model turn.

    More than two would mean the resume (or a chaos retry) re-ran turn 1:
    mid-graph checkpoint resume through the real DoFn failed.
    """
    llm_events = [t for t in actual if t.event_type == TraceEvent.LLM_CALL]  # type: ignore[attr-defined]
    assert len(llm_events) == 2, f"expected exactly two committed LLM calls, got {len(llm_events)}"


def _run_pipeline() -> None:
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            .add_elements([_tool_result(2000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(
            e2e_langgraph_agent,
            config=AgentConfig(provider_factory=make_scripted_provider),
        )
        assert_that(out.output, _check_output, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")
        assert_that(out.intents, _check_committed_intent, label="intents")
        assert_that(out.traces, _check_turn_one_committed_once, label="traces")


@pytest.mark.timeout(120)
def test_suspend_commit_retry_produces_byte_identical_intent() -> None:
    # The suspend commit (which carries the intent) is chaos-failed once; the
    # retried bundle must commit the byte-identical intent, and the run must
    # still complete end to end after the re-injected result.
    _EXECUTED_ALERTS.clear()
    matched: list[str] = []

    def matcher(result: ActivationResult) -> bool:
        if _is_suspend_commit(result):
            matched.append(result.status)
            return True
        return False

    with fail_first_matching_commit(matcher):
        _run_pipeline()
    assert matched, "the chaos fault must actually have fired on the suspend commit"
    assert _EXECUTED_ALERTS == [], "the side-effect tool must never execute in the pipeline"


@pytest.mark.timeout(120)
def test_resume_commit_retry_still_resumes_mid_graph() -> None:
    # The resume commit is chaos-failed once; the retried resume must still
    # complete from the committed mid-graph checkpoint without re-running
    # turn 1 (exactly two committed model calls in total).
    _EXECUTED_ALERTS.clear()
    matched: list[str] = []

    def matcher(result: ActivationResult) -> bool:
        if _is_resume_commit(result):
            matched.append(result.status)
            return True
        return False

    with fail_first_matching_commit(matcher):
        _run_pipeline()
    assert matched, "the chaos fault must actually have fired on the resume commit"
    assert _EXECUTED_ALERTS == []
