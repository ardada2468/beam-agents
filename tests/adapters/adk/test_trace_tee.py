"""Spec: adk-adapter / Requirement: The ADK event stream is teed into the
activation trace.

Scenarios: Inline tool executions appear as TOOL_CALL trace events; Trace bytes
are replay-deterministic.
"""

from __future__ import annotations

import pytest

pytest.importorskip("google.adk")

from beam_agents._protos import TraceEvent
from beam_agents.core.agent import Complete
from beam_agents.observability.traces import ADAPTER, TOOL_NAME, span_id_for, trace_id_for
from tests.adapters._helpers import ENTITY_KEY, make_ctx
from tests.adapters.adk._helpers import call_turn, scripted_adk_agent, text_turn
from tests.conformance._spec import lookup_a, lookup_b

_SCRIPT = [
    call_turn(("lookup_a", {"customer_id": "aa"})),
    call_turn(("lookup_b", {"customer_id": "bb"})),
    text_turn("done-traces"),
]


def _tool_calls(ctx: object) -> list[TraceEvent]:
    return [e for e in ctx.staged_traces if e.event_type == TraceEvent.TOOL_CALL]  # type: ignore[attr-defined]


async def test_inline_tool_executions_appear_as_tool_call_trace_events() -> None:
    # Scenario: Inline tool executions appear as TOOL_CALL trace events — one
    # per execution, each carrying the tool name and the adapter attribute,
    # correlated to the activation's trace and span identity.
    agent, _model = scripted_adk_agent(_SCRIPT, [lookup_a, lookup_b])
    ctx = make_ctx(event=b"go", seq=2)

    outcome = await agent(ctx)
    assert isinstance(outcome, Complete)

    events = _tool_calls(ctx)
    assert [e.attributes[TOOL_NAME] for e in events] == ["lookup_a", "lookup_b"]
    assert all(e.attributes[ADAPTER] == "adk" for e in events)
    assert all(e.trace_id == trace_id_for(ENTITY_KEY, 2) for e in events)
    # Each execution gets its own tool span, from the adapter's own tool_index
    # counter (never the intent step cursor).
    assert [e.span_id for e in events] == [
        span_id_for(ENTITY_KEY, 2, "TOOL_CALL", 0),
        span_id_for(ENTITY_KEY, 2, "TOOL_CALL", 1),
    ]
    assert all(e.parent_span_id == ctx.trace.span_id for e in events)


async def test_the_tee_never_perturbs_the_intent_step_cursor() -> None:
    # The tool_index counter exists precisely so tool spans cannot change the
    # intent_ids the activation goes on to mint.
    agent, _model = scripted_adk_agent(_SCRIPT, [lookup_a, lookup_b])
    ctx = make_ctx(event=b"go", seq=2)
    before = ctx.step_index

    await agent(ctx)

    # Only the model calls advanced the cursor; the two tool calls did not.
    llm_calls = sum(1 for e in ctx.staged_traces if e.event_type == TraceEvent.LLM_CALL)
    assert ctx.step_index - before == llm_calls


async def test_trace_bytes_are_replay_deterministic() -> None:
    # Scenario: Trace bytes are replay-deterministic — two executions from
    # identical committed state stage byte-identical trace events, proving no
    # ADK event id, timestamp, or invocation id leaks into trace bytes.
    first_agent, _ = scripted_adk_agent(_SCRIPT, [lookup_a, lookup_b])
    replay_agent, _ = scripted_adk_agent(_SCRIPT, [lookup_a, lookup_b])

    first_ctx = make_ctx(event=b"go", seq=6)
    await first_agent(first_ctx)
    replay_ctx = make_ctx(event=b"go", seq=6)
    await replay_agent(replay_ctx)

    first_bytes = [e.SerializeToString(deterministic=True) for e in first_ctx.staged_traces]
    replay_bytes = [e.SerializeToString(deterministic=True) for e in replay_ctx.staged_traces]
    assert first_bytes == replay_bytes
    assert first_bytes, "the activation staged no traces at all"


async def test_no_adk_identifiers_leak_into_trace_attributes() -> None:
    # ADK ids are prefixed `adk-`/`e-` and its invocation ids are random; none
    # may appear anywhere in a staged trace event's attributes.
    agent, _model = scripted_adk_agent(_SCRIPT, [lookup_a, lookup_b])
    ctx = make_ctx(event=b"go", seq=2)

    await agent(ctx)

    for event in ctx.staged_traces:
        for key, value in event.attributes.items():
            assert not value.startswith("adk-"), (key, value)
            assert not value.startswith("e-"), (key, value)
