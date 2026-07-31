"""`BeamToolNode`: the side-effect-aware replacement for LangGraph's ToolNode.

Accepts runtime :class:`~beam_agents.tools.registry.Tool` objects (the `@tool`
decorator kind). Tool calls on ``side_effect=False`` tools execute inline with
validated arguments — unchanged LangGraph semantics. Tool calls on
``side_effect=True`` tools never execute in the pipeline: the node batches them
into a single ``interrupt(...)`` carrying the marker payload the adapter
recognizes, the adapter stages one ``ToolIntent`` per call (correctness
invariant 5), and when every result has re-entered, the resumed interrupt
returns ``{tool_call_id: result}`` and the node emits the ``ToolMessage``s with
their original ``tool_call_id``s.

Adoption is exactly "re-tag + swap the node class": tools re-declared with the
runtime `@tool` decorator, LangGraph's prebuilt ``ToolNode`` swapped for this
one, no topology changes. Even a mis-wired graph cannot execute an effect
in-pipeline — calling a ``side_effect=True`` tool directly raises
``SideEffectToolError`` (registry design D3); the shim's interrupt is the
sanctioned detour.

Node re-execution caveat: on resume the node re-runs from its start, so inline
read-only calls in the same batch execute again (they are read-only by
declaration; the runtime already re-runs them on bundle retries).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import interrupt

from beam_agents.tools.errors import ToolError, ToolNotFoundError
from beam_agents.tools.registry import Tool

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "BeamToolNode",
]

# The marker key the adapter looks for in an interrupt payload to tell the
# shim's batched tool calls from a plain approval interrupt. Defined here (the
# producer); the agent imports it.
_TOOL_CALLS_MARKER = "__beam_tool_calls__"


class BeamToolNode:
    """Drop-in ToolNode over runtime tools: read-only inline, side effects via
    suspension. Usable directly as a LangGraph node callable.
    """

    def __init__(self, tools: Sequence[Tool], *, messages_key: str = "messages") -> None:
        for candidate in tools:
            if not isinstance(candidate, Tool):
                raise ToolError(
                    f"BeamToolNode requires runtime Tool objects (@tool-decorated); "
                    f"got {type(candidate).__name__!r}"
                )
        self._tools = {t.name: t for t in tools}
        self._messages_key = messages_key

    def __call__(self, state: Any) -> dict[str, list[ToolMessage]]:
        """Execute the last AI message's tool calls, splitting by ``side_effect``.

        Read-only tools run inline and their results become ``ToolMessage``s;
        side-effecting calls are staged as ``ToolIntent``s and the node
        marks the graph for suspension instead of executing them
        (invariant 5).
        """
        last = self._last_ai_message(state)
        results: dict[str, ToolMessage] = {}
        side_calls: list[dict[str, Any]] = []

        call_ids: list[str] = []
        for call in last.tool_calls:
            call_id = call["id"]
            if call_id is None:
                raise ToolError(f"tool call for {call['name']!r} carries no tool_call_id")
            call_ids.append(call_id)
            registered = self._tools.get(call["name"])
            if registered is None:
                raise ToolNotFoundError(call["name"])
            validated = registered.argument_model(**call["args"]).model_dump()
            if registered.side_effect:
                side_calls.append({"id": call_id, "name": registered.name, "args": validated})
            else:
                value = registered(**validated)
                results[call_id] = ToolMessage(
                    content=value if isinstance(value, str) else json.dumps(value, default=str),
                    tool_call_id=call_id,
                    name=call["name"],
                )

        if side_calls:
            answers = interrupt({_TOOL_CALLS_MARKER: side_calls})
            for staged in side_calls:
                answer = answers[staged["id"]]
                ok = answer["status"] == "OK"
                results[staged["id"]] = ToolMessage(
                    content=answer["payload"] if ok else json.dumps(answer, sort_keys=True),
                    tool_call_id=staged["id"],
                    name=staged["name"],
                    status="success" if ok else "error",
                )

        ordered = [results[call_id] for call_id in call_ids]
        return {self._messages_key: ordered}

    def _last_ai_message(self, state: Any) -> AIMessage:
        messages = state[self._messages_key] if isinstance(state, dict) else state
        if not messages or not isinstance(messages[-1], AIMessage):
            raise ToolError(
                "BeamToolNode expects the last message to be an AIMessage with tool_calls"
            )
        return messages[-1]
