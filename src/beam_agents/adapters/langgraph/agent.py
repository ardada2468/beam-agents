"""`LangGraphAgent`: run a (compiled) LangGraph graph as a runtime `Agent`.

The adapter targets the runtime driver contract directly (change design D1): an
activation builds a :class:`~beam_agents.adapters.langgraph.checkpoint.BeamCheckpointSaver`
over the activation's staged working memory, runs the graph on the bridge event
loop under a per-key thread (``thread_id`` = entity key hex — per-key
serialization makes one live thread per key safe by construction), and maps the
graph's terminal state to ``Complete`` or its pending interrupts to a single
``Suspend`` (design D4).

One suspension covers ALL pending graph work. Each pending interrupt stages an
intent — an approval for a plain ``interrupt(...)``, one ``ToolIntent`` per
side-effect tool call for the ToolNode shim's batched interrupt — and the
``Suspend`` snapshot carries only the correlation the runtime doesn't track:
``intent_id → (kind, interrupt_id, tool_call_id)`` plus results already
collected. Re-injected results accumulate (the activation re-suspends, staging
nothing new) until every pending intent is answered; the graph then resumes
once, from the committed checkpoint, with ``Command(resume={interrupt_id:
value})``.

Node re-execution caveat (LangGraph's documented resume semantics): an
interrupted node re-runs *from its start* when the graph resumes, so code before
an ``interrupt()`` executes again. Side effects can only live behind intents
(correctness invariant 5) and model calls are replay-cached, so re-execution is
deterministic and cheap — but authors should keep pre-interrupt node code
idempotent regardless.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from langgraph.graph import StateGraph
from langgraph.types import Command

from beam_agents._protos import ToolResult
from beam_agents.adapters.langgraph.checkpoint import BeamCheckpointSaver
from beam_agents.adapters.langgraph.toolnode import TOOL_CALLS_MARKER
from beam_agents.adapters.langgraph.transport import (
    _current_activation,
    install_transport,
    warn_fallback,
)
from beam_agents.core.agent import Complete, Outcome, Suspend

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from langchain_core.runnables import RunnableConfig
    from langgraph.graph.state import CompiledStateGraph

    from beam_agents.core.context import ActivationContext

_KIND_APPROVAL = "approval"
_KIND_TOOL = "tool"


def _default_decode(event: bytes) -> object:
    """Default event decode: JSON, with an empty event reading as empty input."""
    return json.loads(event) if event else {}


def _default_encode(result: object) -> bytes:
    """Default output encode: deterministic JSON; non-JSON values via `str`."""
    return json.dumps(result, default=str, sort_keys=True).encode("utf-8")


def _canonical_json(value: object) -> str:
    return json.dumps(value, default=str, sort_keys=True)


class LangGraphAgent:
    """Wraps a LangGraph graph (a `StateGraph` builder or an already-compiled
    graph) as a runtime agent. A user-compiled graph is never mutated: the
    per-activation checkpointer is injected into a copy.
    """

    def __init__(
        self,
        graph: StateGraph | CompiledStateGraph,  # type: ignore[type-arg]
        *,
        decode_event: Callable[[bytes], object] = _default_decode,
        encode_output: Callable[[object], bytes] = _default_encode,
        hitl_timeout_ms: int | None = None,
        chat_models: Sequence[object] = (),
    ) -> None:
        self._graph = graph
        self._decode_event = decode_event
        self._encode_output = encode_output
        self._hitl_timeout_ms = hitl_timeout_ms
        self._chat_models = tuple(chat_models)
        # Instrumentation is deferred to the first activation so it happens on
        # the worker (after any submission-time pickling), and runs once per
        # agent instance — which makes the fallback warning once-per-instance.
        self._instrumented = False

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        self._instrument_chat_models()
        saver = BeamCheckpointSaver(ctx.memory)
        compiled = self._compiled_with(saver)

        if ctx.is_resume:
            snapshot = json.loads(ctx.snapshot) if ctx.snapshot else {"pending": {}, "results": {}}
            self._record_incoming(ctx, snapshot)
            unanswered = [i for i in snapshot["pending"] if i not in snapshot["results"]]
            if unanswered:
                # Accumulate: stage nothing new, keep waiting for the rest.
                return self._suspend_with(snapshot)
            result = await self._run(compiled, ctx, Command(resume=self._resume_map(snapshot)))
        else:
            result = await self._run(compiled, ctx, self._decode_event(ctx.event))

        if isinstance(result, dict) and "__interrupt__" in result:
            return self._stage_and_suspend(ctx, result["__interrupt__"])
        return Complete(self._encode_output(result))

    # -- internals -------------------------------------------------------------

    async def _run(
        self,
        compiled: CompiledStateGraph,  # type: ignore[type-arg]
        ctx: ActivationContext,
        graph_input: object,
    ) -> Any:
        """Invoke the graph with the activation exposed to the transport hook."""
        config: RunnableConfig = {"configurable": {"thread_id": ctx.entity_key.hex()}}
        token = _current_activation.set(ctx)
        try:
            return await compiled.ainvoke(graph_input, config)
        finally:
            _current_activation.reset(token)

    def _instrument_chat_models(self) -> None:
        if self._instrumented:
            return
        self._instrumented = True
        for model in self._chat_models:
            if not install_transport(model):
                warn_fallback(model)

    def _compiled_with(self, saver: BeamCheckpointSaver) -> CompiledStateGraph:  # type: ignore[type-arg]
        if isinstance(self._graph, StateGraph):
            return self._graph.compile(checkpointer=saver)
        return self._graph.copy(update={"checkpointer": saver})

    def _stage_and_suspend(self, ctx: ActivationContext, interrupts: Any) -> Suspend:
        """Stage one intent per pending item and build the resume-map snapshot."""
        pending: dict[str, dict[str, Any]] = {}
        for intr in interrupts:
            if isinstance(intr.value, dict) and TOOL_CALLS_MARKER in intr.value:
                for call in intr.value[TOOL_CALLS_MARKER]:
                    intent_id = ctx.act(call["name"], _canonical_json(call["args"]))
                    pending[intent_id] = {
                        "kind": _KIND_TOOL,
                        "interrupt_id": intr.id,
                        "tool_call_id": call["id"],
                    }
            else:
                intent_id = ctx.request_approval(_canonical_json(intr.value))
                pending[intent_id] = {"kind": _KIND_APPROVAL, "interrupt_id": intr.id}
        return self._suspend_with({"pending": pending, "results": {}})

    def _suspend_with(self, snapshot: dict[str, Any]) -> Suspend:
        return Suspend(
            snapshot=_canonical_json(snapshot).encode("utf-8"),
            adapter="langgraph",
            timeout_ms=self._hitl_timeout_ms,
        )

    def _record_incoming(self, ctx: ActivationContext, snapshot: dict[str, Any]) -> None:
        """Fold the re-injected Approval/ToolResult into the snapshot's results.

        A result for an intent this suspension never staged fails closed
        (invariant 6's spirit): silently dropping it would strand the graph.
        """
        pending = snapshot["pending"]
        if ctx.resume_approval is not None:
            approval = ctx.resume_approval
            if approval.intent_id not in pending:
                raise ValueError(
                    f"approval for unknown intent {approval.intent_id!r}; "
                    f"pending: {sorted(pending)}"
                )
            snapshot["results"][approval.intent_id] = {
                "kind": _KIND_APPROVAL,
                "approved": approval.approved,
                "approver": approval.approver,
                "decided_at_ms": approval.decided_at_ms,
            }
        elif ctx.resume_result is not None:
            result = ctx.resume_result
            if result.intent_id not in pending:
                raise ValueError(
                    f"tool result for unknown intent {result.intent_id!r}; "
                    f"pending: {sorted(pending)}"
                )
            snapshot["results"][result.intent_id] = {
                "kind": _KIND_TOOL,
                "status": ToolResult.Status.Name(result.status),
                "payload": result.payload.decode("utf-8", errors="replace"),
                "error_message": result.error_message,
            }

    def _resume_map(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """Build the per-interrupt resume values from the collected results.

        A plain approval interrupt resumes with the decision object; a shim
        interrupt resumes with ``{tool_call_id: result}`` covering every tool
        call it batched.
        """
        resume: dict[str, Any] = {}
        for intent_id, entry in snapshot["pending"].items():
            result = snapshot["results"][intent_id]
            if entry["kind"] == _KIND_APPROVAL:
                resume[entry["interrupt_id"]] = {
                    "approved": result["approved"],
                    "approver": result["approver"],
                    "decided_at_ms": result["decided_at_ms"],
                }
            else:
                per_call = resume.setdefault(entry["interrupt_id"], {})
                per_call[entry["tool_call_id"]] = {
                    "status": result["status"],
                    "payload": result["payload"],
                    "error_message": result["error_message"],
                }
        return resume
