"""`PydanticAIAgent`: run a Pydantic AI agent as a runtime `Agent`.

The adapter targets the runtime driver contract directly (change design D1):
an activation loads the committed message history (design D2), decodes the
event into the run's user prompt, invokes ``Agent.run`` on the bridge event
loop with the activation exposed to the shared transport contextvar (D5), and
persists ``all_messages()`` back through the ``Memory`` facade before mapping
the run's end state to ``Complete`` or ``Suspend``.

Suspension (design D3): the run ends cleanly at deferred tool calls — a
``DeferredToolRequests`` output — and the adapter stages one intent per
pending call (``ctx.act`` for external execution, ``ctx.request_approval``
for approval gates; deterministic step-indexed IDs, correctness invariants
2/5) and returns a single ``Suspend``. The snapshot carries only the
correlation the runtime doesn't track: ``intent_id → {kind, tool_call_id}``
plus results already collected; history is NOT in the snapshot — it committed
with the suspension. Re-injected results accumulate (the activation
re-suspends, staging nothing new) until every pending intent is answered; the
adapter then re-runs the agent with the committed history plus the built
``DeferredToolResults``. Unlike LangGraph there is no interrupted-node
re-execution: resumption is a fresh run seeded with history + results.

Output-type contract (design D1/the open question's resolution): every run
passes ``output_type=[wrapped_agent.output_type, DeferredToolRequests]`` as a
per-run override, so the user's agent is wrapped as-is — its own declared
output type keeps validating terminal outputs, and the deferred-requests
union member is the adapter's concern, never the author's.

Usage (design D6): each run segment's reported usage folds into the
activation tally via ``ctx.accumulate_usage``. On a resumed activation whose
model turns were replay-cache hits the framework still parses those response
bytes, so the tally reflects tokens *processed* this activation; the billed
signal lives in the per-call trace attributes, unchanged by the adapter.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from pydantic_ai import Agent as PydanticAgent
from pydantic_ai import DeferredToolRequests, DeferredToolResults, ToolApproved, ToolDenied

from beam_agents._protos import ToolResult
from beam_agents.adapters._transport import _current_activation
from beam_agents.adapters.pydantic_ai.history import load_history, save_history
from beam_agents.adapters.pydantic_ai.toolset import BeamToolset
from beam_agents.adapters.pydantic_ai.transport import install_transport, warn_fallback
from beam_agents.core.agent import Complete, Outcome, Suspend
from beam_agents.model.facade import TokenUsage

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence

    from pydantic_ai.run import AgentRunResult

    from beam_agents.core.context import ActivationContext
    from beam_agents.tools.registry import Tool

_KIND_APPROVAL = "approval"
_KIND_TOOL = "tool"


def _default_decode(event: bytes) -> str:
    """Default event decode: the raw bytes as a UTF-8 user prompt."""
    return event.decode("utf-8")


def _default_encode(result: AgentRunResult[Any]) -> bytes:
    """Default output encode: a `str` output as UTF-8; anything else as
    deterministic JSON (non-JSON values via `str`)."""
    output = result.output
    if isinstance(output, str):
        return output.encode("utf-8")
    return json.dumps(output, default=str, sort_keys=True).encode("utf-8")


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class PydanticAIAgent:
    """Wraps a user-constructed ``pydantic_ai.Agent`` as a runtime agent.

    The wrapped agent is never mutated: the adapter's toolset and output-type
    union are per-run arguments, and transport instrumentation touches only
    the model object's httpx client. ``tools`` are runtime ``@tool`` objects
    (re-tag side-effectful tools with ``side_effect=True`` and list
    approval-gated ones in ``approval_required`` — that is the whole adoption
    cost). ``models`` may name additional model objects to instrument (e.g.
    delegated sub-agents' models); the wrapped agent's own model is always
    probed.
    """

    def __init__(
        self,
        agent: PydanticAgent[Any, Any],
        *,
        tools: Sequence[Tool] = (),
        approval_required: Collection[str] = (),
        decode_event: Callable[[bytes], str] = _default_decode,
        encode_output: Callable[[AgentRunResult[Any]], bytes] = _default_encode,
        hitl_timeout_ms: int | None = None,
        models: Sequence[object] = (),
    ) -> None:
        self._agent = agent
        if approval_required and not tools:
            raise ValueError("approval_required given without any tools")
        self._toolsets: tuple[BeamToolset, ...] = (
            (BeamToolset(tools, approval_required=approval_required),) if tools else ()
        )
        self._decode_event = decode_event
        self._encode_output = encode_output
        self._hitl_timeout_ms = hitl_timeout_ms
        self._models = tuple(models)
        # Instrumentation is deferred to the first activation so it happens on
        # the worker (after any submission-time pickling), and runs once per
        # agent instance — which makes the fallback warning once-per-instance.
        self._instrumented = False

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        self._instrument_models()
        history = load_history(ctx.memory)

        if ctx.is_resume:
            snapshot = json.loads(ctx.snapshot) if ctx.snapshot else {"pending": {}, "results": {}}
            self._record_incoming(ctx, snapshot)
            unanswered = [i for i in snapshot["pending"] if i not in snapshot["results"]]
            if unanswered:
                # Accumulate: stage nothing new, keep waiting for the rest.
                return self._suspend_with(snapshot)
            result = await self._run(
                ctx,
                user_prompt=None,
                message_history=history,
                deferred_tool_results=self._deferred_results(snapshot),
            )
        else:
            result = await self._run(
                ctx,
                user_prompt=self._decode_event(ctx.event),
                message_history=history,
                deferred_tool_results=None,
            )

        save_history(ctx.memory, result.all_messages())
        usage = result.usage
        ctx.accumulate_usage(
            TokenUsage(
                prompt_tokens=usage.input_tokens,
                completion_tokens=usage.output_tokens,
                total_tokens=usage.total_tokens,
            )
        )
        output = result.output
        if isinstance(output, DeferredToolRequests):
            return self._stage_and_suspend(ctx, output)
        return Complete(self._encode_output(result))

    # -- internals -------------------------------------------------------------

    async def _run(
        self,
        ctx: ActivationContext,
        *,
        user_prompt: str | None,
        message_history: Sequence[Any],
        deferred_tool_results: DeferredToolResults | None,
    ) -> AgentRunResult[Any]:
        """One `Agent.run` segment with the activation exposed to the transport
        hook and the tool executors."""
        token = _current_activation.set(ctx)
        try:
            return await self._agent.run(
                user_prompt,
                message_history=list(message_history) if message_history else None,
                deferred_tool_results=deferred_tool_results,
                output_type=[self._agent.output_type, DeferredToolRequests],
                toolsets=self._toolsets if self._toolsets else None,
            )
        finally:
            _current_activation.reset(token)

    def _instrument_models(self) -> None:
        if self._instrumented:
            return
        self._instrumented = True
        candidates: list[object] = []
        if self._agent.model is not None:
            candidates.append(self._agent.model)
        candidates.extend(self._models)
        for model in candidates:
            if not install_transport(model):
                warn_fallback(model)

    def _stage_and_suspend(self, ctx: ActivationContext, requests: DeferredToolRequests) -> Suspend:
        """Stage one intent per pending deferred call and build the snapshot."""
        pending: dict[str, dict[str, str]] = {}
        for call in requests.calls:
            intent_id = ctx.act(call.tool_name, _canonical_json(call.args_as_dict()))
            pending[intent_id] = {"kind": _KIND_TOOL, "tool_call_id": call.tool_call_id}
        for call in requests.approvals:
            intent_id = ctx.request_approval(_canonical_json(call.args_as_dict()))
            pending[intent_id] = {"kind": _KIND_APPROVAL, "tool_call_id": call.tool_call_id}
        return self._suspend_with({"pending": pending, "results": {}})

    def _suspend_with(self, snapshot: dict[str, Any]) -> Suspend:
        return Suspend(
            snapshot=json.dumps(snapshot, sort_keys=True).encode("utf-8"),
            adapter="pydantic_ai",
            timeout_ms=self._hitl_timeout_ms,
        )

    def _record_incoming(self, ctx: ActivationContext, snapshot: dict[str, Any]) -> None:
        """Fold the re-injected Approval/ToolResult into the snapshot's results.

        A result for an intent this suspension never staged fails closed
        (invariant 6's spirit): silently dropping it would strand the run.
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

    def _deferred_results(self, snapshot: dict[str, Any]) -> DeferredToolResults:
        """The framework's deferred-results value from the collected results:
        tool results keyed by their original tool call IDs, approvals as
        approved/denied decisions."""
        results = DeferredToolResults()
        for intent_id, entry in snapshot["pending"].items():
            collected = snapshot["results"][intent_id]
            call_id = entry["tool_call_id"]
            if entry["kind"] == _KIND_APPROVAL:
                results.approvals[call_id] = (
                    ToolApproved() if collected["approved"] else ToolDenied()
                )
            elif collected["status"] == ToolResult.Status.Name(ToolResult.OK):
                results.calls[call_id] = collected["payload"]
            else:
                # The model sees the failure verbatim, as the shim adapters do.
                results.calls[call_id] = json.dumps(
                    {
                        "status": collected["status"],
                        "payload": collected["payload"],
                        "error_message": collected["error_message"],
                    },
                    sort_keys=True,
                )
        return results
