"""`AdkAgent`: run a Google ADK agent as a runtime `Agent`.

The adapter targets the runtime driver contract directly (change design D1): an
activation builds a :class:`~beam_agents.adapters.adk.session.BeamSessionService`
over the activation's staged working memory, constructs an ADK ``Runner`` around
the (never mutated) user agent, and drains ``run_async`` on the bridge event loop
under a per-key session (``user_id``/``session_id`` = entity key hex — per-key
serialization makes one live session per key safe by construction). The run's
terminal state maps to ``Complete``; its pending long-running tool calls map to a
single ``Suspend``.

One suspension covers ALL pending work (design D4). Each pending long-running
call stages an intent — ``ctx.act`` for a side-effect tool call, an approval for
the approval shim — and the ``Suspend`` snapshot carries only the correlation the
runtime doesn't track: ``intent_id → {kind, function_call_id, tool_name}`` plus
results already collected. Re-injected results accumulate (the activation
re-suspends, staging nothing new) until every pending intent is answered; the run
then resumes once, from the committed session, with one user-role message whose
parts are the function responses (each carrying its original ADK function-call
id and name), so ADK's model sees the tool round-trip exactly as if the tools had
answered inline.

The session itself is NOT in the snapshot — it is in working memory, committed
atomically with the bundle. Intent bytes come from ``ctx.act``'s step counter and
canonical-JSON arguments, so a replayed bundle stages byte-identical intents even
though ADK's own event ids and function-call ids are framework-generated.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from google.adk.runners import Runner
from google.adk.sessions.session import Session
from google.genai import types

from beam_agents._protos import ToolResult
from beam_agents.adapters.adk.events import ADAPTER_NAME, TraceTee, _current_tee
from beam_agents.adapters.adk.session import BeamSessionService
from beam_agents.adapters.adk.tools import (
    KIND_APPROVAL,
    KIND_TOOL,
    CallCollector,
    PendingCall,
    _current_collector,
)
from beam_agents.adapters.adk.transport import (
    _current_activation,
    install_transport,
    warn_fallback,
)
from beam_agents.core.agent import Complete, Outcome, Suspend

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from google.adk.agents.base_agent import BaseAgent
    from google.adk.events.event import Event

    from beam_agents.core.context import ActivationContext

#: The ADK ``app_name`` every adapter session lives under. An adapter constant:
#: session identity is the entity key, so the app name carries no information.
APP_NAME = "beam_agents"


@dataclass(frozen=True, slots=True)
class RunResult:
    """What one completed ADK run yields to ``encode_output``.

    ``final_text`` is the run's final response text — the default output. The
    committed ``session`` is carried alongside it so an encoder can project the
    richer terminal an application wants (tool results, state) out of committed
    state, without the adapter guessing a shape.
    """

    final_text: str
    session: Session | None


def _default_decode(event: bytes) -> types.Content:
    """Default event decode: the raw event bytes as one user-role text part."""
    return types.Content(role="user", parts=[types.Part(text=event.decode("utf-8"))])


def _default_encode(result: RunResult) -> bytes:
    """Default output encode: the run's final text, UTF-8."""
    return result.final_text.encode("utf-8")


def _canonical_json(value: object) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


class AdkAgent:
    """Wraps a Google ADK agent (an ``LlmAgent``/``BaseAgent`` tree) as a runtime
    agent. The user's agent object is never mutated: the per-activation session
    service lives on a freshly constructed ``Runner``.
    """

    def __init__(
        self,
        agent: BaseAgent,
        *,
        decode_event: Callable[[bytes], types.Content] = _default_decode,
        encode_output: Callable[[RunResult], bytes] = _default_encode,
        hitl_timeout_ms: int | None = None,
        chat_models: Sequence[object] = (),
        max_events: int | None = None,
    ) -> None:
        self._agent = agent
        self._decode_event = decode_event
        self._encode_output = encode_output
        self._hitl_timeout_ms = hitl_timeout_ms
        self._chat_models = tuple(chat_models)
        self._max_events = max_events
        # Instrumentation is deferred to the first activation so it happens on
        # the worker (after any submission-time pickling), and runs once per
        # agent instance — which makes the fallback warning once-per-instance.
        self._instrumented = False

    async def __call__(self, ctx: ActivationContext) -> Outcome:
        self._instrument_chat_models()
        runner = self._runner_for(ctx)

        if ctx.is_resume:
            snapshot = json.loads(ctx.snapshot) if ctx.snapshot else {"pending": {}, "results": {}}
            self._record_incoming(ctx, snapshot)
            unanswered = [i for i in snapshot["pending"] if i not in snapshot["results"]]
            if unanswered:
                # Accumulate: stage nothing new, keep waiting for the rest.
                return self._suspend_with(snapshot)
            message = self._function_response_message(snapshot)
        else:
            message = self._decode_event(ctx.event)

        collector = CallCollector()
        final_text, ordered_ids = await self._run(runner, ctx, message, collector)
        if collector.calls:
            return self._stage_and_suspend(ctx, collector, ordered_ids)
        session_id = ctx.entity_key.hex()
        session = await runner.session_service.get_session(
            app_name=APP_NAME, user_id=session_id, session_id=session_id
        )
        return Complete(self._encode_output(RunResult(final_text, session)))

    # -- internals -------------------------------------------------------------

    def _runner_for(self, ctx: ActivationContext) -> Runner:
        return Runner(
            agent=self._agent,
            app_name=APP_NAME,
            session_service=BeamSessionService(
                ctx.memory, now_ms=ctx.now_ms, max_events=self._max_events
            ),
            auto_create_session=True,
        )

    async def _run(
        self,
        runner: Runner,
        ctx: ActivationContext,
        message: types.Content,
        collector: CallCollector,
    ) -> tuple[str, list[str]]:
        """Drain the run with the activation exposed to the shim/transport/tee.

        Returns the run's final text and the function-call ids of every pending
        long-running call in **event-stream order** — the model's own ordering,
        which is deterministic given the (replay-cached) model responses, unlike
        the completion order of ADK's parallel tool execution.
        """
        session_id = ctx.entity_key.hex()
        final_text = ""
        ordered_ids: list[str] = []
        activation_token = _current_activation.set(ctx)
        collector_token = _current_collector.set(collector)
        tee_token = _current_tee.set(TraceTee(ctx))
        try:
            async for event in runner.run_async(
                user_id=session_id, session_id=session_id, new_message=message
            ):
                ordered_ids.extend(self._pending_ids(event))
                text = _event_text(event)
                if text:
                    final_text = text
        finally:
            _current_tee.reset(tee_token)
            _current_collector.reset(collector_token)
            _current_activation.reset(activation_token)
        return final_text, ordered_ids

    def _pending_ids(self, event: Event) -> list[str]:
        long_running = event.long_running_tool_ids or set()
        return [
            call.id
            for call in event.get_function_calls()
            if call.id is not None and call.id in long_running
        ]

    def _instrument_chat_models(self) -> None:
        if self._instrumented:
            return
        self._instrumented = True
        for model in self._chat_models:
            if not install_transport(model):
                warn_fallback(model)

    def _stage_and_suspend(
        self, ctx: ActivationContext, collector: CallCollector, ordered_ids: list[str]
    ) -> Suspend:
        """Stage one intent per pending call and build the resume-map snapshot."""
        pending: dict[str, dict[str, Any]] = {}
        # Event-stream order first (deterministic), then anything the stream did
        # not surface, by function-call id, so intent step indices are stable.
        seen: set[str] = set()
        ordered: list[PendingCall] = []
        for call_id in ordered_ids:
            call = collector.calls.get(call_id)
            if call is not None and call_id not in seen:
                seen.add(call_id)
                ordered.append(call)
        for call_id in sorted(collector.calls):
            if call_id not in seen:
                ordered.append(collector.calls[call_id])

        for call in ordered:
            if call.kind == KIND_APPROVAL:
                intent_id = ctx.request_approval(_canonical_json(call.args))
            else:
                intent_id = ctx.act(call.tool_name, _canonical_json(call.args))
            pending[intent_id] = {
                "kind": call.kind,
                "function_call_id": call.function_call_id,
                "tool_name": call.tool_name,
            }
        return self._suspend_with({"pending": pending, "results": {}})

    def _suspend_with(self, snapshot: dict[str, Any]) -> Suspend:
        return Suspend(
            snapshot=json.dumps(snapshot, sort_keys=True).encode("utf-8"),
            adapter=ADAPTER_NAME,
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
                "kind": KIND_APPROVAL,
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
                "kind": KIND_TOOL,
                "status": ToolResult.Status.Name(result.status),
                "payload": result.payload.decode("utf-8", errors="replace"),
                "error_message": result.error_message,
            }

    def _function_response_message(self, snapshot: dict[str, Any]) -> types.Content:
        """One user-role message whose parts are the collected function responses,
        each carrying its original ADK function-call identity."""
        parts: list[types.Part] = []
        for intent_id, entry in snapshot["pending"].items():
            result = snapshot["results"][intent_id]
            if entry["kind"] == KIND_APPROVAL:
                payload: dict[str, Any] = {
                    "approved": result["approved"],
                    "approver": result["approver"],
                    "decided_at_ms": result["decided_at_ms"],
                }
            else:
                payload = {
                    "status": result["status"],
                    "payload": result["payload"],
                    "error_message": result["error_message"],
                }
            parts.append(
                types.Part(
                    function_response=types.FunctionResponse(
                        id=entry["function_call_id"],
                        name=entry["tool_name"],
                        response=payload,
                    )
                )
            )
        return types.Content(role="user", parts=parts)


def _event_text(event: Event) -> str:
    """The event's concatenated text parts (empty when it carries none)."""
    if event.content is None or not event.content.parts:
        return ""
    return "".join(part.text for part in event.content.parts if part.text)
