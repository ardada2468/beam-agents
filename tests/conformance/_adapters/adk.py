"""The ADK conformance factory: each ``ScenarioSpec`` as an ADK agent.

The same construction as the LangGraph factory, in ADK's idiom: a ``BaseLlm``
whose ``generate_content_async`` posts provider-shaped JSON through an httpx
client the adapter's transport hook instruments (so every model call rides the
runtime's cache-first ``ctx.call_model`` path), ``beam_tools``-wrapped scenario
tools (read-only inline, side effects as long-running declarations), and the
approval shim for the approval scenarios. The scripted responses are the shared
directive vocabulary from ``_spec.turn_response`` — the same bytes the reference
provider serves.

ADK is imported lazily inside the build functions (never at module level): this
module must import cleanly in environments without the extra, where the
adapter's cells report as clean skips. Agents are built worker-side (the
module-level factory is referenced by name; nothing holding an httpx client is
ever pickled).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

# httpx is a core beam-agents dependency (unlike the ADK distributions, which
# stay lazily imported below).
import httpx

from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import Behavior, FakeLLM, Matcher, respond_with
from tests.conformance._spec import SCENARIOS_BY_NAME, ScenarioSpec, tool_for, turn_response

if TYPE_CHECKING:
    from beam_agents.adapters.adk import AdkAgent

_PROVIDER_URL = "https://provider.example/v1/chat"

#: The canonical terminal-output shape's segment separator (shared with the
#: reference and LangGraph factories).
_SEP = "|"


class _ApiClient:
    """The genai ``BaseApiClient`` layout the transport hook probes."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._async_httpx_client = httpx.AsyncClient(transport=transport)


class _GenaiClient:
    """The ``google.genai.Client`` layout the transport hook probes."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._api_client = _ApiClient(transport)


def _tripwire(request: httpx.Request) -> httpx.Response:
    raise AssertionError("the ADK model's own transport must never be reached")


def _directive_of(response: httpx.Response) -> dict[str, Any]:
    payload: dict[str, Any] = response.json()
    return payload


def build_adk_agent(spec: ScenarioSpec) -> AdkAgent:
    """Translate ``spec`` into an ``LlmAgent`` + shim tools + transport-routed
    model, wrapped as a fresh ``AdkAgent``. Imports ADK — call only where the
    extra is installed (worker-side, behind importorskip)."""
    from google.adk.agents import LlmAgent
    from google.adk.models.base_llm import BaseLlm
    from google.adk.models.llm_response import LlmResponse as AdkLlmResponse
    from google.genai import types

    from beam_agents.adapters.adk import AdkAgent, beam_tools
    from beam_agents.adapters.adk.tools import BeamApprovalTool

    class _ScenarioModel(BaseLlm):
        """Posts the scenario's provider-shaped request and turns the scripted
        directive into ADK content: an answer becomes text, a tool/act call
        becomes a function call, an approval becomes the shim's call."""

        api_client: Any = None

        async def generate_content_async(self, llm_request: Any, stream: bool = False) -> Any:
            response = await self.api_client._api_client._async_httpx_client.post(
                _PROVIDER_URL,
                json={
                    "model": spec.model_id,
                    "messages": _to_wire(llm_request),
                    "temperature": 0,
                },
            )
            directive = _directive_of(response)
            if "answer" in directive:
                yield AdkLlmResponse(
                    content=types.Content(
                        role="model", parts=[types.Part(text=directive["answer"])]
                    )
                )
                return
            call = directive.get("run_tool") or directive.get("act")
            if call is not None:
                yield AdkLlmResponse(
                    content=types.Content(
                        role="model",
                        parts=[
                            types.Part(
                                function_call=types.FunctionCall(
                                    name=call["name"], args=dict(call["args"])
                                )
                            )
                        ],
                    )
                )
                return
            approval = directive["request_approval"]
            yield AdkLlmResponse(
                content=types.Content(
                    role="model",
                    parts=[
                        types.Part(
                            function_call=types.FunctionCall(
                                name="request_approval", args=dict(approval["args"])
                            )
                        )
                    ],
                )
            )

    model = _ScenarioModel(
        model=spec.model_id, api_client=_GenaiClient(httpx.MockTransport(_tripwire))
    )
    tools: list[Any] = list(beam_tools([tool_for(t.name) for t in spec.tools]))
    if any(turn.directive == "request_approval" for turn in spec.turns):
        tools.append(BeamApprovalTool())

    return AdkAgent(
        LlmAgent(name="conformance", model=model, tools=tools),
        decode_event=_DecodeEvent(),
        encode_output=_EncodeOutput(spec.name),
        hitl_timeout_ms=spec.hitl_timeout_ms,
        chat_models=[model],
    )


def _to_wire(llm_request: Any) -> list[dict[str, str]]:
    """The ADK request's contents as the flat transcript the matcher counts.

    Only role/content pairs: the matcher keys on how many tool round-trips the
    conversation already carries, exactly like the LangGraph factory's.
    """
    wire: list[dict[str, str]] = []
    for content in llm_request.contents or []:
        for part in content.parts or []:
            if part.function_response is not None:
                wire.append({"role": "tool", "content": json.dumps(_response_of(part))})
            elif part.function_call is not None:
                wire.append({"role": "assistant", "content": part.function_call.name or ""})
            elif part.text:
                role = "user" if content.role == "user" else "assistant"
                wire.append({"role": role, "content": part.text})
    return wire


def _response_of(part: Any) -> Any:
    response = part.function_response.response
    return response if response is not None else {}


class _DecodeEvent:
    """Picklable event decode: raw bytes as one user-role text part."""

    def __call__(self, event: bytes) -> Any:
        from google.genai import types

        return types.Content(role="user", parts=[types.Part(text=event.decode() if event else "")])


class _EncodeOutput:
    """Picklable ``encode_output`` bound to one scenario (module-level class).

    Reconstructs the canonical terminal-output shape from the run's final text
    plus the committed session — ``seen=<n>`` (memory scenarios) | tool results
    | ``resumed:<payload>`` | the model's final answer, pipe-joined with empty
    segments omitted — the same segments the reference agent emits, so every
    adapter produces byte-identical terminals for the same conversation.
    """

    def __init__(self, scenario: str) -> None:
        self._scenario = scenario

    def __call__(self, result: Any) -> bytes:
        spec = SCENARIOS_BY_NAME[self._scenario]
        events = list(result.session.events) if result.session is not None else []
        readonly = {t.name for t in spec.tools if not t.side_effect}

        segments: list[str] = []
        if spec.uses_memory:
            # One user-authored *text* event per external element this key has
            # seen; working-memory TTL GC wipes the session, resetting the count
            # exactly as the reference agent's memory ring does.
            segments.append(f"seen={sum(1 for e in events if _is_user_text(e))}")

        inline: list[str] = []
        resumed: str | None = None
        for name, payload in _function_responses(events):
            if name in readonly:
                inline.append(str(payload.get("result", "")))
            else:
                resumed = str(payload.get("payload", payload.get("approved", "")))
        if inline:
            segments.append(",".join(inline))
        if resumed is not None:
            segments.append(f"resumed:{resumed}")
        if result.final_text:
            segments.append(result.final_text)
        return _SEP.join(segments).encode()


def _is_user_text(event: Any) -> bool:
    if event.author != "user" or event.content is None or not event.content.parts:
        return False
    return any(part.text for part in event.content.parts)


def _function_responses(events: list[Any]) -> list[tuple[str, dict[str, Any]]]:
    """(tool name, response payload) for every function response, in order."""
    found: list[tuple[str, dict[str, Any]]] = []
    for event in events:
        if event.content is None or not event.content.parts:
            continue
        for part in event.content.parts:
            response = part.function_response
            if response is not None:
                found.append((response.name or "", dict(response.response or {})))
    return found


# -- the scripted provider --------------------------------------------------------


def _match_turn(model_id: str, tool_messages: int) -> Matcher:
    """Turn *i* of a transport-routed ADK conversation carries exactly *i*
    tool-role messages (every completed prior turn contributed one)."""

    def matcher(request: LlmRequest) -> bool:
        if request.model_id != model_id or not isinstance(request.messages, list):
            return False
        count = sum(1 for m in request.messages if isinstance(m, dict) and m.get("role") == "tool")
        return count == tool_messages

    return matcher


def adk_rules(spec: ScenarioSpec) -> list[tuple[Matcher, Behavior]]:
    """One FakeLLM rule per scripted turn, scoped by the scenario's model_id."""
    return [
        (_match_turn(spec.model_id, index), respond_with(turn_response(turn)))
        for index, turn in enumerate(spec.turns)
    ]


def build_adk_provider(spec: ScenarioSpec) -> FakeLLM:
    return FakeLLM(adk_rules(spec))
