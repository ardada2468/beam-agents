"""The Pydantic AI conformance factory: each ``ScenarioSpec`` as a
``pydantic_ai.Agent``.

Mirrors the LangGraph factory's shape: a model object whose httpx client the
adapter's transport hook instruments (so every model call rides the runtime's
cache-first ``ctx.call_model`` path), ``BeamToolset`` for tools (read-only
inline through ``ctx.run_tool``, side effects deferred to intents), and an
approval-gated tool for the ``request_approval`` turns. The scripted responses
are the shared directive vocabulary from ``_spec.turn_response`` — the same
bytes the reference provider serves.

Design D8's model-object choice, resolved: a **minimal custom ``Model``
subclass** posting provider-shaped JSON through an instrumented httpx client
(the LangGraph factory's ``_ChatModel`` double pattern). A provider-flavored
model would pull a provider SDK (``openai``/``anthropic``) into the test group
for no additional scenario coverage; recognition against the real SDK layout is
covered by the layout doubles in ``tests/adapters/pydantic_ai``. The choice is
invisible to the scenario bodies.

Pydantic AI is imported lazily inside the build functions (never at module
level): this module must import cleanly in environments without the extra,
where the adapter's cells report as clean skips. Agents are built worker-side
(the module-level factory is referenced by name; nothing holding an httpx
client is ever pickled).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# httpx is a core beam-agents dependency (unlike the Pydantic AI distribution,
# which stays lazily imported below).
import httpx

from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import Behavior, FakeLLM, Matcher, respond_with
from tests.conformance._spec import SCENARIOS_BY_NAME, ScenarioSpec, tool_for, turn_response

if TYPE_CHECKING:
    from beam_agents.adapters.pydantic_ai import PydanticAIAgent

_PROVIDER_URL = "https://provider.example/v1/chat"

#: The read-only tool the approval-gated scenarios call. Runtime tools are
#: module-level so they pickle by reference; the approval scenarios' spec
#: declares no tools, so this one is registered only on the framework side
#: (the runtime registry stays exactly the spec's tool set, per
#: ``validate_bundle``), and its execution is a no-op marker.
_APPROVAL_TOOL_NAME = "approve"

#: Pydantic AI always closes a run through the model, so a scenario whose
#: script ends at a deferred call still needs one terminal model turn after the
#: result re-enters — and that turn must add ZERO provider calls (the
#: bundle_retry_cache cell asserts the exact count). The scripted model
#: therefore answers locally, without posting, once the script's turns are
#: exhausted; it says this sentinel, which ``encode_output`` drops, because the
#: framework rejects an empty text output.
_SILENT = "\x00silent\x00"


def _turn_index(messages: Any) -> int:
    """Which scripted turn this request is: turn *i* carries exactly *i*
    tool-return parts (every answered tool call contributed one)."""
    return sum(
        1 for message in messages for part in message.parts if str(part.part_kind) == "tool-return"
    )


class _SdkClient:
    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._client = httpx.AsyncClient(transport=transport)


def _tripwire(request: httpx.Request) -> httpx.Response:
    raise AssertionError("the model's own transport must never be reached")


def _to_wire(messages: Any) -> list[dict[str, str]]:
    wire: list[dict[str, str]] = []
    for message in messages:
        for part in message.parts:
            content = getattr(part, "content", None)
            if content is None:
                content = getattr(part, "args", "")
            wire.append({"role": str(part.part_kind), "content": str(content)})
    return wire


def encode_output(scenario: str, result: Any) -> bytes:
    """Reconstruct the canonical terminal-output shape from the finished run —
    the same segments the reference agent emits, so all adapters produce
    byte-identical terminals for the same conversation."""
    spec = SCENARIOS_BY_NAME[scenario]
    messages = result.all_messages()
    readonly = {t.name for t in spec.tools if not t.side_effect}
    segments: list[str] = []
    user_prompts = 0
    inline_values: list[str] = []
    deferred_values: list[str] = []
    for message in messages:
        for part in message.parts:
            kind = str(part.part_kind)
            if kind == "user-prompt":
                user_prompts += 1
            elif kind == "tool-return":
                value = str(part.content)
                if part.tool_name in readonly:
                    inline_values.append(value)
                else:
                    deferred_values.append(value)
    if spec.uses_memory:
        segments.append(f"seen={user_prompts}")
    if inline_values:
        segments.append(",".join(inline_values))
    if deferred_values:
        segments.append(f"resumed:{deferred_values[-1]}")
    answer = str(result.output)
    if answer and answer != _SILENT:
        segments.append(answer)
    return "|".join(segments).encode()


class _EncodeOutput:
    """Picklable ``encode_output`` bound to one scenario (module-level class)."""

    def __init__(self, scenario: str) -> None:
        self._scenario = scenario

    def __call__(self, result: Any) -> bytes:
        return encode_output(self._scenario, result)


def build_pydantic_ai_agent(spec: ScenarioSpec) -> PydanticAIAgent:
    """Translate ``spec`` into a Pydantic AI agent + BeamToolset + transport-routed
    model, wrapped as a fresh ``PydanticAIAgent``. Imports Pydantic AI — call
    only where the extra is installed (worker-side, behind importorskip)."""
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
    from pydantic_ai.models import Model
    from pydantic_ai.usage import RequestUsage

    from beam_agents.adapters.pydantic_ai import PydanticAIAgent
    from beam_agents.tools import tool

    sdk = _SdkClient(httpx.MockTransport(_tripwire))

    @tool(name=_APPROVAL_TOOL_NAME)
    def approve(amount: str) -> str:
        """Approval-gated read-only tool: the approval scenarios' call target."""
        return f"approved:{amount}"

    class _ScriptedModel(Model):
        """Minimal provider-shaped model the adapter's probing recognizes:
        ``client._client`` is the httpx client the replay transport rewires."""

        @property
        def client(self) -> _SdkClient:
            return sdk

        @property
        def model_name(self) -> str:
            return spec.model_id

        @property
        def system(self) -> str:
            return "conformance"

        async def request(
            self, messages: Any, model_settings: Any, model_request_parameters: Any
        ) -> ModelResponse:
            if _turn_index(messages) >= len(spec.turns):
                # Script exhausted: close the run without touching the provider.
                return ModelResponse(
                    parts=[TextPart(content=_SILENT)],
                    usage=RequestUsage(input_tokens=1, output_tokens=1),
                    model_name=spec.model_id,
                )
            response = await sdk._client.post(
                _PROVIDER_URL,
                json={
                    "model": spec.model_id,
                    "messages": _to_wire(messages),
                    "temperature": 0,
                },
            )
            directive = response.json()
            parts: list[Any]
            if "answer" in directive:
                parts = [TextPart(content=directive["answer"])]
            else:
                call = directive.get("run_tool") or directive.get("act")
                if call is not None:
                    name, args = call["name"], call["args"]
                else:
                    name, args = _APPROVAL_TOOL_NAME, directive["request_approval"]["args"]
                parts = [
                    ToolCallPart(tool_name=name, args=args, tool_call_id=f"call-{len(messages)}")
                ]
            return ModelResponse(
                parts=parts,
                usage=RequestUsage(input_tokens=1, output_tokens=1),
                model_name=spec.model_id,
            )

    tools = [tool_for(t.name) for t in spec.tools]
    approval_required: list[str] = []
    if any(turn.directive == "request_approval" for turn in spec.turns):
        tools = [*tools, approve]
        approval_required = [_APPROVAL_TOOL_NAME]

    return PydanticAIAgent(
        Agent(_ScriptedModel()),
        tools=tools,
        approval_required=approval_required,
        encode_output=_EncodeOutput(spec.name),
        hitl_timeout_ms=spec.hitl_timeout_ms,
    )


# -- the scripted provider --------------------------------------------------------


def _match_turn(model_id: str, tool_returns: int) -> Matcher:
    """Turn *i* of a transport-routed conversation carries exactly *i*
    tool-return parts (every answered tool call contributed one)."""

    def matcher(request: LlmRequest) -> bool:
        if request.model_id != model_id or not isinstance(request.messages, list):
            return False
        count = sum(
            1 for m in request.messages if isinstance(m, dict) and m.get("role") == "tool-return"
        )
        return count == tool_returns

    return matcher


def pydantic_ai_rules(spec: ScenarioSpec) -> list[tuple[Matcher, Behavior]]:
    """One FakeLLM rule per scripted turn, scoped by the scenario's model_id."""
    return [
        (_match_turn(spec.model_id, index), respond_with(turn_response(turn)))
        for index, turn in enumerate(spec.turns)
    ]


def build_pydantic_ai_provider(spec: ScenarioSpec) -> FakeLLM:
    return FakeLLM(pydantic_ai_rules(spec))
