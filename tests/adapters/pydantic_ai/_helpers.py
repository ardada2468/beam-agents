"""Shared fixtures for the Pydantic AI adapter tests.

The model double is shaped like the framework's real provider models: a
``client`` property holding an SDK object whose ``_client`` is an
``httpx.AsyncClient`` (the anthropic/openai layout the adapter's probing table
recognizes). Its ``request`` posts provider-shaped JSON through that client, so
every model call rides the adapter's transport hook into
``ActivationContext.call_model`` and is answered by a scripted FakeLLM with the
shared directive vocabulary (``answer`` / ``run_tool`` / ``act`` /
``request_approval``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models import Model
from pydantic_ai.usage import RequestUsage

from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import Behavior, FakeLLM, Matcher, respond_with

if TYPE_CHECKING:
    from beam_agents._protos import AgentEnvelope, LlmCacheBlob, MemoryBlob, ToolResult
    from beam_agents.model.client import LLMClient
    from beam_agents.tools.registry import ToolRegistry

NOW_MS = 1_700_000_000_000
ENTITY_KEY = b"entity-1"
PROVIDER_URL = "https://provider.example/v1/chat"

#: Tokens every scripted model turn reports, so a completed run's tally is
#: non-zero and predictable.
TURN_INPUT_TOKENS = 3
TURN_OUTPUT_TOKENS = 5


def make_ctx(
    *,
    event: bytes = b"go",
    seq: int = 1,
    provider: LLMClient | None = None,
    memory_blob: MemoryBlob | None = None,
    cache_blob: LlmCacheBlob | None = None,
    resume_result: ToolResult | None = None,
    resume_approval: AgentEnvelope.Approval | None = None,
    snapshot: bytes = b"",
    step_index: int = 0,
    now_ms: int = NOW_MS,
    tool_registry: ToolRegistry | None = None,
) -> ActivationContext:
    return ActivationContext(
        entity_key=ENTITY_KEY,
        seq=seq,
        now_ms=now_ms,
        provider=provider if provider is not None else FakeLLM(),
        memory_blob=memory_blob,
        cache_blob=cache_blob,
        event=event,
        resume_result=resume_result,
        resume_approval=resume_approval,
        snapshot=snapshot,
        step_index=step_index,
        tool_registry=tool_registry,
    )


class SdkClient:
    """Shaped like ``openai.AsyncOpenAI`` / ``anthropic.AsyncAnthropic``: the SDK
    object whose ``_client`` is the httpx client the adapter must rewire."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._client = httpx.AsyncClient(transport=transport)


def tripwire(hits: list[httpx.Request] | None = None) -> httpx.MockTransport:
    """A transport the replay hook must displace; records anything that slips."""

    def handler(request: httpx.Request) -> httpx.Response:
        if hits is None:
            raise AssertionError("the model's own transport must never be reached")
        hits.append(request)
        return httpx.Response(200, json={"answer": "from-upstream"})

    return httpx.MockTransport(handler)


class RecognizedModel(Model):
    """A Pydantic AI model whose SDK client the adapter recognizes."""

    def __init__(self, model_id: str, transport: httpx.AsyncBaseTransport) -> None:
        super().__init__()
        self._model_id = model_id
        self._sdk = SdkClient(transport)
        self.turn_calls = 0

    @property
    def client(self) -> SdkClient:
        return self._sdk

    @property
    def model_name(self) -> str:
        return self._model_id

    @property
    def system(self) -> str:
        return "beam-agents-test"

    async def request(
        self, messages: Any, model_settings: Any, model_request_parameters: Any
    ) -> ModelResponse:
        self.turn_calls += 1
        response = await self._sdk._client.post(
            PROVIDER_URL,
            json={"model": self._model_id, "messages": to_wire(messages), "temperature": 0},
        )
        return build_response(response.json(), len(messages))


class UnrecognizedModel(Model):
    """No httpx client at any attribute path the adapter recognizes."""

    def __init__(self, model_id: str, transport: httpx.AsyncBaseTransport) -> None:
        super().__init__()
        self._model_id = model_id
        self._hidden = httpx.AsyncClient(transport=transport)

    @property
    def model_name(self) -> str:
        return self._model_id

    @property
    def system(self) -> str:
        return "beam-agents-test"

    async def request(
        self, messages: Any, model_settings: Any, model_request_parameters: Any
    ) -> ModelResponse:
        response = await self._hidden.post(
            PROVIDER_URL,
            json={"model": self._model_id, "messages": to_wire(messages), "temperature": 0},
        )
        return build_response(response.json(), len(messages))


def to_wire(messages: Any) -> list[dict[str, str]]:
    """The conversation as flat provider-shaped JSON: one entry per message
    part, tagged with the part kind (the matcher's turn signal)."""
    wire: list[dict[str, str]] = []
    for message in messages:
        for part in message.parts:
            content = getattr(part, "content", None)
            if content is None:
                content = getattr(part, "args", "")
            wire.append({"role": str(part.part_kind), "content": str(content)})
    return wire


def build_response(directive: dict[str, Any], position: int) -> ModelResponse:
    """One scripted turn's directive as a framework ``ModelResponse``."""
    parts: list[Any]
    if "answer" in directive:
        parts = [TextPart(content=directive["answer"])]
    else:
        call = directive.get("run_tool") or directive.get("act") or directive["request_approval"]
        calls = call if isinstance(call, list) else [call]
        parts = [
            ToolCallPart(
                tool_name=item["name"],
                args=item["args"],
                tool_call_id=f"call-{position}-{index}",
            )
            for index, item in enumerate(calls)
        ]
    return ModelResponse(
        parts=parts,
        usage=RequestUsage(input_tokens=TURN_INPUT_TOKENS, output_tokens=TURN_OUTPUT_TOKENS),
        model_name="beam-agents-test",
    )


def match_turn(model_id: str, tool_returns: int) -> Matcher:
    """Turn *i* of a conversation carries exactly *i* tool-return parts (every
    answered tool call contributed one)."""

    def matcher(request: LlmRequest) -> bool:
        if request.model_id != model_id or not isinstance(request.messages, list):
            return False
        count = sum(
            1 for m in request.messages if isinstance(m, dict) and m.get("role") == "tool-return"
        )
        return count == tool_returns

    return matcher


def scripted(model_id: str, directives: list[bytes]) -> FakeLLM:
    """One FakeLLM rule per scripted turn, scoped by ``model_id``."""
    rules: list[tuple[Matcher, Behavior]] = [
        (match_turn(model_id, index), respond_with(payload))
        for index, payload in enumerate(directives)
    ]
    return FakeLLM(rules)
