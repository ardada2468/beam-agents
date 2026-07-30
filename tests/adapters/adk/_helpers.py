"""Shared fixtures for the ADK adapter tests: a scripted ADK model and agents.

The scripted model is a ``BaseLlm`` whose responses are driven by a directive
list — one entry per model turn — so a test declares a conversation rather than
a transport script. The transport-routed variant (used by the replay-cache
tests) instead posts provider-shaped JSON through an httpx client the adapter's
transport hook instruments, exactly like the conformance factory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx
from google.adk.agents import LlmAgent
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse as AdkLlmResponse
from google.genai import types
from pydantic import Field

from beam_agents.adapters.adk import AdkAgent, beam_tools
from beam_agents.adapters.adk.tools import BeamApprovalTool

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from google.adk.models.llm_request import LlmRequest as AdkLlmRequest

    from beam_agents.tools.registry import Tool

PROVIDER_URL = "https://provider.example/v1/chat"


def text_turn(text: str) -> types.Content:
    return types.Content(role="model", parts=[types.Part(text=text)])


def call_turn(*calls: tuple[str, dict[str, Any]]) -> types.Content:
    return types.Content(
        role="model",
        parts=[
            types.Part(function_call=types.FunctionCall(name=name, args=args))
            for name, args in calls
        ],
    )


class ScriptedLlm(BaseLlm):
    """Serves the next scripted turn per *distinct* request shape.

    Turn selection is by the number of function responses already in the
    request's contents, so the same script drives an activation and its replay
    identically (no hidden call counter that a bundle retry would advance).
    """

    turns: list[types.Content] = Field(default_factory=list)
    calls: list[int] = Field(default_factory=list)

    async def generate_content_async(
        self, llm_request: AdkLlmRequest, stream: bool = False
    ) -> AsyncGenerator[AdkLlmResponse, None]:
        index = _turn_index(llm_request)
        self.calls.append(index)
        yield AdkLlmResponse(content=self.turns[min(index, len(self.turns) - 1)])


def _turn_index(llm_request: AdkLlmRequest) -> int:
    responses = 0
    for content in llm_request.contents or []:
        for part in content.parts or []:
            if part.function_response is not None:
                responses += 1
    return responses


def scripted_agent(
    turns: Sequence[types.Content],
    tools: Sequence[Tool] = (),
    *,
    approval: bool = False,
    name: str = "probe",
) -> tuple[LlmAgent, ScriptedLlm]:
    """An ``LlmAgent`` over the scripted model, with shim-wrapped tools."""
    model = ScriptedLlm(model="scripted", turns=list(turns), calls=[])
    adk_tools: list[Any] = list(beam_tools(list(tools)))
    if approval:
        adk_tools.append(BeamApprovalTool())
    return LlmAgent(name=name, model=model, tools=adk_tools), model


def scripted_adk_agent(
    turns: Sequence[types.Content],
    tools: Sequence[Tool] = (),
    *,
    approval: bool = False,
    hitl_timeout_ms: int | None = None,
) -> tuple[AdkAgent, ScriptedLlm]:
    agent, model = scripted_agent(turns, tools, approval=approval)
    return AdkAgent(agent, hitl_timeout_ms=hitl_timeout_ms), model


# -- the transport-routed model seam ---------------------------------------------


class SdkClient:
    """Shaped like ``google.genai.Client``: ``_api_client._async_httpx_client``
    is the ``httpx.AsyncClient`` the adapter must rewire."""

    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._api_client = _ApiClient(transport)


class _ApiClient:
    def __init__(self, transport: httpx.AsyncBaseTransport) -> None:
        self._async_httpx_client = httpx.AsyncClient(transport=transport)


class RecognizedGenaiModel(BaseLlm):
    """Shaped like ``google.adk.models.Gemini``: exposes ``api_client``."""

    api_client: Any = None

    async def generate_content_async(
        self, llm_request: AdkLlmRequest, stream: bool = False
    ) -> AsyncGenerator[AdkLlmResponse, None]:
        response = await self.api_client._api_client._async_httpx_client.post(
            PROVIDER_URL,
            json={
                "model": self.model,
                "messages": [{"role": "user", "content": _flatten(llm_request)}],
                "temperature": 0,
            },
        )
        yield AdkLlmResponse(content=text_turn(response.json()["answer"]))


class UnrecognizedModel(BaseLlm):
    """No httpx client at any attribute path the adapter recognizes."""

    hidden: Any = None

    async def generate_content_async(
        self, llm_request: AdkLlmRequest, stream: bool = False
    ) -> AsyncGenerator[AdkLlmResponse, None]:
        response = await self.hidden.post(PROVIDER_URL, json={"model": self.model})
        yield AdkLlmResponse(content=text_turn(response.json()["answer"]))


def _flatten(llm_request: AdkLlmRequest) -> str:
    chunks: list[str] = []
    for content in llm_request.contents or []:
        for part in content.parts or []:
            if part.text:
                chunks.append(part.text)
    return "|".join(chunks)
