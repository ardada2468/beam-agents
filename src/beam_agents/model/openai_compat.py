"""OpenAI-compatible (`/chat/completions`-shaped) `LLMClient` and its paired
`Decode`.

Covers OpenAI, vLLM's OpenAI server, and any compatible gateway by base URL
alone. Non-streaming, single-shot, byte-identical response passthrough — see
`model.anthropic` module docstring for the shared rationale (design D1/D2/D4);
this provider differs only in endpoint shape and auth header.

Importing this module has no side effects.
"""

from __future__ import annotations

import json

import httpx

from beam_agents.model._http import raise_for_status_taxonomy, wrap_timeout
from beam_agents.model.client import LlmRequest, LlmResponse, ProviderRequestError
from beam_agents.model.facade import DecodedResponse, TokenUsage

_DEFAULT_BASE_URL = "https://api.openai.com/v1"
_DEFAULT_TIMEOUT_S = 60.0


def decode(response_bytes: bytes) -> DecodedResponse:
    """Extract `TokenUsage` and response text from a chat-completions body."""
    body = _parse(response_bytes)
    usage = body.get("usage")
    if not isinstance(usage, dict):
        raise ValueError("OpenAI-compatible response missing 'usage' object")
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    if not isinstance(prompt_tokens, int) or not isinstance(completion_tokens, int):
        raise ValueError("OpenAI-compatible response 'usage' missing integer token counts")
    total_tokens = usage.get("total_tokens")
    if not isinstance(total_tokens, int):
        total_tokens = prompt_tokens + completion_tokens

    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("OpenAI-compatible response missing non-empty 'choices'")
    first = choices[0]
    if not isinstance(first, dict):
        raise ValueError("OpenAI-compatible response 'choices[0]' is not an object")
    message = first.get("message")
    if not isinstance(message, dict):
        raise ValueError("OpenAI-compatible response 'choices[0].message' missing")
    text = message.get("content")
    if not isinstance(text, str):
        raise ValueError("OpenAI-compatible response 'choices[0].message.content' missing")

    return DecodedResponse(
        usage=TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        ),
        text=text,
    )


def _parse(data: bytes) -> dict[str, object]:
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError("OpenAI-compatible response is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI-compatible response is not a JSON object")
    return parsed


class OpenAICompatProvider:
    """`LLMClient` for any `/chat/completions`-shaped endpoint.

    Base URL alone selects the target (OpenAI, a vLLM OpenAI server, or any
    compatible gateway); `LlmRequest` never carries an API key.
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _DEFAULT_BASE_URL,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(transport=self._transport, timeout=self._timeout_s)
        return self._client

    async def complete(self, request: LlmRequest) -> LlmResponse:
        client = self._ensure_client()
        body: dict[str, object] = {
            "model": request.model_id,
            "messages": request.messages,
            "stream": False,
        }
        if request.tools_schema is not None:
            body["tools"] = request.tools_schema
        if isinstance(request.sampling_params, dict):
            body.update(request.sampling_params)

        headers = {
            "authorization": f"Bearer {self._api_key}",
            "content-type": "application/json",
        }

        async with wrap_timeout():
            response = await client.post(
                f"{self._base_url}/chat/completions", json=body, headers=headers
            )

        raise_for_status_taxonomy(response)

        try:
            decode(response.content)
        except ValueError as exc:
            raise ProviderRequestError(status=response.status_code) from exc

        return LlmResponse(response.content)
