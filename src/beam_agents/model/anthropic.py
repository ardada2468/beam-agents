"""Anthropic Messages `LLMClient` and its paired `Decode`.

Non-streaming, single-shot: one POST per `complete` call, the raw response
body returned verbatim as `LlmResponse.response` (design D1 — the cache
payload must be byte-identical to what the provider sent, so no vendor SDK
and no re-serialization). HTTP outcomes route through the shared
`model._http` taxonomy mapper (design D2). The `httpx.AsyncClient` is built
lazily on first `complete`, on the bridge loop it will run on (design D4).

Importing this module has no side effects.
"""

from __future__ import annotations

import json

import httpx

from beam_agents.model._http import raise_for_status_taxonomy, wrap_timeout
from beam_agents.model.client import LlmRequest, LlmResponse, ProviderRequestError
from beam_agents.model.facade import DecodedResponse, TokenUsage

__all__ = [
    "AnthropicProvider",
]

_DEFAULT_BASE_URL = "https://api.anthropic.com"
_DEFAULT_ANTHROPIC_VERSION = "2023-06-01"
_DEFAULT_TIMEOUT_S = 60.0


def _decode(response_bytes: bytes) -> DecodedResponse:
    """Extract `TokenUsage` and response text from an Anthropic Messages body."""
    body = _parse(response_bytes)
    usage = body.get("usage")
    if not isinstance(usage, dict):
        raise ValueError("Anthropic response missing 'usage' object")
    prompt_tokens = usage.get("input_tokens")
    completion_tokens = usage.get("output_tokens")
    if not isinstance(prompt_tokens, int) or not isinstance(completion_tokens, int):
        raise ValueError("Anthropic response 'usage' missing integer token counts")

    content = body.get("content")
    if not isinstance(content, list) or not content:
        raise ValueError("Anthropic response missing non-empty 'content'")
    text_blocks = (
        block for block in content if isinstance(block, dict) and block.get("type") == "text"
    )
    text = "".join(block.get("text", "") for block in text_blocks)

    return DecodedResponse(
        usage=TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        ),
        text=text,
    )


def _parse(data: bytes) -> dict[str, object]:
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError("Anthropic response is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Anthropic response is not a JSON object")
    return parsed


class AnthropicProvider:
    """`LLMClient` for the Anthropic Messages API.

    Constructed with its own credentials/endpoint; `LlmRequest` never carries
    an API key (correctness invariant: request material stays cache-clean and
    credential-free).
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = _DEFAULT_BASE_URL,
        anthropic_version: str = _DEFAULT_ANTHROPIC_VERSION,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._anthropic_version = anthropic_version
        self._timeout_s = timeout_s
        self._transport = transport
        self._client: httpx.AsyncClient | None = None

    def _ensure_client(self) -> httpx.AsyncClient:
        """Lazily build the shared client on first use (design D4): binding it
        here, on the loop that is actually running the coroutine, avoids the
        setup()/bridge-thread loop-affinity hazard of building it eagerly.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(transport=self._transport, timeout=self._timeout_s)
        return self._client

    async def complete(self, request: LlmRequest) -> LlmResponse:
        """POST one Messages-API request and return the raw response bytes.

        HTTP status is mapped to the :class:`ProviderError` taxonomy and
        ``httpx`` timeouts to :class:`ProviderTimeout`; no retry happens
        here.
        """
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
            "x-api-key": self._api_key,
            "anthropic-version": self._anthropic_version,
            "content-type": "application/json",
        }

        async with wrap_timeout():
            response = await client.post(
                f"{self._base_url}/v1/messages", json=body, headers=headers
            )

        raise_for_status_taxonomy(response)

        try:
            _decode(response.content)
        except ValueError as exc:
            raise ProviderRequestError(status=response.status_code) from exc

        return LlmResponse(response.content)
