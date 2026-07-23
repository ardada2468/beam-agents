"""Protocol-conformance tests for the `model-client` capability.

Covers: a class defining `async def complete` structurally satisfies
`LLMClient` with no subclassing, and `complete` returns an awaitable.
"""

from __future__ import annotations

import inspect

from beam_agents.model import LLMClient, LlmRequest, LlmResponse

_REQUEST = LlmRequest(model_id="m-1", messages=[], tools_schema=[], sampling_params={})


class _ConformingClient:
    async def complete(self, request: LlmRequest) -> LlmResponse:
        return LlmResponse(b"ok")


# --- Requirement: Async LLMClient protocol -----------------------------------


def test_a_conforming_client_structurally_satisfies_the_protocol() -> None:
    # Scenario: A conforming client structurally satisfies the protocol.
    client: LLMClient = _ConformingClient()

    assert isinstance(client, LLMClient)


async def test_complete_is_a_coroutine() -> None:
    # Scenario: complete is a coroutine.
    client = _ConformingClient()

    awaitable = client.complete(_REQUEST)
    assert inspect.isawaitable(awaitable)

    response = await awaitable
    assert isinstance(response, LlmResponse)
