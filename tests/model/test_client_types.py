"""Value-type tests for the `model-client` capability.

Covers: `LlmRequest`/`LlmResponse` carry their fields, are immutable, and
`LlmResponse.response_digest` is the sha256 of `response`.
"""

from __future__ import annotations

import dataclasses
import hashlib

import pytest

from beam_agents.model import LlmRequest, LlmResponse

_MESSAGES = [{"role": "user", "content": "hi"}]
_TOOLS = [{"name": "lookup"}]
_OUTPUT_SCHEMA = {"type": "object", "properties": {"answer": {"type": "string"}}}
_PARAMS = {"temperature": 0.0}


# --- Requirement: LLM request value type -------------------------------------


def test_request_carries_the_request_material_components() -> None:
    # Scenario: Request carries all request-material components.
    request = LlmRequest(
        model_id="m-1",
        messages=_MESSAGES,
        tools_schema=_TOOLS,
        output_schema=_OUTPUT_SCHEMA,
        sampling_params=_PARAMS,
    )

    assert request.model_id == "m-1"
    assert request.messages == _MESSAGES
    assert request.tools_schema == _TOOLS
    assert request.output_schema == _OUTPUT_SCHEMA
    assert request.sampling_params == _PARAMS
    assert {f.name for f in dataclasses.fields(request)} == {
        "model_id",
        "messages",
        "tools_schema",
        "output_schema",
        "sampling_params",
    }


def test_request_is_immutable() -> None:
    # Scenario: Request is immutable.
    request = LlmRequest(
        model_id="m-1",
        messages=_MESSAGES,
        tools_schema=_TOOLS,
        output_schema=None,
        sampling_params=_PARAMS,
    )

    with pytest.raises(dataclasses.FrozenInstanceError):
        request.model_id = "m-2"  # type: ignore[misc]


# --- Requirement: LLM response value type ------------------------------------


def test_response_exposes_cacheable_bytes_digest_and_usage_metadata() -> None:
    # Scenario: Response exposes cacheable bytes, digest, and usage metadata.
    payload = b"the provider response bytes"
    response = LlmResponse(
        payload,
        input_tokens=11,
        output_tokens=7,
        total_tokens=18,
        from_replay_cache=False,
    )

    assert response.response == payload
    assert response.response_digest == hashlib.sha256(payload).digest()
    assert response.input_tokens == 11
    assert response.output_tokens == 7
    assert response.total_tokens == 18
    assert response.from_replay_cache is False


def test_response_is_immutable() -> None:
    # Scenario: Response is immutable.
    response = LlmResponse(b"payload")

    with pytest.raises(dataclasses.FrozenInstanceError):
        response.response = b"other"  # type: ignore[misc]
