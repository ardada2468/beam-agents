"""Matching tests for the `fake-llm` capability.

Covers: FakeLLM satisfies `LLMClient`, first-match-wins, match-by-`model_id`,
and scripted response bytes are returned verbatim with the correct digest.
"""

from __future__ import annotations

import hashlib

from beam_agents.model import (
    FakeLLM,
    LLMClient,
    LlmRequest,
    match_any,
    match_contains,
    match_model_id,
    respond_with,
)


def _request(model_id: str = "m-1") -> LlmRequest:
    return LlmRequest(model_id=model_id, messages=[], tools_schema=[], sampling_params={})


# --- Requirement: FakeLLM implements the model-client protocol ---------------


def test_fake_llm_is_usable_wherever_an_llm_client_is_expected() -> None:
    # Scenario: FakeLLM is usable wherever an LLMClient is expected.
    fake = FakeLLM()
    assert isinstance(fake, LLMClient)


# --- Requirement: Scripted responses via ordered matchers --------------------


async def test_first_matching_rule_wins() -> None:
    # Scenario: First matching rule wins.
    fake = FakeLLM(
        [
            (match_any(), respond_with(b"first")),
            (match_any(), respond_with(b"second")),
        ]
    )

    response = await fake.complete(_request())

    assert response.response == b"first"


async def test_convenience_matcher_by_model_id() -> None:
    # Scenario: Convenience matcher by model id.
    fake = FakeLLM([(match_model_id("m-1"), respond_with(b"scripted"))])

    response = await fake.complete(_request("m-1"))

    assert response.response == b"scripted"


async def test_convenience_matcher_by_substring() -> None:
    # match_contains matches requests whose material contains the substring.
    fake = FakeLLM([(match_contains("fraud"), respond_with(b"scripted"))])
    request = LlmRequest(
        model_id="m-1",
        messages=[{"role": "user", "content": "score this fraud case"}],
        tools_schema=[],
        sampling_params={},
    )

    response = await fake.complete(request)

    assert response.response == b"scripted"


async def test_scripted_response_bytes_are_returned_verbatim() -> None:
    # Scenario: Scripted response bytes are returned verbatim.
    payload = b"the exact scripted bytes"
    fake = FakeLLM([(match_any(), respond_with(payload))])

    response = await fake.complete(_request())

    assert response.response == payload
    assert response.response_digest == hashlib.sha256(payload).digest()
