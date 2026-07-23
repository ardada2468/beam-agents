"""Request-recording tests for the `fake-llm` capability.

Covers: requests are recorded in call order, a failing call is still
recorded, and the exposed log is read-only to callers.
"""

from __future__ import annotations

import pytest

from beam_agents.model import FakeLLM, LlmRequest, ServerError, match_any, raise_error, respond_with


def _request(model_id: str) -> LlmRequest:
    return LlmRequest(model_id=model_id, messages=[], tools_schema=[], sampling_params={})


# --- Requirement: Request recording -------------------------------------------


async def test_requests_are_recorded_in_call_order() -> None:
    # Scenario: Requests are recorded in call order.
    fake = FakeLLM([(match_any(), respond_with(b"ok"))])
    requests = [_request("m-1"), _request("m-2"), _request("m-3")]

    for request in requests:
        await fake.complete(request)

    assert fake.requests == tuple(requests)


async def test_a_failing_call_is_still_recorded() -> None:
    # Scenario: A failing call is still recorded.
    fake = FakeLLM([(match_any(), raise_error(ServerError(status=503)))])
    request = _request("m-1")

    with pytest.raises(ServerError):
        await fake.complete(request)

    assert request in fake.requests


async def test_the_exposed_log_is_read_only_to_callers() -> None:
    # Scenario: recorded log cannot be mutated by a caller.
    fake = FakeLLM([(match_any(), respond_with(b"ok"))])
    await fake.complete(_request("m-1"))

    log = fake.requests
    with pytest.raises(AttributeError):
        log.append(_request("m-2"))  # type: ignore[attr-defined]
