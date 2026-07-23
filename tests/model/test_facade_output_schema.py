"""Constrained-JSON `output_schema` tests for the `model-facade` capability.

Covers: valid JSON parses into the Pydantic model, schema-violating/invalid
output raises `OutputSchemaError` without a transport retry, and omitting the
schema means no parsing occurs.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from beam_agents.model import FakeLLM, LlmRequest, OutputSchemaError, respond_with
from beam_agents.model.facade import RetryPolicy

from ._facade_helpers import make_facade

_REQUEST = LlmRequest(
    model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params={}
)


class _Answer(BaseModel):
    text: str
    confidence: float


# --- Requirement: Constrained JSON output via output_schema ------------------


async def test_valid_structured_output_is_parsed_into_the_model() -> None:
    # Scenario: Valid structured output is parsed into the model.
    fake = FakeLLM([(lambda _r: True, respond_with(b'{"text": "hi", "confidence": 0.9}'))])
    facade, _ = make_facade(fake)

    result = await facade.complete(
        _REQUEST, entity_key=b"key-1", seq=0, step_index=0, output_schema=_Answer
    )

    assert result.parsed == _Answer(text="hi", confidence=0.9)


async def test_schema_violating_output_raises_a_typed_error() -> None:
    # Scenario: Schema-violating output raises a typed error.
    fake = FakeLLM([(lambda _r: True, respond_with(b"not json at all"))])
    # A retry-permitting policy proves the transport is not retried for this error.
    facade, _ = make_facade(fake, retry_policy=RetryPolicy(max_attempts=3, base_ms=1, max_ms=1))

    with pytest.raises(OutputSchemaError):
        await facade.complete(
            _REQUEST, entity_key=b"key-1", seq=0, step_index=0, output_schema=_Answer
        )

    assert fake.call_count == 1  # exactly one transport attempt, no retry


async def test_schema_field_violation_also_raises() -> None:
    # Scenario: Schema-violating output raises a typed error (missing required field).
    fake = FakeLLM([(lambda _r: True, respond_with(b'{"text": "hi"}'))])  # missing confidence
    facade, _ = make_facade(fake)

    with pytest.raises(OutputSchemaError):
        await facade.complete(
            _REQUEST, entity_key=b"key-1", seq=0, step_index=0, output_schema=_Answer
        )


async def test_no_schema_means_no_parsing() -> None:
    # Scenario: No schema means no parsing.
    fake = FakeLLM([(lambda _r: True, respond_with(b"not json, never parsed"))])
    facade, _ = make_facade(fake)

    result = await facade.complete(_REQUEST, entity_key=b"key-1", seq=0, step_index=0)

    assert result.parsed is None
    assert result.response.response == b"not json, never parsed"


async def test_output_schema_folds_into_a_non_dict_sampling_params() -> None:
    # Non-dict sampling_params (LlmRequest types it as `object`) still folds the
    # schema in deterministically instead of crashing.
    request = LlmRequest(
        model_id="m-1", messages=[{"role": "user"}], tools_schema=[], sampling_params="raw-params"
    )
    fake = FakeLLM([(lambda _r: True, respond_with(b'{"text": "hi", "confidence": 0.5}'))])
    facade, _ = make_facade(fake)

    result = await facade.complete(
        request, entity_key=b"key-1", seq=0, step_index=0, output_schema=_Answer
    )

    assert result.parsed == _Answer(text="hi", confidence=0.5)
