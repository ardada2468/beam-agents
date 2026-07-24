"""Pydantic-derived schema generation for the tool-registry capability.

Covers the "Tool schema is generated from the function signature via Pydantic
v2" requirement.
"""

from __future__ import annotations

import pytest

from beam_agents.tools import ToolDefinitionError, tool


def test_schema_reflects_parameter_types_and_required_ness() -> None:
    # Scenario: Schema reflects parameter types and required-ness.
    @tool
    def f(customer_id: str, limit: int = 10) -> None:
        return None

    parameters = f.schema["parameters"]
    assert isinstance(parameters, dict)
    properties = parameters["properties"]
    assert properties["customer_id"]["type"] == "string"
    assert properties["limit"]["type"] == "integer"
    assert parameters["required"] == ["customer_id"]
    assert properties["limit"]["default"] == 10


def test_missing_type_annotation_raises_at_decoration_time() -> None:
    # Scenario: Missing type annotations are rejected at decoration time.
    with pytest.raises(ToolDefinitionError, match="limit"):

        @tool
        def f(customer_id: str, limit=10) -> None:  # type: ignore[no-untyped-def]
            return None


def test_var_args_or_kwargs_raise_at_decoration_time() -> None:
    # A signature with *args/**kwargs can't be modeled as a Pydantic schema,
    # so it is rejected the same way an un-annotated parameter is.
    with pytest.raises(ToolDefinitionError, match="kwargs"):

        @tool
        def f(customer_id: str, **kwargs: object) -> None:
            return None
