"""Inline execution of read-only tools for the tool-registry capability.

Covers the "The ToolRunner executes read-only tools inline with argument
validation" requirement.
"""

from __future__ import annotations

import pytest

from beam_agents.tools import ToolArgumentError, ToolRunner, tool


def test_valid_arguments_are_validated_and_the_tool_runs() -> None:
    # Scenario: Valid arguments are validated and the tool runs.
    calls: list[tuple[str, int]] = []

    @tool
    def lookup(customer_id: str, limit: int = 10) -> str:
        calls.append((customer_id, limit))
        return customer_id

    runner = ToolRunner()
    result = runner.run(lookup, {"customer_id": "abc", "limit": 5})

    assert result == "abc"
    assert calls == [("abc", 5)]


def test_invalid_arguments_are_rejected_before_the_callable_runs() -> None:
    # Scenario: Invalid arguments are rejected before the callable runs.
    calls: list[object] = []

    @tool
    def lookup(customer_id: str) -> str:
        calls.append(customer_id)
        return customer_id

    runner = ToolRunner()

    with pytest.raises(ToolArgumentError):
        runner.run(lookup, {})  # missing required field

    with pytest.raises(ToolArgumentError):
        runner.run(lookup, {"customer_id": object()})  # wrong type, not coercible to str

    assert calls == []
