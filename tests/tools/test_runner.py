"""Inline execution of read-only tools for the tool-registry capability.

Covers the "The ToolRunner executes read-only tools inline with argument
validation" requirement.
"""

from __future__ import annotations

import pytest

from beam_agents.tools import ToolArgumentError, ToolRunner, tool


async def test_valid_arguments_are_validated_and_a_sync_tool_runs() -> None:
    # Scenario: Valid arguments are validated and a sync tool runs.
    calls: list[tuple[str, int]] = []

    @tool
    def lookup(customer_id: str, limit: int = 10) -> str:
        calls.append((customer_id, limit))
        return customer_id

    runner = ToolRunner()
    result = await runner.run(lookup, {"customer_id": "abc", "limit": 5})

    assert result == "abc"
    assert calls == [("abc", 5)]


async def test_invalid_arguments_are_rejected_before_the_callable_runs() -> None:
    # Scenario: Invalid arguments are rejected before the callable runs.
    calls: list[object] = []

    @tool
    def lookup(customer_id: str) -> str:
        calls.append(customer_id)
        return customer_id

    runner = ToolRunner()

    with pytest.raises(ToolArgumentError):
        await runner.run(lookup, {})  # missing required field

    with pytest.raises(ToolArgumentError):
        await runner.run(lookup, {"customer_id": object()})  # wrong type, not coercible to str

    assert calls == []


async def test_an_async_tool_is_awaited_and_its_result_returned() -> None:
    # Scenario: An async tool is awaited and its result returned.
    calls: list[tuple[str, int]] = []

    @tool
    async def lookup(customer_id: str, limit: int = 10) -> str:
        calls.append((customer_id, limit))
        return customer_id

    runner = ToolRunner()
    result = await runner.run(lookup, {"customer_id": "abc", "limit": 5})

    assert result == "abc"
    assert calls == [("abc", 5)]
