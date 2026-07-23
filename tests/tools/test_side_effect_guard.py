"""Direct-invocation guard for side-effecting tools (correctness invariant 5).

Covers the "Direct invocation of a side-effecting tool raises" requirement.
"""

from __future__ import annotations

import pytest

from beam_agents.tools import SideEffectToolError, ToolRunner, tool


async def test_tool_runner_refuses_a_side_effecting_tool() -> None:
    # Scenario: ToolRunner refuses a side-effecting tool.
    calls: list[object] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)

    runner = ToolRunner()

    with pytest.raises(SideEffectToolError, match="charge"):
        await runner.run(charge, {"amount_cents": 100})

    assert calls == []


def test_calling_a_side_effecting_tool_directly_raises() -> None:
    # Scenario: Calling a side-effecting tool directly raises.
    calls: list[object] = []

    @tool(side_effect=True)
    def charge(amount_cents: int) -> None:
        calls.append(amount_cents)

    with pytest.raises(SideEffectToolError, match="charge"):
        charge(amount_cents=100)

    assert calls == []
