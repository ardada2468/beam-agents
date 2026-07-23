"""Registration and resolution for the tool-registry capability.

Covers the "The ToolRegistry collects and resolves tools" requirement.
"""

from __future__ import annotations

import pytest

from beam_agents.tools import ToolNotFoundError, ToolRegistry, tool


def test_registered_tool_is_resolvable_and_appears_in_tools_schema() -> None:
    # Scenario: A registered tool is resolvable and appears in tools_schema.
    @tool
    def lookup_customer(customer_id: str) -> str:
        return customer_id

    registry = ToolRegistry()
    registry.register(lookup_customer)

    assert registry.get("lookup_customer") is lookup_customer
    assert lookup_customer.schema in registry.tools_schema


def test_duplicate_registration_is_rejected() -> None:
    # Scenario: Duplicate registration is rejected.
    @tool(name="dup")
    def a(x: str) -> str:
        return x

    @tool(name="dup")
    def b(y: str) -> str:
        return y

    registry = ToolRegistry()
    registry.register(a)

    with pytest.raises(Exception, match="dup"):
        registry.register(b)


def test_resolving_unknown_tool_raises() -> None:
    # Scenario: Resolving an unknown tool raises.
    registry = ToolRegistry()

    with pytest.raises(ToolNotFoundError, match="does_not_exist"):
        registry.get("does_not_exist")
