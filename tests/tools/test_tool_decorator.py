"""Bare and parameterized `@tool` usage for the tool-registry capability.

Covers the "The @tool decorator registers a callable as a Tool" requirement.
"""

from __future__ import annotations

from beam_agents.tools import tool


def test_bare_decorator_derives_name_and_description() -> None:
    # Scenario: Bare decorator derives name and description from the function.
    @tool
    def lookup_customer(customer_id: str) -> str:
        """Look up a customer by id."""
        return customer_id

    assert lookup_customer.name == "lookup_customer"
    assert lookup_customer.description == "Look up a customer by id."
    assert lookup_customer.side_effect is False


def test_parameterized_decorator_overrides_name_and_declares_side_effect() -> None:
    # Scenario: Parameterized decorator overrides name and declares a side effect.
    @tool(name="charge", side_effect=True)
    def charge_card(amount_cents: int) -> None:
        return None

    assert charge_card.name == "charge"
    assert charge_card.side_effect is True
