"""The `TokenBudget` meter itself (`token-budgets` capability).

The meter is the decision surface: a budget is not a measurement, so every
value it reads has to be a pure function of the activation's deterministic walk
and its trip has to be a latch rather than an advisory reading. These tests pin
the arithmetic and the exception's class/`repr`; the enforcement sites are
covered by `test_facade_budget.py` (the facade path) and
`tests/core/test_context.py` (the raw `call_model` path).
"""

from __future__ import annotations

import pytest

from beam_agents.model import BudgetExceeded, TokenBudget
from beam_agents.model.client import ProviderError

# --- Requirement: The crossing call trips the budget and the trip is a sticky latch


def test_charges_accumulate_across_calls() -> None:
    budget = TokenBudget(1_000)

    budget.charge(250)
    budget.charge(250)

    assert budget.consumed == 500
    assert budget.limit == 1_000
    assert budget.exhausted is False


def test_exactly_at_the_limit_is_within_budget() -> None:
    # Scenario: Exactly at the limit is within budget. The trip is strictly
    # greater: an activation that lands exactly on its budget spent exactly
    # what it was allowed to.
    budget = TokenBudget(600)

    budget.charge(300)
    budget.charge(300)

    assert budget.consumed == 600
    assert budget.exhausted is False
    budget.check()  # does not raise


def test_the_crossing_charge_raises_carrying_the_limit_and_the_consumed_total() -> None:
    # Scenario: The crossing call fails fast. 250 + 250 + 250 crosses 600 on
    # the third charge, and the exception names both sides of the comparison so
    # a dead letter built from it is triageable without re-deriving them.
    budget = TokenBudget(600)
    budget.charge(250)
    budget.charge(250)

    with pytest.raises(BudgetExceeded) as excinfo:
        budget.charge(250)

    assert excinfo.value.limit == 600
    assert excinfo.value.consumed == 750
    assert budget.consumed == 750


def test_the_trip_is_a_sticky_latch_on_both_charge_and_check() -> None:
    # Scenario: A swallowed trip cannot spend again. The latch is what makes
    # fail-fast robust rather than advisory: an agent whose `except Exception:`
    # swallows the trip still cannot spend, because the entry check raises too.
    budget = TokenBudget(10)
    with pytest.raises(BudgetExceeded):
        budget.charge(11)

    assert budget.exhausted is True
    with pytest.raises(BudgetExceeded):
        budget.check()
    with pytest.raises(BudgetExceeded):
        budget.charge(0)
    # A post-trip charge does not move the reported total: the record built
    # from the trip must stay byte-identical however many times it is retried.
    assert budget.consumed == 11


def test_the_exception_repr_is_a_pure_function_of_limit_and_consumed() -> None:
    # The dead-letter detail leads with this `repr`, and the errors sink encodes
    # that string, so a value that varied per attempt would make a replayed
    # budget kill's records differ byte-for-byte.
    first = TokenBudget(5)
    second = TokenBudget(5)
    with pytest.raises(BudgetExceeded) as one:
        first.charge(9)
    with pytest.raises(BudgetExceeded) as two:
        second.charge(9)

    assert repr(one.value) == repr(two.value)
    assert repr(one.value) == "BudgetExceeded(limit=5, consumed=9)"


def test_budget_exceeded_is_not_a_provider_error() -> None:
    # Scenario: A tripped budget is never retried as a transport failure. The
    # facade's retry loop classifies by class (`except ProviderError`), so the
    # only thing keeping a trip out of it is this inheritance fact.
    assert not issubclass(BudgetExceeded, ProviderError)
    assert issubclass(BudgetExceeded, Exception)


def test_a_non_positive_limit_is_refused_by_the_meter() -> None:
    # `AgentConfig` rejects it first, but the meter is constructible directly
    # (both context surfaces build one), so it re-checks rather than metering
    # against a limit nothing can stay under.
    with pytest.raises(ValueError, match="max_tokens_per_activation"):
        TokenBudget(0)
    with pytest.raises(ValueError, match="max_tokens_per_activation"):
        TokenBudget(-1)
