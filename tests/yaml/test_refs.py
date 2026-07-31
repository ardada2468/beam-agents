"""Tests for the `yaml-provider` capability's reference convention.

Requirement: "Agent and callable config values are module:object references
resolved at construction time" — all five scenarios. Every failure is a
construction-time `ValueError` naming the offending reference, so a typo in a
YAML document never reaches a bundle.
"""

from __future__ import annotations

import pytest

from beam_agents.yaml import run_agent
from beam_agents.yaml._refs import REFERENCE_GRAMMAR, resolve_agent, resolve_reference
from tests.yaml import _fixtures

FIXTURES = "tests.yaml._fixtures"
PROVIDER = f"{FIXTURES}:make_fake_llm"


# --- Scenario: A valid agent reference resolves to the module-level agent -----


def test_a_valid_agent_reference_resolves_to_the_module_level_agent() -> None:
    transform = run_agent(agent=f"{FIXTURES}:echo_agent", provider=PROVIDER)
    resolved: object = transform.agent
    assert resolved is _fixtures.echo_agent


def test_a_dotted_attribute_path_after_the_colon_resolves() -> None:
    # Entry-point semantics: the right-hand side may be a dotted attribute path.
    resolved = resolve_reference(
        f"{FIXTURES}:ActivateOnlyAgent.activate", field="agent", expected="an agent"
    )
    assert resolved is _fixtures.ActivateOnlyAgent.activate


def test_an_object_with_activate_is_accepted_as_an_agent() -> None:
    # The structural check's second arm: `StreamAgent`-shaped, not callable.
    resolved: object = resolve_agent(f"{FIXTURES}:activate_only_agent", field="agent")
    assert resolved is _fixtures.activate_only_agent


# --- Scenario: A malformed reference is rejected with the grammar -------------


@pytest.mark.parametrize(
    "reference",
    [
        "tests.yaml._fixtures.echo_agent",  # no colon
        ":echo_agent",  # empty module
        "tests.yaml._fixtures:",  # empty attribute
        "",  # empty reference
    ],
)
def test_a_malformed_reference_is_rejected_with_the_grammar(reference: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=reference, provider=PROVIDER)
    message = str(excinfo.value)
    assert repr(reference) in message
    assert REFERENCE_GRAMMAR in message
    assert "agent" in message


def test_a_non_string_reference_is_rejected_with_the_grammar() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=42, provider=PROVIDER)  # type: ignore[arg-type]
    assert REFERENCE_GRAMMAR in str(excinfo.value)


# --- Scenario: An unimportable module is rejected naming the module -----------


def test_an_unimportable_module_is_rejected_naming_the_module() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent="no_such_pkg.agents:fraud_agent", provider=PROVIDER)
    message = str(excinfo.value)
    assert "no_such_pkg.agents" in message
    assert "installed" in message
    assert "launch environment" in message
    # Chained from the underlying ImportError, so the original traceback survives.
    assert isinstance(excinfo.value.__cause__, ImportError)


# --- Scenario: A missing attribute is rejected naming both sides --------------


def test_a_missing_attribute_is_rejected_naming_attribute_and_module() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=f"{FIXTURES}:no_such_agent", provider=PROVIDER)
    message = str(excinfo.value)
    assert "no_such_agent" in message
    assert FIXTURES in message
    assert isinstance(excinfo.value.__cause__, AttributeError)


def test_a_missing_intermediate_attribute_is_rejected_naming_the_path() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=f"{FIXTURES}:ActivateOnlyAgent.nope", provider=PROVIDER)
    assert "ActivateOnlyAgent.nope" in str(excinfo.value)


# --- Scenario: A resolved non-agent object is rejected ------------------------


def test_a_resolved_non_agent_object_is_rejected() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=f"{FIXTURES}:NOT_AN_AGENT", provider=PROVIDER)
    message = str(excinfo.value)
    assert "NOT_AN_AGENT" in message
    assert "str" in message  # what it resolved to
    assert "agent" in message  # what was expected


def test_a_reference_to_a_module_is_rejected_as_an_agent() -> None:
    # `agent: "my_pkg.agents"` spelled with a colon still resolves to a module.
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent="tests.yaml:_fixtures", provider=PROVIDER)
    assert "module" in str(excinfo.value)


def test_a_non_callable_provider_is_rejected_naming_its_position() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=f"{FIXTURES}:echo_agent", provider=f"{FIXTURES}:NOT_AN_AGENT")
    message = str(excinfo.value)
    assert "provider" in message
    assert "callable" in message


# --- No dynamic code is evaluated from the document --------------------------


@pytest.mark.parametrize(
    "reference",
    [
        "lambda ctx: None",
        "def agent(ctx):\n  return None",
        "os:system('echo hi')",
    ],
)
def test_reference_resolution_never_evaluates_document_code(reference: str) -> None:
    # Each of these is rejected by the grammar/import path rather than executed.
    with pytest.raises(ValueError):
        run_agent(agent=reference, provider=PROVIDER)
