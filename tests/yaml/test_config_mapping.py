"""Tests for the `yaml-provider` capability's YAML-config → `AgentConfig` mapping.

Requirement: "YAML config maps totally onto AgentConfig and rejects unknown
keys" — all four scenarios. The YAML layer owns *shape* (key set, reference
grammar, mapping types); every value check is delegated to `AgentConfig`,
`HitlPolicy`, and the sink resolver, so the two layers cannot drift.
"""

from __future__ import annotations

import pickle

import pytest

from beam_agents.core.agent import FallbackContext
from beam_agents.core.transform import AgentConfig, UnknownSinkSchemeError
from beam_agents.hitl import Drop, HitlPolicy
from beam_agents.model.fake import FakeLLM
from beam_agents.yaml import run_agent
from beam_agents.yaml._config import CONFIG_KEYS, HITL_KEYS
from tests.yaml import _fixtures


def _is(resolved: object, expected: object) -> bool:
    """Identity check that survives strict typing of the resolved-object seams."""
    return resolved is expected


FIXTURES = "tests.yaml._fixtures"
AGENT = f"{FIXTURES}:echo_agent"
PROVIDER = f"{FIXTURES}:make_fake_llm"


# --- Scenario: A full YAML config round-trips onto AgentConfig ----------------


def test_a_full_yaml_config_round_trips_onto_agent_config() -> None:
    transform = run_agent(
        agent=AGENT,
        provider=f"{FIXTURES}:make_scripted_llm",
        provider_config={"payload": "pong", "latency_ms": 0},
        decode=f"{FIXTURES}:decode_len",
        tool_registry=f"{FIXTURES}:TOOL_REGISTRY",
        activation_timeout_s=12.5,
        ttl_ms=90_000,
        cancel_grace_s=2.0,
        intents_to="kafka://broker:9092/intents",
        traces_to="otlp://collector:4318",
        errors_to="pubsub://proj/errors",
        hitl={
            "timeout_ms": 45_000,
            "intent_ttl_ms": 120_000,
            "approval_channel": "ops-approvals",
            "max_escalations": 2,
            "on_timeout": f"{FIXTURES}:drop_on_timeout",
        },
    )
    config = transform.config
    assert isinstance(config, AgentConfig)

    # Scalars pass through.
    assert config.activation_timeout_s == 12.5
    assert config.ttl_ms == 90_000
    assert config.cancel_grace_s == 2.0

    # Sink URIs pass through verbatim.
    assert config.intents_to == "kafka://broker:9092/intents"
    assert config.traces_to == "otlp://collector:4318"
    assert config.errors_to == "pubsub://proj/errors"

    # References resolve to the module-level objects themselves.
    assert _is(config.decode, _fixtures.decode_len)
    assert _is(config.tool_registry, _fixtures.TOOL_REGISTRY)

    # `hitl` maps onto HitlPolicy, `on_timeout` through the same reference machinery.
    assert config.hitl_policy == HitlPolicy(
        timeout_ms=45_000,
        intent_ttl_ms=120_000,
        approval_channel="ops-approvals",
        max_escalations=2,
        on_timeout=_fixtures.drop_on_timeout,
    )
    route = config.hitl_policy.on_timeout(FallbackContext(entity_key=b"k", seq=1))
    assert isinstance(route, Drop)


async def test_the_provider_factory_pickles_and_binds_the_provider_config_kwargs() -> None:
    transform = run_agent(
        agent=AGENT,
        provider=f"{FIXTURES}:make_scripted_llm",
        provider_config={"payload": "pong"},
    )
    factory = transform.config.provider_factory
    # Picklable: the DoFn serializes it into the runner.
    client = pickle.loads(pickle.dumps(factory))()
    assert isinstance(client, FakeLLM)
    # Called with no arguments, it invoked the referenced callable with exactly
    # the provider_config kwargs — so the script it built serves "pong".
    response = await client.complete(_fixtures.request())
    assert response.response == b"pong"


def test_a_zero_argument_provider_needs_no_provider_config() -> None:
    transform = run_agent(agent=AGENT, provider=PROVIDER)
    assert isinstance(transform.config.provider_factory(), FakeLLM)


def test_unset_knobs_fall_through_to_the_agent_config_defaults() -> None:
    config = run_agent(agent=AGENT, provider=PROVIDER).config
    defaults = AgentConfig(provider_factory=_fixtures.make_fake_llm)
    assert config.activation_timeout_s == defaults.activation_timeout_s
    assert config.ttl_ms == defaults.ttl_ms
    assert config.cancel_grace_s == defaults.cancel_grace_s
    assert config.hitl_policy == defaults.hitl_policy
    assert config.decode is None
    assert config.intents_to is None
    assert config.traces_to is None
    assert config.errors_to is None


# --- Scenario: An unknown config key is rejected with the valid-key list ------


def test_an_unknown_top_level_key_is_rejected_with_the_valid_key_list() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, ttl=1000)
    message = str(excinfo.value)
    assert "ttl" in message
    for key in CONFIG_KEYS:
        assert key in message


def test_an_unknown_hitl_key_is_rejected_with_the_valid_key_list() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, hitl={"timeout": 1000})
    message = str(excinfo.value)
    assert "hitl" in message
    assert "timeout" in message
    for key in HITL_KEYS:
        assert key in message


def test_a_non_mapping_hitl_block_is_rejected() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, hitl="timeout_ms=5")  # type: ignore[arg-type]
    assert "hitl" in str(excinfo.value)
    assert "mapping" in str(excinfo.value)


def test_a_non_mapping_provider_config_is_rejected() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, provider_config=["payload"])  # type: ignore[arg-type]
    assert "provider_config" in str(excinfo.value)
    assert "mapping" in str(excinfo.value)


# --- Scenario: Delegated validation still fires at the YAML boundary ----------


def test_an_unknown_sink_scheme_propagates_the_resolver_error() -> None:
    with pytest.raises(UnknownSinkSchemeError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, intents_to="ftp://host/topic")
    assert "intents_to" in str(excinfo.value)


def test_a_non_positive_knob_propagates_the_agent_config_error() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, ttl_ms=0)
    assert "AgentConfig.ttl_ms" in str(excinfo.value)


def test_a_non_positive_hitl_knob_propagates_the_policy_error() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, hitl={"timeout_ms": -1})
    assert "HitlPolicy.timeout_ms" in str(excinfo.value)


def test_otlp_on_a_non_trace_sink_propagates_the_resolver_error() -> None:
    with pytest.raises(UnknownSinkSchemeError) as excinfo:
        run_agent(agent=AGENT, provider=PROVIDER, errors_to="otlp://collector:4318")
    assert "traces_to" in str(excinfo.value)


# --- Scenario: A misspelled provider kwarg fails at construction --------------


def test_a_misspelled_provider_kwarg_fails_at_construction() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(
            agent=AGENT,
            provider=f"{FIXTURES}:make_scripted_llm",
            provider_config={"paylod": "pong"},
        )
    message = str(excinfo.value)
    assert "paylod" in message
    assert f"{FIXTURES}:make_scripted_llm" in message


def test_the_provider_factory_is_not_invoked_at_construction() -> None:
    # A factory may open network clients, so construction must not call it —
    # this one raises if it ever is.
    transform = run_agent(agent=AGENT, provider=f"{FIXTURES}:exploding_factory")
    with pytest.raises(RuntimeError):
        transform.config.provider_factory()


def test_an_unpicklable_provider_reference_fails_at_the_yaml_boundary() -> None:
    with pytest.raises(ValueError) as excinfo:
        run_agent(agent=AGENT, provider=f"{FIXTURES}:LOCAL_FACTORY")
    message = str(excinfo.value)
    assert f"{FIXTURES}:LOCAL_FACTORY" in message
    assert "pickle" in message
