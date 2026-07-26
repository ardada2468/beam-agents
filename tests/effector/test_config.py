"""Effector configuration for the effector-service capability.

Covers the "Configuration is validated eagerly, before any client is
constructed" requirement. The no-client-imports scenario is proved under a
hard import block in ``test_boundary.py``; this module covers URI grammar and
the budget invariant.
"""

from __future__ import annotations

import pytest

from beam_agents.effector import EffectorConfig
from beam_agents.effector.config import (
    EffectorConfigError,
    parse_dedup_uri,
    parse_transport_uri,
)

_VALID = {
    "intents_from": "kafka://localhost:9092/intents",
    "results_to": "kafka://localhost:9092/results",
    "approvals_to": "kafka://localhost:9092/approvals",
    "dedup": "redis://localhost:6379",
    "consumer_group": "effector",
}


def _config(**overrides: object) -> EffectorConfig:
    return EffectorConfig(**{**_VALID, **overrides})  # type: ignore[arg-type]


def test_an_unknown_source_scheme_is_rejected_at_construction() -> None:
    # Scenario: An unknown source scheme is rejected at construction.
    with pytest.raises(EffectorConfigError) as excinfo:
        _config(intents_from="amqp://localhost/intents")

    message = str(excinfo.value)
    assert "amqp" in message
    assert "kafka" in message and "pubsub" in message
    assert isinstance(excinfo.value, ValueError)


def test_a_lease_shorter_than_the_tool_timeout_is_rejected() -> None:
    # Scenario: A lease shorter than the tool timeout is rejected.
    with pytest.raises(ValueError, match="lease_ms") as excinfo:
        _config(lease_ms=1_000, tool_timeout_ms=1_000)

    assert "tool_timeout_ms" in str(excinfo.value)


def test_a_lease_longer_than_the_tool_timeout_is_accepted() -> None:
    # The invariant is strict inequality: an unexpired lease must imply a live
    # owner, so the lease has to outlast a full-length tool execution.
    config = _config(lease_ms=1_001, tool_timeout_ms=1_000)

    assert config.lease_ms > config.tool_timeout_ms


@pytest.mark.parametrize(
    "field",
    ["intents_from", "results_to", "approvals_to"],
)
def test_a_malformed_transport_uri_is_rejected_for_every_transport_field(field: str) -> None:
    # The grammar is the one core/transform.py's sink resolver uses: a
    # kafka/pubsub URI needs both an authority and exactly one path segment.
    with pytest.raises(EffectorConfigError, match=field):
        _config(**{field: "kafka://localhost:9092"})


def test_an_unknown_dedup_scheme_is_rejected() -> None:
    with pytest.raises(EffectorConfigError, match="dedup") as excinfo:
        _config(dedup="postgres://localhost/dedup")

    assert "bigtable" in str(excinfo.value)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("lease_ms", 0),
        ("result_ttl_ms", 0),
        ("tool_timeout_ms", -1),
        ("publish_max_attempts", 0),
        ("max_concurrent_partitions", 0),
    ],
)
def test_non_positive_budgets_are_rejected(name: str, value: int) -> None:
    with pytest.raises(ValueError, match=name):
        _config(**{name: value})


def test_an_empty_consumer_group_is_rejected() -> None:
    with pytest.raises(ValueError, match="consumer_group"):
        _config(consumer_group="")


def test_transport_uris_parse_into_scheme_and_parts() -> None:
    assert parse_transport_uri("intents_from", "kafka://a:9092,b:9092/intents") == (
        "kafka",
        ("a:9092,b:9092", "intents"),
    )
    assert parse_transport_uri("intents_from", "pubsub://proj/sub") == ("pubsub", ("proj", "sub"))


def test_dedup_uris_parse_into_scheme_and_parts() -> None:
    assert parse_dedup_uri("dedup", "memory://") == ("memory", ())
    assert parse_dedup_uri("dedup", "bigtable://proj/inst/table") == (
        "bigtable",
        ("proj", "inst", "table"),
    )
    scheme, parts = parse_dedup_uri("dedup", "redis://localhost:6379/0")
    assert scheme == "redis"
    assert parts[0] == "redis://localhost:6379/0"


def test_a_malformed_bigtable_uri_is_rejected() -> None:
    with pytest.raises(EffectorConfigError, match="bigtable"):
        parse_dedup_uri("dedup", "bigtable://proj/inst")


def test_the_config_is_frozen() -> None:
    # Budgets are validated once at construction; a mutable config would let a
    # caller invalidate the lease/timeout invariant after the fact.
    config = _config()
    with pytest.raises(Exception, match=r"cannot assign|frozen"):
        config.lease_ms = 5  # type: ignore[misc]
