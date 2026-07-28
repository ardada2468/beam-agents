"""Effector configuration: URIs, budgets, and eager, import-free validation.

The URI grammar and error semantics mirror
``core/transform.py::DefaultSinkResolver`` — ``kafka://<brokers>/<topic>`` and
``pubsub://<project>/<topic|subscription>`` — but the symbols are deliberately
*not* imported from there: ``core`` imports Beam, and the effector runs outside
the pipeline (see the change design, D1). Duplicating a 20-line parser is the
cost of that boundary.

Validation runs at construction and imports no client library, so a
misconfigured deployment fails immediately with an actionable message rather
than on its first message hours later (D8).
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

# Lifetime of a claim, after which an uncompleted intent becomes re-claimable.
# Must outlive a full-length tool execution so a live lease implies a live
# owner (see `validate`).
DEFAULT_LEASE_MS = 300_000

# Lifetime of a terminal dedup record. A redelivery arriving after this window
# re-executes, so it must exceed the maximum plausible redelivery lag.
DEFAULT_RESULT_TTL_MS = 86_400_000

# Wall-clock budget for one tool invocation before it is cancelled and reported
# as ERROR (the effect is unknown, not un-attempted).
DEFAULT_TOOL_TIMEOUT_MS = 60_000

_TRANSPORT_SCHEMES = ("kafka", "pubsub")
_DEDUP_SCHEMES = ("bigtable", "memory", "redis")

_BIGTABLE_URI_SEGMENTS = 3


class EffectorConfigError(ValueError):
    """A configured URI's scheme is unrecognized, or the URI is malformed for its scheme."""


def parse_transport_uri(field_name: str, uri: str) -> tuple[str, tuple[str, ...]]:
    """Parse a ``kafka://``/``pubsub://`` URI into ``(scheme, parts)``.

    Both schemes take an authority (brokers / project) and exactly one path
    segment (topic / subscription). Raises :class:`EffectorConfigError` naming
    ``field_name`` and the expected shape.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if scheme not in _TRANSPORT_SCHEMES:
        raise EffectorConfigError(
            f"{field_name}: unknown transport URI scheme {(scheme or uri)!r}; "
            f"expected one of {sorted(_TRANSPORT_SCHEMES)}"
        )
    segments = [s for s in parsed.path.split("/") if s]
    if not parsed.netloc or len(segments) != 1:
        expected = (
            "kafka://<bootstrap-servers>/<topic>"
            if scheme == "kafka"
            else "pubsub://<project>/<topic-or-subscription>"
        )
        raise EffectorConfigError(
            f"{field_name}: malformed {scheme} URI {uri!r}; expected {expected}"
        )
    return scheme, (parsed.netloc, segments[0])


def parse_dedup_uri(field_name: str, uri: str) -> tuple[str, tuple[str, ...]]:
    """Parse a dedup-store URI into ``(scheme, parts)``.

    ``redis://...`` keeps its full URI as the single part (the Redis client
    consumes the URI directly), ``bigtable://<project>/<instance>/<table>``
    yields its three segments, and ``memory://`` yields none.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if scheme not in _DEDUP_SCHEMES:
        raise EffectorConfigError(
            f"{field_name}: unknown dedup URI scheme {(scheme or uri)!r}; "
            f"expected one of {sorted(_DEDUP_SCHEMES)}"
        )
    if scheme == "memory":
        return scheme, ()
    if scheme == "redis":
        if not parsed.netloc:
            raise EffectorConfigError(
                f"{field_name}: malformed redis URI {uri!r}; expected redis://<host>:<port>[/<db>]"
            )
        return scheme, (uri,)
    segments = [s for s in parsed.path.split("/") if s]
    if not parsed.netloc or len(segments) != _BIGTABLE_URI_SEGMENTS - 1:
        raise EffectorConfigError(
            f"{field_name}: malformed bigtable URI {uri!r}; "
            "expected bigtable://<project>/<instance>/<table>"
        )
    return scheme, (parsed.netloc, *segments)


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"EffectorConfig.{name} must be positive, got {value!r}")


@dataclass(frozen=True)
class EffectorConfig:
    """Everything the effector service needs, validated at construction.

    ``intents_from`` is the outbox topic ``WriteIntents`` publishes to;
    ``results_to`` is the results topic the pipeline re-injects from;
    ``approvals_to`` is the channel approval-kind intents are routed to.
    """

    intents_from: str
    results_to: str
    approvals_to: str
    dedup: str
    consumer_group: str
    lease_ms: int = DEFAULT_LEASE_MS
    result_ttl_ms: int = DEFAULT_RESULT_TTL_MS
    tool_timeout_ms: int = DEFAULT_TOOL_TIMEOUT_MS
    publish_max_attempts: int = 5
    publish_backoff_ms: int = 100
    in_flight_backoff_ms: int = 250
    in_flight_backoff_max_ms: int = 5_000
    max_concurrent_partitions: int = 8

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Reject malformed URIs and budget settings, importing no client library."""
        parse_transport_uri("intents_from", self.intents_from)
        parse_transport_uri("results_to", self.results_to)
        parse_transport_uri("approvals_to", self.approvals_to)
        parse_dedup_uri("dedup", self.dedup)

        if not self.consumer_group:
            raise ValueError("EffectorConfig.consumer_group must be a non-empty string")

        for name in (
            "lease_ms",
            "result_ttl_ms",
            "tool_timeout_ms",
            "publish_max_attempts",
            "publish_backoff_ms",
            "in_flight_backoff_ms",
            "in_flight_backoff_max_ms",
            "max_concurrent_partitions",
        ):
            _require_positive(name, getattr(self, name))

        if self.lease_ms <= self.tool_timeout_ms:
            raise ValueError(
                f"EffectorConfig.lease_ms ({self.lease_ms}) must exceed tool_timeout_ms "
                f"({self.tool_timeout_ms}): a lease has to outlive a full-length tool "
                "execution so that an unexpired lease implies a live owner, and an expired "
                "one implies a dead one"
            )
