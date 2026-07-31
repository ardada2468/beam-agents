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

import logging
import re
import ssl
from dataclasses import dataclass, fields
from typing import Literal
from urllib.parse import urlparse

from beam_agents.intent_signing import resolve_secret_reference, validate_secret_reference

_LOG = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_LEASE_MS",
    "DEFAULT_RESULT_TTL_MS",
    "DEFAULT_TOOL_TIMEOUT_MS",
    "VERIFICATION_MODES",
    "EffectorConfig",
    "EffectorConfigError",
    "TransportSecurity",
    "VerificationMode",
    "parse_dedup_uri",
    "parse_transport_uri",
    "redact_uri",
]

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


# Verification modes, in rollout order. `off` is byte-for-byte the pre-signing
# effector; `permissive` is the coexistence window; `require` is the end state.
VerificationMode = Literal["off", "permissive", "require"]
VERIFICATION_MODES: tuple[VerificationMode, ...] = ("off", "permissive", "require")

# Kafka security protocols that carry a SASL exchange.
_SASL_PROTOCOLS = ("SASL_PLAINTEXT", "SASL_SSL")
_SECURITY_PROTOCOLS = ("PLAINTEXT", "SSL", *_SASL_PROTOCOLS)

# Matches the `userinfo@` component of a URI authority, anywhere in a string.
# Applied to whole messages rather than to bare URIs, so an interpolation site
# added later is redacted by default rather than by remembering to.
_USERINFO = re.compile(r"(?<=//)[^/@\s]*@")


def redact_uri(text: str) -> str:
    """Mask URI userinfo (username *and* password) anywhere in ``text``.

    ``redis://admin:hunter2@host:6379`` renders as ``redis://***@host:6379``.
    Redaction, not rejection: passing credentials inside a URI keeps working
    (the docs steer to ``env:``/``file:`` references instead), it just stops
    reaching error messages, ``repr``s, logs, and stderr.
    """
    return _USERINFO.sub("***@", text)


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
            f"{field_name}: unknown transport URI scheme {(scheme or redact_uri(uri))!r}; "
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
            f"{field_name}: malformed {scheme} URI {redact_uri(uri)!r}; expected {expected}"
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
            f"{field_name}: unknown dedup URI scheme {(scheme or redact_uri(uri))!r}; "
            f"expected one of {sorted(_DEDUP_SCHEMES)}"
        )
    if scheme == "memory":
        return scheme, ()
    if scheme == "redis":
        # `hostname`, not `netloc`: a URI that is nothing but userinfo
        # (`redis://user:password@`) has a truthy netloc and no host at all —
        # exactly the shape a credential-carrying typo takes, and the one whose
        # error message must not echo the credential.
        if not parsed.hostname:
            raise EffectorConfigError(
                f"{field_name}: malformed redis URI {redact_uri(uri)!r}; "
                "expected redis://<host>:<port>[/<db>]"
            )
        return scheme, (uri,)
    segments = [s for s in parsed.path.split("/") if s]
    if not parsed.netloc or len(segments) != _BIGTABLE_URI_SEGMENTS - 1:
        raise EffectorConfigError(
            f"{field_name}: malformed bigtable URI {redact_uri(uri)!r}; "
            "expected bigtable://<project>/<instance>/<table>"
        )
    return scheme, (parsed.netloc, *segments)


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"EffectorConfig.{name} must be positive, got {value!r}")


@dataclass(frozen=True)
class TransportSecurity:
    """Broker authentication settings, with credentials given by reference.

    The library cannot *enforce* broker authentication — that is the broker's
    job (Kafka SASL/mTLS plus per-principal ACLs, Pub/Sub IAM). What it can do
    is make it configurable on every Kafka client it constructs: the effector's
    intent source and message sinks, and ``WriteIntents``' producer. Pub/Sub
    needs nothing here; its auth is Application Default Credentials and its
    least-privilege role matrix is documented in ``docs/security.md``.

    Credentials are ``env:VAR``/``file:/path`` *references*, validated eagerly
    and import-free and resolved only at client construction, so a resolved
    secret exists in the client object and nowhere else — not on this object,
    not in its ``repr``, not in an error it raises, and not in ``argv``
    (design D7).

    The TLS material paths are consumed differently by the two client families
    they reach, which is a property of the clients, not of this block: the
    Python (aiokafka) clients read PEM files, while Beam's cross-language Kafka
    writer is the Java client and reads Java keystores. ``docs/security.md``
    says so explicitly.
    """

    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username_reference: str | None = None
    sasl_password_reference: str | None = None
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Reject malformed settings and references, importing no client library."""
        if self.security_protocol is not None and self.security_protocol not in _SECURITY_PROTOCOLS:
            raise ValueError(
                f"TransportSecurity.security_protocol {self.security_protocol!r} is not "
                f"recognized; expected one of {sorted(_SECURITY_PROTOCOLS)}"
            )
        if self.sasl_mechanism is not None and self.security_protocol not in _SASL_PROTOCOLS:
            raise ValueError(
                f"TransportSecurity.sasl_mechanism {self.sasl_mechanism!r} needs a SASL-carrying "
                f"security_protocol (one of {sorted(_SASL_PROTOCOLS)}), got "
                f"{self.security_protocol!r}"
            )
        for name in ("sasl_username_reference", "sasl_password_reference"):
            reference = getattr(self, name)
            if reference is not None:
                validate_secret_reference(f"TransportSecurity.{name}", reference)
        if self.sasl_mechanism is not None and (
            self.sasl_username_reference is None or self.sasl_password_reference is None
        ):
            raise ValueError(
                f"TransportSecurity.sasl_mechanism {self.sasl_mechanism!r} needs both "
                "sasl_username_reference and sasl_password_reference"
            )

    def client_kwargs(self) -> dict[str, object]:
        """aiokafka constructor keyword arguments, with references resolved.

        Called at client construction, never at configuration time: this is the
        only moment a secret value exists, and it goes straight into the client.
        """
        kwargs: dict[str, object] = {}
        if self.security_protocol is not None:
            kwargs["security_protocol"] = self.security_protocol
        if self.sasl_mechanism is not None:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username_reference is not None:
            kwargs["sasl_plain_username"] = resolve_secret_reference(
                "TransportSecurity.sasl_username_reference", self.sasl_username_reference
            )
        if self.sasl_password_reference is not None:
            kwargs["sasl_plain_password"] = resolve_secret_reference(
                "TransportSecurity.sasl_password_reference", self.sasl_password_reference
            )
        context = self._ssl_context()
        if context is not None:
            kwargs["ssl_context"] = context
        return kwargs

    def java_producer_config(self) -> dict[str, str]:
        """Java-client properties for Beam's cross-language Kafka writer.

        The same settings, in the vocabulary the Java client speaks. The SASL
        credential lands in ``sasl.jaas.config`` because that is the only place
        the Java client reads it from — which is exactly why the value must
        arrive from a reference at construction rather than from a flag in
        ``argv``.
        """
        config: dict[str, str] = {}
        if self.security_protocol is not None:
            config["security.protocol"] = self.security_protocol
        if self.sasl_mechanism is not None:
            assert self.sasl_username_reference is not None  # validate() guarantees both
            assert self.sasl_password_reference is not None
            username = resolve_secret_reference(
                "TransportSecurity.sasl_username_reference", self.sasl_username_reference
            )
            password = resolve_secret_reference(
                "TransportSecurity.sasl_password_reference", self.sasl_password_reference
            )
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.jaas.config"] = (
                f"{_jaas_login_module(self.sasl_mechanism)} required "
                f'username="{username}" password="{password}";'
            )
        if self.ssl_ca_location is not None:
            config["ssl.truststore.location"] = self.ssl_ca_location
        if self.ssl_certificate_location is not None:
            config["ssl.keystore.location"] = self.ssl_certificate_location
        return config

    def _ssl_context(self) -> ssl.SSLContext | None:
        if self.security_protocol not in ("SSL", "SASL_SSL"):
            return None
        context = ssl.create_default_context(cafile=self.ssl_ca_location)
        if self.ssl_certificate_location is not None:
            context.load_cert_chain(self.ssl_certificate_location, self.ssl_key_location)
        return context


def _jaas_login_module(mechanism: str) -> str:
    if mechanism.startswith("SCRAM-"):
        return "org.apache.kafka.common.security.scram.ScramLoginModule"
    return "org.apache.kafka.common.security.plain.PlainLoginModule"


@dataclass(frozen=True)
class EffectorConfig:
    """Everything the effector service needs, validated at construction.

    ``intents_from`` is the outbox topic ``WriteIntents`` publishes to;
    ``results_to`` is the results topic the pipeline re-injects from;
    ``approvals_to`` is the channel approval-kind intents are routed to;
    ``dead_letters_to`` is where deliveries that fail signature verification
    are preserved verbatim.
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
    # The rollout dial. `off` by default, so upgrading the effector changes
    # nothing at runtime and the new code path stays dormant until a deployment
    # turns it on (migration plan step 2).
    verify_intents: VerificationMode = "off"
    # `env:VAR`/`file:/path` reference to a `key_id=base64(key)` keyring.
    signing_keys: str | None = None
    dead_letters_to: str | None = None
    transport_security: TransportSecurity | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Reject malformed URIs and budget settings, importing no client library."""
        parse_transport_uri("intents_from", self.intents_from)
        parse_transport_uri("results_to", self.results_to)
        parse_transport_uri("approvals_to", self.approvals_to)
        parse_dedup_uri("dedup", self.dedup)
        if self.dead_letters_to is not None:
            parse_transport_uri("dead_letters_to", self.dead_letters_to)

        if not self.consumer_group:
            raise ValueError("EffectorConfig.consumer_group must be a non-empty string")

        self._validate_verification()
        if self.transport_security is not None:
            self.transport_security.validate()

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

    def _validate_verification(self) -> None:
        """Reject a verifying mode that cannot verify; warn about a silent one."""
        if self.verify_intents not in VERIFICATION_MODES:
            raise ValueError(
                f"EffectorConfig.verify_intents {self.verify_intents!r} is not a verification "
                f"mode; expected one of {list(VERIFICATION_MODES)}"
            )
        if self.verify_intents == "off":
            return
        if not self.signing_keys:
            raise ValueError(
                f"EffectorConfig.verify_intents={self.verify_intents!r} needs signing_keys: a "
                "reference ('env:VAR' or 'file:/path') to a 'key_id=base64(key)' keyring. "
                "Without one every signed intent would fail verification."
            )
        validate_secret_reference("EffectorConfig.signing_keys", self.signing_keys)
        if self.verify_intents == "require" and not self.dead_letters_to:
            # Not fatal — logs and counters are a defensible (if lossy) choice —
            # but silent by default is not, so say it out loud once at startup.
            _LOG.warning(
                "verify_intents=require without dead_letters_to: deliveries that fail "
                "verification will exist only as log records and counters, with no payload "
                "retained to re-drive after a keyring fix"
            )

    def __repr__(self) -> str:
        """Render with URI userinfo masked.

        An explicit ``__repr__`` because the default dataclass one renders every
        URI verbatim, and a ``repr`` reaches logs, tracebacks, and test output
        without anyone deciding it should. The credential *references* are safe
        to show — that is the point of them.
        """
        rendered = ", ".join(
            f"{field.name}={redact_uri(repr(getattr(self, field.name)))}"
            for field in fields(self)
            if field.repr
        )
        return f"{type(self).__name__}({rendered})"
