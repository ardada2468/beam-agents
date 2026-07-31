"""Console entry point: ``beam-agents-effector``.

Wires an :class:`~beam_agents.effector.service.EffectorService` from CLI flags
(each falling back to an environment variable) and runs it until the process is
signalled. The tool registry is supplied as an import path — the effector
executes *the agent's* tools, so it must load the same registry the pipeline
declares them from.

Shutdown drains in-flight work rather than dropping it: unexecuted claims are
handed back so a restarting replica does not have to wait out their leases.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import logging
import os
import signal
import sys
from typing import TYPE_CHECKING

from beam_agents.effector.config import (
    DEFAULT_LEASE_MS,
    DEFAULT_RESULT_TTL_MS,
    DEFAULT_TOOL_TIMEOUT_MS,
    VERIFICATION_MODES,
    EffectorConfig,
    TransportSecurity,
    parse_dedup_uri,
    parse_transport_uri,
    redact_uri,
)
from beam_agents.effector.dedup import build_dedup_store
from beam_agents.effector.service import EffectorService
from beam_agents.effector.sinks import build_message_sink, build_result_sink
from beam_agents.effector.sources import build_intent_source
from beam_agents.intent_signing import Keyring, load_keyring

if TYPE_CHECKING:
    from beam_agents.tools.registry import ToolRegistry

_LOG = logging.getLogger("beam_agents.effector")


def load_registry(path: str) -> ToolRegistry:
    """Import a ``module:attribute`` path and return the `ToolRegistry` it names."""
    module_name, _, attribute = path.partition(":")
    if not module_name or not attribute:
        raise ValueError(
            f"--registry must be given as 'module:attribute', got {path!r} "
            "(e.g. 'myapp.agent:TOOLS')"
        )
    module = importlib.import_module(module_name)
    try:
        registry = getattr(module, attribute)
    except AttributeError as exc:
        raise ValueError(f"module {module_name!r} has no attribute {attribute!r}") from exc
    return registry  # type: ignore[no-any-return]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="beam-agents-effector",
        description="Consume ToolIntents, dedup, execute, publish ToolResults.",
    )
    parser.add_argument(
        "--registry",
        default=os.environ.get("EFFECTOR_REGISTRY"),
        help="import path of the ToolRegistry to execute from, as module:attribute",
    )
    parser.add_argument("--intents-from", default=os.environ.get("EFFECTOR_INTENTS_FROM"))
    parser.add_argument("--results-to", default=os.environ.get("EFFECTOR_RESULTS_TO"))
    parser.add_argument("--approvals-to", default=os.environ.get("EFFECTOR_APPROVALS_TO"))
    parser.add_argument("--dedup", default=os.environ.get("EFFECTOR_DEDUP", "memory://"))
    parser.add_argument(
        "--consumer-group",
        default=os.environ.get("EFFECTOR_CONSUMER_GROUP", "beam-agents-effector"),
    )
    parser.add_argument("--lease-ms", type=int, default=DEFAULT_LEASE_MS)
    parser.add_argument("--result-ttl-ms", type=int, default=DEFAULT_RESULT_TTL_MS)
    parser.add_argument("--tool-timeout-ms", type=int, default=DEFAULT_TOOL_TIMEOUT_MS)
    parser.add_argument("--max-concurrent-partitions", type=int, default=8)
    parser.add_argument("--log-level", default=os.environ.get("EFFECTOR_LOG_LEVEL", "INFO"))
    parser.add_argument(
        "--dead-letters-to",
        default=os.environ.get("EFFECTOR_DEAD_LETTERS_TO"),
        help="channel that preserves deliveries which fail signature verification",
    )
    parser.add_argument(
        "--verify-intents",
        default=os.environ.get("EFFECTOR_VERIFY_INTENTS", "off"),
        choices=list(VERIFICATION_MODES),
        help="intent-signature verification mode; roll out off -> permissive -> require",
    )
    parser.add_argument(
        "--signing-keys",
        default=os.environ.get("EFFECTOR_SIGNING_KEYS"),
        help=(
            "reference to the 'key_id=base64(key)' signing keyring, as 'env:VAR' or "
            "'file:/path'. Never the key material itself: a flag value is visible in "
            "process listings."
        ),
    )
    # Broker transport security. Credentials are references on purpose — see
    # --signing-keys — and every one of these also reads an environment
    # variable so a deployment never has to put them on a command line.
    parser.add_argument(
        "--kafka-security-protocol", default=os.environ.get("EFFECTOR_KAFKA_SECURITY_PROTOCOL")
    )
    parser.add_argument(
        "--kafka-sasl-mechanism", default=os.environ.get("EFFECTOR_KAFKA_SASL_MECHANISM")
    )
    parser.add_argument(
        "--kafka-sasl-username-reference",
        default=os.environ.get("EFFECTOR_KAFKA_SASL_USERNAME_REFERENCE"),
    )
    parser.add_argument(
        "--kafka-sasl-password-reference",
        default=os.environ.get("EFFECTOR_KAFKA_SASL_PASSWORD_REFERENCE"),
    )
    parser.add_argument("--kafka-ssl-ca", default=os.environ.get("EFFECTOR_KAFKA_SSL_CA"))
    parser.add_argument("--kafka-ssl-cert", default=os.environ.get("EFFECTOR_KAFKA_SSL_CERT"))
    parser.add_argument("--kafka-ssl-key", default=os.environ.get("EFFECTOR_KAFKA_SSL_KEY"))
    return parser


def transport_security_from_args(args: argparse.Namespace) -> TransportSecurity | None:
    """Build the security block, or ``None`` when nothing was configured.

    ``None`` rather than an all-defaults block so an unconfigured deployment's
    clients are constructed exactly as they were before this change.
    """
    settings = {
        "security_protocol": args.kafka_security_protocol,
        "sasl_mechanism": args.kafka_sasl_mechanism,
        "sasl_username_reference": args.kafka_sasl_username_reference,
        "sasl_password_reference": args.kafka_sasl_password_reference,
        "ssl_ca_location": args.kafka_ssl_ca,
        "ssl_certificate_location": args.kafka_ssl_cert,
        "ssl_key_location": args.kafka_ssl_key,
    }
    if not any(value is not None for value in settings.values()):
        return None
    return TransportSecurity(**settings)


def config_from_args(args: argparse.Namespace) -> EffectorConfig:
    missing = [
        name
        for name in ("registry", "intents_from", "results_to", "approvals_to")
        if getattr(args, name) is None
    ]
    if missing:
        raise ValueError(
            "missing required settings: "
            + ", ".join(f"--{name.replace('_', '-')}" for name in missing)
        )
    return EffectorConfig(
        intents_from=args.intents_from,
        results_to=args.results_to,
        approvals_to=args.approvals_to,
        dedup=args.dedup,
        consumer_group=args.consumer_group,
        lease_ms=args.lease_ms,
        result_ttl_ms=args.result_ttl_ms,
        tool_timeout_ms=args.tool_timeout_ms,
        max_concurrent_partitions=args.max_concurrent_partitions,
        verify_intents=args.verify_intents,
        signing_keys=args.signing_keys,
        dead_letters_to=args.dead_letters_to,
        transport_security=transport_security_from_args(args),
    )


def load_verification_keyring(config: EffectorConfig) -> Keyring | None:
    """Resolve the signing keyring for a verifying mode, before any client exists.

    A mis-provisioned keyring drains genuine intents to the dead-letter channel,
    so it is a startup failure rather than a per-message discovery.
    """
    if config.verify_intents == "off":
        return None
    assert config.signing_keys is not None  # EffectorConfig.validate() guarantees it
    return load_keyring(config.signing_keys)


def build_service(config: EffectorConfig, registry: ToolRegistry) -> EffectorService:
    """Construct the service and its adapters from a validated config."""
    # Keys first: a verifying mode with an unresolvable keyring must fail
    # before a single client is constructed.
    keyring = load_verification_keyring(config)
    security = config.transport_security
    source_scheme, source_parts = parse_transport_uri("intents_from", config.intents_from)
    results_scheme, results_parts = parse_transport_uri("results_to", config.results_to)
    approvals_scheme, approvals_parts = parse_transport_uri("approvals_to", config.approvals_to)
    dedup_scheme, dedup_parts = parse_dedup_uri("dedup", config.dedup)
    dead_letter_sink = None
    if config.dead_letters_to is not None:
        dead_letter_scheme, dead_letter_parts = parse_transport_uri(
            "dead_letters_to", config.dead_letters_to
        )
        dead_letter_sink = build_message_sink(
            dead_letter_scheme, dead_letter_parts, security=security
        )
    return EffectorService(
        config=config,
        registry=registry,
        source=build_intent_source(
            source_scheme, source_parts, consumer_group=config.consumer_group, security=security
        ),
        result_sink=build_result_sink(results_scheme, results_parts, security=security),
        approval_sink=build_message_sink(approvals_scheme, approvals_parts, security=security),
        dedup=build_dedup_store(dedup_scheme, dedup_parts),
        keyring=keyring,
        dead_letter_sink=dead_letter_sink,
    )


async def serve(service: EffectorService) -> None:
    """Run until cancelled by a signal, then shut down cleanly."""
    loop = asyncio.get_running_loop()
    runner = asyncio.create_task(service.run())
    for signal_name in ("SIGINT", "SIGTERM"):
        with contextlib.suppress(NotImplementedError, AttributeError):
            loop.add_signal_handler(getattr(signal, signal_name), runner.cancel)
    try:
        await runner
    except asyncio.CancelledError:
        _LOG.info("shutting down: draining in-flight work and releasing unexecuted claims")
    finally:
        await service.aclose()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper())
    try:
        config = config_from_args(args)
        registry = load_registry(args.registry)
    except ValueError as exc:
        # Misconfiguration is a startup failure with an actionable message, not
        # a crash on the first message hours later. Redacted on the way out:
        # startup failure is the single most likely moment for a credentialed
        # URI to reach a terminal, a CI log, or a crash report.
        print(f"error: {redact_uri(str(exc))}", file=sys.stderr)
        return 2
    asyncio.run(_build_and_serve(config, registry))
    return 0


async def _build_and_serve(config: EffectorConfig, registry: ToolRegistry) -> None:
    """Construct the service *inside* the running loop, then serve it.

    Order matters: the Kafka adapters construct aiokafka clients in their
    initializers, and aiokafka requires a running event loop at construction.
    Building as an argument to ``asyncio.run(serve(build_service(...)))``
    evaluates the builder before any loop exists and crashes at startup for
    every real (non-``memory://``) transport.
    """
    await serve(build_service(config, registry))


if __name__ == "__main__":
    raise SystemExit(main())
