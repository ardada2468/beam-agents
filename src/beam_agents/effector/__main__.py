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
    EffectorConfig,
    parse_dedup_uri,
    parse_transport_uri,
)
from beam_agents.effector.dedup import build_dedup_store
from beam_agents.effector.service import EffectorService
from beam_agents.effector.sinks import build_message_sink, build_result_sink
from beam_agents.effector.sources import build_intent_source

if TYPE_CHECKING:
    from beam_agents.tools.registry import ToolRegistry

__all__ = [
    "build_parser",
    "config_from_args",
    "main",
]

_LOG = logging.getLogger("beam_agents.effector")


def _load_registry(path: str) -> ToolRegistry:
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
    """Build the CLI parser: the effector's real user-facing contract.

    Every flag maps to one :class:`EffectorConfig` field. The URI flags
    (``--intents-from``, ``--results-to``, ``--approvals-to``,
    ``--dedup``) are validated at construction, not at first use, so a
    typo fails the process rather than the first intent.
    """
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
    return parser


def config_from_args(args: argparse.Namespace) -> EffectorConfig:
    """Turn parsed arguments into a validated :class:`EffectorConfig`.

    Raises ``ValueError`` naming every missing required flag at once —
    a deployment fixes one command line, not four in sequence.
    """
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
    )


def _build_service(config: EffectorConfig, registry: ToolRegistry) -> EffectorService:
    """Construct the service and its adapters from a validated config."""
    source_scheme, source_parts = parse_transport_uri("intents_from", config.intents_from)
    results_scheme, results_parts = parse_transport_uri("results_to", config.results_to)
    approvals_scheme, approvals_parts = parse_transport_uri("approvals_to", config.approvals_to)
    dedup_scheme, dedup_parts = parse_dedup_uri("dedup", config.dedup)
    return EffectorService(
        config=config,
        registry=registry,
        source=build_intent_source(
            source_scheme, source_parts, consumer_group=config.consumer_group
        ),
        result_sink=build_result_sink(results_scheme, results_parts),
        approval_sink=build_message_sink(approvals_scheme, approvals_parts),
        dedup=build_dedup_store(dedup_scheme, dedup_parts),
    )


async def _serve(service: EffectorService) -> None:
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
    """CLI entry point: build the service from ``argv`` and serve until stopped.

    Returns the process exit status: ``2`` for a configuration or
    registry-loading error (reported to stderr without a traceback,
    since it is a user mistake), ``0`` for a clean shutdown.
    """
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper())
    try:
        config = config_from_args(args)
        registry = _load_registry(args.registry)
    except ValueError as exc:
        # Misconfiguration is a startup failure with an actionable message, not
        # a crash on the first message hours later.
        print(f"error: {exc}", file=sys.stderr)
        return 2
    asyncio.run(_build_and_serve(config, registry))
    return 0


async def _build_and_serve(config: EffectorConfig, registry: ToolRegistry) -> None:
    """Construct the service *inside* the running loop, then serve it.

    Order matters: the Kafka adapters construct aiokafka clients in their
    initializers, and aiokafka requires a running event loop at construction.
    Building as an argument to ``asyncio.run(_serve(_build_service(...)))``
    evaluates the builder before any loop exists and crashes at startup for
    every real (non-``memory://``) transport.
    """
    await _serve(_build_service(config, registry))


if __name__ == "__main__":
    raise SystemExit(main())
