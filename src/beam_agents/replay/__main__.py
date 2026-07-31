"""Console entry point: ``beam-agents-replay``.

Loads a `StateSnapshot`, a framed `TraceEvent` stream, and the triggering
`AgentEnvelope` off disk; imports *the operator's* agent (and optionally its
tool registry and provider decoder) by ``module:attribute`` path; re-runs the
activation locally against a provider that can serve nothing; and reports
whether the re-run reproduced the traced record.

The exit codes make it scriptable:

===  =========================================================================
0    reproduced — the re-run matched the traced record
1    diverged — a structured diff is printed to stdout
2    usage or configuration error, including a schema version this package
     cannot interpret (guessing forward is how silent corruption happens)
3    irreproducible — a cache miss, a digest-only entry, or a migration chain
     with a gap: the bundle lacks an input the re-run needs
===  =========================================================================

The CLI reads local files by design: fetching bytes out of a topic or a table is
existing operator tooling, and building fetchers is an explicit non-goal. See
``docs/replay.md`` for the end-to-end workflow.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Any

from beam_agents.replay.bundle import (
    ReplayIrreproducibleError,
    ReplayUsageError,
    build_bundle,
    load_envelope,
    load_snapshot,
    parse_trace_stream,
    run_replay,
)
from beam_agents.replay.diff import compare

if TYPE_CHECKING:
    from beam_agents.replay.bundle import ReplayBundle

EXIT_REPRODUCED = 0
EXIT_DIVERGED = 1
EXIT_USAGE = 2
EXIT_IRREPRODUCIBLE = 3

__all__ = [
    "EXIT_DIVERGED",
    "EXIT_IRREPRODUCIBLE",
    "EXIT_REPRODUCED",
    "EXIT_USAGE",
    "build_parser",
    "import_object",
    "main",
]


def import_object(path: str, *, flag: str) -> Any:
    """Import a ``module:attribute`` path and return what it names.

    The same shape as the effector's ``load_registry``, with the flag named in
    the error so a typo says which argument to fix.
    """
    module_name, _, attribute = path.partition(":")
    if not module_name or not attribute:
        raise ValueError(
            f"{flag} must be given as 'module:attribute', got {path!r} (e.g. 'myapp.agent:AGENT')"
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValueError(f"{flag}: could not import module {module_name!r}: {exc}") from exc
    try:
        return getattr(module, attribute)
    except AttributeError as exc:
        raise ValueError(f"module {module_name!r} has no attribute {attribute!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="beam-agents-replay",
        description=(
            "Reconstruct one activation from a StateSnapshot plus its trace and "
            "re-run it locally against the snapshot's replay cache."
        ),
    )
    parser.add_argument("--snapshot", required=True, help="path to a serialized StateSnapshot")
    parser.add_argument(
        "--traces",
        required=True,
        help="path to a varint-length-delimited TraceEvent stream",
    )
    parser.add_argument(
        "--event",
        required=True,
        help="path to the triggering AgentEnvelope, fetched off the events bus",
    )
    parser.add_argument(
        "--agent", required=True, help="import path of the agent, as module:attribute"
    )
    parser.add_argument(
        "--registry",
        default=None,
        help="import path of the ToolRegistry read-only tools run from, as module:attribute",
    )
    parser.add_argument(
        "--decode",
        default=None,
        help="import path of the provider's response decoder, as module:attribute",
    )
    parser.add_argument(
        "--seq",
        type=int,
        default=None,
        help="activation seq to replay (default: the highest seq in the trace stream)",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def _read(path: str, *, flag: str) -> bytes:
    try:
        return Path(path).read_bytes()
    except OSError as exc:
        raise ValueError(f"{flag}: could not read {path}: {exc}") from exc


def _load_bundle(args: argparse.Namespace) -> ReplayBundle:
    return build_bundle(
        snapshot=load_snapshot(_read(args.snapshot, flag="--snapshot")),
        traces=parse_trace_stream(_read(args.traces, flag="--traces")),
        envelope=load_envelope(_read(args.event, flag="--event")),
        seq=args.seq,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper())
    try:
        agent = import_object(args.agent, flag="--agent")
        registry = (
            import_object(args.registry, flag="--registry") if args.registry is not None else None
        )
        decode = import_object(args.decode, flag="--decode") if args.decode is not None else None
        bundle = _load_bundle(args)
    except (ValueError, ReplayUsageError) as exc:
        # Misconfiguration and unreadable or uninterpretable input are startup
        # failures with actionable messages, not crashes mid-replay.
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    # The header, before any verdict: replay runs *the operator's* code, and
    # skew between the binary that ran in the pipeline and the one imported here
    # is a real divergence source, so both are named up front.
    print(f"beam-agents-replay {_package_version()}")
    print(f"  agent:  {args.agent}")
    print(f"  key:    {bundle.entity_key.hex()}  seq: {bundle.seq}  now_ms: {bundle.now_ms}")
    print(f"  kind:   {'resume' if bundle.is_resume else 'start'}")

    try:
        outcome = run_replay(bundle, agent, tool_registry=registry, decode=decode)
    except ReplayIrreproducibleError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_IRREPRODUCIBLE
    except ReplayUsageError as exc:  # pragma: no cover - defensive
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    report = compare(bundle, outcome)
    print(report.render())
    return EXIT_REPRODUCED if report.reproduced else EXIT_DIVERGED


def _package_version() -> str:
    try:
        return version("beam-agents")
    except PackageNotFoundError:  # pragma: no cover - editable/source checkouts
        return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
