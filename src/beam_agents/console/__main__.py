"""The ``beam-agents-console`` entry point.

Follows the effector CLI's conventions exactly (``effector/__main__.py``): a
single long-running service rather than a command tree, argparse with every flag
falling back to an environment variable, exit ``2`` on a configuration error
reported without a traceback because it is a user mistake, and exit ``0`` on
clean shutdown.

The only required argument is a database path, and with nothing else supplied
the console runs with no broker, no cloud project, and no network egress. Every
ingest source beyond the HTTP endpoints is opt-in, because "start it and look at
it" has to be the shortest possible command.

Three decisions are load-bearing:

- **Validation imports no client library.** The ingest URIs are parsed here with
  ``urlparse`` against the same grammar ``DefaultSinkResolver`` publishes, the
  discipline ``SinkResolver.validate`` follows: a typo in ``--kafka-traces-from``
  must be rejected on a machine that has never installed ``aiokafka``, and a
  console started with no ingest source must not import a broker client at all.
- **Everything rejected is named and redacted.** Startup failure is the single
  most likely moment for a credentialed URI to reach a terminal or a CI log, so
  every message out of here goes through ``redact_uri`` — applied to the whole
  message rather than to bare URIs, so an interpolation site added later is
  redacted by default rather than by remembering to.
- **The store is opened by ``serve``, not here.** Ingest sources need a store,
  so this module resolves and validates their configuration and hands it to
  ``serve`` as keywords; it constructs none of them. That keeps the CLI free of
  every optional dependency and keeps store lifetime in one place.

Importing this module has no side effects.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from beam_agents.console._app import DEFAULT_HOST, DEFAULT_PORT, STATIC_DIR_ENV, serve
from beam_agents.effector.config import redact_uri

__all__ = [
    "DEFAULT_DATABASE",
    "build_parser",
    "main",
]

# Under the working directory rather than a temp path: a console's whole value
# is that the run you are investigating is still there tomorrow.
DEFAULT_DATABASE = "beam-agents-console.db"

_LOG = logging.getLogger("beam_agents.console")

# One prefix for every variable, so a container configures the console without
# ever composing a command line. `--static-dir` is the exception: it falls back
# to the variable `_app.resolve_static_dir` already documents, because two names
# for one bundle directory is a resolution order nobody can predict.
_ENV_PREFIX = "BEAM_AGENTS_CONSOLE_"

# The URI grammars the runtime already publishes, as (rendered form, path
# segment count). Copied rather than imported: `core.transform` imports Beam and
# raises `UnknownSinkSchemeError` mentioning every scheme it knows, which is the
# wrong vocabulary for a flag that accepts exactly one of them.
_URI_GRAMMAR = {
    "kafka": ("kafka://<bootstrap-servers>/<topic>", 1),
    "bigquery": ("bigquery://<project>/<dataset>/<table>", 2),
}

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

_MAX_PORT = 65535


class _ConfigError(ValueError):
    """A flag or environment variable the console cannot start with.

    A ``ValueError`` so it reads as the user mistake it is, and so ``main``
    catches it alongside the ones the standard library raises.
    """


def _env(name: str, default: str | None = None) -> str | None:
    """Read ``BEAM_AGENTS_CONSOLE_<name>``, falling back to ``default``."""
    return os.environ.get(f"{_ENV_PREFIX}{name}", default)


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Flags: ``--db``, ``--host``, ``--port``, ``--static-dir``,
    ``--retention-hours``, ``--kafka-traces-from``, ``--kafka-from-beginning``,
    ``--bigquery-traces-from``, ``--import-traces``, ``--import-snapshot``,
    ``--cors-origin``, and ``--log-level``. Each falls back to a
    ``BEAM_AGENTS_CONSOLE_*`` environment variable.

    Total by construction: the defaults are read here, but nothing is rejected
    here beyond argparse's own type conversion, so every actionable failure is
    raised from ``_serve_options`` where ``main`` can report it without a
    traceback.
    """
    parser = argparse.ArgumentParser(
        prog="beam-agents-console",
        description=(
            "Serve a local console over the trace, error, and snapshot records "
            "the runtime emits. Needs no broker and no cloud project."
        ),
        epilog=(
            "Every flag falls back to a BEAM_AGENTS_CONSOLE_* environment "
            "variable (--static-dir falls back to BEAM_AGENTS_CONSOLE_STATIC)."
        ),
    )
    parser.add_argument(
        "--db",
        default=_env("DB", DEFAULT_DATABASE),
        metavar="PATH",
        help="SQLite database file; created if absent (default: %(default)s)",
    )
    parser.add_argument(
        "--host",
        default=_env("HOST", DEFAULT_HOST),
        help="interface to bind; loopback by default, as the console has no auth",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_env("PORT", str(DEFAULT_PORT)),
        help="port to bind (default: %(default)s)",
    )
    parser.add_argument(
        "--static-dir",
        default=os.environ.get(STATIC_DIR_ENV),
        metavar="PATH",
        help="built UI bundle to serve at /; the packaged bundle is used when unset",
    )
    parser.add_argument(
        "--retention-hours",
        type=float,
        default=_env("RETENTION_HOURS"),
        metavar="HOURS",
        help="prune records older than this window; unbounded when unset",
    )
    parser.add_argument(
        "--kafka-traces-from",
        default=_env("KAFKA_TRACES_FROM"),
        metavar="URI",
        help="consume trace events from kafka://<bootstrap-servers>/<topic>",
    )
    # `None` rather than `False` so an absent flag is distinguishable from an
    # explicit one, which is what lets the environment variable be the fallback
    # rather than a value the flag can only ever agree with.
    parser.add_argument(
        "--kafka-from-beginning",
        action="store_true",
        default=None,
        help="start the consumer at the earliest offset instead of the end",
    )
    parser.add_argument(
        "--bigquery-traces-from",
        default=_env("BIGQUERY_TRACES_FROM"),
        metavar="URI",
        help="read trace events from bigquery://<project>/<dataset>/<table>",
    )
    parser.add_argument(
        "--import-traces",
        default=_env("IMPORT_TRACES"),
        metavar="PATH",
        help="import a captured trace stream (the file beam-agents-replay reads)",
    )
    parser.add_argument(
        "--import-snapshot",
        default=_env("IMPORT_SNAPSHOT"),
        metavar="PATH",
        help="import a captured StateSnapshot",
    )
    parser.add_argument(
        "--cors-origin",
        action="append",
        default=None,
        metavar="ORIGIN",
        help=(
            "allow browser requests from this origin; repeatable. Falls back to "
            "a comma-separated BEAM_AGENTS_CONSOLE_CORS_ORIGIN."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=_env("LOG_LEVEL", "INFO"),
        help="root logging level (default: %(default)s)",
    )
    return parser


def _validated_log_level(value: str) -> str:
    """Normalize a logging level name, rejecting one ``logging`` does not know."""
    level = value.strip().upper()
    known = logging.getLevelNamesMapping()
    if level not in known:
        raise _ConfigError(
            f"--log-level: {value!r} is not a logging level; "
            f"expected one of {sorted(name for name in known if not name.isdigit())}"
        )
    return level


def _validated_database(value: str) -> Path:
    """Resolve the database path, rejecting one the store could not open.

    Checked rather than created: naming the rejected path is the contract, and a
    console that silently invents a directory hides the typo that produced it.
    """
    path = Path(value).expanduser()
    if path.is_dir():
        raise _ConfigError(f"--db: {str(path)!r} is a directory, not a database file")
    if path.exists():
        if not os.access(path, os.W_OK):
            raise _ConfigError(f"--db: {str(path)!r} exists and is not writable")
        return path
    parent = path.parent
    if not parent.is_dir():
        raise _ConfigError(
            f"--db: cannot create {str(path)!r}: {str(parent)!r} is not an existing directory"
        )
    if not os.access(parent, os.W_OK | os.X_OK):
        raise _ConfigError(f"--db: cannot create {str(path)!r}: {str(parent)!r} is not writable")
    return path


def _validated_uri(flag: str, uri: str | None, scheme: str) -> str | None:
    """Validate an ingest URI against its published grammar, importing nothing.

    The client library for a source is imported inside that source's
    constructor, so this has to be a pure parse: a malformed
    ``--kafka-traces-from`` is rejected identically whether or not ``aiokafka``
    is installed.
    """
    if uri is None:
        return None
    expected, segment_count = _URI_GRAMMAR[scheme]
    parsed = urlparse(uri)
    if parsed.scheme != scheme:
        raise _ConfigError(
            f"{flag}: rejected {redact_uri(uri)!r}: not a {scheme}:// URI; expected {expected}"
        )
    segments = [segment for segment in parsed.path.split("/") if segment]
    if not parsed.netloc or len(segments) != segment_count:
        raise _ConfigError(
            f"{flag}: rejected {redact_uri(uri)!r}: malformed {scheme} URI; expected {expected}"
        )
    return uri


def _validated_input_file(flag: str, value: str | None) -> Path | None:
    """Resolve a capture file to import, rejecting one that cannot be read."""
    if value is None:
        return None
    path = Path(value).expanduser()
    if not path.is_file():
        raise _ConfigError(f"{flag}: {str(path)!r} is not an existing file")
    if not os.access(path, os.R_OK):
        raise _ConfigError(f"{flag}: {str(path)!r} is not readable")
    return path


def _validated_port(port: int) -> int:
    """Reject a port number outside the range a socket can bind."""
    if not 0 < port <= _MAX_PORT:
        raise _ConfigError(f"--port: {port} is not a port number; expected 1..{_MAX_PORT}")
    return port


def _validated_retention(hours: float | None) -> float | None:
    """Reject a retention window that would prune everything on the first sweep."""
    if hours is not None and hours <= 0:
        raise _ConfigError(
            f"--retention-hours: {hours} is not a window; expected a positive number "
            "of hours, or omit the flag to retain everything"
        )
    return hours


def _flag_or_env(value: bool | None, name: str) -> bool:
    """Resolve a store_true flag against its environment variable.

    An unset flag consults the variable; an unparseable variable is a
    configuration error rather than a silent ``False``, because a deployment
    that typed ``flase`` would otherwise never learn its setting was ignored.
    """
    if value is not None:
        return value
    raw = _env(name)
    if raw is None:
        return False
    normalized = raw.strip().lower()
    # An empty variable is off: `FOO=` in a compose file means "unset it", not
    # "set it to something unparseable".
    if not normalized or normalized in _FALSE_VALUES:
        return False
    if normalized in _TRUE_VALUES:
        return True
    raise _ConfigError(
        f"{_ENV_PREFIX}{name}: {raw!r} is not a boolean; "
        f"expected one of {sorted(_TRUE_VALUES | _FALSE_VALUES)}"
    )


def _cors_origins(values: list[str] | None) -> tuple[str, ...]:
    """Resolve the repeatable origin flag, falling back to a comma-separated list.

    Flags *replace* the variable rather than extending it: a container that sets
    a default origin must still be overridable from a command line.
    """
    if values is None:
        raw = _env("CORS_ORIGIN")
        values = [] if raw is None else raw.split(",")
    return tuple(origin.strip() for origin in values if origin.strip())


def _serve_options(args: argparse.Namespace) -> dict[str, Any]:
    """Turn parsed arguments into ``serve``'s keywords, validating every one.

    Raises :class:`_ConfigError` naming the rejected flag or variable. This is
    the whole configuration contract between the CLI and the service: the CLI
    resolves and validates, ``serve`` opens the store and wires the sources.
    """
    return {
        "database": _validated_database(args.db),
        "host": args.host,
        "port": _validated_port(args.port),
        "static_dir": args.static_dir,
        "retention_hours": _validated_retention(args.retention_hours),
        "cors_origins": _cors_origins(args.cors_origin),
        "kafka_traces_from": _validated_uri("--kafka-traces-from", args.kafka_traces_from, "kafka"),
        "kafka_from_beginning": _flag_or_env(args.kafka_from_beginning, "KAFKA_FROM_BEGINNING"),
        "bigquery_traces_from": _validated_uri(
            "--bigquery-traces-from", args.bigquery_traces_from, "bigquery"
        ),
        "import_traces": _validated_input_file("--import-traces", args.import_traces),
        "import_snapshot": _validated_input_file("--import-snapshot", args.import_snapshot),
        "log_level": _validated_log_level(args.log_level),
    }


def main(argv: list[str] | None = None) -> int:
    """Start the console and serve until stopped.

    Returns ``2`` for a configuration error — a malformed ingest URI, an
    unwritable database path — reported to stderr with the rejected value named
    and any credentials redacted, and ``0`` for a clean shutdown.
    """
    args = build_parser().parse_args(argv)
    try:
        options = _serve_options(args)
    except ValueError as exc:
        # A user mistake, not a crash: an actionable line on stderr and no
        # traceback, redacted on the way out.
        print(f"error: {redact_uri(str(exc))}", file=sys.stderr)
        return 2
    logging.basicConfig(level=options["log_level"])
    # `basicConfig` does nothing once the root logger has a handler, and
    # importing this package imports Beam, which logs at import time and
    # installs one. Setting the level explicitly is what makes `--log-level`
    # mean anything at all here.
    logging.getLogger().setLevel(options["log_level"])
    _LOG.info(
        "serving on http://%s:%s over %s", options["host"], options["port"], options["database"]
    )
    try:
        serve(**options)
    except KeyboardInterrupt:
        # SIGINT reaches a blocking server as KeyboardInterrupt. A signalled
        # shutdown is the normal way this process ends, so it exits 0 and a
        # supervisor sees a stop rather than a crash loop.
        _LOG.info("shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
