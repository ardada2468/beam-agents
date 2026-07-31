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

Importing this module has no side effects.
"""

from __future__ import annotations

import argparse

__all__ = [
    "DEFAULT_DATABASE",
    "build_parser",
    "main",
]

# Under the working directory rather than a temp path: a console's whole value
# is that the run you are investigating is still there tomorrow.
DEFAULT_DATABASE = "beam-agents-console.db"


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Flags: ``--db``, ``--host``, ``--port``, ``--static-dir``,
    ``--retention-hours``, ``--kafka-traces-from``, ``--kafka-from-beginning``,
    ``--bigquery-traces-from``, ``--import-traces``, ``--import-snapshot``,
    ``--cors-origin``, and ``--log-level``. Each falls back to a
    ``BEAM_AGENTS_CONSOLE_*`` environment variable.
    """
    raise NotImplementedError


def main(argv: list[str] | None = None) -> int:
    """Start the console and serve until stopped.

    Returns ``2`` for a configuration error — a malformed ingest URI, an
    unwritable database path — reported to stderr with the rejected value named
    and any credentials redacted, and ``0`` for a clean shutdown.
    """
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
