"""The ASGI application: ingest in, API and UI out.

Ingest and reads are assembled here together because they share the store and
the broadcaster, but they are kept visibly separate: the read routes come from
``_api.build_router`` and cannot write, and the write routes are the four
defined here and cannot read.

Four inbound paths, of which one is not ours:

- ``POST /ingest/traces``, ``/ingest/errors``, ``/ingest/snapshots`` — the
  native encodings, which are the protos themselves.
- ``POST /v1/traces`` — the path an OTLP exporter posts to. Accepting it means a
  pipeline already configured with ``otlp://`` reaches the console by changing
  only the host, which is the cheapest possible adoption step. It is lossy on
  the way in exactly as it is on the way out (no ``ACTIVATION_START``), and
  records arriving this way are marked so the UI can say so.

Static assets resolve in a documented order — ``static_dir`` argument, then
``$BEAM_AGENTS_CONSOLE_STATIC``, then the packaged ``console/static/`` — and
when none exists the API still serves and ``/`` returns an actionable message
naming the Docker command (design D9). A ``pip install`` user gets a working API
and a pointer; a ``docker compose up`` user gets everything.

``fastapi`` and ``uvicorn`` are imported inside the functions that need them, so
``import beam_agents.console`` works with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from fastapi import FastAPI

    from beam_agents.console._sse import Broadcaster
    from beam_agents.console._store import ConsoleStore

__all__ = [
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "STATIC_DIR_ENV",
    "create_app",
    "resolve_static_dir",
    "serve",
]

# Bound to loopback by default. The console is a trusted-network tool with no
# authentication: telemetry ingest causes no side effects, so it is not signed
# the way intents are, and the compensating control is that it does not listen
# on a public interface unless asked to.
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787

STATIC_DIR_ENV = "BEAM_AGENTS_CONSOLE_STATIC"


def resolve_static_dir(static_dir: str | Path | None = None) -> Path | None:
    """Resolve the UI bundle directory, or ``None`` when no bundle is present.

    Order: the ``static_dir`` argument, then ``$BEAM_AGENTS_CONSOLE_STATIC``,
    then the packaged ``console/static/``. Returns ``None`` rather than raising:
    an API-only console is a supported state, not an error.
    """
    raise NotImplementedError


def create_app(
    store: ConsoleStore,
    *,
    static_dir: str | Path | None = None,
    broadcaster: Broadcaster | None = None,
    cors_origins: tuple[str, ...] = (),
    **options: Any,
) -> FastAPI:
    """Build the ASGI application over ``store``.

    Mounts the read API at ``/api``, the live stream at ``/api/stream``, the
    native and OTLP ingest endpoints, liveness at ``/healthz``, and the UI
    bundle at ``/`` when one is present.
    """
    raise NotImplementedError


def serve(
    *,
    database: str | Path,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    static_dir: str | Path | None = None,
    retention_hours: float | None = None,
    **options: Any,
) -> None:
    """Open the store and run the server until interrupted.

    The one-call entry point behind the ``beam-agents-console`` script. Blocks;
    returns normally on a clean shutdown.
    """
    raise NotImplementedError
