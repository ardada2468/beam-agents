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

Every one of them decodes with an ``_ingest`` decoder and normalizes through
``_ingest.normalize``; no route builds a row itself (design D7). Decoding also
completes *before* the store is touched, which is what makes "a malformed
payload writes nothing" a property of the shape of these handlers rather than a
promise about the store's transaction handling.

Static assets resolve in a documented order — ``static_dir`` argument, then
``$BEAM_AGENTS_CONSOLE_STATIC``, then the packaged ``console/static/`` — and
when none exists the API still serves and ``/`` returns an actionable message
naming the Docker command (design D9). A ``pip install`` user gets a working API
and a pointer; a ``docker compose up`` user gets everything.

``fastapi``, ``sse_starlette`` and ``uvicorn`` are imported inside the functions
that need them, so ``import beam_agents.console`` works with no extras
installed.

Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from google.protobuf.message import DecodeError

from beam_agents.console import _api, _ingest
from beam_agents.console._dto import Health
from beam_agents.console._records import PROVENANCE_NATIVE, PROVENANCE_OTLP
from beam_agents.console._schema import SCHEMA_VERSION
from beam_agents.console._sse import Broadcaster

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from fastapi import FastAPI

    from beam_agents.console._records import RecordBatch
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

_LOG = logging.getLogger("beam_agents.console")

# How often the retention sweep runs. Retention is measured in hours, so an
# hourly sweep is as precise as the setting it enforces; anything faster is
# repeated work against a store a pipeline is writing.
_PRUNE_INTERVAL_S = 3600.0


def _now_ms() -> int:
    """Wall-clock milliseconds, read only by the retention sweep.

    Nothing that derives a record reads a clock — every timestamp in the store
    came from the runtime's own injected clock. Retention is the one thing that
    is genuinely about now.
    """
    return int(time.time() * 1000)


# The bundle hatchling ships *if present* (design D9). A module constant rather
# than an expression buried in the resolver, so a test can point it somewhere
# known — otherwise "is there a packaged bundle?" answers differently on a clean
# checkout and on a machine where the frontend has been built.
PACKAGED_STATIC = Path(__file__).resolve().parent / "static"

# The path the runtime's `otlp://` sink posts to — `core/transform.py` appends
# it, and the URI grammar refuses to let anyone spell it — and the one encoding
# `WriteTracesToOtlp` sends.
OTLP_TRACES_PATH = "/v1/traces"
OTLP_CONTENT_TYPE = "application/x-protobuf"

# What `/` says when there is no bundle. The API is fully functional in this
# state, so this is a pointer, not an error page.
_NO_BUNDLE_MESSAGE = """\
The Beam Agents Console API is running. No UI bundle is installed.

The UI is a build artifact, not source: a wheel ships it only when the frontend
was built into the package first, so `pip install beam-agents[console]` gets the
API and this message. To get the UI as well:

    docker compose -f docker/compose.console.yaml up

Or build it yourself with `make console-build`, then start the console with
--static-dir=<path> or set BEAM_AGENTS_CONSOLE_STATIC=<path>.

The API is unaffected:

    GET  /healthz          liveness
    GET  /api/overview     the read API
    GET  /api/stream       the live event stream
    POST /ingest/traces    native trace ingest
    POST /v1/traces        OTLP trace ingest
"""

# `_ingest`'s decoders raise `TruncatedStreamError`, a ValueError; the protobuf
# parsers underneath them raise `DecodeError`, which is not one.
_DECODE_FAILURES = (ValueError, DecodeError)

_Decoded = TypeVar("_Decoded")


def resolve_static_dir(static_dir: str | Path | None = None) -> Path | None:
    """Resolve the UI bundle directory, or ``None`` when no bundle is present.

    Order: the ``static_dir`` argument, then ``$BEAM_AGENTS_CONSOLE_STATIC``,
    then the packaged ``console/static/``. Returns ``None`` rather than raising:
    an API-only console is a supported state, not an error.

    A candidate counts only when it holds an ``index.html``. The bundle is a
    single-page app served from that file, so a directory without one is a
    directory, not a bundle — and falling through to the pointer at ``/`` is
    more use than mounting something that answers every request with a 404.
    """
    candidates: list[Path] = []
    if static_dir:
        candidates.append(Path(static_dir))
    from_env = os.environ.get(STATIC_DIR_ENV)
    if from_env:
        candidates.append(Path(from_env))
    candidates.append(PACKAGED_STATIC)
    for candidate in candidates:
        if (candidate / "index.html").is_file():
            return candidate
    return None


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

    The only extra option is ``sources``: labels for the ingest sources the
    caller wired up, reported by ``/healthz``. Anything else is a typo, and is
    named rather than silently ignored.
    """
    from fastapi import FastAPI, HTTPException  # noqa: PLC0415
    from fastapi.responses import JSONResponse, PlainTextResponse, Response  # noqa: PLC0415
    from starlette.routing import Route  # noqa: PLC0415

    sources = tuple(options.pop("sources", ()))
    if options:
        raise TypeError(
            "create_app() got unexpected keyword argument(s): "
            f"{', '.join(repr(name) for name in sorted(options))}"
        )

    feed = broadcaster if broadcaster is not None else Broadcaster()
    static = resolve_static_dir(static_dir)
    app = FastAPI(title="Beam Agents Console", docs_url=None, redoc_url=None)

    if cors_origins:
        # Same-origin is the normal deployment — the bundle is served from this
        # app — so this exists for the Vite dev server on another port, and stays
        # off unless a caller asks for it.
        from fastapi.middleware.cors import CORSMiddleware  # noqa: PLC0415

        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(cors_origins),
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
        )

    # Plain Starlette routes, not FastAPI ones. There is no model to validate
    # here — the payloads are protobuf bytes, and the decoders own what they
    # mean — and FastAPI's dependency injection resolves parameter annotations
    # against *module* globals, which under `from __future__ import annotations`
    # a lazily-imported `Request` is not in. A body param that silently degrades
    # into a query param is the failure mode; sidestepping introspection is both
    # the fix and the honest description of these endpoints.
    async def ingest_traces(request: Any) -> Any:
        """Ingest a varint-length-delimited ``TraceEvent`` stream."""
        events = _decode(_ingest.decode_trace_stream, await request.body(), what="traces")
        batch = _ingest.normalize(events=events, provenance=PROVENANCE_NATIVE)
        return JSONResponse(await _commit(store, feed, batch))

    async def ingest_errors(request: Any) -> Any:
        """Ingest ``ActivationErrorRecord``s, bare or ``AgentEnvelope``-wrapped."""
        errors = _decode(_ingest.decode_error_payload, await request.body(), what="errors")
        batch = _ingest.normalize(errors=errors, provenance=PROVENANCE_NATIVE)
        return JSONResponse(await _commit(store, feed, batch))

    async def ingest_snapshots(request: Any) -> Any:
        """Ingest serialized ``StateSnapshot`` messages."""
        snapshots = _decode(_ingest.decode_snapshot_payload, await request.body(), what="snapshots")
        batch = _ingest.normalize(snapshots=snapshots, provenance=PROVENANCE_NATIVE)
        return JSONResponse(await _commit(store, feed, batch))

    async def ingest_otlp(request: Any) -> Any:
        """Accept an OTLP ``ExportTraceServiceRequest``, as a collector would."""
        media_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
        if media_type and media_type != OTLP_CONTENT_TYPE:
            raise HTTPException(
                status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                detail=(
                    f"{OTLP_TRACES_PATH} accepts OTLP traces encoded as "
                    f"{OTLP_CONTENT_TYPE}, got {media_type!r}"
                ),
            )
        events = _decode(_ingest.decode_otlp_request, await request.body(), what="OTLP traces")
        # Marked lossy at the door: OTLP carries no ACTIVATION_START, so an
        # activation assembled from these events cannot say start-vs-resume, and
        # the UI keys that warning off the provenance stamped here.
        batch = _ingest.normalize(events=events, provenance=PROVENANCE_OTLP)
        await _commit(store, feed, batch)
        # An empty body is a valid and complete `ExportTraceServiceResponse` —
        # no `partial_success` means everything was accepted — so a real OTLP
        # client parses this instead of choking on JSON it never asked for.
        return Response(content=b"", media_type=OTLP_CONTENT_TYPE)

    app.router.routes.extend(
        [
            Route("/ingest/traces", ingest_traces, methods=["POST"]),
            Route("/ingest/errors", ingest_errors, methods=["POST"]),
            Route("/ingest/snapshots", ingest_snapshots, methods=["POST"]),
            Route(OTLP_TRACES_PATH, ingest_otlp, methods=["POST"]),
        ]
    )
    # No `prefix=` here: `build_router` already carries `/api`. Adding it again
    # puts every read route at `/api/api/...`, where nothing 404s — the SPA
    # mount at `/` answers `/api/overview` with `index.html` and a 200, so the
    # whole read API silently returns HTML.
    app.include_router(_api.build_router(store))

    @app.get("/api/stream", response_model=None)
    async def stream() -> Any:
        """Stream one event per ingested batch, as server-sent events."""
        from sse_starlette.sse import EventSourceResponse  # noqa: PLC0415

        async def frames() -> AsyncIterator[dict[str, str]]:
            async for event in feed.subscribe():
                yield {"data": json.dumps(event.to_dict(), separators=(",", ":"))}

        return EventSourceResponse(frames())

    # Built once: every field is fixed at construction, and reading the
    # installed distribution's metadata per request would put a filesystem
    # lookup on the path a container healthcheck polls every few seconds.
    health = Health(
        version=_package_version(),
        schema_version=SCHEMA_VERSION,
        ui_bundled=static is not None,
        sources=list(sources),
    )

    @app.get("/healthz")
    async def healthz() -> Health:
        """Report liveness, answerable before any record has been ingested."""
        return health

    # Registered last, and only now: a mount at "/" matches every path, and
    # Starlette resolves routes in registration order, so anything added after
    # it would be unreachable.
    if static is not None:
        _mount_ui(app, static)
    else:

        @app.get("/", response_class=PlainTextResponse)
        async def index() -> str:
            """Say how to get the UI, without pretending its absence is an error."""
            return _NO_BUNDLE_MESSAGE

    return app


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
    returns normally on a clean shutdown, with the store closed.

    This is where the pull sources and the retention sweep live, because both
    need the store and the store is opened here. The CLI resolves and validates
    configuration; it constructs nothing, precisely so that a source's lifetime
    is bounded by the server's rather than by a caller remembering to stop it.
    Every option beyond those consumed here is forwarded to :func:`create_app`,
    which names any it does not recognize.
    """
    import uvicorn  # noqa: PLC0415

    from beam_agents.console._store import ConsoleStore  # noqa: PLC0415

    log_level = str(options.pop("log_level", "info"))
    kafka_uri = options.pop("kafka_traces_from", None)
    kafka_from_beginning = bool(options.pop("kafka_from_beginning", False))
    bigquery_uri = options.pop("bigquery_traces_from", None)
    import_traces = options.pop("import_traces", None)
    import_snapshot = options.pop("import_snapshot", None)

    with ConsoleStore(database, retention_hours=retention_hours) as store:
        labels: list[str] = []
        # The import is one-shot and finite, so it runs before the port opens:
        # a console started to look at a captured run should be showing it by
        # the time the first request arrives, not filling in underneath one.
        if import_traces is not None or import_snapshot is not None:
            from beam_agents.console._sources._bundle import import_bundle  # noqa: PLC0415

            result = import_bundle(store, traces=import_traces, snapshot=import_snapshot)
            _LOG.info("imported bundle: %s", result.detail or result)
            labels.append("bundle")

        if kafka_uri:
            labels.append("kafka")
        if bigquery_uri:
            labels.append("bigquery")

        app = create_app(store, static_dir=static_dir, sources=tuple(labels), **options)
        _attach_lifecycle(
            app,
            store,
            kafka_uri=kafka_uri,
            kafka_from_beginning=kafka_from_beginning,
            bigquery_uri=bigquery_uri,
        )
        uvicorn.run(app, host=host, port=port, log_level=log_level)


def _attach_lifecycle(
    app: FastAPI,
    store: ConsoleStore,
    *,
    kafka_uri: str | None,
    kafka_from_beginning: bool,
    bigquery_uri: str | None,
) -> None:
    """Run the pull sources and the retention sweep for the server's lifetime.

    Each background task is cancelled on shutdown and awaited, so a stopped
    console leaves no consumer holding a broker connection.

    A source that fails to start is logged and skipped rather than taken as
    fatal. The console's own records are already in the store; refusing to serve
    them because a broker is unreachable would withhold exactly the history
    someone opened the console to read.
    """
    import asyncio  # noqa: PLC0415
    import contextlib  # noqa: PLC0415

    tasks: list[asyncio.Task[None]] = []

    async def _prune_loop() -> None:
        """Sweep expired records hourly, on the store's own clock."""
        while True:
            await asyncio.sleep(_PRUNE_INTERVAL_S)
            removed = await asyncio.to_thread(store.prune, now_ms=_now_ms())
            if removed:
                _LOG.info("retention: pruned %d rows", removed)

    @app.on_event("startup")
    async def _start() -> None:
        if kafka_uri:
            from beam_agents.console._sources._kafka import KafkaTraceSource  # noqa: PLC0415

            try:
                source = KafkaTraceSource(kafka_uri, store, from_beginning=kafka_from_beginning)
            except Exception:
                _LOG.exception("kafka source not started: %s", kafka_uri)
            else:
                tasks.append(asyncio.create_task(source.run()))

        if bigquery_uri:
            from beam_agents.console._sources._bigquery import (  # noqa: PLC0415
                BigQueryTraceSource,
            )

            try:
                reader = BigQueryTraceSource(bigquery_uri, store)
            except Exception:
                _LOG.exception("bigquery source not started: %s", bigquery_uri)
            else:
                tasks.append(asyncio.create_task(reader.run()))

        if store.retention_hours is not None:
            tasks.append(asyncio.create_task(_prune_loop()))

    @app.on_event("shutdown")
    async def _stop() -> None:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        tasks.clear()


# -- internals -----------------------------------------------------------------


def _package_version() -> str:
    """Report the installed version, or ``unknown`` outside an installed tree."""
    from importlib.metadata import PackageNotFoundError, version  # noqa: PLC0415

    try:
        return version("beam-agents")
    except PackageNotFoundError:  # pragma: no cover - editable/source checkouts
        return "unknown"


def _decode(
    decoder: Callable[[bytes], tuple[_Decoded, ...]], payload: bytes, *, what: str
) -> tuple[_Decoded, ...]:
    """Decode, or fail the request naming what was rejected and why.

    A client error, not a server one: no retry makes the console understand
    these bytes. Nothing has been written when this raises, and nothing will be
    — the caller has not reached the store yet.
    """
    from fastapi import HTTPException  # noqa: PLC0415

    try:
        return decoder(payload)
    except _DECODE_FAILURES as exc:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST, detail=f"malformed {what} payload: {exc}"
        ) from exc


async def _commit(store: ConsoleStore, feed: Broadcaster, batch: RecordBatch) -> dict[str, int]:
    """Write one batch and announce it; report what arrived and what changed.

    ``written`` is legitimately smaller than ``accepted``: ingest is idempotent
    on ``(trace_id, span_id, event_type)``, so a retried POST or a replayed run
    changes zero rows, and that is the correct answer rather than a failure.

    The write goes to a thread because it is SQLite — blocking the event loop on
    it would stall the live stream for every connected tab.
    """
    if not batch:
        return {"accepted": 0, "written": 0}
    written = await asyncio.to_thread(store.write, batch)
    feed.publish_batch(batch)
    return {"accepted": len(batch), "written": written}


def _mount_ui(app: FastAPI, static: Path) -> None:
    """Mount the bundle at ``/``, resolving unknown paths to the SPA entry.

    The UI routes on the path (``wouter``), so ``/activations/orders%2F7/3`` is
    a bookmark someone will paste back in. A plain static mount answers that
    with 404 because no such file exists; the app itself has to be handed the
    URL, which is what falling back to ``index.html`` does.
    """
    from fastapi.staticfiles import StaticFiles  # noqa: PLC0415
    from starlette.exceptions import HTTPException as StarletteHTTPException  # noqa: PLC0415

    class _SinglePageFiles(StaticFiles):
        # `scope` and the return are Starlette types this module must not import
        # at module scope, and the override is a pass-through, so both are Any.
        async def get_response(self, path: str, scope: Any) -> Any:
            try:
                return await super().get_response(path, scope)
            except StarletteHTTPException as exc:
                if exc.status_code != HTTPStatus.NOT_FOUND:
                    raise
                # An API path that reaches here is a routing mistake, and
                # answering it with the SPA turns that into a 200 full of HTML —
                # which is what a double `/api` prefix once did to every read
                # route at once. Let it 404 so the mistake is visible.
                if path.startswith(("api/", "ingest/")) or path == "api":
                    raise
                return await super().get_response("index.html", scope)

    app.mount("/", _SinglePageFiles(directory=static, html=True), name="ui")
