"""The read API's route table: a thin mapping onto ``_queries``.

Deliberately thin. Every question lives in ``_queries.py`` where it can be
tested without an HTTP client; this module turns request parameters into a
filter object, calls one query, and returns its model. A route that grows logic
belongs in the query layer instead.

Read-only with respect to agent state: nothing here writes to a running
pipeline, and there is no endpoint that could. Ingest lives in ``_app.py``,
separately, so the distinction is visible in the file layout.

``fastapi`` is imported inside :func:`build_router`, not at module scope, so
``import beam_agents.console`` works with no extras installed.

Three consequences of that lazy import are worth stating where they bite:

**Query parameters are declared as plain defaults, not ``Query(...)``.** With
``from __future__ import annotations`` every annotation is a string that FastAPI
resolves against this module's globals; an ``Annotated[int, Query(...)]``
annotation would therefore need ``Query`` at module scope, which is exactly what
the lazy import forbids. Bare scalar defaults are already query parameters to
FastAPI, and the bounds they would have carried are applied by :func:`_clamp`.

**The ``_dto`` models are imported eagerly.** They are the return annotations
FastAPI resolves, and they depend only on pydantic, which is a core dependency.

**``_queries`` is referenced through the module, never by ``from``-import**, so
the function actually called is the one bound at request time. That is what lets
the route tests substitute the query layer wholesale.

Endpoints are declared with ``def``, not ``async def``: every query is a
blocking SQLite read, and FastAPI runs a sync endpoint in a worker thread rather
than on the event loop, so one slow scan cannot stall the live stream sharing
the same process.

The returned router already carries its ``/api`` prefix. Mount it with
``app.include_router(build_router(store))`` and no further prefix — the paths
below are transcribed from ``frontend/src/lib/api.ts`` and the UI calls them
absolutely.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from beam_agents.console import _queries
from beam_agents.console._dto import (
    ActivationDetail,
    ActivationSummary,
    ApprovalSummary,
    EntitySummary,
    ErrorGroup,
    ErrorRecord,
    ModelSummary,
    Overview,
    Page,
    SearchHit,
    StoreStatus,
    ToolSummary,
    TraceDetail,
    TraceSummary,
)

if TYPE_CHECKING:
    from fastapi import APIRouter

    from beam_agents.console._store import ConsoleStore

__all__ = ["DEFAULT_PAGE_SIZE", "MAX_PAGE_SIZE", "build_router"]

DEFAULT_PAGE_SIZE = 50
# A ceiling, not a suggestion: the UI never asks for more, and an unbounded
# `limit` on a store a pipeline is actively writing is a way to hold a read
# connection open long enough to matter.
MAX_PAGE_SIZE = 500

# The approval queue is a working list rather than a paged view, and the client
# asks for 100 of it (`api.ts`). Same ceiling applies.
_DEFAULT_APPROVAL_LIMIT = 100

# 24 hours, matching `DEFAULT_WINDOW_MS` in `api.ts`. Declared here too so a
# bare `GET /api/overview` — from curl, or from a healthcheck — answers instead
# of 422-ing on a parameter the UI always sends.
_DEFAULT_WINDOW_MS = 24 * 60 * 60 * 1000
_DEFAULT_BUCKETS = 48
# One bucket per two pixels of a very wide screen. Past this the series costs
# more to serialize than it can convey, and the cost is paid by the store.
_MAX_BUCKETS = 1024


def _clamp(value: int, low: int, high: int) -> int:
    """Return ``value`` confined to ``[low, high]``.

    Clamping rather than rejecting: an out-of-range page size is a caller
    mistake with an obvious sane reading, and a console that answers 422 to a
    hand-typed URL is not helping anyone. The bound that matters — the ceiling —
    is enforced either way.
    """
    return max(low, min(value, high))


def build_router(store: ConsoleStore, **options: Any) -> APIRouter:
    """Build the ``/api`` router over ``store``.

    Routes: ``/overview``, ``/activations``, ``/activations/{entity_key}/{seq}``,
    ``/traces``, ``/traces/{trace_id}``, ``/errors``, ``/errors/groups``,
    ``/models``, ``/tools``, ``/approvals``, ``/entities``, ``/search``, and
    ``/store``.

    ``options`` recognizes ``max_page_size``. Anything else is ignored rather
    than rejected: ``_app.create_app`` forwards one options bag to several
    builders, and a key meant for the ingest side must not fail this one.
    """
    # Imported here, not at module scope: `beam_agents.console` must import with
    # none of the `console` extra installed (spec: "the core install is
    # unchanged"), and this is the module that would otherwise require FastAPI.
    from fastapi import APIRouter, HTTPException  # noqa: PLC0415

    max_page_size = int(options.get("max_page_size", MAX_PAGE_SIZE))

    def page_limit(limit: int) -> int:
        return _clamp(limit, 1, max_page_size)

    # Nested rather than module-level so it can name the lazily-imported
    # `HTTPException` without a second lazy import of its own.
    def not_found(resource: str, **identity: Any) -> HTTPException:
        """Build the 404 for a record the store does not hold.

        RFC-9457-shaped (``type``/``title``/``status``) plus the identifiers
        that were looked up, so the body says *which* activation was missing
        rather than only that something was. FastAPI nests it under ``detail``,
        which is what the TypeScript client surfaces as ``ApiError.detail``.
        """
        return HTTPException(
            status_code=404,
            detail={
                "type": "about:blank",
                "title": f"{resource.capitalize()} not found",
                "status": 404,
                "resource": resource,
            }
            | identity,
        )

    router = APIRouter(prefix="/api", tags=["console"])

    @router.get("/overview")
    def overview(window_ms: int = _DEFAULT_WINDOW_MS, buckets: int = _DEFAULT_BUCKETS) -> Overview:
        """Headline figures and contiguous series for the landing page."""
        return _queries.overview(
            store, window_ms=window_ms, buckets=_clamp(buckets, 1, _MAX_BUCKETS)
        )

    @router.get("/activations")
    def activations(
        entity_key: str | None = None,
        status: str | None = None,
        kind: str | None = None,
        model: str | None = None,
        tool: str | None = None,
        reason: str | None = None,
        since_ms: int | None = None,
        until_ms: int | None = None,
        query: str | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
    ) -> Page[ActivationSummary]:
        """One page of activations, newest first, narrowed by every filter given."""
        filters = _queries.ActivationFilter(
            entity_key=entity_key,
            status=status,
            kind=kind,
            model=model,
            tool=tool,
            reason=reason,
            since_ms=since_ms,
            until_ms=until_ms,
            query=query,
        )
        return _queries.activations(store, filters=filters, cursor=cursor, limit=page_limit(limit))

    # `:path` on a *non-terminal* parameter, deliberately. An entity key is
    # whatever the pipeline keyed on and may contain slashes; the client
    # percent-encodes it, but every ASGI server unquotes the path before routing
    # (uvicorn and httpx's ASGITransport both do), so `%2F` arrives as `/` and a
    # `[^/]+` segment would 404 on a key that is perfectly valid. The greedy
    # `.*` binds up to the last slash, which leaves `{seq}` — always a single
    # numeric segment — as the final one.
    @router.get("/activations/{entity_key:path}/{seq}")
    def activation(entity_key: str, seq: int) -> ActivationDetail:
        """Everything recorded about one activation."""
        detail = _queries.activation_detail(store, entity_key=entity_key, seq=seq)
        if detail is None:
            # A 404, not a 200 carrying null: "no such activation" and "an
            # activation with nothing in it" are different answers, and only one
            # of them is a routing mistake the UI should surface.
            raise not_found("activation", entity_key=entity_key, seq=seq)
        return detail

    @router.get("/traces")
    def traces(
        query: str | None = None, cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE
    ) -> Page[TraceSummary]:
        """One page of traces, matched by trace ID, entity key, or attribute."""
        return _queries.traces(store, query=query, cursor=cursor, limit=page_limit(limit))

    @router.get("/traces/{trace_id}")
    def trace(trace_id: str) -> TraceDetail:
        """One trace with its assembled span tree."""
        detail = _queries.trace_detail(store, trace_id=trace_id)
        if detail is None:
            raise not_found("trace", trace_id=trace_id)
        return detail

    @router.get("/errors")
    def errors(
        reason: str | None = None,
        entity_key: str | None = None,
        since_ms: int | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
    ) -> Page[ErrorRecord]:
        """One page of individual activation errors."""
        return _queries.errors(
            store,
            reason=reason,
            entity_key=entity_key,
            since_ms=since_ms,
            cursor=cursor,
            limit=page_limit(limit),
        )

    @router.get("/errors/groups")
    def error_groups(since_ms: int | None = None, bucket_ms: int | None = None) -> list[ErrorGroup]:
        """Errors grouped by the runtime's closed `reason` vocabulary."""
        return _queries.error_groups(store, since_ms=since_ms, bucket_ms=bucket_ms)

    @router.get("/models")
    def models(since_ms: int | None = None) -> list[ModelSummary]:
        """Per-model call volume, token spend, and cache-hit ratio."""
        return _queries.models(store, since_ms=since_ms)

    @router.get("/tools")
    def tools(since_ms: int | None = None) -> list[ToolSummary]:
        """Per-tool call volume and failure ratio."""
        return _queries.tools(store, since_ms=since_ms)

    @router.get("/approvals")
    def approvals(
        pending_only: bool = False, limit: int = _DEFAULT_APPROVAL_LIMIT
    ) -> list[ApprovalSummary]:
        """Human-approval intents with their deadlines and decisions."""
        return _queries.approvals(store, pending_only=pending_only, limit=page_limit(limit))

    @router.get("/entities")
    def entities(cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE) -> Page[EntitySummary]:
        """One page of entity keys with their activity across all sequences."""
        return _queries.entities(store, cursor=cursor, limit=page_limit(limit))

    @router.get("/search")
    def search(q: str = "", limit: int = DEFAULT_PAGE_SIZE) -> list[SearchHit]:
        """Identifier and attribute-value search, returning located hits."""
        return _queries.search(store, query=q, limit=page_limit(limit))

    @router.get("/store")
    def store_status() -> StoreStatus:
        """Row counts, retention window, and database extent."""
        return _queries.store_status(store)

    return router
