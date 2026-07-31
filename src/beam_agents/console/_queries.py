"""The read side: every question the API can ask the store.

Kept apart from ``_api.py`` so the SQL is testable without an HTTP client and so
the route table stays a thin mapping rather than a place where query logic
accumulates.

Two rules hold throughout. Dimensioned numbers — per model, per tool, per
reason, cache-hit ratio — come from ``TraceEvent.attributes``, never from Beam's
metrics, which carry no labels and count attempted rather than committed work.
And a measurement that was never recorded is ``None``, not ``0``: the runtime
already omits token counts it does not know rather than writing zero, and that
distinction has to survive all the way to the screen.

Pagination is keyset, not offset. A console is read while a pipeline is writing,
and an offset page silently skips or repeats rows the moment an insert lands in
a scanned range.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
    from beam_agents.console._store import ConsoleStore

__all__ = [
    "ActivationFilter",
    "activation_detail",
    "activations",
    "approvals",
    "entities",
    "error_groups",
    "errors",
    "models",
    "overview",
    "search",
    "store_status",
    "tools",
    "trace_detail",
    "traces",
]


class ActivationFilter:
    """The filters the activation list composes.

    Every field is optional and they conjoin: supplying two narrows to their
    intersection. ``None`` means "do not filter on this", which is why an empty
    filter is the whole list rather than an empty one.
    """

    def __init__(
        self,
        *,
        entity_key: str | None = None,
        status: str | None = None,
        kind: str | None = None,
        model: str | None = None,
        tool: str | None = None,
        reason: str | None = None,
        since_ms: int | None = None,
        until_ms: int | None = None,
        query: str | None = None,
    ) -> None:
        """Record the supplied filters; ``None`` fields do not constrain."""
        raise NotImplementedError


def activations(
    store: ConsoleStore,
    *,
    filters: ActivationFilter | None = None,
    cursor: str | None = None,
    limit: int = 50,
) -> Page[ActivationSummary]:
    """Return one page of activations, newest first, in a stable total order."""
    raise NotImplementedError


def activation_detail(store: ConsoleStore, *, entity_key: str, seq: int) -> ActivationDetail | None:
    """Return everything recorded about one activation, or ``None`` if absent."""
    raise NotImplementedError


def traces(
    store: ConsoleStore, *, query: str | None = None, cursor: str | None = None, limit: int = 50
) -> Page[TraceSummary]:
    """Return one page of traces, matched by trace ID, entity key, or attribute."""
    raise NotImplementedError


def trace_detail(store: ConsoleStore, *, trace_id: str) -> TraceDetail | None:
    """Return a trace with its assembled span tree, or ``None`` if absent."""
    raise NotImplementedError


def errors(
    store: ConsoleStore,
    *,
    reason: str | None = None,
    entity_key: str | None = None,
    since_ms: int | None = None,
    cursor: str | None = None,
    limit: int = 50,
) -> Page[ErrorRecord]:
    """Return one page of individual error records."""
    raise NotImplementedError


def error_groups(
    store: ConsoleStore, *, since_ms: int | None = None, bucket_ms: int | None = None
) -> list[ErrorGroup]:
    """Group errors by reason and error type, with an occurrence series each."""
    raise NotImplementedError


def models(store: ConsoleStore, *, since_ms: int | None = None) -> list[ModelSummary]:
    """Return per-model call volume, token spend, and cache-hit ratio."""
    raise NotImplementedError


def tools(store: ConsoleStore, *, since_ms: int | None = None) -> list[ToolSummary]:
    """Return per-tool call volume and failure ratio."""
    raise NotImplementedError


def approvals(
    store: ConsoleStore, *, pending_only: bool = False, limit: int = 100
) -> list[ApprovalSummary]:
    """Return human-approval intents with their deadlines and decisions."""
    raise NotImplementedError


def entities(
    store: ConsoleStore, *, cursor: str | None = None, limit: int = 50
) -> Page[EntitySummary]:
    """Return one page of entity keys with their activity across all sequences."""
    raise NotImplementedError


def search(store: ConsoleStore, *, query: str, limit: int = 50) -> list[SearchHit]:
    """Search identifiers and attribute values, returning located hits."""
    raise NotImplementedError


def overview(store: ConsoleStore, *, window_ms: int, buckets: int = 48) -> Overview:
    """Return the headline figures and contiguous series for the landing page.

    Buckets are contiguous with explicit zeros: a gap in the data must read as a
    gap, not as a line interpolated across missing time.
    """
    raise NotImplementedError


def store_status(store: ConsoleStore) -> StoreStatus:
    """Return row counts, retention window, and database extent."""
    raise NotImplementedError
