"""The read side: every question the API can ask the store.

Kept apart from ``_api.py`` so the SQL is testable without an HTTP client and so
the route table stays a thin mapping rather than a place where query logic
accumulates.

Two rules hold throughout. Dimensioned numbers — per model, per tool, per
reason, cache-hit ratio — come from ``TraceEvent.attributes``, never from Beam's
metrics, which carry no labels and count attempted rather than committed work.
And a measurement that was never recorded is ``None``, not ``0``: the runtime
already omits token counts it does not know rather than writing zero, and that
distinction has to survive all the way to the screen. Every aggregate here
therefore lets SQL's ``SUM``/``MAX`` return ``NULL`` over an all-``NULL`` group
instead of coalescing it, and every ratio is ``None`` when its denominator was
never measured — as opposed to ``0.0``, which is a measured zero.

Pagination is keyset, not offset. A console is read while a pipeline is writing,
and an offset page silently skips or repeats rows the moment an insert lands in
a scanned range. Each list orders on a tuple that is unique per row, and the
cursor is that tuple; ``next_cursor`` is ``None`` exactly when the scan is
exhausted, which is decided by reading one row past the page rather than by
noticing a short page.

Three decisions are load-bearing enough to state here rather than only at their
call sites:

**An error reaches the store twice and must be counted once.** The same failure
arrives as an ``ActivationErrorRecord`` on the errors sink and as an ``ERROR``
trace event carrying ``error.type`` and the ``beam_agents.failure.*`` position
scalars. :data:`_ERROR_ROWS` unifies them on ``(entity_key, seq, reason)`` — the
record supplies ``detail``, the trace event supplies the attributes neither the
record nor Beam's metrics can — and emits a trace-only failure just once, so no
error is double-counted and none is lost when only one path is configured.

**Percentiles come from ``wall_ms``.** Spans are zero-width by design
(``start_ms == end_ms``), so any percentile taken over span widths would be
identically zero. ``wall_ms`` is the ``ACTIVATION_START`` → ``ACTIVATION_END``
clock delta, which is real because those two events are stamped by different
clock reads.

**The overview window is anchored on the newest stored record, not on a wall
clock.** A console is routinely opened over an imported bundle or a BigQuery
window that ended hours ago; anchoring on ``time.time()`` would render that
store as an empty page. Anchoring on the data also makes every figure here a
pure function of the database, which is what lets these queries be tested
exactly.

Importing this module has no side effects.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
import sqlite3
from typing import TYPE_CHECKING, cast

from beam_agents.console._dto import (
    ActivationDetail,
    ActivationSummary,
    ApprovalSummary,
    AttemptSummary,
    BucketPoint,
    EntitySummary,
    ErrorGroup,
    ErrorRecord,
    EventRecord,
    IntentSummary,
    ModelSummary,
    Overview,
    Page,
    SearchHit,
    SnapshotSummary,
    SpanNode,
    StoreStatus,
    ToolSummary,
    TraceDetail,
    TraceSummary,
)
from beam_agents.console._records import PROVENANCE
from beam_agents.hitl import REASON_HITL_TIMEOUT
from beam_agents.observability.traces import (
    ACTIVATION_KIND,
    ACTIVATION_STATUS,
    ATTEMPTS,
    CACHE_HIT,
    CIRCUIT_STATE,
    DEADLINE_MS,
    ERROR_TYPE,
    EXPIRES_AT_MS,
    FAILURE_LAST_EVENT,
    FAILURE_LLM_CALLS,
    FAILURE_STAGED_INTENTS,
    FAILURE_STEP,
    INTENT_ID,
    INTENT_KIND,
    REASON,
    REQUEST_MODEL,
    ROLE_ACTIVATION,
    TOOL_NAME,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from contextlib import AbstractContextManager

    from beam_agents.console._dto import (
        ActivationKind,
        ActivationStatus,
        Provenance,
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

# `TraceEvent.EventType` enum names, as `_records.EventRow.event_type` stores
# them. Spelled out rather than derived from the proto so the SQL literals and
# the Python comparisons are the same strings.
_ACTIVATION_START = "ACTIVATION_START"
_ACTIVATION_END = "ACTIVATION_END"
_LLM_CALL = "LLM_CALL"
_TOOL_CALL = "TOOL_CALL"
_INTENT_EMITTED = "INTENT_EMITTED"
_ERROR = "ERROR"
_SUSPENDED = "SUSPENDED"

# `ToolIntent.Kind.Name(APPROVAL)` — what `core/context.py` writes into the
# `beam_agents.intent_kind` attribute for a human-approval intent.
_APPROVAL = "APPROVAL"

_STATUSES: frozenset[str] = frozenset({"completed", "suspended", "error", "in_flight"})
_KINDS: frozenset[str] = frozenset({"start", "resume", "unknown"})
_PROVENANCES: frozenset[str] = frozenset(PROVENANCE)

_DEFAULT_BUCKETS = 48
_TOP_N = 5
_P50 = 0.5
_P95 = 0.95
_SEARCH_KINDS = 4

_TRUE = "true"
# SQLite reads a negative LIMIT as no limit. Used where the caller wants the
# whole set — an activation's own errors, a bucketed series — rather than a page.
_NO_LIMIT = -1


def _attr(name: str, *, table: str = "events") -> str:
    """Return SQL reading attribute ``name`` for the event row in scope.

    Attributes live in their own table rather than as a JSON column on
    ``events``, so this is a correlated subquery on ``event_attributes``'s
    primary key ``(trace_id, span_id, event_type, key)`` — an exact index probe
    per row, not a JSON parse per row. That is the whole reason the schema
    normalizes them: every dimensioned query names a specific key, and a JSON
    column would make all of them scans.

    ``table`` must name the *outer* event relation and is never empty. An
    unqualified ``trace_id`` inside the subquery binds to ``event_attributes``
    itself, which silently degenerates the correlation into
    ``_a.trace_id = _a.trace_id`` — always true — so every row reads whichever
    attribute row happens to come first. It fails as wrong numbers, not as an
    error.

    ``name`` is always one of the constants in ``observability/traces.py``, none
    of which contains a quote, so the interpolation is over a closed set of
    literals rather than over anything a request can reach.
    """
    return (
        "(SELECT _a.value FROM event_attributes _a "
        f"WHERE _a.trace_id = {table}.trace_id AND _a.span_id = {table}.span_id "
        f"AND _a.event_type = {table}.event_type AND _a.key = '{name}')"
    )


def _attributes_json(table: str = "events") -> str:
    """Return SQL rebuilding an event's whole attribute map as a JSON object.

    Only for the paths that hand a complete event to the API. Anything asking
    for *one* attribute uses :func:`_attr`, which probes the index instead of
    materializing the map. ``table`` carries the same correlation requirement.
    """
    return (
        "(SELECT json_group_object(_m.key, _m.value) FROM event_attributes _m "
        f"WHERE _m.trace_id = {table}.trace_id AND _m.span_id = {table}.span_id "
        f"AND _m.event_type = {table}.event_type)"
    )


# The unified error vocabulary. See the module docstring: one failure reaches
# the store as a record *and* as a trace event, and neither alone is complete.
_ERROR_ROWS = f"""
WITH trace_errors AS (
    SELECT entity_key,
           seq,
           {_attr(REASON)} AS reason,
           MIN(start_ms) AS event_time_ms,
           MAX({_attr(ERROR_TYPE)}) AS error_type,
           MAX(CAST({_attr(FAILURE_STEP)} AS INTEGER)) AS failure_step,
           MAX({_attr(FAILURE_LAST_EVENT)}) AS failure_last_event,
           MAX(CAST({_attr(FAILURE_STAGED_INTENTS)} AS INTEGER)) AS failure_staged_intents,
           MAX(CAST({_attr(FAILURE_LLM_CALLS)} AS INTEGER)) AS failure_llm_calls
    FROM events
    WHERE event_type = '{_ERROR}' AND {_attr(REASON)} IS NOT NULL
    GROUP BY entity_key, seq, reason
),
error_rows AS (
    SELECT r.entity_key AS entity_key,
           r.seq AS seq,
           -- `seq` is nullable (timer routes have no activation), and a NULL
           -- inside a row-value comparison makes the whole comparison NULL, so
           -- the keyset orders on this non-null surrogate instead.
           IFNULL(r.seq, -1) AS seq_key,
           r.reason AS reason,
           r.detail AS detail,
           r.event_time_ms AS event_time_ms,
           t.error_type AS error_type,
           t.failure_step AS failure_step,
           t.failure_last_event AS failure_last_event,
           t.failure_staged_intents AS failure_staged_intents,
           t.failure_llm_calls AS failure_llm_calls
    FROM errors r
    LEFT JOIN trace_errors t
           ON t.entity_key = r.entity_key
          AND IFNULL(t.seq, -1) = IFNULL(r.seq, -1)
          AND t.reason = r.reason
    UNION ALL
    SELECT t.entity_key,
           t.seq,
           IFNULL(t.seq, -1),
           t.reason,
           '' AS detail,
           t.event_time_ms,
           t.error_type,
           t.failure_step,
           t.failure_last_event,
           t.failure_staged_intents,
           t.failure_llm_calls
    FROM trace_errors t
    WHERE NOT EXISTS (
        SELECT 1 FROM errors r
        WHERE r.entity_key = t.entity_key
          AND IFNULL(r.seq, -1) = IFNULL(t.seq, -1)
          AND r.reason = t.reason
    )
)
"""

_ACTIVATION_COLUMNS = """
    entity_key, seq, trace_id, status, kind, attempts, started_ms, ended_ms, wall_ms,
    model, llm_calls, tool_calls, intents, errors, prompt_tokens, completion_tokens,
    total_tokens, cache_hits, tools, reasons, provenance, complete_provenance
"""


# -- the store seam ------------------------------------------------------------


def _reader(store: ConsoleStore) -> AbstractContextManager[sqlite3.Connection]:
    """Return ``store.reader()`` as the context manager it is at runtime.

    ``ConsoleStore.reader`` is declared with the generator's return type, which
    is what ``@contextmanager`` decorates rather than what it produces. The cast
    is the one place that gap is bridged, so the rest of this module reads as
    ordinary ``with`` blocks over a connection.
    """
    return cast("AbstractContextManager[sqlite3.Connection]", store.reader())


def _rows(
    connection: sqlite3.Connection, sql: str, params: Sequence[object] = ()
) -> list[sqlite3.Row]:
    """Run ``sql`` and return every row, keyed by column name.

    ``row_factory`` is set on the cursor rather than the connection so a store
    that hands out a shared or pooled connection is not mutated by a read.
    """
    cursor = connection.cursor()
    cursor.row_factory = sqlite3.Row
    return cursor.execute(sql, tuple(params)).fetchall()


# -- typed row access ----------------------------------------------------------


def _text(row: sqlite3.Row, key: str) -> str:
    value = row[key]
    return "" if value is None else str(value)


def _opt_text(row: sqlite3.Row, key: str) -> str | None:
    value = row[key]
    return None if value is None else str(value)


def _int(row: sqlite3.Row, key: str) -> int:
    value = row[key]
    return 0 if value is None else int(value)


def _opt_int(row: sqlite3.Row, key: str) -> int | None:
    value = row[key]
    return None if value is None else int(value)


def _strings(row: sqlite3.Row, key: str) -> list[str]:
    """Decode a JSON array column into a list of strings."""
    raw = row[key]
    if not raw:
        return []
    decoded: object = json.loads(str(raw))
    return [str(item) for item in decoded] if isinstance(decoded, list) else []


def _status(value: str) -> ActivationStatus:
    """Narrow a stored status to the literal, defaulting to the honest answer."""
    return cast("ActivationStatus", value if value in _STATUSES else "in_flight")


def _kind(value: str) -> ActivationKind:
    """Narrow a stored kind; an unrecorded kind is ``unknown``, not ``start``."""
    return cast("ActivationKind", value if value in _KINDS else "unknown")


def _provenances(row: sqlite3.Row, key: str) -> list[Provenance]:
    return [cast("Provenance", item) for item in _strings(row, key) if item in _PROVENANCES]


def _ratio(numerator: int, denominator: int) -> float | None:
    """Return the ratio, or ``None`` when the denominator was never measured."""
    return numerator / denominator if denominator else None


# -- cursors -------------------------------------------------------------------


def _encode_cursor(values: Sequence[object]) -> str:
    """Encode a keyset position opaquely, so no client depends on its shape."""
    packed = base64.urlsafe_b64encode(json.dumps(list(values)).encode())
    return packed.decode().rstrip("=")


def _decode_cursor(cursor: str, arity: int) -> list[object]:
    """Decode a keyset position, rejecting anything this module did not issue."""
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        decoded: object = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except (ValueError, binascii.Error) as exc:
        message = f"malformed cursor: {cursor!r}"
        raise ValueError(message) from exc
    if not isinstance(decoded, list) or len(decoded) != arity:
        message = f"malformed cursor: {cursor!r}"
        raise ValueError(message)
    return list(decoded)


def _paginate(
    rows: list[sqlite3.Row], limit: int, keys: Sequence[str]
) -> tuple[list[sqlite3.Row], str | None]:
    """Split an over-fetched result into a page and the cursor that follows it.

    One row beyond the page is read so exhaustion is *observed* rather than
    inferred from a short page — a page that exactly consumes the remainder must
    still end the scan.
    """
    if len(rows) <= limit:
        return rows, None
    page = rows[:limit]
    last = page[-1]
    return page, _encode_cursor([last[key] for key in keys])


# -- filters -------------------------------------------------------------------


class ActivationFilter:
    """The filters the activation list composes.

    Every field is optional and they conjoin: supplying two narrows to their
    intersection. ``None`` means "do not filter on this", which is why an empty
    filter is the whole list rather than an empty one.
    """

    __slots__ = (
        "entity_key",
        "kind",
        "model",
        "query",
        "reason",
        "since_ms",
        "status",
        "tool",
        "until_ms",
    )

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
        self.entity_key = entity_key
        self.status = status
        self.kind = kind
        self.model = model
        self.tool = tool
        self.reason = reason
        self.since_ms = since_ms
        self.until_ms = until_ms
        self.query = query

    def _clauses(self) -> tuple[list[str], list[object]]:
        """Return the conjoined SQL predicates and their bound parameters."""
        clauses: list[str] = []
        params: list[object] = []
        for column, value in (
            ("entity_key", self.entity_key),
            ("status", self.status),
            ("kind", self.kind),
            ("model", self.model),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        # `tools` and `reasons` are JSON arrays on the rollup, so membership is a
        # containment test rather than an equality one.
        for column, value in (("tools", self.tool), ("reasons", self.reason)):
            if value is not None:
                clauses.append(f"EXISTS (SELECT 1 FROM json_each({column}) WHERE value = ?)")
                params.append(value)
        if self.since_ms is not None:
            clauses.append("started_ms >= ?")
            params.append(self.since_ms)
        if self.until_ms is not None:
            # Half-open, so two adjacent windows neither overlap nor drop a row
            # that lands exactly on their shared edge.
            clauses.append("started_ms < ?")
            params.append(self.until_ms)
        if self.query is not None:
            clauses.append("(entity_key LIKE ? OR trace_id LIKE ? OR IFNULL(model, '') LIKE ?)")
            params.extend([f"%{self.query}%"] * 3)
        return clauses, params


def _where(clauses: Sequence[str]) -> str:
    return f"WHERE {' AND '.join(clauses)}" if clauses else ""


def _count(
    connection: sqlite3.Connection,
    *,
    source: str,
    clauses: Sequence[str],
    params: Sequence[object],
    prefix: str = "",
) -> int:
    """Count the filtered set — the whole list, not the page the keyset cut.

    ``Page.total`` is documented as exact or absent, never an estimate, and over
    a store bounded by retention an indexed ``COUNT`` is cheap enough to be
    exact.
    """
    sql = f"{prefix}SELECT COUNT(*) AS n FROM {source} {_where(clauses)}"
    return _int(_rows(connection, sql, params)[0], "n")


# -- activations ---------------------------------------------------------------


def _activation_summary(row: sqlite3.Row) -> ActivationSummary:
    return ActivationSummary(
        entity_key=_text(row, "entity_key"),
        seq=_int(row, "seq"),
        trace_id=_text(row, "trace_id"),
        status=_status(_text(row, "status")),
        kind=_kind(_text(row, "kind")),
        attempts=_int(row, "attempts"),
        started_ms=_int(row, "started_ms"),
        ended_ms=_opt_int(row, "ended_ms"),
        wall_ms=_opt_int(row, "wall_ms"),
        model=_opt_text(row, "model"),
        llm_calls=_int(row, "llm_calls"),
        tool_calls=_int(row, "tool_calls"),
        intents=_int(row, "intents"),
        errors=_int(row, "errors"),
        prompt_tokens=_opt_int(row, "prompt_tokens"),
        completion_tokens=_opt_int(row, "completion_tokens"),
        total_tokens=_opt_int(row, "total_tokens"),
        cache_hits=_int(row, "cache_hits"),
        tools=_strings(row, "tools"),
        reasons=_strings(row, "reasons"),
        provenance=_provenances(row, "provenance"),
        complete_provenance=bool(_int(row, "complete_provenance")),
    )


def activations(
    store: ConsoleStore,
    *,
    filters: ActivationFilter | None = None,
    cursor: str | None = None,
    limit: int = 50,
) -> Page[ActivationSummary]:
    """Return one page of activations, newest first, in a stable total order."""
    limit = max(1, limit)
    filtered, params = (filters or ActivationFilter())._clauses()
    scanned = [*filtered, "(started_ms, entity_key, seq) < (?, ?, ?)"] if cursor else filtered
    keyset = list(_decode_cursor(cursor, 3)) if cursor else []
    sql = (
        f"SELECT {_ACTIVATION_COLUMNS} FROM activations {_where(scanned)} "
        "ORDER BY started_ms DESC, entity_key DESC, seq DESC LIMIT ?"
    )
    with _reader(store) as connection:
        rows = _rows(connection, sql, [*params, *keyset, limit + 1])
        total = _count(connection, source="activations", clauses=filtered, params=params)
    page, next_cursor = _paginate(rows, limit, ("started_ms", "entity_key", "seq"))
    return Page[ActivationSummary](
        items=[_activation_summary(row) for row in page], next_cursor=next_cursor, total=total
    )


# -- events, spans, attempts ---------------------------------------------------

_EVENT_COLUMNS = f"""
    trace_id, span_id, parent_span_id, entity_key, seq, step_index, event_type,
    start_ms, end_ms, {_attributes_json()} AS attributes, provenance
"""
# Stable across ties: `start_ms` alone is not unique, because a whole activation
# can be stamped from one clock read.
_EVENT_ORDER = "ORDER BY start_ms, step_index, event_type, span_id"


def _event_record(row: sqlite3.Row) -> EventRecord:
    raw: object = json.loads(_text(row, "attributes") or "{}")
    attributes = (
        {str(key): str(value) for key, value in raw.items()} if isinstance(raw, dict) else {}
    )
    provenance = _text(row, "provenance")
    return EventRecord(
        trace_id=_text(row, "trace_id"),
        span_id=_text(row, "span_id"),
        parent_span_id=_text(row, "parent_span_id"),
        entity_key=_text(row, "entity_key"),
        seq=_int(row, "seq"),
        step_index=_int(row, "step_index"),
        event_type=_text(row, "event_type"),
        start_ms=_int(row, "start_ms"),
        end_ms=_int(row, "end_ms"),
        attributes=attributes,
        provenance=cast("Provenance", provenance if provenance in _PROVENANCES else "native"),
    )


def _role(event_type: str) -> str:
    """Mirror ``traces.role_for_event_type``: the span role for an event type."""
    if event_type in (_ACTIVATION_START, _ACTIVATION_END):
        return ROLE_ACTIVATION
    return event_type


class _Span:
    """One span being assembled from the events that landed on it."""

    __slots__ = ("events", "parent_span_id", "role", "span_id", "step_index")

    def __init__(self, event: EventRecord) -> None:
        self.span_id = event.span_id
        self.parent_span_id = event.parent_span_id
        self.role = _role(event.event_type)
        self.step_index = event.step_index
        self.events: list[EventRecord] = []


def _collect_spans(events: Iterable[EventRecord]) -> dict[str, _Span]:
    spans: dict[str, _Span] = {}
    for event in events:
        span = spans.get(event.span_id)
        if span is None:
            span = _Span(event)
            spans[event.span_id] = span
        span.events.append(event)
        span.step_index = min(span.step_index, event.step_index)
    return spans


def _span_tree(events: Sequence[EventRecord]) -> list[SpanNode]:
    """Flatten an activation's spans into depth-first order.

    ``SpanNode`` carries ``depth`` and ``order`` and no children, deliberately:
    the structure is real and is encoded, while width is not, because the
    runtime measures no span duration to encode (design D4).
    """
    spans = _collect_spans(events)
    first_ms = {span_id: min(e.start_ms for e in span.events) for span_id, span in spans.items()}
    children: dict[str, list[str]] = {span_id: [] for span_id in spans}
    roots: list[str] = []
    for span_id, span in spans.items():
        parent = span.parent_span_id
        if parent and parent in spans and parent != span_id:
            children[parent].append(span_id)
        else:
            roots.append(span_id)

    def _sorted(span_ids: list[str]) -> list[str]:
        return sorted(span_ids, key=lambda span_id: (first_ms[span_id], span_id))

    nodes: list[SpanNode] = []
    visited: set[str] = set()
    stack: list[tuple[str, int]] = [(span_id, 0) for span_id in reversed(_sorted(roots))]
    while stack:
        span_id, depth = stack.pop()
        if span_id in visited:
            continue
        visited.add(span_id)
        span = spans[span_id]
        nodes.append(
            SpanNode(
                span_id=span_id,
                parent_span_id=span.parent_span_id,
                role=span.role,
                step_index=span.step_index,
                depth=depth,
                order=len(nodes),
                events=list(span.events),
            )
        )
        stack.extend((child, depth + 1) for child in reversed(_sorted(children[span_id])))
    return nodes


def _attempt_status(events: Sequence[EventRecord], errored: bool) -> ActivationStatus:
    for event in events:
        if event.event_type == _ACTIVATION_END:
            return _status(event.attributes.get(ACTIVATION_STATUS, ""))
    return "error" if errored else "in_flight"


def _attempts(events: Sequence[EventRecord]) -> list[AttemptSummary]:
    """Assemble one attempt per activation-role span.

    A suspend and its resume are two attempts under one ``(entity_key, seq)``,
    because trace identity is scoped to the activation and a resume recomputes
    the same trace ID.

    ``entry_step_index`` is the lowest step index among the attempt's non-
    activation children. It cannot be read off the attempt's own events:
    ``ACTIVATION_START`` is stamped at step ``0`` for every attempt, so the only
    evidence of where a resume re-entered is where its work resumed.
    """
    spans = _collect_spans(events)
    summaries: list[AttemptSummary] = []
    for span_id, span in spans.items():
        if span.role != ROLE_ACTIVATION:
            continue
        children = [
            other
            for other in spans.values()
            if other.parent_span_id == span_id and other.role != ROLE_ACTIVATION
        ]
        errored = any(child.role == _ERROR for child in children)
        starts = [e for e in span.events if e.event_type == _ACTIVATION_START]
        ends = [e for e in span.events if e.event_type == _ACTIVATION_END]
        kind = ""
        for event in (*starts, *ends):
            kind = event.attributes.get(ACTIVATION_KIND, kind)
        summaries.append(
            AttemptSummary(
                span_id=span_id,
                kind=_kind(kind),
                entry_step_index=min((child.step_index for child in children), default=0),
                start_ms=min(e.start_ms for e in span.events),
                end_ms=ends[0].start_ms if ends else None,
                status=_attempt_status(span.events, errored),
            )
        )
    return sorted(summaries, key=lambda attempt: (attempt.start_ms, attempt.span_id))


def _intents(events: Sequence[EventRecord]) -> list[IntentSummary]:
    return [
        IntentSummary(
            intent_id=event.attributes.get(INTENT_ID, ""),
            tool_name=event.attributes.get(TOOL_NAME, ""),
            intent_kind=event.attributes.get(INTENT_KIND, ""),
            step_index=event.step_index,
            expires_at_ms=_as_int(event.attributes.get(EXPIRES_AT_MS)),
            emitted_at_ms=event.start_ms,
        )
        for event in events
        if event.event_type == _INTENT_EMITTED
    ]


def _as_int(value: str | None) -> int | None:
    """Parse an attribute that carries a number, or ``None`` if it carries none."""
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


# -- activation detail ---------------------------------------------------------


def _error_records(
    connection: sqlite3.Connection, where: str, params: Sequence[object], order: str, limit: int
) -> list[sqlite3.Row]:
    return _rows(
        connection,
        f"{_ERROR_ROWS} SELECT * FROM error_rows {where} {order} LIMIT ?",
        [*params, limit],
    )


def _error_record(row: sqlite3.Row) -> ErrorRecord:
    return ErrorRecord(
        entity_key=_text(row, "entity_key"),
        seq=_opt_int(row, "seq"),
        reason=_text(row, "reason"),
        detail=_text(row, "detail"),
        error_type=_opt_text(row, "error_type"),
        event_time_ms=_int(row, "event_time_ms"),
        failure_step=_opt_int(row, "failure_step"),
        failure_last_event=_opt_text(row, "failure_last_event"),
        failure_staged_intents=_opt_int(row, "failure_staged_intents"),
        failure_llm_calls=_opt_int(row, "failure_llm_calls"),
    )


def _snapshot_summary(row: sqlite3.Row) -> SnapshotSummary:
    return SnapshotSummary(
        entity_key=_text(row, "entity_key"),
        seq=_int(row, "seq"),
        snapshot_at_ms=_int(row, "snapshot_at_ms"),
        state_schema_version=_int(row, "state_schema_version"),
        request_id=_text(row, "request_id"),
        memory_entries=_int(row, "memory_entries"),
        memory_bytes=_int(row, "memory_bytes"),
        llm_cache_entries=_int(row, "llm_cache_entries"),
        pending_intent_ids=_strings(row, "pending_intent_ids"),
        continuation_step_index=_opt_int(row, "continuation_step_index"),
        continuation_deadline_ms=_opt_int(row, "continuation_deadline_ms"),
        continuation_adapter=_text(row, "continuation_adapter"),
    )


def _replay_command(entity_key: str, seq: int) -> str:
    """Build the ``beam-agents-replay`` invocation for one activation.

    The two inputs telemetry cannot supply — the agent's import path and the
    triggering envelope — stay as named placeholders. Guessing them would
    produce a command that fails at exactly the moment an operator is least able
    to afford a wrong lead.
    """
    stem = f"{entity_key}-{seq}"
    return (
        f"beam-agents-replay --snapshot {stem}.snapshot --traces {stem}.traces "
        f"--event {stem}.event --agent <module:attribute> --seq {seq}"
    )


def activation_detail(store: ConsoleStore, *, entity_key: str, seq: int) -> ActivationDetail | None:
    """Return everything recorded about one activation, or ``None`` if absent."""
    with _reader(store) as connection:
        summaries = _rows(
            connection,
            f"SELECT {_ACTIVATION_COLUMNS} FROM activations WHERE entity_key = ? AND seq = ?",
            (entity_key, seq),
        )
        if not summaries:
            return None
        event_rows = _rows(
            connection,
            f"SELECT {_EVENT_COLUMNS} FROM events WHERE entity_key = ? AND seq = ? {_EVENT_ORDER}",
            (entity_key, seq),
        )
        error_rows = _error_records(
            connection,
            "WHERE entity_key = ? AND seq = ?",
            (entity_key, seq),
            "ORDER BY event_time_ms",
            limit=_NO_LIMIT,
        )
        snapshots = _rows(
            connection,
            "SELECT * FROM snapshots WHERE entity_key = ? AND seq = ? "
            "ORDER BY snapshot_at_ms DESC LIMIT 1",
            (entity_key, seq),
        )
    events = [_event_record(row) for row in event_rows]
    snapshot = _snapshot_summary(snapshots[0]) if snapshots else None
    return ActivationDetail(
        summary=_activation_summary(summaries[0]),
        attempts=_attempts(events),
        spans=_span_tree(events),
        events=events,
        intents=_intents(events),
        errors=[_error_record(row) for row in error_rows],
        snapshot=snapshot,
        # Replay reconstructs an activation *from* a StateSnapshot; without one
        # there is no command to offer.
        replay_command=_replay_command(entity_key, seq) if snapshot is not None else None,
    )


# -- traces --------------------------------------------------------------------

_TRACE_COLUMNS = """
    a.trace_id AS trace_id, a.entity_key AS entity_key, a.seq AS seq,
    a.started_ms AS started_ms, a.ended_ms AS ended_ms, a.status AS status,
    (SELECT COUNT(*) FROM events e WHERE e.trace_id = a.trace_id) AS events,
    (SELECT COUNT(DISTINCT e.span_id) FROM events e WHERE e.trace_id = a.trace_id) AS spans
"""


def _trace_summary(row: sqlite3.Row) -> TraceSummary:
    return TraceSummary(
        trace_id=_text(row, "trace_id"),
        entity_key=_text(row, "entity_key"),
        seq=_int(row, "seq"),
        events=_int(row, "events"),
        spans=_int(row, "spans"),
        started_ms=_int(row, "started_ms"),
        ended_ms=_opt_int(row, "ended_ms"),
        status=_status(_text(row, "status")),
    )


def traces(
    store: ConsoleStore, *, query: str | None = None, cursor: str | None = None, limit: int = 50
) -> Page[TraceSummary]:
    """Return one page of traces, matched by trace ID, entity key, or attribute."""
    limit = max(1, limit)
    clauses: list[str] = []
    params: list[object] = []
    if query is not None:
        clauses.append(
            "(a.trace_id LIKE ? OR a.entity_key LIKE ? OR EXISTS ("
            "SELECT 1 FROM event_attributes _s "
            "WHERE _s.trace_id = a.trace_id AND _s.value LIKE ?))"
        )
        params.extend([f"%{query}%"] * 3)
    scanned = [*clauses, "(a.started_ms, a.trace_id) < (?, ?)"] if cursor else clauses
    keyset = list(_decode_cursor(cursor, 2)) if cursor else []
    with _reader(store) as connection:
        rows = _rows(
            connection,
            f"SELECT {_TRACE_COLUMNS} FROM activations a {_where(scanned)} "
            "ORDER BY a.started_ms DESC, a.trace_id DESC LIMIT ?",
            [*params, *keyset, limit + 1],
        )
        total = _count(connection, source="activations a", clauses=clauses, params=params)
    page, next_cursor = _paginate(rows, limit, ("started_ms", "trace_id"))
    return Page[TraceSummary](
        items=[_trace_summary(row) for row in page], next_cursor=next_cursor, total=total
    )


def trace_detail(store: ConsoleStore, *, trace_id: str) -> TraceDetail | None:
    """Return a trace with its assembled span tree, or ``None`` if absent."""
    with _reader(store) as connection:
        summaries = _rows(
            connection,
            f"SELECT {_TRACE_COLUMNS} FROM activations a WHERE a.trace_id = ?",
            (trace_id,),
        )
        if not summaries:
            return None
        event_rows = _rows(
            connection,
            f"SELECT {_EVENT_COLUMNS} FROM events WHERE trace_id = ? {_EVENT_ORDER}",
            (trace_id,),
        )
    events = [_event_record(row) for row in event_rows]
    # `roots` carries the whole tree, flattened: `SpanNode` has no children
    # field, so nesting is expressed by `depth`/`order` and truncating to the
    # depth-0 nodes would drop the tree rather than root it.
    return TraceDetail(
        summary=_trace_summary(summaries[0]), roots=_span_tree(events), attempts=_attempts(events)
    )


# -- errors --------------------------------------------------------------------


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
    limit = max(1, limit)
    clauses: list[str] = []
    params: list[object] = []
    for column, value in (("reason", reason), ("entity_key", entity_key)):
        if value is not None:
            clauses.append(f"{column} = ?")
            params.append(value)
    if since_ms is not None:
        clauses.append("event_time_ms >= ?")
        params.append(since_ms)
    scanned = (
        [*clauses, "(event_time_ms, entity_key, seq_key, reason) < (?, ?, ?, ?)"]
        if cursor
        else clauses
    )
    keyset = list(_decode_cursor(cursor, 4)) if cursor else []
    with _reader(store) as connection:
        rows = _error_records(
            connection,
            _where(scanned),
            [*params, *keyset],
            "ORDER BY event_time_ms DESC, entity_key DESC, seq_key DESC, reason DESC",
            limit + 1,
        )
        total = _count(
            connection,
            source="error_rows",
            clauses=clauses,
            params=params,
            prefix=_ERROR_ROWS,
        )
    page, next_cursor = _paginate(rows, limit, ("event_time_ms", "entity_key", "seq_key", "reason"))
    return Page[ErrorRecord](
        items=[_error_record(row) for row in page], next_cursor=next_cursor, total=total
    )


def _bucket_size(span_ms: int, buckets: int) -> int:
    return max(1, math.ceil(span_ms / max(1, buckets)))


def _series(
    counts: Mapping[int, float], *, first_ms: int, last_ms: int, bucket_ms: int
) -> list[BucketPoint]:
    """Build a contiguous series with an explicit zero in every empty bucket.

    A gap in the data has to read as a gap. A series that simply omits its empty
    buckets is drawn as a straight line across missing time, which is a claim
    about a period in which nothing was recorded.
    """
    first = (first_ms // bucket_ms) * bucket_ms
    last = (last_ms // bucket_ms) * bucket_ms
    return [
        BucketPoint(bucket_ms=bucket, value=counts.get(bucket, 0.0))
        for bucket in range(first, last + bucket_ms, bucket_ms)
    ]


def error_groups(
    store: ConsoleStore, *, since_ms: int | None = None, bucket_ms: int | None = None
) -> list[ErrorGroup]:
    """Group errors by reason and error type, with an occurrence series each."""
    where = "WHERE event_time_ms >= ?" if since_ms is not None else ""
    params: list[object] = [since_ms] if since_ms is not None else []
    with _reader(store) as connection:
        rows = _error_records(connection, where, params, "ORDER BY event_time_ms", _NO_LIMIT)
    if not rows:
        return []
    first_ms = min(_int(row, "event_time_ms") for row in rows)
    last_ms = max(_int(row, "event_time_ms") for row in rows)
    size = (
        bucket_ms
        if bucket_ms is not None
        else _bucket_size(last_ms - first_ms + 1, _DEFAULT_BUCKETS)
    )

    grouped: dict[tuple[str, str | None], list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault((_text(row, "reason"), _opt_text(row, "error_type")), []).append(row)

    groups: list[ErrorGroup] = []
    for (reason, error_type), members in grouped.items():
        counts: dict[int, float] = {}
        for row in members:
            bucket = (_int(row, "event_time_ms") // size) * size
            counts[bucket] = counts.get(bucket, 0.0) + 1.0
        times = [_int(row, "event_time_ms") for row in members]
        details = [_text(row, "detail") for row in members if _text(row, "detail")]
        groups.append(
            ErrorGroup(
                reason=reason,
                error_type=error_type,
                count=len(members),
                entities=len({_text(row, "entity_key") for row in members}),
                first_seen_ms=min(times),
                last_seen_ms=max(times),
                series=_series(counts, first_ms=min(times), last_ms=max(times), bucket_ms=size),
                sample_detail=details[0] if details else "",
            )
        )
    return sorted(groups, key=lambda group: (-group.count, group.reason, group.error_type or ""))


# -- models and tools ----------------------------------------------------------


def _since(since_ms: int | None, column: str = "start_ms") -> tuple[str, list[object]]:
    return (f"AND {column} >= ?", [since_ms]) if since_ms is not None else ("", [])


def _total_tokens(prompt: int | None, completion: int | None) -> int | None:
    """Sum what was recorded; ``None`` only when neither side was."""
    if prompt is None and completion is None:
        return None
    return (prompt or 0) + (completion or 0)


def models(store: ConsoleStore, *, since_ms: int | None = None) -> list[ModelSummary]:
    """Return per-model call volume, token spend, and cache-hit ratio."""
    with _reader(store) as connection:
        return _models(connection, since_ms)


def _models(connection: sqlite3.Connection, since_ms: int | None) -> list[ModelSummary]:
    window, params = _since(since_ms)
    calls_sql = f"""
        WITH llm AS (
            SELECT {_attr(REQUEST_MODEL)} AS model,
                   CAST({_attr(USAGE_INPUT_TOKENS)} AS INTEGER) AS prompt_tokens,
                   CAST({_attr(USAGE_OUTPUT_TOKENS)} AS INTEGER) AS completion_tokens,
                   {_attr(CACHE_HIT)} AS cache_hit,
                   CAST({_attr(ATTEMPTS)} AS INTEGER) AS attempts,
                   {_attr(ERROR_TYPE)} AS error_type
            FROM events
            WHERE event_type = '{_LLM_CALL}' AND {_attr(REQUEST_MODEL)} IS NOT NULL {window}
        )
        SELECT model,
               COUNT(*) AS calls,
               SUM(prompt_tokens) AS prompt_tokens,
               SUM(completion_tokens) AS completion_tokens,
               SUM(CASE WHEN cache_hit = '{_TRUE}' THEN 1 ELSE 0 END) AS cache_hits,
               SUM(CASE WHEN cache_hit IS NOT NULL THEN 1 ELSE 0 END) AS cache_measured,
               SUM(CASE WHEN error_type IS NOT NULL THEN 1 ELSE 0 END) AS errors,
               MAX(attempts) AS max_attempts
        FROM llm GROUP BY model ORDER BY calls DESC, model ASC
    """
    circuits_sql = f"""
        SELECT {_attr(REQUEST_MODEL)} AS model,
               {_attr(CIRCUIT_STATE)} AS circuit_state,
               COUNT(*) AS n
        FROM events
        WHERE event_type = '{_LLM_CALL}' AND {_attr(REQUEST_MODEL)} IS NOT NULL
          AND {_attr(CIRCUIT_STATE)} IS NOT NULL {window}
        GROUP BY model, circuit_state
    """
    rows = _rows(connection, calls_sql, params)
    circuit_rows = _rows(connection, circuits_sql, params)
    circuits: dict[str, dict[str, int]] = {}
    for row in circuit_rows:
        circuits.setdefault(_text(row, "model"), {})[_text(row, "circuit_state")] = _int(row, "n")
    return [
        ModelSummary(
            model=_text(row, "model"),
            calls=_int(row, "calls"),
            prompt_tokens=_opt_int(row, "prompt_tokens"),
            completion_tokens=_opt_int(row, "completion_tokens"),
            total_tokens=_total_tokens(
                _opt_int(row, "prompt_tokens"), _opt_int(row, "completion_tokens")
            ),
            cache_hits=_int(row, "cache_hits"),
            # `None` when no call carried a cache attribute at all: "never
            # cached" and "not measured" are different answers.
            cache_hit_ratio=_ratio(_int(row, "cache_hits"), _int(row, "cache_measured")),
            errors=_int(row, "errors"),
            max_attempts=_opt_int(row, "max_attempts"),
            circuit_states=circuits.get(_text(row, "model"), {}),
        )
        for row in rows
    ]


def tools(store: ConsoleStore, *, since_ms: int | None = None) -> list[ToolSummary]:
    """Return per-tool call volume and failure ratio."""
    with _reader(store) as connection:
        return _tools(connection, since_ms)


def _tools(connection: sqlite3.Connection, since_ms: int | None) -> list[ToolSummary]:
    window, params = _since(since_ms)
    sql = f"""
        WITH tool_events AS (
            SELECT {_attr(TOOL_NAME)} AS tool_name,
                   event_type,
                   start_ms,
                   {_attr(ERROR_TYPE)} AS error_type
            FROM events
            WHERE {_attr(TOOL_NAME)} IS NOT NULL {window}
        )
        SELECT tool_name,
               SUM(CASE WHEN event_type = '{_TOOL_CALL}' THEN 1 ELSE 0 END) AS calls,
               SUM(CASE WHEN event_type = '{_INTENT_EMITTED}' THEN 1 ELSE 0 END) AS intents,
               SUM(CASE WHEN error_type IS NOT NULL THEN 1 ELSE 0 END) AS errors,
               MAX(start_ms) AS last_seen_ms
        FROM tool_events
        GROUP BY tool_name
        ORDER BY calls DESC, intents DESC, tool_name ASC
    """
    rows = _rows(connection, sql, params)
    return [
        ToolSummary(
            tool_name=_text(row, "tool_name"),
            calls=_int(row, "calls"),
            intents=_int(row, "intents"),
            errors=_int(row, "errors"),
            # A tool that only ever staged intents made no calls, so it has no
            # failure rate — reporting `0.0` would claim it never failed.
            failure_ratio=_ratio(_int(row, "errors"), _int(row, "calls")),
            last_seen_ms=_opt_int(row, "last_seen_ms"),
        )
        for row in rows
    ]


# -- approvals -----------------------------------------------------------------

_APPROVALS_SQL = f"""{_ERROR_ROWS},
approval_intents AS (
    SELECT i.entity_key AS entity_key,
           i.seq AS seq,
           i.step_index AS step_index,
           i.start_ms AS requested_ms,
           {_attr(INTENT_ID, table="i")} AS intent_id,
           {_attr(TOOL_NAME, table="i")} AS tool_name,
           CAST({_attr(EXPIRES_AT_MS, table="i")} AS INTEGER) AS expires_at_ms,
           (SELECT CAST({_attr(DEADLINE_MS, table="s")} AS INTEGER)
              FROM events s
             WHERE s.entity_key = i.entity_key AND s.seq = i.seq
               AND s.event_type = '{_SUSPENDED}'
             ORDER BY s.start_ms DESC LIMIT 1) AS deadline_ms,
           (SELECT COUNT(*)
              FROM events l
             WHERE l.entity_key = i.entity_key AND l.seq = i.seq
               AND l.event_type = '{_INTENT_EMITTED}'
               AND {_attr(INTENT_KIND, table="l")} = '{_APPROVAL}'
               AND l.start_ms > i.start_ms) AS escalations,
           (SELECT MIN(e.event_time_ms)
              FROM error_rows e
             WHERE e.entity_key = i.entity_key
               AND (e.seq = i.seq OR e.seq IS NULL)
               AND e.reason = '{REASON_HITL_TIMEOUT}') AS decided_ms
    FROM events i
    WHERE i.event_type = '{_INTENT_EMITTED}'
      AND {_attr(INTENT_KIND, table="i")} = '{_APPROVAL}'
)
SELECT * FROM approval_intents
WHERE (? = 0 OR decided_ms IS NULL)
ORDER BY requested_ms DESC, intent_id DESC
LIMIT ?
"""


def approvals(
    store: ConsoleStore, *, pending_only: bool = False, limit: int = 100
) -> list[ApprovalSummary]:
    """Return human-approval intents with their deadlines and decisions.

    The runtime records no approved/denied signal — an ``AgentEnvelope.Approval``
    is an input, not telemetry — so the only decision that can be reported is the
    one it does record: an ``hitl_timeout`` error, which is an expiry. Inferring
    "approved" from the fact that an activation resumed would be a guess printed
    as a fact.
    """
    with _reader(store) as connection:
        rows = _rows(connection, _APPROVALS_SQL, (1 if pending_only else 0, max(1, limit)))
    return [
        ApprovalSummary(
            intent_id=_text(row, "intent_id"),
            entity_key=_text(row, "entity_key"),
            seq=_int(row, "seq"),
            tool_name=_text(row, "tool_name"),
            step_index=_int(row, "step_index"),
            requested_ms=_int(row, "requested_ms"),
            deadline_ms=_opt_int(row, "deadline_ms"),
            expires_at_ms=_opt_int(row, "expires_at_ms"),
            escalations=_int(row, "escalations"),
            decision="pending" if row["decided_ms"] is None else "expired",
            decided_ms=_opt_int(row, "decided_ms"),
        )
        for row in rows
    ]


# -- entities ------------------------------------------------------------------

_ENTITY_COLUMNS = """
    a.entity_key AS entity_key,
    COUNT(*) AS activations,
    MIN(a.started_ms) AS first_seen_ms,
    MAX(a.started_ms) AS last_seen_ms,
    SUM(a.errors) AS errors,
    SUM(a.total_tokens) AS total_tokens,
    (SELECT b.seq FROM activations b WHERE b.entity_key = a.entity_key
      ORDER BY b.started_ms DESC, b.seq DESC LIMIT 1) AS latest_seq,
    (SELECT b.status FROM activations b WHERE b.entity_key = a.entity_key
      ORDER BY b.started_ms DESC, b.seq DESC LIMIT 1) AS latest_status
"""


def entities(
    store: ConsoleStore, *, cursor: str | None = None, limit: int = 50
) -> Page[EntitySummary]:
    """Return one page of entity keys with their activity across all sequences."""
    limit = max(1, limit)
    having = ""
    params: list[object] = []
    if cursor is not None:
        # The keyset applies to the aggregate, so it is a HAVING rather than a
        # WHERE: the ordering column is MAX(started_ms), which does not exist
        # until the group does.
        having = "HAVING (MAX(a.started_ms), a.entity_key) < (?, ?)"
        params = list(_decode_cursor(cursor, 2))
    with _reader(store) as connection:
        rows = _rows(
            connection,
            f"SELECT {_ENTITY_COLUMNS} FROM activations a GROUP BY a.entity_key {having} "
            "ORDER BY last_seen_ms DESC, entity_key DESC LIMIT ?",
            [*params, limit + 1],
        )
        total = _int(
            _rows(connection, "SELECT COUNT(DISTINCT entity_key) AS n FROM activations")[0], "n"
        )
    page, next_cursor = _paginate(rows, limit, ("last_seen_ms", "entity_key"))
    items = [
        EntitySummary(
            entity_key=_text(row, "entity_key"),
            activations=_int(row, "activations"),
            first_seen_ms=_int(row, "first_seen_ms"),
            last_seen_ms=_int(row, "last_seen_ms"),
            errors=_int(row, "errors"),
            total_tokens=_opt_int(row, "total_tokens"),
            latest_seq=_opt_int(row, "latest_seq"),
            latest_status=_status(_text(row, "latest_status")),
        )
        for row in page
    ]
    return Page[EntitySummary](items=items, next_cursor=next_cursor, total=total)


# -- search --------------------------------------------------------------------


def _search_entities(connection: sqlite3.Connection, like: str, limit: int) -> list[SearchHit]:
    rows = _rows(
        connection,
        "SELECT entity_key, MAX(started_ms) AS at_ms, COUNT(*) AS n FROM activations "
        "WHERE entity_key LIKE ? GROUP BY entity_key ORDER BY at_ms DESC LIMIT ?",
        (like, limit),
    )
    return [
        SearchHit(
            kind="entity",
            entity_key=_text(row, "entity_key"),
            label=f"{_text(row, 'entity_key')} ({_int(row, 'n')} activations)",
            matched_field="entity_key",
            matched_value=_text(row, "entity_key"),
            at_ms=_int(row, "at_ms"),
        )
        for row in rows
    ]


def _search_activations(connection: sqlite3.Connection, like: str, limit: int) -> list[SearchHit]:
    rows = _rows(
        connection,
        "SELECT entity_key, seq, trace_id, started_ms, model FROM activations "
        "WHERE entity_key LIKE ? OR trace_id LIKE ? ORDER BY started_ms DESC LIMIT ?",
        (like, like, limit),
    )
    hits: list[SearchHit] = []
    for row in rows:
        entity_key = _text(row, "entity_key")
        matched_field = "entity_key" if like.strip("%") in entity_key else "trace_id"
        hits.append(
            SearchHit(
                kind="activation",
                entity_key=entity_key,
                seq=_int(row, "seq"),
                trace_id=_text(row, "trace_id"),
                label=f"{entity_key}#{_int(row, 'seq')}",
                matched_field=matched_field,
                matched_value=entity_key
                if matched_field == "entity_key"
                else _text(row, "trace_id"),
                at_ms=_int(row, "started_ms"),
            )
        )
    return hits


def _search_events(connection: sqlite3.Connection, like: str, limit: int) -> list[SearchHit]:
    rows = _rows(
        connection,
        "SELECT e.trace_id, e.span_id, e.entity_key, e.seq, e.event_type, e.step_index, "
        "e.start_ms, _s.key AS attribute, _s.value AS matched "
        "FROM events e JOIN event_attributes _s ON _s.trace_id = e.trace_id "
        "AND _s.span_id = e.span_id AND _s.event_type = e.event_type "
        "WHERE _s.value LIKE ? ORDER BY e.start_ms DESC LIMIT ?",
        (like, limit),
    )
    return [
        SearchHit(
            kind="event",
            entity_key=_text(row, "entity_key"),
            seq=_int(row, "seq"),
            trace_id=_text(row, "trace_id"),
            span_id=_text(row, "span_id"),
            label=f"{_text(row, 'event_type')} @ step {_int(row, 'step_index')}",
            matched_field=_text(row, "attribute"),
            matched_value=_text(row, "matched"),
            at_ms=_int(row, "start_ms"),
        )
        for row in rows
    ]


def _search_errors(connection: sqlite3.Connection, like: str, limit: int) -> list[SearchHit]:
    rows = _error_records(
        connection,
        "WHERE reason LIKE ? OR detail LIKE ?",
        (like, like),
        "ORDER BY event_time_ms DESC",
        limit,
    )
    hits: list[SearchHit] = []
    for row in rows:
        reason = _text(row, "reason")
        matched_field = "reason" if like.strip("%") in reason else "detail"
        hits.append(
            SearchHit(
                kind="error",
                entity_key=_text(row, "entity_key"),
                seq=_opt_int(row, "seq"),
                label=reason,
                matched_field=matched_field,
                matched_value=reason if matched_field == "reason" else _text(row, "detail"),
                at_ms=_int(row, "event_time_ms"),
            )
        )
    return hits


def search(store: ConsoleStore, *, query: str, limit: int = 50) -> list[SearchHit]:
    """Search identifiers and attribute values, returning located hits.

    Every hit names where it was found — ``matched_field`` and ``matched_value``
    — because a search result that cannot be traced back to the record it
    matched is a dead end in the one workflow this view exists for.
    """
    limit = max(1, limit)
    like = f"%{query}%"
    # Each kind is capped so one prolific kind cannot crowd the others out of a
    # result set the operator is scanning for a specific record.
    per_kind = max(1, limit // _SEARCH_KINDS)
    with _reader(store) as connection:
        hits = [
            *_search_entities(connection, like, per_kind),
            *_search_activations(connection, like, per_kind),
            *_search_events(connection, like, per_kind),
            *_search_errors(connection, like, per_kind),
        ]
    return sorted(hits, key=lambda hit: (-hit.at_ms, hit.kind, hit.label))[:limit]


# -- store status --------------------------------------------------------------

_EXTENT_SQL = """
SELECT MIN(t) AS oldest, MAX(t) AS newest FROM (
    SELECT MIN(start_ms) AS t FROM events
    UNION ALL SELECT MAX(start_ms) FROM events
    UNION ALL SELECT MIN(event_time_ms) FROM errors
    UNION ALL SELECT MAX(event_time_ms) FROM errors
    UNION ALL SELECT MIN(snapshot_at_ms) FROM snapshots
    UNION ALL SELECT MAX(snapshot_at_ms) FROM snapshots
    UNION ALL SELECT MIN(started_ms) FROM activations
    UNION ALL SELECT MAX(started_ms) FROM activations
    UNION ALL SELECT MAX(ended_ms) FROM activations
)
"""

_COUNTS_SQL = """
SELECT (SELECT COUNT(*) FROM activations) AS activations,
       (SELECT COUNT(*) FROM errors) AS errors,
       (SELECT COUNT(*) FROM events) AS events,
       (SELECT COUNT(*) FROM snapshots) AS snapshots
"""


def _extent(connection: sqlite3.Connection) -> tuple[int | None, int | None]:
    row = _rows(connection, _EXTENT_SQL)[0]
    return _opt_int(row, "oldest"), _opt_int(row, "newest")


def store_status(store: ConsoleStore) -> StoreStatus:
    """Return row counts, retention window, and database extent.

    Counts and extent come from the same read connection as everything else
    rather than from ``ConsoleStore.counts()``, so an operator reading "42 events
    spanning this window" is reading one consistent snapshot instead of two
    observations taken either side of an ingest.
    """
    with _reader(store) as connection:
        return _store_status(connection, store)


def _store_status(connection: sqlite3.Connection, store: ConsoleStore) -> StoreStatus:
    path = store.path
    counts = _rows(connection, _COUNTS_SQL)[0]
    oldest, newest = _extent(connection)
    # The schema version the store records on the file itself, read through the
    # same connection rather than re-derived from `_schema`.
    version = _rows(connection, "PRAGMA user_version")[0]
    return StoreStatus(
        row_counts={
            key: _int(counts, key) for key in ("activations", "errors", "events", "snapshots")
        },
        retention_hours=store.retention_hours,
        database_path=str(path),
        database_bytes=path.stat().st_size if path.exists() else None,
        oldest_record_ms=oldest,
        newest_record_ms=newest,
        schema_version=_int(version, "user_version"),
    )


# -- overview ------------------------------------------------------------------


def _percentile(values: Sequence[int], fraction: float) -> int | None:
    """Nearest-rank percentile: an observed value, never an interpolated one."""
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


_OVERVIEW_SQL = """
SELECT COUNT(*) AS activations,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended,
       SUM(CASE WHEN status = 'in_flight' THEN 1 ELSE 0 END) AS in_flight,
       SUM(total_tokens) AS total_tokens,
       SUM(llm_calls) AS llm_calls,
       SUM(tool_calls) AS tool_calls
FROM activations WHERE started_ms >= ?
"""

_CACHE_SQL = f"""
SELECT SUM(CASE WHEN {_attr(CACHE_HIT)} = '{_TRUE}' THEN 1 ELSE 0 END) AS hits,
       SUM(CASE WHEN {_attr(CACHE_HIT)} IS NOT NULL THEN 1 ELSE 0 END) AS measured
FROM events WHERE event_type = '{_LLM_CALL}' AND start_ms >= ?
"""


def overview(store: ConsoleStore, *, window_ms: int, buckets: int = 48) -> Overview:
    """Return the headline figures and contiguous series for the landing page.

    Buckets are contiguous with explicit zeros: a gap in the data must read as a
    gap, not as a line interpolated across missing time.

    The window ends at the newest stored record rather than at "now", so a store
    populated from a bundle or a BigQuery window reads as data rather than as an
    empty page, and every figure stays a pure function of the database.
    """
    buckets = max(1, buckets)
    # One connection for the whole page: eight aggregates taken either side of an
    # ingest would not add up, and the overview is read while a pipeline writes.
    with _reader(store) as connection:
        _, newest = _extent(connection)
        if newest is None:
            return Overview(
                window_ms=window_ms,
                activations=0,
                completed=0,
                suspended=0,
                in_flight=0,
                errors=0,
                store=_store_status(connection, store),
            )
        since = newest - window_ms
        headline = _rows(connection, _OVERVIEW_SQL, (since,))[0]
        cache = _rows(connection, _CACHE_SQL, (since,))[0]
        walls = [
            _int(row, "wall_ms")
            for row in _rows(
                connection,
                "SELECT wall_ms FROM activations WHERE started_ms >= ? AND wall_ms IS NOT NULL",
                (since,),
            )
        ]
        windowed = _rows(
            connection,
            "SELECT started_ms, total_tokens FROM activations WHERE started_ms >= ?",
            (since,),
        )
        error_rows = _error_records(
            connection,
            "WHERE event_time_ms >= ?",
            (since,),
            "ORDER BY event_time_ms DESC",
            _NO_LIMIT,
        )
        size = _bucket_size(window_ms, buckets)
        activation_counts: dict[int, float] = {}
        token_counts: dict[int, float] = {}
        for row in windowed:
            bucket = (_int(row, "started_ms") // size) * size
            activation_counts[bucket] = activation_counts.get(bucket, 0.0) + 1.0
            tokens = _opt_int(row, "total_tokens")
            if tokens is not None:
                token_counts[bucket] = token_counts.get(bucket, 0.0) + tokens
        error_counts: dict[int, float] = {}
        for row in error_rows:
            bucket = (_int(row, "event_time_ms") // size) * size
            error_counts[bucket] = error_counts.get(bucket, 0.0) + 1.0

        def series(counts: Mapping[int, float]) -> list[BucketPoint]:
            return _series(counts, first_ms=since, last_ms=newest, bucket_ms=size)

        activation_count = _int(headline, "activations")
        return Overview(
            window_ms=window_ms,
            activations=activation_count,
            completed=_int(headline, "completed"),
            suspended=_int(headline, "suspended"),
            in_flight=_int(headline, "in_flight"),
            errors=len(error_rows),
            error_ratio=_ratio(len(error_rows), activation_count),
            total_tokens=_opt_int(headline, "total_tokens"),
            llm_calls=_int(headline, "llm_calls"),
            tool_calls=_int(headline, "tool_calls"),
            cache_hit_ratio=_ratio(_int(cache, "hits"), _int(cache, "measured")),
            p50_wall_ms=_percentile(walls, _P50),
            p95_wall_ms=_percentile(walls, _P95),
            activation_series=series(activation_counts),
            error_series=series(error_counts),
            token_series=series(token_counts),
            top_models=_models(connection, since)[:_TOP_N],
            top_tools=_tools(connection, since)[:_TOP_N],
            recent_errors=[_error_record(row) for row in error_rows[:_TOP_N]],
            store=_store_status(connection, store),
        )
