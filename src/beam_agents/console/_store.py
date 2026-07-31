"""The console's record store: a single WAL-mode SQLite file.

Writes are serialized through one connection; reads open their own, which WAL
makes concurrent with the writer. That is the whole concurrency model, and it is
sized for what this is — a viewer a developer runs beside a pipeline, not a
metrics backend.

Two properties are load-bearing and are worth stating where they are
implemented rather than only in the design:

**Ingest is idempotent** (design D5). Trace records are at-least-once and
byte-identical under replay, so a retried bundle, a replayed run, and a Kafka
consumer restarting from an earlier offset must all converge. Events upsert on
``(trace_id, span_id, event_type)`` — the dedup key ``docs/traces.md``
publishes — and a later copy *merges* its attributes rather than being
discarded, because the OTLP form of an event carries strictly fewer attributes
than the native form and the richer copy should win.

**Rollups are derived, never written.** An activation's status, kind, token
totals, and call counts are recomputed from its events on every write, so they
are correct after any subset has arrived and stay correct when the rest does.
An activation with no ``ACTIVATION_END`` is in flight, not guessed at.

Three consequences of "merge, don't replace" are decided here:

- A merge never replaces a known value with an unknown one. An empty
  ``entity_key`` or ``parent_span_id`` on the incoming copy keeps the stored
  one, because a decoder that could not recover a field must not be able to
  erase a field another decoder did. ``seq`` and ``step_index`` are exempt: ``0``
  is a legitimate value for both, so there is no "unknown" to test for and the
  incoming value always wins.
- Provenance never degrades. A duplicate arriving over OTLP does not relabel a
  natively-ingested record as lossy, because ``ActivationSummary``'s
  incomplete-provenance warning keys off exactly that label.
- A failure recorded on both ``.traces`` (as an ``ERROR`` event) and ``.errors``
  (as an ``ActivationErrorRecord``) is *one* failure. The rollup takes the
  larger of the two counts rather than their sum: summing double-counts a
  failure delivered on both paths, and picking one view reports zero whenever
  only the other path is wired.

``prune`` takes ``now_ms`` as an argument and never reads a clock, so retention
is testable and a caller replaying history prunes against the time it is
replaying rather than the time it is running.

Importing this module has no side effects.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Self

from beam_agents._protos import TraceEvent
from beam_agents.console._records import PROVENANCE_OTLP
from beam_agents.console._schema import TABLES, apply_schema
from beam_agents.observability.traces import (
    ACTIVATION_KIND,
    ACTIVATION_STATUS,
    CACHE_HIT,
    REASON,
    REQUEST_MODEL,
    ROLE_ACTIVATION,
    TOOL_NAME,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
    trace_id_for,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence
    from types import TracebackType

    from beam_agents.console._records import ErrorRow, EventRow, RecordBatch, SnapshotRow

__all__ = ["ConsoleStore"]

_MS_PER_HOUR = 3_600_000
# How long a writer waits on a lock before giving up. WAL keeps readers out of
# the writer's way, so contention here means two writers, which the store does
# not have; the timeout exists for the checkpointer, not for throughput.
_BUSY_TIMEOUT_MS = 5_000

# Event-type spellings, read off the proto's own enum rather than written out,
# so the store cannot drift from the names the BigQuery encoder and every
# decoder produce.
_ACTIVATION_START = TraceEvent.EventType.Name(TraceEvent.ACTIVATION_START)
_ACTIVATION_END = TraceEvent.EventType.Name(TraceEvent.ACTIVATION_END)
_LLM_CALL = TraceEvent.EventType.Name(TraceEvent.LLM_CALL)
_TOOL_CALL = TraceEvent.EventType.Name(TraceEvent.TOOL_CALL)
_INTENT_EMITTED = TraceEvent.EventType.Name(TraceEvent.INTENT_EMITTED)
_ERROR = TraceEvent.EventType.Name(TraceEvent.ERROR)

# `_dto.ActivationStatus`, split by where each value comes from: the two in
# `_DECLARED_STATUSES` are written by the runtime as an `ACTIVATION_END`
# attribute, and the other two are the store's own readings of the evidence.
_STATUS_IN_FLIGHT = "in_flight"
_STATUS_ERROR = "error"
_DECLARED_STATUSES = frozenset({"completed", "suspended"})
_KINDS = frozenset({"start", "resume"})
_KIND_UNKNOWN = "unknown"


def _upsert(
    table: str,
    columns: Sequence[str],
    *,
    conflict: Sequence[str],
    merge: Sequence[tuple[str, str]],
) -> str:
    """Build an idempotent upsert whose no-op case changes no row.

    The ``WHERE`` guard on the ``DO UPDATE`` is what makes ``write`` able to
    report ``0`` for a re-ingested batch: without it SQLite counts a write that
    stores the values already there, and every retried bundle would look like
    new data.
    """
    values = ", ".join("?" * len(columns))
    assignments = ", ".join(f"{column} = {expression}" for column, expression in merge)
    # `IS NOT`, not `<>`: the comparison has to be null-safe, because most of
    # what merges here is nullable and `NULL <> NULL` is NULL, which would read
    # as "unchanged" and skip a real update.
    guard = " OR ".join(f"{table}.{column} IS NOT ({expression})" for column, expression in merge)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values}) "
        f"ON CONFLICT({', '.join(conflict)}) DO UPDATE SET {assignments} WHERE {guard}"
    )


def _keep_if_empty(table: str, column: str) -> str:
    """An incoming empty string does not erase a stored non-empty one."""
    return f"CASE WHEN excluded.{column} <> '' THEN excluded.{column} ELSE {table}.{column} END"


def _keep_if_zero(table: str, column: str) -> str:
    """An incoming zero timestamp does not erase a stored one."""
    return f"CASE WHEN excluded.{column} <> 0 THEN excluded.{column} ELSE {table}.{column} END"


def _keep_lossless(table: str) -> str:
    """A duplicate arriving over OTLP never relabels a lossless record as lossy."""
    return (
        f"CASE WHEN excluded.provenance = '{PROVENANCE_OTLP}' "
        f"AND {table}.provenance <> '{PROVENANCE_OTLP}' "
        f"THEN {table}.provenance ELSE excluded.provenance END"
    )


_EVENT_COLUMNS = (
    "trace_id",
    "span_id",
    "event_type",
    "parent_span_id",
    "entity_key",
    "seq",
    "step_index",
    "start_ms",
    "end_ms",
    "provenance",
)
_EVENT_UPSERT = _upsert(
    "events",
    _EVENT_COLUMNS,
    conflict=("trace_id", "span_id", "event_type"),
    merge=(
        ("parent_span_id", _keep_if_empty("events", "parent_span_id")),
        ("entity_key", _keep_if_empty("events", "entity_key")),
        ("seq", "excluded.seq"),
        ("step_index", "excluded.step_index"),
        ("start_ms", _keep_if_zero("events", "start_ms")),
        ("end_ms", _keep_if_zero("events", "end_ms")),
        ("provenance", _keep_lossless("events")),
    ),
)

_ATTRIBUTE_UPSERT = _upsert(
    "event_attributes",
    ("trace_id", "span_id", "event_type", "key", "value"),
    conflict=("trace_id", "span_id", "event_type", "key"),
    merge=(("value", "excluded.value"),),
)

_ERROR_COLUMNS = (
    "error_id",
    "entity_key",
    "seq",
    "reason",
    "detail",
    "event_time_ms",
    "provenance",
)
# Every payload field is folded into `error_id`, so provenance is the only
# column a second delivery of the same record can legitimately move.
_ERROR_UPSERT = _upsert(
    "errors",
    _ERROR_COLUMNS,
    conflict=("error_id",),
    merge=(("provenance", _keep_lossless("errors")),),
)

_SNAPSHOT_COLUMNS = (
    "entity_key",
    "seq",
    "request_id",
    "snapshot_at_ms",
    "state_schema_version",
    "memory_entries",
    "memory_bytes",
    "llm_cache_entries",
    "pending_intent_ids",
    "continuation_step_index",
    "continuation_deadline_ms",
    "continuation_adapter",
    "raw",
    "provenance",
)
_SNAPSHOT_UPSERT = _upsert(
    "snapshots",
    _SNAPSHOT_COLUMNS,
    conflict=("entity_key", "seq", "request_id", "snapshot_at_ms"),
    merge=(
        ("state_schema_version", "excluded.state_schema_version"),
        ("memory_entries", "excluded.memory_entries"),
        ("memory_bytes", "excluded.memory_bytes"),
        ("llm_cache_entries", "excluded.llm_cache_entries"),
        ("pending_intent_ids", "excluded.pending_intent_ids"),
        ("continuation_step_index", "excluded.continuation_step_index"),
        ("continuation_deadline_ms", "excluded.continuation_deadline_ms"),
        ("continuation_adapter", "excluded.continuation_adapter"),
        # The state image is opaque and expensive; an incoming empty one is a
        # summary-only delivery, not an instruction to forget the bytes.
        ("raw", "CASE WHEN length(excluded.raw) > 0 THEN excluded.raw ELSE snapshots.raw END"),
        ("provenance", _keep_lossless("snapshots")),
    ),
)

_ACTIVATION_COLUMNS = (
    "entity_key",
    "seq",
    "trace_id",
    "status",
    "kind",
    "attempts",
    "started_ms",
    "ended_ms",
    "wall_ms",
    "model",
    "llm_calls",
    "tool_calls",
    "intents",
    "errors",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "cache_hits",
    "tools",
    "reasons",
    "provenance",
    "complete_provenance",
)
# Wholly derived, so every column is replaced rather than merged: this row is a
# function of the records, and a stale field would be a second opinion about
# them.
_ACTIVATION_UPSERT = _upsert(
    "activations",
    _ACTIVATION_COLUMNS,
    conflict=("entity_key", "seq"),
    merge=tuple((column, f"excluded.{column}") for column in _ACTIVATION_COLUMNS[2:]),
)

_TRACE_COLUMNS = ("trace_id", "entity_key", "seq", "started_ms", "last_event_ms", "events", "spans")
_TRACE_UPSERT = _upsert(
    "traces",
    _TRACE_COLUMNS,
    conflict=("trace_id",),
    merge=tuple((column, f"excluded.{column}") for column in _TRACE_COLUMNS[1:]),
)

_SPAN_COLUMNS = (
    "trace_id",
    "span_id",
    "parent_span_id",
    "entity_key",
    "seq",
    "role",
    "step_index",
    "first_ms",
    "last_ms",
    "events",
)
_SPAN_UPSERT = _upsert(
    "spans",
    _SPAN_COLUMNS,
    conflict=("trace_id", "span_id"),
    merge=tuple((column, f"excluded.{column}") for column in _SPAN_COLUMNS[2:]),
)

_ENTITY_COLUMNS = (
    "entity_key",
    "first_seen_ms",
    "last_seen_ms",
    "activations",
    "errors",
    "total_tokens",
    "latest_seq",
    "latest_status",
)
_ENTITY_UPSERT = _upsert(
    "entities",
    _ENTITY_COLUMNS,
    conflict=("entity_key",),
    merge=tuple((column, f"excluded.{column}") for column in _ENTITY_COLUMNS[1:]),
)


class _StoredEvent(NamedTuple):
    """One event as the rollup reads it back: identity, position, and attributes."""

    trace_id: str
    span_id: str
    event_type: str
    start_ms: int
    provenance: str
    attributes: dict[str, str]


class _StoredError(NamedTuple):
    """One ``.errors`` record as the rollup reads it back."""

    reason: str
    event_time_ms: int
    provenance: str


class _Rollup(NamedTuple):
    """An activation's derived summary — exactly ``_dto.ActivationSummary``'s fields."""

    trace_id: str
    status: str
    kind: str
    attempts: int
    started_ms: int
    ended_ms: int | None
    wall_ms: int | None
    model: str | None
    llm_calls: int
    tool_calls: int
    intents: int
    errors: int
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    cache_hits: int
    tools: tuple[str, ...]
    reasons: tuple[str, ...]
    provenance: tuple[str, ...]
    complete_provenance: bool


def _error_id(error: ErrorRow) -> str:
    """Derive a stable identity for an error record from its whole payload.

    ``ActivationErrorRecord`` carries no ID and the runtime mints none, but it
    *is* replay-deterministic — ``event_time_ms`` is an element time or a
    timer's scheduled firing, never a wall clock — so hashing the payload gives
    a re-delivered record the identity it needs to collapse onto itself.
    """
    material = "\x00".join(
        (
            error.entity_key,
            "" if error.seq is None else str(error.seq),
            error.reason,
            str(error.event_time_ms),
            error.detail,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _derived_trace_id(entity_key: str, seq: int) -> str:
    """Recompute an activation's trace ID from its scope.

    Trace identity is ``uuid5(entity_key, seq)``, so an activation known only
    from a ``.errors`` record can still be linked to the trace its events will
    land in when they arrive.
    """
    try:
        return trace_id_for(bytes.fromhex(entity_key), seq).hex()
    except ValueError:
        return ""


def _int_attribute(attributes: dict[str, str], key: str) -> int | None:
    """Read an integer attribute, or ``None`` when it is absent or unreadable."""
    raw = attributes.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _sum_attribute(events: Sequence[_StoredEvent], key: str) -> int | None:
    """Sum an attribute across events, or ``None`` when no event recorded it.

    ``None`` rather than ``0`` all the way through: ``usage_attributes`` omits a
    token count it does not know precisely so that nothing summing the stream
    reads a placeholder zero as a real zero-token call.
    """
    values = [
        value
        for value in (_int_attribute(event.attributes, key) for event in events)
        if value is not None
    ]
    return sum(values) if values else None


def _latest(events: Sequence[_StoredEvent]) -> _StoredEvent:
    """Return the last event in the activation's total order."""
    return max(events, key=lambda event: (event.start_ms, event.span_id, event.event_type))


def _summarize(
    entity_key: str,
    seq: int,
    events: Sequence[_StoredEvent],
    errors: Sequence[_StoredError],
) -> _Rollup | None:
    """Derive an activation's rollup, or ``None`` when nothing is left of it."""
    if not events and not errors:
        return None

    starts = [event for event in events if event.event_type == _ACTIVATION_START]
    ends = [event for event in events if event.event_type == _ACTIVATION_END]
    error_events = [event for event in events if event.event_type == _ERROR]
    llm_calls = [event for event in events if event.event_type == _LLM_CALL]
    tool_calls = [event for event in events if event.event_type == _TOOL_CALL]
    intents = [event for event in events if event.event_type == _INTENT_EMITTED]

    # An attempt is one activation-role span, and both of its events carry that
    # span: a suspend and its resume share `(entity_key, seq)` — and therefore
    # one trace — and differ only by the entry step index their span ID is
    # derived from, which is what makes them two attempts of one activation
    # rather than two activations. The union, not either side: OTLP carries no
    # ACTIVATION_START, so counting starts alone would undercount an activation
    # assembled from a lossy source, and counting ends alone would undercount
    # one still in flight.
    start_spans = {event.span_id for event in starts}
    end_spans = {event.span_id for event in ends}
    open_attempts = start_spans - end_spans

    terminal_ms = [event.start_ms for event in error_events]
    terminal_ms += [error.event_time_ms for error in errors]
    latest_end = _latest(ends) if ends else None
    if terminal_ms:
        # A failed activation gets no ACTIVATION_END — the loop only emits one
        # for `completed` and `suspended` — so the ERROR record is its terminal
        # evidence, and reporting it as in flight would strand it there.
        status = _STATUS_ERROR
        ended_ms: int | None = max(terminal_ms)
    elif latest_end is not None and not open_attempts:
        declared = latest_end.attributes.get(ACTIVATION_STATUS, "")
        # An END whose status this build does not recognize is a terminal event
        # whose outcome is unreadable; naming one anyway would be a guess, and
        # `ActivationStatus` has no value for "ended, cause unknown".
        status = declared if declared in _DECLARED_STATUSES else _STATUS_IN_FLIGHT
        ended_ms = latest_end.start_ms if status in _DECLARED_STATUSES else None
    else:
        status = _STATUS_IN_FLIGHT
        ended_ms = None

    kind_source = starts or ends
    kind = _latest(kind_source).attributes.get(ACTIVATION_KIND, "") if kind_source else ""

    models = [event for event in llm_calls if event.attributes.get(REQUEST_MODEL)]
    prompt_tokens = _sum_attribute(llm_calls, USAGE_INPUT_TOKENS)
    completion_tokens = _sum_attribute(llm_calls, USAGE_OUTPUT_TOKENS)

    provenance = {event.provenance for event in events} | {error.provenance for error in errors}
    started = [event.start_ms for event in events] + [error.event_time_ms for error in errors]

    return _Rollup(
        trace_id=events[0].trace_id if events else _derived_trace_id(entity_key, seq),
        status=status,
        kind=kind if kind in _KINDS else _KIND_UNKNOWN,
        attempts=max(len(start_spans | end_spans), 1),
        started_ms=min(started),
        ended_ms=ended_ms,
        # The only duration here, and it is a real one: two clock reads, one per
        # attempt boundary. It is `0` for a single attempt because both events
        # carry that attempt's single activation-clock read (traces D7) — a
        # measured zero, not a missing measurement.
        wall_ms=(
            ended_ms - min(event.start_ms for event in starts)
            if starts and ended_ms is not None
            else None
        ),
        model=_latest(models).attributes[REQUEST_MODEL] if models else None,
        llm_calls=len(llm_calls),
        tool_calls=len(tool_calls),
        intents=len(intents),
        errors=max(len(error_events), len(errors)),
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=(
            None
            if prompt_tokens is None and completion_tokens is None
            else (prompt_tokens or 0) + (completion_tokens or 0)
        ),
        cache_hits=sum(1 for event in llm_calls if event.attributes.get(CACHE_HIT) == "true"),
        tools=tuple(
            sorted(
                {
                    event.attributes[TOOL_NAME]
                    for event in (*tool_calls, *intents)
                    if event.attributes.get(TOOL_NAME)
                }
            )
        ),
        reasons=tuple(
            sorted(
                {event.attributes[REASON] for event in error_events if event.attributes.get(REASON)}
                | {error.reason for error in errors if error.reason}
            )
        ),
        provenance=tuple(sorted(provenance)),
        # OTLP is the one encoding that cannot carry ACTIVATION_START, so an
        # activation touched by it and missing that event has an unknowable
        # start-vs-resume. Any other source arriving without it yet is merely
        # incomplete, not lossy.
        complete_provenance=bool(starts) or PROVENANCE_OTLP not in provenance,
    )


def _delete(connection: sqlite3.Connection, sql: str, parameters: Sequence[object] = ()) -> int:
    """Run a delete and return how many rows it removed."""
    return max(connection.execute(sql, parameters).rowcount, 0)


class ConsoleStore:
    """A WAL-mode SQLite store for trace, error, and snapshot records.

    Opening a path that does not exist creates the file and its schema, so a
    fresh database and an existing one are both valid starting states. Use it as
    a context manager, or call :meth:`close` when done.
    """

    def __init__(self, path: str | Path, *, retention_hours: float | None = None) -> None:
        """Open (or create) the store at ``path``.

        ``retention_hours`` bounds how far back :meth:`prune` keeps records;
        ``None`` retains everything and makes :meth:`prune` a no-op.
        """
        self._path = Path(path)
        self._retention_hours = retention_hours
        self._closed = False
        # One writer, serialized: SQLite allows a single writer per database and
        # a lock here turns a would-be `database is locked` into a queue.
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = self._connect()
        try:
            # WAL is what makes the readers below concurrent with this writer;
            # it is a property of the file, so readers inherit it. `NORMAL`
            # trades a fsync per commit for the last few milliseconds of
            # telemetry on a machine crash, which is the right trade for a
            # viewer over records the pipeline still holds.
            self._writer.execute("PRAGMA journal_mode = WAL")
            self._writer.execute("PRAGMA synchronous = NORMAL")
            apply_schema(self._writer)
        except BaseException:
            self._writer.close()
            self._closed = True
            raise

    def __enter__(self) -> Self:
        """Return the open store."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the store."""
        self.close()

    def close(self) -> None:
        """Close every connection the store holds. Idempotent."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._writer.close()

    @property
    def path(self) -> Path:
        """The filesystem path this store was opened on."""
        return self._path

    @property
    def retention_hours(self) -> float | None:
        """The configured retention window, or ``None`` when unbounded."""
        return self._retention_hours

    def write(self, batch: RecordBatch) -> int:
        """Write ``batch`` idempotently; return the number of rows it changed.

        A return of ``0`` means every record in the batch was already present
        and identical — the expected outcome when a bundle is retried or a
        replayed run is re-ingested.
        """
        if not batch:
            return 0
        with self._lock:
            connection = self._open_connection()
            before = connection.total_changes
            connection.execute("BEGIN IMMEDIATE")
            try:
                activations = self._write_events(connection, batch.events)
                activations |= self._write_errors(connection, batch.errors)
                self._write_snapshots(connection, batch.snapshots)
                self._recompute(
                    connection,
                    activations=activations,
                    traces={event.trace_id for event in batch.events},
                    entities={key for key, _ in activations}
                    | {error.entity_key for error in batch.errors},
                )
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")
            return connection.total_changes - before

    def counts(self) -> dict[str, int]:
        """Return row counts per table, for the API's store-status report."""
        with self.reader() as connection:
            return {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in TABLES
            }

    def prune(self, *, now_ms: int) -> int:
        """Delete records older than the retention window; return rows removed.

        ``now_ms`` is passed in rather than read from a clock so pruning is
        testable and so a caller replaying history can prune against the time it
        is replaying, not the time it is running.
        """
        if self._retention_hours is None:
            return 0
        cutoff_ms = now_ms - int(self._retention_hours * _MS_PER_HOUR)
        with self._lock:
            connection = self._open_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                removed = self._prune(connection, cutoff_ms)
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")
            return removed

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        """Yield a read-only connection, as a context manager.

        The query layer's single entry point to the database. Separate from the
        writer connection so a long read cannot delay ingest.

        ``query_only`` rather than SQLite's ``mode=ro``: the guarantee wanted
        here is "this connection cannot write", and a genuinely read-only file
        handle additionally cannot create the WAL's shared-memory index, which
        would make a reader's success depend on whether a writer happened to
        open first.
        """
        if self._closed:
            raise RuntimeError(f"console store at {self._path} is closed")
        connection = self._connect()
        try:
            connection.execute("PRAGMA query_only = ON")
            yield connection
        finally:
            connection.close()

    # -- connections ----------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._path,
            # Transactions are opened explicitly (`BEGIN IMMEDIATE`) so a write
            # and the rollups it implies commit together or not at all; the
            # driver's implicit-transaction mode would start one before the
            # first DML and leave the DDL outside it.
            isolation_level=None,
            # The writer is shared by whatever thread ingests — a sink's sender
            # thread, an ASGI worker — and is serialized by `self._lock`
            # instead of by the driver's per-thread check.
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS:d}")
        return connection

    def _open_connection(self) -> sqlite3.Connection:
        if self._closed:
            raise RuntimeError(f"console store at {self._path} is closed")
        return self._writer

    # -- record writes --------------------------------------------------------

    def _write_events(
        self, connection: sqlite3.Connection, events: Sequence[EventRow]
    ) -> set[tuple[str, int]]:
        for event in events:
            connection.execute(
                _EVENT_UPSERT,
                (
                    event.trace_id,
                    event.span_id,
                    event.event_type,
                    event.parent_span_id,
                    event.entity_key,
                    event.seq,
                    event.step_index,
                    event.start_ms,
                    event.end_ms,
                    event.provenance,
                ),
            )
            for key, value in event.attributes.items():
                connection.execute(
                    _ATTRIBUTE_UPSERT,
                    (event.trace_id, event.span_id, event.event_type, key, value),
                )
        return {(event.entity_key, event.seq) for event in events}

    def _write_errors(
        self, connection: sqlite3.Connection, errors: Sequence[ErrorRow]
    ) -> set[tuple[str, int]]:
        for error in errors:
            connection.execute(
                _ERROR_UPSERT,
                (
                    _error_id(error),
                    error.entity_key,
                    error.seq,
                    error.reason,
                    error.detail,
                    error.event_time_ms,
                    error.provenance,
                ),
            )
        # Several reasons fire from timer callbacks with no activation, so an
        # error without a `seq` belongs to the entity and to nothing narrower.
        return {(error.entity_key, error.seq) for error in errors if error.seq is not None}

    def _write_snapshots(
        self, connection: sqlite3.Connection, snapshots: Sequence[SnapshotRow]
    ) -> None:
        for snapshot in snapshots:
            connection.execute(
                _SNAPSHOT_UPSERT,
                (
                    snapshot.entity_key,
                    snapshot.seq,
                    snapshot.request_id,
                    snapshot.snapshot_at_ms,
                    snapshot.state_schema_version,
                    snapshot.memory_entries,
                    snapshot.memory_bytes,
                    snapshot.llm_cache_entries,
                    json.dumps(list(snapshot.pending_intent_ids)),
                    snapshot.continuation_step_index,
                    snapshot.continuation_deadline_ms,
                    snapshot.continuation_adapter,
                    snapshot.raw,
                    snapshot.provenance,
                ),
            )

    # -- derived rollups ------------------------------------------------------

    def _recompute(
        self,
        connection: sqlite3.Connection,
        *,
        activations: Iterable[tuple[str, int]],
        traces: Iterable[str],
        entities: Iterable[str],
    ) -> int:
        """Rebuild every derived row the touched records feed; return rows removed."""
        removed = 0
        for trace_id in traces:
            removed += self._recompute_trace(connection, trace_id)
        for entity_key, seq in activations:
            removed += self._recompute_activation(connection, entity_key, seq)
        for entity_key in entities:
            removed += self._recompute_entity(connection, entity_key)
        return removed

    def _recompute_activation(
        self, connection: sqlite3.Connection, entity_key: str, seq: int
    ) -> int:
        rollup = _summarize(
            entity_key,
            seq,
            self._read_events(connection, entity_key, seq),
            [
                _StoredError(row["reason"], int(row["event_time_ms"]), row["provenance"])
                for row in connection.execute(
                    "SELECT reason, event_time_ms, provenance FROM errors "
                    "WHERE entity_key = ? AND seq = ?",
                    (entity_key, seq),
                )
            ],
        )
        if rollup is None:
            return sum(
                _delete(
                    connection,
                    f"DELETE FROM {table} WHERE entity_key = ? AND seq = ?",
                    (entity_key, seq),
                )
                for table in ("activations", "activation_tools", "activation_reasons")
            )

        connection.execute(
            _ACTIVATION_UPSERT,
            (
                entity_key,
                seq,
                rollup.trace_id,
                rollup.status,
                rollup.kind,
                rollup.attempts,
                rollup.started_ms,
                rollup.ended_ms,
                rollup.wall_ms,
                rollup.model,
                rollup.llm_calls,
                rollup.tool_calls,
                rollup.intents,
                rollup.errors,
                rollup.prompt_tokens,
                rollup.completion_tokens,
                rollup.total_tokens,
                rollup.cache_hits,
                json.dumps(list(rollup.tools)),
                json.dumps(list(rollup.reasons)),
                json.dumps(list(rollup.provenance)),
                int(rollup.complete_provenance),
            ),
        )
        removed = self._sync_facet(
            connection, "activation_tools", "tool_name", entity_key, seq, rollup.tools
        )
        return removed + self._sync_facet(
            connection, "activation_reasons", "reason", entity_key, seq, rollup.reasons
        )

    def _sync_facet(
        self,
        connection: sqlite3.Connection,
        table: str,
        column: str,
        entity_key: str,
        seq: int,
        values: Sequence[str],
    ) -> int:
        for value in values:
            connection.execute(
                f"INSERT OR IGNORE INTO {table} (entity_key, seq, {column}) VALUES (?, ?, ?)",
                (entity_key, seq, value),
            )
        if not values:
            return _delete(
                connection,
                f"DELETE FROM {table} WHERE entity_key = ? AND seq = ?",
                (entity_key, seq),
            )
        placeholders = ", ".join("?" * len(values))
        return _delete(
            connection,
            f"DELETE FROM {table} WHERE entity_key = ? AND seq = ? "
            f"AND {column} NOT IN ({placeholders})",
            (entity_key, seq, *values),
        )

    def _read_events(
        self, connection: sqlite3.Connection, entity_key: str, seq: int
    ) -> list[_StoredEvent]:
        """Read one activation's events with their attributes already attached."""
        rows = connection.execute(
            "SELECT e.trace_id, e.span_id, e.event_type, e.start_ms, e.provenance, "
            "       a.key, a.value "
            "FROM events AS e "
            "LEFT JOIN event_attributes AS a "
            "  ON a.trace_id = e.trace_id AND a.span_id = e.span_id "
            " AND a.event_type = e.event_type "
            "WHERE e.entity_key = ? AND e.seq = ? "
            "ORDER BY e.start_ms, e.span_id, e.event_type",
            (entity_key, seq),
        )
        events: dict[tuple[str, str, str], _StoredEvent] = {}
        for row in rows:
            identity = (row["trace_id"], row["span_id"], row["event_type"])
            event = events.get(identity)
            if event is None:
                event = _StoredEvent(
                    trace_id=row["trace_id"],
                    span_id=row["span_id"],
                    event_type=row["event_type"],
                    start_ms=int(row["start_ms"]),
                    provenance=row["provenance"],
                    attributes={},
                )
                events[identity] = event
            if row["key"] is not None:
                event.attributes[row["key"]] = row["value"]
        return list(events.values())

    def _recompute_trace(self, connection: sqlite3.Connection, trace_id: str) -> int:
        summary = connection.execute(
            "SELECT COUNT(*) AS events, COUNT(DISTINCT span_id) AS spans, "
            "       MIN(start_ms) AS started_ms, MAX(start_ms) AS last_event_ms, "
            "       MAX(entity_key) AS entity_key, MAX(seq) AS seq "
            "FROM events WHERE trace_id = ?",
            (trace_id,),
        ).fetchone()
        if int(summary["events"]) == 0:
            return _delete(
                connection, "DELETE FROM spans WHERE trace_id = ?", (trace_id,)
            ) + _delete(connection, "DELETE FROM traces WHERE trace_id = ?", (trace_id,))

        connection.execute(
            _TRACE_UPSERT,
            (
                trace_id,
                summary["entity_key"],
                int(summary["seq"]),
                int(summary["started_ms"]),
                int(summary["last_event_ms"]),
                int(summary["events"]),
                int(summary["spans"]),
            ),
        )
        span_ids: list[str] = []
        for row in connection.execute(
            "SELECT span_id, MAX(parent_span_id) AS parent_span_id, "
            "       MAX(entity_key) AS entity_key, MAX(seq) AS seq, MIN(step_index) AS step_index, "
            "       MIN(start_ms) AS first_ms, MAX(end_ms) AS last_ms, COUNT(*) AS events, "
            "       MAX(event_type IN (?, ?)) AS is_activation, MIN(event_type) AS event_type "
            "FROM events WHERE trace_id = ? GROUP BY span_id",
            (_ACTIVATION_START, _ACTIVATION_END, trace_id),
        ).fetchall():
            span_ids.append(row["span_id"])
            connection.execute(
                _SPAN_UPSERT,
                (
                    trace_id,
                    row["span_id"],
                    row["parent_span_id"],
                    row["entity_key"],
                    int(row["seq"]),
                    # The same rule `role_for_event_type` applies at emission:
                    # the two activation events share the attempt's own span,
                    # everything else names its span after its event type.
                    ROLE_ACTIVATION if row["is_activation"] else row["event_type"],
                    int(row["step_index"]),
                    int(row["first_ms"]),
                    int(row["last_ms"]),
                    int(row["events"]),
                ),
            )
        placeholders = ", ".join("?" * len(span_ids))
        return _delete(
            connection,
            f"DELETE FROM spans WHERE trace_id = ? AND span_id NOT IN ({placeholders})",
            (trace_id, *span_ids),
        )

    def _recompute_entity(self, connection: sqlite3.Connection, entity_key: str) -> int:
        activations = connection.execute(
            "SELECT COUNT(*) AS activations, MIN(started_ms) AS first_ms, "
            "       MAX(COALESCE(ended_ms, started_ms)) AS last_ms, SUM(errors) AS errors, "
            "       SUM(total_tokens) AS total_tokens, COUNT(total_tokens) AS token_rows "
            "FROM activations WHERE entity_key = ?",
            (entity_key,),
        ).fetchone()
        errors = connection.execute(
            "SELECT COUNT(*) AS errors, MIN(event_time_ms) AS first_ms, "
            "       MAX(event_time_ms) AS last_ms, "
            "       COUNT(*) FILTER (WHERE seq IS NULL) AS unscoped "
            "FROM errors WHERE entity_key = ?",
            (entity_key,),
        ).fetchone()
        if int(activations["activations"]) == 0 and int(errors["errors"]) == 0:
            return _delete(connection, "DELETE FROM entities WHERE entity_key = ?", (entity_key,))

        extents = [
            value for value in (activations["first_ms"], errors["first_ms"]) if value is not None
        ]
        latest = connection.execute(
            "SELECT seq, status FROM activations WHERE entity_key = ? ORDER BY seq DESC LIMIT 1",
            (entity_key,),
        ).fetchone()
        connection.execute(
            _ENTITY_UPSERT,
            (
                entity_key,
                min(int(value) for value in extents),
                max(
                    int(value)
                    for value in (activations["last_ms"], errors["last_ms"])
                    if value is not None
                ),
                int(activations["activations"]),
                # Per-activation counts already reconcile the ERROR event and
                # the `.errors` record for the same failure; only the errors
                # that belong to no activation are outside that reckoning.
                int(activations["errors"] or 0) + int(errors["unscoped"]),
                int(activations["total_tokens"]) if int(activations["token_rows"]) else None,
                int(latest["seq"]) if latest is not None else None,
                latest["status"] if latest is not None else None,
            ),
        )
        return 0

    # -- retention ------------------------------------------------------------

    def _prune(self, connection: sqlite3.Connection, cutoff_ms: int) -> int:
        activations = {
            (row["entity_key"], int(row["seq"]))
            for row in connection.execute(
                "SELECT DISTINCT entity_key, seq FROM events WHERE start_ms < ?", (cutoff_ms,)
            )
        }
        activations |= {
            (row["entity_key"], int(row["seq"]))
            for row in connection.execute(
                "SELECT DISTINCT entity_key, seq FROM errors "
                "WHERE event_time_ms < ? AND seq IS NOT NULL",
                (cutoff_ms,),
            )
        }
        traces = {
            row["trace_id"]
            for row in connection.execute(
                "SELECT DISTINCT trace_id FROM events WHERE start_ms < ?", (cutoff_ms,)
            )
        }
        entities = {entity_key for entity_key, _ in activations}
        entities |= {
            row["entity_key"]
            for row in connection.execute(
                "SELECT DISTINCT entity_key FROM errors WHERE event_time_ms < ?", (cutoff_ms,)
            )
        }

        removed = _delete(
            connection,
            "DELETE FROM event_attributes WHERE (trace_id, span_id, event_type) IN "
            "(SELECT trace_id, span_id, event_type FROM events WHERE start_ms < ?)",
            (cutoff_ms,),
        )
        removed += _delete(connection, "DELETE FROM events WHERE start_ms < ?", (cutoff_ms,))
        removed += _delete(connection, "DELETE FROM errors WHERE event_time_ms < ?", (cutoff_ms,))
        removed += _delete(
            connection, "DELETE FROM snapshots WHERE snapshot_at_ms < ?", (cutoff_ms,)
        )
        # The derived rows follow their records: an activation with events left
        # inside the window keeps a rollup recomputed over exactly those, and
        # one with nothing left loses its row entirely.
        return removed + self._recompute(
            connection, activations=activations, traces=traces, entities=entities
        )
