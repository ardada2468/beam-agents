"""The console's SQLite schema and its migration ladder.

Kept separate from ``_store.py`` so the DDL is reviewable as a document rather
than as string literals threaded through connection handling, and so a schema
change is a diff with an obvious shape.

The store is a single WAL-mode file (design D6): it is the only option that
satisfies "one ``docker run`` and it works" without a second container, survives
a restart, and still supports the grouping and time-bucketing the UI needs.

Three shapes of table live here, and the split is the whole point:

**Records** — ``events``, ``event_attributes``, ``errors``, ``snapshots`` — are
what a producer delivered. ``events`` is keyed on
``(trace_id, span_id, event_type)``, the dedup key ``docs/traces.md`` publishes,
and attributes are a side table rather than a JSON blob so a later, richer copy
of an event merges key-by-key (design D5) and so attribute search is an index
lookup instead of a scan.

**Rollups** — ``activations``, ``activation_tools``, ``activation_reasons``,
``traces``, ``spans``, ``entities`` — are *derived*: no producer writes them,
``_store`` recomputes them from the records on every write, and pruning a record
recomputes or removes whatever it fed. ``activations`` carries exactly the
fields of ``_dto.ActivationSummary``, because a list page that has to assemble
its rows from events is a list page that cannot be filtered or paginated.

**Facets** — ``activation_tools`` and ``activation_reasons`` — exist only
because ``ActivationSummary.tools`` and ``.reasons`` are lists and
``ActivationFilter`` filters on a *member* of each. A JSON array column would
make those two filters the only unindexed ones in the query layer.

Every index below sits behind a filter, an ordering, or a join that
``_queries.py`` exposes; none is speculative.

The DDL and the store's queries need SQLite 3.30 or newer — ``WITHOUT ROWID``
(3.8.2), row values in ``IN (SELECT …)`` (3.15), ``UPSERT`` (3.24), and
``COUNT(*) FILTER (WHERE …)`` (3.30). CPython 3.11 itself requires 3.7.15, so
this is a real floor: it is met by every distribution the console's image and
supported platforms ship, and it is stated here rather than discovered at
runtime.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

__all__ = [
    "SCHEMA_VERSION",
    "TABLES",
    "apply_schema",
    "schema_version_of",
]

# Bumped whenever `apply_schema` gains a step. The store records this in
# `PRAGMA user_version`, so an existing file can be brought forward without the
# operator being asked to delete it — a console is opened *because* something
# went wrong, and destroying the history at that moment is the wrong default.
SCHEMA_VERSION = 1

# Every table the schema owns, in dependency order. `_store.counts()` reports
# one row count per entry, so the API's store-status page is a property of this
# tuple rather than of a second list that can fall behind it.
TABLES = (
    "events",
    "event_attributes",
    "errors",
    "snapshots",
    "traces",
    "spans",
    "activations",
    "activation_tools",
    "activation_reasons",
    "entities",
)

# `WITHOUT ROWID` on the record and facet tables: each is keyed by its natural
# identity and read by it, so the extra rowid indirection buys nothing and the
# clustered form makes the dedup probe a single b-tree seek. `errors` and
# `snapshots` stay rowid tables on purpose — they carry a free-form `detail`
# and an opaque state image, and large payloads in a clustered index are what
# SQLite explicitly warns against.
_V1: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS events (
        trace_id       TEXT    NOT NULL,
        span_id        TEXT    NOT NULL,
        event_type     TEXT    NOT NULL,
        parent_span_id TEXT    NOT NULL DEFAULT '',
        entity_key     TEXT    NOT NULL DEFAULT '',
        seq            INTEGER NOT NULL DEFAULT 0,
        step_index     INTEGER NOT NULL DEFAULT 0,
        start_ms       INTEGER NOT NULL DEFAULT 0,
        end_ms         INTEGER NOT NULL DEFAULT 0,
        provenance     TEXT    NOT NULL DEFAULT 'native',
        PRIMARY KEY (trace_id, span_id, event_type)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX IF NOT EXISTS idx_events_activation ON events (entity_key, seq, start_ms)",
    "CREATE INDEX IF NOT EXISTS idx_events_trace ON events (trace_id, span_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_parent ON events (trace_id, parent_span_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, start_ms)",
    "CREATE INDEX IF NOT EXISTS idx_events_time ON events (start_ms)",
    """
    CREATE TABLE IF NOT EXISTS event_attributes (
        trace_id   TEXT NOT NULL,
        span_id    TEXT NOT NULL,
        event_type TEXT NOT NULL,
        key        TEXT NOT NULL,
        value      TEXT NOT NULL,
        PRIMARY KEY (trace_id, span_id, event_type, key)
    ) WITHOUT ROWID
    """,
    # Attribute search is "which record carries this value", so the index leads
    # with `key` (every dimensioned query names one) and carries `value` for
    # the equality probe; the second index answers the key-agnostic search box.
    "CREATE INDEX IF NOT EXISTS idx_attributes_key_value ON event_attributes (key, value)",
    "CREATE INDEX IF NOT EXISTS idx_attributes_value ON event_attributes (value)",
    """
    CREATE TABLE IF NOT EXISTS errors (
        error_id      TEXT    NOT NULL PRIMARY KEY,
        entity_key    TEXT    NOT NULL,
        seq           INTEGER,
        reason        TEXT    NOT NULL,
        detail        TEXT    NOT NULL DEFAULT '',
        event_time_ms INTEGER NOT NULL,
        provenance    TEXT    NOT NULL DEFAULT 'native'
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_errors_reason ON errors (reason, event_time_ms)",
    "CREATE INDEX IF NOT EXISTS idx_errors_activation ON errors (entity_key, seq)",
    "CREATE INDEX IF NOT EXISTS idx_errors_time ON errors (event_time_ms)",
    """
    CREATE TABLE IF NOT EXISTS snapshots (
        entity_key             TEXT    NOT NULL,
        seq                    INTEGER NOT NULL,
        request_id             TEXT    NOT NULL DEFAULT '',
        snapshot_at_ms         INTEGER NOT NULL,
        state_schema_version   INTEGER NOT NULL DEFAULT 0,
        memory_entries         INTEGER NOT NULL DEFAULT 0,
        memory_bytes           INTEGER NOT NULL DEFAULT 0,
        llm_cache_entries      INTEGER NOT NULL DEFAULT 0,
        pending_intent_ids     TEXT    NOT NULL DEFAULT '[]',
        continuation_step_index INTEGER,
        continuation_deadline_ms INTEGER,
        continuation_adapter   TEXT    NOT NULL DEFAULT '',
        raw                    BLOB    NOT NULL DEFAULT x'',
        provenance             TEXT    NOT NULL DEFAULT 'native',
        PRIMARY KEY (entity_key, seq, request_id, snapshot_at_ms)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_snapshots_activation ON snapshots (entity_key, seq)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots (snapshot_at_ms)",
    """
    CREATE TABLE IF NOT EXISTS traces (
        trace_id      TEXT    NOT NULL PRIMARY KEY,
        entity_key    TEXT    NOT NULL,
        seq           INTEGER NOT NULL,
        started_ms    INTEGER NOT NULL,
        last_event_ms INTEGER NOT NULL,
        events        INTEGER NOT NULL DEFAULT 0,
        spans         INTEGER NOT NULL DEFAULT 0
    ) WITHOUT ROWID
    """,
    "CREATE INDEX IF NOT EXISTS idx_traces_activation ON traces (entity_key, seq)",
    "CREATE INDEX IF NOT EXISTS idx_traces_time ON traces (started_ms)",
    """
    CREATE TABLE IF NOT EXISTS spans (
        trace_id       TEXT    NOT NULL,
        span_id        TEXT    NOT NULL,
        parent_span_id TEXT    NOT NULL DEFAULT '',
        entity_key     TEXT    NOT NULL DEFAULT '',
        seq            INTEGER NOT NULL DEFAULT 0,
        role           TEXT    NOT NULL DEFAULT '',
        step_index     INTEGER NOT NULL DEFAULT 0,
        first_ms       INTEGER NOT NULL DEFAULT 0,
        last_ms        INTEGER NOT NULL DEFAULT 0,
        events         INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (trace_id, span_id)
    ) WITHOUT ROWID
    """,
    # The span tree is assembled by walking children of a parent within one
    # trace; that walk is this index.
    "CREATE INDEX IF NOT EXISTS idx_spans_parent ON spans (trace_id, parent_span_id)",
    "CREATE INDEX IF NOT EXISTS idx_spans_activation ON spans (entity_key, seq)",
    """
    CREATE TABLE IF NOT EXISTS activations (
        entity_key          TEXT    NOT NULL,
        seq                 INTEGER NOT NULL,
        trace_id            TEXT    NOT NULL DEFAULT '',
        status              TEXT    NOT NULL,
        kind                TEXT    NOT NULL,
        attempts            INTEGER NOT NULL DEFAULT 1,
        started_ms          INTEGER NOT NULL,
        ended_ms            INTEGER,
        wall_ms             INTEGER,
        model               TEXT,
        llm_calls           INTEGER NOT NULL DEFAULT 0,
        tool_calls          INTEGER NOT NULL DEFAULT 0,
        intents             INTEGER NOT NULL DEFAULT 0,
        errors              INTEGER NOT NULL DEFAULT 0,
        prompt_tokens       INTEGER,
        completion_tokens   INTEGER,
        total_tokens        INTEGER,
        cache_hits          INTEGER NOT NULL DEFAULT 0,
        tools               TEXT    NOT NULL DEFAULT '[]',
        reasons             TEXT    NOT NULL DEFAULT '[]',
        provenance          TEXT    NOT NULL DEFAULT '[]',
        complete_provenance INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (entity_key, seq)
    ) WITHOUT ROWID
    """,
    # The list's total order, and therefore its keyset cursor: newest first,
    # broken by the primary key so the order is total even when two activations
    # share a millisecond.
    "CREATE INDEX IF NOT EXISTS idx_activations_order ON activations (started_ms, entity_key, seq)",
    "CREATE INDEX IF NOT EXISTS idx_activations_entity ON activations (entity_key, started_ms)",
    "CREATE INDEX IF NOT EXISTS idx_activations_status ON activations (status, started_ms)",
    "CREATE INDEX IF NOT EXISTS idx_activations_kind ON activations (kind, started_ms)",
    "CREATE INDEX IF NOT EXISTS idx_activations_model ON activations (model, started_ms)",
    "CREATE INDEX IF NOT EXISTS idx_activations_trace ON activations (trace_id)",
    """
    CREATE TABLE IF NOT EXISTS activation_tools (
        entity_key TEXT    NOT NULL,
        seq        INTEGER NOT NULL,
        tool_name  TEXT    NOT NULL,
        PRIMARY KEY (entity_key, seq, tool_name)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX IF NOT EXISTS idx_activation_tools_name "
    "ON activation_tools (tool_name, entity_key, seq)",
    """
    CREATE TABLE IF NOT EXISTS activation_reasons (
        entity_key TEXT    NOT NULL,
        seq        INTEGER NOT NULL,
        reason     TEXT    NOT NULL,
        PRIMARY KEY (entity_key, seq, reason)
    ) WITHOUT ROWID
    """,
    "CREATE INDEX IF NOT EXISTS idx_activation_reasons_name "
    "ON activation_reasons (reason, entity_key, seq)",
    """
    CREATE TABLE IF NOT EXISTS entities (
        entity_key    TEXT    NOT NULL PRIMARY KEY,
        first_seen_ms INTEGER NOT NULL,
        last_seen_ms  INTEGER NOT NULL,
        activations   INTEGER NOT NULL DEFAULT 0,
        errors        INTEGER NOT NULL DEFAULT 0,
        total_tokens  INTEGER,
        latest_seq    INTEGER,
        latest_status TEXT
    ) WITHOUT ROWID
    """,
    "CREATE INDEX IF NOT EXISTS idx_entities_last_seen ON entities (last_seen_ms, entity_key)",
)

# `_MIGRATIONS[n]` takes a database at `user_version = n` to `n + 1`. Appending
# a step and bumping SCHEMA_VERSION is the whole procedure; nothing is ever
# edited in place, because an edited step would silently not run on a file that
# already recorded the version it belongs to.
_MIGRATIONS: tuple[tuple[str, ...], ...] = (_V1,)


def apply_schema(connection: sqlite3.Connection) -> int:
    """Create or migrate the schema on ``connection``; return the resulting version.

    Idempotent: applying it to an already-current database is a no-op, so it is
    safe to call on every open. Applies exactly the migration steps between the
    file's recorded ``user_version`` and :data:`SCHEMA_VERSION`.

    A file recorded at a *higher* version than this build knows is refused
    rather than read: a console that silently ignores columns it does not
    understand would answer questions wrongly instead of not at all.
    """
    current = schema_version_of(connection)
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"database schema version {current} is newer than this console understands "
            f"({SCHEMA_VERSION}); use a newer beam-agents or a different database path"
        )
    if current == SCHEMA_VERSION:
        return current

    opened_here = not connection.in_transaction
    if opened_here:
        connection.execute("BEGIN IMMEDIATE")
    try:
        for step in _MIGRATIONS[current:SCHEMA_VERSION]:
            for statement in step:
                connection.execute(statement)
        # Not parameterizable — PRAGMA takes no bind parameters — so the value
        # is an int literal from this module, never from a caller.
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION:d}")
    except BaseException:
        if opened_here:
            connection.execute("ROLLBACK")
        raise
    if opened_here:
        connection.execute("COMMIT")
    return SCHEMA_VERSION


def schema_version_of(connection: sqlite3.Connection) -> int:
    """Return the schema version recorded in ``connection``'s ``user_version``."""
    row = connection.execute("PRAGMA user_version").fetchone()
    return int(row[0])
