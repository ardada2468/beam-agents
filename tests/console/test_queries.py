"""The console's read layer, driven against a real SQLite database.

These tests do not go through ``ConsoleStore``: they create the tables directly
and hand ``_queries`` a stand-in exposing the one seam the query layer uses,
``reader()``. That keeps this suite honest about the thing it is actually
testing — the SQL — and lets it run while the store is still being built.

ASSUMED SCHEMA
==============

The DDL in :data:`_DDL` below is what ``_queries.py`` is written against. It is
implied by ``_records.py`` (the row vocabulary), ``_dto.py`` (the fields a
rollup must be able to answer), and design D5/D6 (idempotent upsert on
``(trace_id, span_id, event_type)``, one WAL SQLite file). The coordinator
should reconcile it with the store unit's ``_schema.py``:

- ``events`` — one row per ``EventRow``, primary key ``(trace_id, span_id,
  event_type)``, ``attributes`` as a JSON object of string -> string.
- ``errors`` — one row per ``ErrorRow``. ``seq`` is nullable, because several
  reasons fire from timer callbacks that have no activation.
- ``snapshots`` — one row per ``SnapshotRow``, keyed
  ``(entity_key, seq, snapshot_at_ms)`` so repeated exports of one key do not
  overwrite each other. ``pending_intent_ids`` is a JSON array.
- ``activations`` — the derived rollup, keyed ``(entity_key, seq)``, one column
  per ``ActivationSummary`` field. ``tools``/``reasons``/``provenance`` are JSON
  arrays; ``complete_provenance`` is 0/1.
- ``PRAGMA user_version`` carries the schema version.

Nothing in ``_queries.py`` writes, so column *types* matter here only as far as
SQLite's affinities go; what matters is the names, the JSON encodings, and the
nullability of ``errors.seq`` and every ``*_tokens`` column — the query layer
depends on NULL meaning "not recorded" rather than zero.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from itertools import pairwise
from typing import TYPE_CHECKING, Any, cast

import pytest

from beam_agents.console import _queries
from beam_agents.console._queries import ActivationFilter
from beam_agents.core.dofn import (
    REASON_BATCH_OVERFLOW,
    REASON_BUDGET_EXCEEDED,
    REASON_ERROR,
    REASON_INTENT_DEAD_LETTER,
    REASON_ORPHANED,
    REASON_TIMEOUT,
    REASON_TTL_WIPED_BATCH,
    REASON_TTL_WIPED_SUSPENSION,
)
from beam_agents.hitl import REASON_HITL_TIMEOUT

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from pathlib import Path

    from beam_agents.console._store import ConsoleStore

_DDL = """
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS events (
    trace_id       TEXT    NOT NULL,
    span_id        TEXT    NOT NULL,
    event_type     TEXT    NOT NULL,
    parent_span_id TEXT    NOT NULL DEFAULT '',
    entity_key     TEXT    NOT NULL,
    seq            INTEGER NOT NULL,
    step_index     INTEGER NOT NULL DEFAULT 0,
    start_ms       INTEGER NOT NULL,
    end_ms         INTEGER NOT NULL,
    attributes     TEXT    NOT NULL DEFAULT '{}',
    provenance     TEXT    NOT NULL DEFAULT 'native',
    PRIMARY KEY (trace_id, span_id, event_type)
);
CREATE INDEX IF NOT EXISTS events_activation ON events (entity_key, seq, start_ms);
CREATE INDEX IF NOT EXISTS events_type_time  ON events (event_type, start_ms);
CREATE INDEX IF NOT EXISTS events_trace      ON events (trace_id);

CREATE TABLE IF NOT EXISTS errors (
    id            INTEGER PRIMARY KEY,
    entity_key    TEXT    NOT NULL,
    seq           INTEGER,
    reason        TEXT    NOT NULL,
    detail        TEXT    NOT NULL DEFAULT '',
    event_time_ms INTEGER NOT NULL,
    provenance    TEXT    NOT NULL DEFAULT 'native'
);
CREATE UNIQUE INDEX IF NOT EXISTS errors_identity
    ON errors (entity_key, IFNULL(seq, -1), reason, detail, event_time_ms);
CREATE INDEX IF NOT EXISTS errors_time ON errors (event_time_ms);

CREATE TABLE IF NOT EXISTS snapshots (
    entity_key              TEXT    NOT NULL,
    seq                     INTEGER NOT NULL,
    snapshot_at_ms          INTEGER NOT NULL,
    state_schema_version    INTEGER NOT NULL DEFAULT 0,
    request_id              TEXT    NOT NULL DEFAULT '',
    memory_entries          INTEGER NOT NULL DEFAULT 0,
    memory_bytes            INTEGER NOT NULL DEFAULT 0,
    llm_cache_entries       INTEGER NOT NULL DEFAULT 0,
    pending_intent_ids      TEXT    NOT NULL DEFAULT '[]',
    continuation_step_index INTEGER,
    continuation_deadline_ms INTEGER,
    continuation_adapter    TEXT    NOT NULL DEFAULT '',
    raw                     BLOB    NOT NULL DEFAULT x'',
    provenance              TEXT    NOT NULL DEFAULT 'native',
    PRIMARY KEY (entity_key, seq, snapshot_at_ms)
);

CREATE TABLE IF NOT EXISTS activations (
    entity_key          TEXT    NOT NULL,
    seq                 INTEGER NOT NULL,
    trace_id            TEXT    NOT NULL DEFAULT '',
    status              TEXT    NOT NULL DEFAULT 'in_flight',
    kind                TEXT    NOT NULL DEFAULT 'unknown',
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
    provenance          TEXT    NOT NULL DEFAULT '["native"]',
    complete_provenance INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (entity_key, seq)
);
CREATE INDEX IF NOT EXISTS activations_started ON activations (started_ms, entity_key, seq);
"""


class _Store:
    """A stand-in exposing the only seam `_queries` uses: `reader()`.

    Deliberately not `ConsoleStore`: the query layer is being built against the
    schema, not against the store's write path, and a real store would couple
    this suite to a unit landing in parallel.
    """

    def __init__(self, path: Path, *, retention_hours: float | None = None) -> None:
        self._path = path
        self._retention_hours = retention_hours
        self._connection = sqlite3.connect(path)
        self._connection.executescript(_DDL)
        self._connection.commit()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def retention_hours(self) -> float | None:
        return self._retention_hours

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._path)
        try:
            yield connection
        finally:
            connection.close()

    def write(self, sql: str, params: Sequence[object] = ()) -> None:
        self._connection.execute(sql, tuple(params))
        self._connection.commit()

    def as_store(self) -> ConsoleStore:
        return cast("ConsoleStore", self)


@pytest.fixture
def store(tmp_path: Path) -> _Store:
    return _Store(tmp_path / "console.db")


# --- fixture writers ---------------------------------------------------------


def _event(
    store: _Store,
    *,
    entity_key: str,
    seq: int,
    event_type: str,
    start_ms: int,
    span_id: str | None = None,
    parent_span_id: str = "",
    step_index: int = 0,
    attributes: Mapping[str, str] | None = None,
    provenance: str = "native",
) -> None:
    store.write(
        "INSERT OR REPLACE INTO events (trace_id, span_id, event_type, parent_span_id, "
        "entity_key, seq, step_index, start_ms, end_ms, attributes, provenance) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            f"trace-{entity_key}-{seq}",
            span_id if span_id is not None else f"span-{event_type}-{step_index}",
            event_type,
            parent_span_id,
            entity_key,
            seq,
            step_index,
            start_ms,
            start_ms,
            json.dumps(dict(attributes or {})),
            provenance,
        ),
    )


def _error(
    store: _Store,
    *,
    entity_key: str,
    reason: str,
    event_time_ms: int,
    seq: int | None = None,
    detail: str = "",
) -> None:
    store.write(
        "INSERT OR REPLACE INTO errors (entity_key, seq, reason, detail, event_time_ms) "
        "VALUES (?, ?, ?, ?, ?)",
        (entity_key, seq, reason, detail, event_time_ms),
    )


def _activation(
    store: _Store, *, entity_key: str, seq: int, started_ms: int, **columns: Any
) -> None:
    row: dict[str, Any] = {
        "entity_key": entity_key,
        "seq": seq,
        "trace_id": f"trace-{entity_key}-{seq}",
        "started_ms": started_ms,
        **columns,
    }
    for name in ("tools", "reasons", "provenance"):
        if isinstance(row.get(name), list):
            row[name] = json.dumps(row[name])
    placeholders = ", ".join("?" for _ in row)
    store.write(
        f"INSERT OR REPLACE INTO activations ({', '.join(row)}) VALUES ({placeholders})",
        tuple(row.values()),
    )


def _snapshot(
    store: _Store, *, entity_key: str, seq: int, snapshot_at_ms: int, **columns: Any
) -> None:
    row: dict[str, Any] = {
        "entity_key": entity_key,
        "seq": seq,
        "snapshot_at_ms": snapshot_at_ms,
        **columns,
    }
    if isinstance(row.get("pending_intent_ids"), list):
        row["pending_intent_ids"] = json.dumps(row["pending_intent_ids"])
    placeholders = ", ".join("?" for _ in row)
    store.write(
        f"INSERT OR REPLACE INTO snapshots ({', '.join(row)}) VALUES ({placeholders})",
        tuple(row.values()),
    )


_T0 = 1_700_000_000_000


# --- Requirement: The console serves a read-only HTTP API over the store -----


def test_an_empty_store_answers_every_query(store: _Store) -> None:
    # Scenario: An empty store answers every endpoint. Every query must return a
    # well-formed empty result, not raise and not invent a zero where nothing
    # was measured.
    handle = store.as_store()

    page = _queries.activations(handle)
    assert page.items == []
    assert page.next_cursor is None
    assert page.total == 0

    assert _queries.activation_detail(handle, entity_key="missing", seq=0) is None
    assert _queries.traces(handle).items == []
    assert _queries.trace_detail(handle, trace_id="missing") is None
    assert _queries.errors(handle).items == []
    assert _queries.error_groups(handle) == []
    assert _queries.models(handle) == []
    assert _queries.tools(handle) == []
    assert _queries.approvals(handle) == []
    assert _queries.entities(handle).items == []
    assert _queries.search(handle, query="anything") == []

    view = _queries.overview(handle, window_ms=3_600_000)
    assert view.activations == 0
    assert view.error_ratio is None
    assert view.total_tokens is None
    assert view.cache_hit_ratio is None
    assert view.p50_wall_ms is None
    assert view.activation_series == []

    status = _queries.store_status(handle)
    assert status.row_counts == {"activations": 0, "errors": 0, "events": 0, "snapshots": 0}
    assert status.oldest_record_ms is None


def _seed_list(store: _Store) -> None:
    """Eight activations spanning statuses, models, tools, and reasons."""
    for index in range(8):
        _activation(
            store,
            entity_key=f"key-{index}",
            seq=1,
            started_ms=_T0 + index * 1_000,
            status="completed" if index % 2 == 0 else "error",
            kind="resume" if index == 3 else "start",
            model="m-a" if index < 4 else "m-b",
            tools=["search"] if index % 2 == 0 else ["write"],
            reasons=[] if index % 2 == 0 else ["activation_error"],
            errors=0 if index % 2 == 0 else 1,
            wall_ms=10 * (index + 1),
        )


def test_an_empty_filter_is_the_whole_list(store: _Store) -> None:
    # Scenario: Filters narrow the activation list — the degenerate case. An
    # unsupplied filter must not constrain, or the default view is empty.
    _seed_list(store)

    assert len(_queries.activations(store.as_store()).items) == 8
    assert len(_queries.activations(store.as_store(), filters=ActivationFilter()).items) == 8


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"entity_key": "key-3"}, {"key-3"}),
        ({"status": "error"}, {"key-1", "key-3", "key-5", "key-7"}),
        ({"kind": "resume"}, {"key-3"}),
        ({"model": "m-b"}, {"key-4", "key-5", "key-6", "key-7"}),
        ({"tool": "write"}, {"key-1", "key-3", "key-5", "key-7"}),
        ({"reason": "activation_error"}, {"key-1", "key-3", "key-5", "key-7"}),
        ({"since_ms": _T0 + 6_000}, {"key-6", "key-7"}),
        ({"until_ms": _T0 + 2_000}, {"key-0", "key-1"}),
    ],
)
def test_each_filter_narrows_the_activation_list(
    store: _Store, kwargs: dict[str, object], expected: set[str]
) -> None:
    # Scenario: Filters narrow the activation list. One case per filter axis the
    # spec names: entity key, status, kind, model, tool, error reason, time range.
    _seed_list(store)

    page = _queries.activations(store.as_store(), filters=ActivationFilter(**kwargs))  # type: ignore[arg-type]

    assert {item.entity_key for item in page.items} == expected
    assert page.total == len(expected)


def test_filters_conjoin_to_their_intersection(store: _Store) -> None:
    # Scenario: Filters narrow the activation list — "matching every supplied
    # filter". Two filters intersect; they do not union.
    _seed_list(store)

    page = _queries.activations(
        store.as_store(), filters=ActivationFilter(status="error", model="m-b")
    )

    assert {item.entity_key for item in page.items} == {"key-5", "key-7"}


def test_the_activation_list_is_newest_first_in_a_stable_order(store: _Store) -> None:
    # Scenario: Filters narrow the activation list — "in a stable order".
    _seed_list(store)

    page = _queries.activations(store.as_store())

    assert [item.entity_key for item in page.items] == [f"key-{i}" for i in reversed(range(8))]


def test_the_cursor_resumes_the_same_ordering_across_a_page_boundary(store: _Store) -> None:
    # Scenario: Filters narrow the activation list — "with a cursor that resumes
    # the same ordering". Paging must visit every row exactly once.
    _seed_list(store)

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = _queries.activations(store.as_store(), cursor=cursor, limit=3)
        seen.extend(item.entity_key for item in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert seen == [f"key-{i}" for i in reversed(range(8))]
    assert len(seen) == len(set(seen))


def test_the_cursor_is_none_exactly_when_the_list_is_exhausted(store: _Store) -> None:
    # `next_cursor` is None when the list is exhausted, not when a page comes
    # back short — a page that exactly consumes the remainder still ends.
    _seed_list(store)

    first = _queries.activations(store.as_store(), limit=4)
    assert first.next_cursor is not None

    second = _queries.activations(store.as_store(), cursor=first.next_cursor, limit=4)
    assert len(second.items) == 4
    assert second.next_cursor is None


def test_a_concurrent_insert_does_not_make_paging_skip_or_repeat(store: _Store) -> None:
    # The reason pagination is keyset rather than offset: a console is read while
    # a pipeline writes. An insert landing inside an already-scanned range would
    # shift every offset page by one.
    _seed_list(store)

    first = _queries.activations(store.as_store(), limit=3)
    assert first.next_cursor is not None
    # Lands *older* than the first page, inside the range the next page scans.
    _activation(store, entity_key="key-new", seq=1, started_ms=_T0 + 4_500, status="completed")

    second = _queries.activations(store.as_store(), cursor=first.next_cursor, limit=3)

    assert [item.entity_key for item in first.items] == ["key-7", "key-6", "key-5"]
    # The new row takes its place by *position in the ordering*, not by an
    # offset: nothing from the first page repeats and nothing between the pages
    # is skipped.
    assert [item.entity_key for item in second.items] == ["key-new", "key-4", "key-3"]


def test_a_malformed_cursor_is_rejected(store: _Store) -> None:
    # A cursor is opaque, so a client cannot be trusted to have produced one.
    with pytest.raises(ValueError, match="cursor"):
        _queries.activations(store.as_store(), cursor="not-a-cursor")


# --- Requirement: Activation rollups are derived, never written --------------


def _seed_suspend_resume(store: _Store) -> None:
    """One activation that suspended and resumed: two attempts, one trace."""
    key, seq = "key-a", 4
    _activation(
        store,
        entity_key=key,
        seq=seq,
        started_ms=_T0,
        ended_ms=_T0 + 900,
        wall_ms=900,
        status="completed",
        kind="resume",
        attempts=2,
        model="m-a",
        llm_calls=2,
        tool_calls=1,
        intents=1,
        prompt_tokens=30,
        completion_tokens=12,
        total_tokens=42,
        tools=["search"],
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="ACTIVATION_START",
        span_id="span-att-0",
        start_ms=_T0,
        attributes={"beam_agents.activation.kind": "start"},
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="LLM_CALL",
        span_id="span-llm-0",
        parent_span_id="span-att-0",
        step_index=0,
        start_ms=_T0 + 10,
        attributes={
            "gen_ai.request.model": "m-a",
            "gen_ai.usage.input_tokens": "10",
            "gen_ai.usage.output_tokens": "4",
            "beam_agents.cache_hit": "false",
            "beam_agents.attempts": "1",
            "beam_agents.circuit_state": "closed",
        },
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="INTENT_EMITTED",
        span_id="span-intent-1",
        parent_span_id="span-att-0",
        step_index=1,
        start_ms=_T0 + 20,
        attributes={
            "beam_agents.intent_id": "intent-1",
            "beam_agents.tool_name": "approval",
            "beam_agents.intent_kind": "APPROVAL",
            "beam_agents.expires_at_ms": str(_T0 + 3_600_000),
        },
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="SUSPENDED",
        span_id="span-susp-1",
        parent_span_id="span-att-0",
        step_index=1,
        start_ms=_T0 + 25,
        attributes={"beam_agents.deadline_ms": str(_T0 + 86_400_000)},
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="ACTIVATION_END",
        span_id="span-att-0",
        start_ms=_T0 + 30,
        step_index=1,
        attributes={
            "beam_agents.activation.status": "suspended",
            "beam_agents.activation.kind": "start",
        },
    )
    # The resume: a second attempt under the same (entity_key, seq).
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="ACTIVATION_START",
        span_id="span-att-1",
        parent_span_id="span-att-0",
        start_ms=_T0 + 800,
        attributes={"beam_agents.activation.kind": "resume"},
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="TOOL_CALL",
        span_id="span-tool-0",
        parent_span_id="span-att-1",
        step_index=2,
        start_ms=_T0 + 810,
        attributes={"beam_agents.tool_name": "search"},
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="LLM_CALL",
        span_id="span-llm-2",
        parent_span_id="span-att-1",
        step_index=2,
        start_ms=_T0 + 850,
        attributes={
            "gen_ai.request.model": "m-a",
            "gen_ai.usage.input_tokens": "20",
            "gen_ai.usage.output_tokens": "8",
            "beam_agents.cache_hit": "true",
            "beam_agents.attempts": "2",
            "beam_agents.circuit_state": "closed",
        },
    )
    _event(
        store,
        entity_key=key,
        seq=seq,
        event_type="ACTIVATION_END",
        span_id="span-att-1",
        start_ms=_T0 + 900,
        step_index=3,
        attributes={
            "beam_agents.activation.status": "completed",
            "beam_agents.activation.kind": "resume",
        },
    )


def test_a_suspend_and_resume_is_one_activation_with_two_attempts(store: _Store) -> None:
    # Scenario: A suspend and resume are one activation. Trace identity is
    # scoped to (entity_key, seq), so the resume recomputes the same trace ID.
    _seed_suspend_resume(store)

    detail = _queries.activation_detail(store.as_store(), entity_key="key-a", seq=4)

    assert detail is not None
    assert [attempt.kind for attempt in detail.attempts] == ["start", "resume"]
    assert [attempt.status for attempt in detail.attempts] == ["suspended", "completed"]
    assert detail.attempts[1].entry_step_index == 2
    assert len(_queries.activations(store.as_store()).items) == 1


def test_an_activation_detail_lists_every_recorded_event(store: _Store) -> None:
    # Scenario: An activation's detail shows every recorded event, with its type,
    # step index, timestamps, and complete attribute map.
    _seed_suspend_resume(store)

    detail = _queries.activation_detail(store.as_store(), entity_key="key-a", seq=4)

    assert detail is not None
    assert len(detail.events) == 9
    assert [event.start_ms for event in detail.events] == sorted(
        event.start_ms for event in detail.events
    )
    llm = next(
        event for event in detail.events if event.attributes.get("beam_agents.attempts") == "2"
    )
    assert llm.attributes["gen_ai.request.model"] == "m-a"
    assert llm.start_ms == llm.end_ms
    assert [intent.intent_id for intent in detail.intents] == ["intent-1"]


def test_the_span_tree_carries_depth_and_order_but_no_duration(store: _Store) -> None:
    # Scenario: Zero-width spans are not drawn as durations. The API encodes
    # nesting and sequence; there is no width field to fabricate one from.
    _seed_suspend_resume(store)

    detail = _queries.activation_detail(store.as_store(), entity_key="key-a", seq=4)

    assert detail is not None
    depths = {node.span_id: node.depth for node in detail.spans}
    assert depths["span-att-0"] == 0
    assert depths["span-llm-0"] == 1
    assert depths["span-att-1"] == 1
    assert depths["span-tool-0"] == 2
    assert [node.order for node in detail.spans] == list(range(len(detail.spans)))
    assert not any(hasattr(node, "duration_ms") for node in detail.spans)


def test_a_trace_detail_assembles_the_same_scope(store: _Store) -> None:
    # A trace is exactly one activation scope, so its span tree and attempts are
    # the activation's.
    _seed_suspend_resume(store)

    page = _queries.traces(store.as_store())
    assert [summary.trace_id for summary in page.items] == ["trace-key-a-4"]
    assert page.items[0].events == 9
    assert page.items[0].spans == 7

    detail = _queries.trace_detail(store.as_store(), trace_id="trace-key-a-4")
    assert detail is not None
    assert len(detail.attempts) == 2
    assert detail.roots[0].span_id == "span-att-0"


def test_traces_are_searchable_by_entity_key_and_attribute(store: _Store) -> None:
    _seed_suspend_resume(store)

    assert _queries.traces(store.as_store(), query="key-a").items != []
    assert _queries.traces(store.as_store(), query="m-a").items != []
    assert _queries.traces(store.as_store(), query="nothing-here").items == []


# --- Requirement: Errors are grouped by the runtime's own vocabulary ---------

# Imported rather than restated: the vocabulary is closed, and a reason added or
# renamed in the runtime should show up here as a failing test, not as a stale
# string literal that still passes.
_CLOSED_VOCABULARY = (
    REASON_TIMEOUT,
    REASON_ERROR,
    REASON_ORPHANED,
    REASON_TTL_WIPED_SUSPENSION,
    REASON_TTL_WIPED_BATCH,
    REASON_BATCH_OVERFLOW,
    REASON_BUDGET_EXCEEDED,
    REASON_INTENT_DEAD_LETTER,
    REASON_HITL_TIMEOUT,
)


def test_grouping_by_reason_covers_the_closed_vocabulary(store: _Store) -> None:
    # Scenario: Errors are grouped by reason. The vocabulary is closed and small
    # (core/dofn.py, hitl.py), which is what makes `reason` a navigation axis.
    for index, reason in enumerate(_CLOSED_VOCABULARY):
        _error(
            store,
            entity_key=f"key-{index % 3}",
            seq=index,
            reason=reason,
            detail=f"detail-{index}",
            event_time_ms=_T0 + index * 1_000,
        )

    groups = _queries.error_groups(store.as_store())

    assert {group.reason for group in groups} == set(_CLOSED_VOCABULARY)
    assert all(group.count == 1 for group in groups)
    assert all(group.entities == 1 for group in groups)
    assert all(group.sample_detail.startswith("detail-") for group in groups)


def test_an_error_group_splits_by_error_type(store: _Store) -> None:
    # Grouped by reason *and* error.type: two failures with one reason and
    # different exception types are two different triage problems.
    for index, error_type in enumerate(("TimeoutError", "ValueError")):
        _error(store, entity_key="key-0", seq=index, reason="activation_error", event_time_ms=_T0)
        _event(
            store,
            entity_key="key-0",
            seq=index,
            event_type="ERROR",
            span_id=f"span-err-{index}",
            start_ms=_T0,
            attributes={"beam_agents.reason": "activation_error", "error.type": error_type},
        )

    groups = _queries.error_groups(store.as_store())

    assert sorted(group.error_type or "" for group in groups) == ["TimeoutError", "ValueError"]
    assert sum(group.count for group in groups) == 2


def test_an_error_record_and_its_trace_event_are_one_error(store: _Store) -> None:
    # The same failure reaches the store twice — once as an ActivationErrorRecord
    # on the errors sink, once as an ERROR trace event. Counting both would
    # double every error in the console.
    _error(
        store,
        entity_key="key-0",
        seq=1,
        reason="activation_error",
        detail="boom",
        event_time_ms=_T0,
    )
    _event(
        store,
        entity_key="key-0",
        seq=1,
        event_type="ERROR",
        span_id="span-err",
        start_ms=_T0,
        attributes={
            "beam_agents.reason": "activation_error",
            "error.type": "ValueError",
            "beam_agents.failure.step": "3",
            "beam_agents.failure.last_event": "LLM_CALL",
            "beam_agents.failure.staged_intents": "2",
            "beam_agents.failure.llm_calls": "5",
        },
    )

    page = _queries.errors(store.as_store())

    assert len(page.items) == 1
    record = page.items[0]
    assert record.detail == "boom"
    assert record.error_type == "ValueError"
    assert record.failure_step == 3
    assert record.failure_last_event == "LLM_CALL"
    assert record.failure_staged_intents == 2
    assert record.failure_llm_calls == 5


def test_failure_position_is_none_where_the_route_could_not_reach_a_context(store: _Store) -> None:
    # Scenario: Failure position is surfaced when recorded — and only then. The
    # timeout route has no reachable context, so the scalars are absent, not zero.
    _error(store, entity_key="key-0", reason="activation_timeout", event_time_ms=_T0)

    record = _queries.errors(store.as_store()).items[0]

    assert record.seq is None
    assert record.error_type is None
    assert record.failure_step is None
    assert record.failure_llm_calls is None


def test_errors_filter_by_reason_entity_and_time(store: _Store) -> None:
    for index, reason in enumerate(("activation_error", "budget_exceeded")):
        _error(store, entity_key=f"key-{index}", seq=1, reason=reason, event_time_ms=_T0 + index)

    handle = store.as_store()
    assert len(_queries.errors(handle, reason="budget_exceeded").items) == 1
    assert len(_queries.errors(handle, entity_key="key-0").items) == 1
    assert len(_queries.errors(handle, since_ms=_T0 + 1).items) == 1


def test_paging_errors_visits_every_record_exactly_once(store: _Store) -> None:
    for index in range(7):
        _error(
            store,
            entity_key=f"key-{index}",
            seq=index,
            reason="activation_error",
            event_time_ms=_T0 + index,
        )

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = _queries.errors(store.as_store(), cursor=cursor, limit=2)
        seen.extend(f"{item.entity_key}#{item.seq}" for item in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert len(seen) == 7
    assert len(set(seen)) == 7


def test_an_error_series_is_contiguous_with_explicit_zeros(store: _Store) -> None:
    # A quiet period must read as a gap, not as a line interpolated across
    # missing time — so every bucket in range is present, including empty ones.
    _error(store, entity_key="key-0", seq=1, reason="activation_error", event_time_ms=_T0)
    _error(store, entity_key="key-0", seq=2, reason="activation_error", event_time_ms=_T0 + 40_000)

    group = _queries.error_groups(store.as_store(), bucket_ms=10_000)[0]

    assert [point.value for point in group.series] == [1.0, 0.0, 0.0, 0.0, 1.0]
    starts = [point.bucket_ms for point in group.series]
    assert starts == [starts[0] + 10_000 * i for i in range(5)]


def test_time_buckets_are_stable_at_a_bucket_edge(store: _Store) -> None:
    # Bucket starts are floor(t / bucket) * bucket, so an event exactly on an
    # edge belongs to the bucket it opens and the series does not shift with the
    # first observation.
    edge = (_T0 // 10_000) * 10_000
    _error(store, entity_key="key-0", seq=1, reason="activation_error", event_time_ms=edge)
    _error(store, entity_key="key-0", seq=2, reason="activation_error", event_time_ms=edge + 9_999)
    _error(store, entity_key="key-0", seq=3, reason="activation_error", event_time_ms=edge + 10_000)

    group = _queries.error_groups(store.as_store(), bucket_ms=10_000)[0]

    assert [(point.bucket_ms, point.value) for point in group.series] == [
        (edge, 2.0),
        (edge + 10_000, 1.0),
    ]


# --- Requirement: The UI reports model, tool, and approval activity ----------


def test_model_usage_is_broken_down_per_model(store: _Store) -> None:
    # Scenario: Model usage is broken down per model. Every figure comes from
    # TraceEvent attributes; Beam's metrics carry no model label at all.
    for index, model in enumerate(("m-a", "m-a", "m-b")):
        _event(
            store,
            entity_key="key-0",
            seq=index,
            event_type="LLM_CALL",
            span_id=f"span-llm-{index}",
            start_ms=_T0 + index,
            attributes={
                "gen_ai.request.model": model,
                "gen_ai.usage.input_tokens": "10",
                "gen_ai.usage.output_tokens": "5",
                "beam_agents.cache_hit": "true" if index == 0 else "false",
                "beam_agents.attempts": str(index + 1),
                "beam_agents.circuit_state": "closed",
            },
        )

    summaries = {summary.model: summary for summary in _queries.models(store.as_store())}

    assert summaries["m-a"].calls == 2
    assert summaries["m-a"].prompt_tokens == 20
    assert summaries["m-a"].completion_tokens == 10
    assert summaries["m-a"].total_tokens == 30
    assert summaries["m-a"].cache_hits == 1
    assert summaries["m-a"].cache_hit_ratio == 0.5
    assert summaries["m-a"].max_attempts == 2
    assert summaries["m-a"].circuit_states == {"closed": 2}
    assert summaries["m-b"].calls == 1


def test_cache_hit_ratio_is_none_when_nothing_recorded_a_cache_attribute(store: _Store) -> None:
    # "Never cached" and "not measured" are different answers, and only one of
    # them is a number.
    _event(
        store,
        entity_key="key-0",
        seq=0,
        event_type="LLM_CALL",
        span_id="span-llm",
        start_ms=_T0,
        attributes={"gen_ai.request.model": "m-a"},
    )

    summary = _queries.models(store.as_store())[0]

    assert summary.cache_hit_ratio is None
    assert summary.cache_hits == 0
    assert summary.prompt_tokens is None
    assert summary.total_tokens is None
    assert summary.max_attempts is None
    assert _queries.overview(store.as_store(), window_ms=3_600_000).cache_hit_ratio is None


def test_a_measured_zero_cache_hit_ratio_is_zero_not_none(store: _Store) -> None:
    _event(
        store,
        entity_key="key-0",
        seq=0,
        event_type="LLM_CALL",
        span_id="span-llm",
        start_ms=_T0,
        attributes={"gen_ai.request.model": "m-a", "beam_agents.cache_hit": "false"},
    )

    assert _queries.models(store.as_store())[0].cache_hit_ratio == 0.0
    assert _queries.overview(store.as_store(), window_ms=3_600_000).cache_hit_ratio == 0.0


def test_tool_volume_and_failure_ratio_come_from_attributes(store: _Store) -> None:
    # Scenario: per-tool views of call volume and failure rate.
    for index in range(3):
        _event(
            store,
            entity_key="key-0",
            seq=index,
            event_type="TOOL_CALL",
            span_id=f"span-tool-{index}",
            start_ms=_T0 + index,
            attributes=(
                {"beam_agents.tool_name": "search", "error.type": "TimeoutError"}
                if index == 2
                else {"beam_agents.tool_name": "search"}
            ),
        )
    _event(
        store,
        entity_key="key-0",
        seq=9,
        event_type="INTENT_EMITTED",
        span_id="span-intent",
        start_ms=_T0 + 9,
        attributes={"beam_agents.tool_name": "publish", "beam_agents.intent_kind": "TOOL"},
    )

    summaries = {summary.tool_name: summary for summary in _queries.tools(store.as_store())}

    assert summaries["search"].calls == 3
    assert summaries["search"].errors == 1
    assert summaries["search"].failure_ratio == pytest.approx(1 / 3)
    assert summaries["search"].last_seen_ms == _T0 + 2
    assert summaries["publish"].intents == 1
    assert summaries["publish"].calls == 0
    # No call was made, so there is no ratio to report — not a zero.
    assert summaries["publish"].failure_ratio is None


def test_pending_approvals_are_listed_with_their_deadlines(store: _Store) -> None:
    # Scenario: Pending approvals are listed with their deadlines.
    _seed_suspend_resume(store)

    queue = _queries.approvals(store.as_store())

    assert len(queue) == 1
    approval = queue[0]
    assert approval.intent_id == "intent-1"
    assert approval.tool_name == "approval"
    assert approval.entity_key == "key-a"
    assert approval.expires_at_ms == _T0 + 3_600_000
    assert approval.deadline_ms == _T0 + 86_400_000
    # The runtime records no approved/denied signal, so claiming one would be a
    # fabrication: only an expiry is recorded, and none has happened here.
    assert approval.decision == "pending"
    assert approval.decided_ms is None


def test_an_expired_approval_reports_its_recorded_decision(store: _Store) -> None:
    _seed_suspend_resume(store)
    _error(store, entity_key="key-a", seq=4, reason=REASON_HITL_TIMEOUT, event_time_ms=_T0 + 5_000)

    approval = _queries.approvals(store.as_store())[0]

    assert approval.decision == "expired"
    assert approval.decided_ms == _T0 + 5_000
    assert _queries.approvals(store.as_store(), pending_only=True) == []


# --- Requirement: The console serves per-entity timelines and search ---------


def test_entities_summarize_activity_across_every_sequence(store: _Store) -> None:
    for seq in range(3):
        _activation(
            store,
            entity_key="key-0",
            seq=seq,
            started_ms=_T0 + seq,
            status="error" if seq == 2 else "completed",
            errors=1 if seq == 2 else 0,
            total_tokens=10,
        )
    _activation(store, entity_key="key-1", seq=0, started_ms=_T0 - 1_000, status="completed")

    page = _queries.entities(store.as_store())

    summaries = {summary.entity_key: summary for summary in page.items}
    assert [summary.entity_key for summary in page.items] == ["key-0", "key-1"]
    assert summaries["key-0"].activations == 3
    assert summaries["key-0"].first_seen_ms == _T0
    assert summaries["key-0"].last_seen_ms == _T0 + 2
    assert summaries["key-0"].errors == 1
    assert summaries["key-0"].total_tokens == 30
    assert summaries["key-0"].latest_seq == 2
    assert summaries["key-0"].latest_status == "error"
    # Nothing recorded a token count for this key, so there is no total.
    assert summaries["key-1"].total_tokens is None


def test_paging_entities_visits_every_key_exactly_once(store: _Store) -> None:
    for index in range(5):
        _activation(store, entity_key=f"key-{index}", seq=0, started_ms=_T0 + index)

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = _queries.entities(store.as_store(), cursor=cursor, limit=2)
        seen.extend(summary.entity_key for summary in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert sorted(seen) == [f"key-{i}" for i in range(5)]


def test_search_locates_identifiers_and_attribute_values(store: _Store) -> None:
    _seed_suspend_resume(store)
    _error(
        store, entity_key="key-a", seq=4, reason="budget_exceeded", detail="over", event_time_ms=_T0
    )

    by_attribute = _queries.search(store.as_store(), query="m-a")
    assert any(hit.kind == "event" for hit in by_attribute)
    assert all(hit.matched_value == "m-a" for hit in by_attribute if hit.kind == "event")
    assert {hit.matched_field for hit in by_attribute if hit.kind == "event"} == {
        "gen_ai.request.model"
    }

    by_key = _queries.search(store.as_store(), query="key-a")
    assert {hit.kind for hit in by_key} >= {"entity", "activation"}

    by_reason = _queries.search(store.as_store(), query="budget_exceeded")
    assert [hit.kind for hit in by_reason] == ["error"]


# --- Requirement: The console's overview is derived, bucketed, and honest ----


def test_the_overview_headline_figures_come_from_stored_records(store: _Store) -> None:
    _seed_suspend_resume(store)
    _activation(store, entity_key="key-b", seq=1, started_ms=_T0 + 100, status="in_flight")
    _activation(
        store, entity_key="key-c", seq=1, started_ms=_T0 + 200, status="suspended", wall_ms=50
    )
    _error(store, entity_key="key-c", seq=1, reason="budget_exceeded", event_time_ms=_T0 + 250)

    view = _queries.overview(store.as_store(), window_ms=3_600_000)

    assert view.window_ms == 3_600_000
    assert view.activations == 3
    assert view.completed == 1
    assert view.suspended == 1
    assert view.in_flight == 1
    assert view.errors == 1
    assert view.error_ratio == pytest.approx(1 / 3)
    assert view.total_tokens == 42
    assert view.llm_calls == 2
    assert view.tool_calls == 1
    assert [model.model for model in view.top_models] == ["m-a"]
    # The inline call outranks the staged approval intent: `top_tools` is
    # ordered by call volume first.
    assert view.top_tools[0].tool_name == "search"
    assert [error.reason for error in view.recent_errors] == ["budget_exceeded"]
    assert view.store is not None


def test_the_overview_percentiles_come_from_wall_ms(store: _Store) -> None:
    # p50/p95 are the ACTIVATION_START -> ACTIVATION_END clock delta, never a
    # span width: spans are zero-width by design, so a span-derived percentile
    # would be identically zero.
    for index in range(20):
        _activation(
            store,
            entity_key=f"key-{index:02d}",
            seq=1,
            started_ms=_T0 + index,
            status="completed",
            wall_ms=(index + 1) * 10,
        )

    view = _queries.overview(store.as_store(), window_ms=3_600_000)

    assert view.p50_wall_ms == 100
    assert view.p95_wall_ms == 190


def test_the_overview_percentiles_are_none_when_nothing_completed(store: _Store) -> None:
    _activation(store, entity_key="key-0", seq=1, started_ms=_T0, status="in_flight")

    view = _queries.overview(store.as_store(), window_ms=3_600_000)

    assert view.p50_wall_ms is None
    assert view.p95_wall_ms is None
    assert view.total_tokens is None


def test_the_overview_series_are_contiguous_with_explicit_zeros(store: _Store) -> None:
    _activation(
        store, entity_key="key-0", seq=1, started_ms=_T0, status="completed", total_tokens=7
    )
    _activation(store, entity_key="key-1", seq=1, started_ms=_T0 + 3_000, status="completed")

    view = _queries.overview(store.as_store(), window_ms=4_000, buckets=4)

    assert len(view.activation_series) == len(view.error_series) == len(view.token_series)
    assert sum(point.value for point in view.activation_series) == 2.0
    assert sum(point.value for point in view.token_series) == 7.0
    starts = [point.bucket_ms for point in view.activation_series]
    assert starts == sorted(starts)
    assert all(later - earlier == 1_000 for earlier, later in pairwise(starts))


def test_the_overview_window_excludes_older_activations(store: _Store) -> None:
    # The window is anchored on the newest record rather than a wall clock, so an
    # imported bundle from last week reads as data instead of as an empty page.
    _activation(store, entity_key="old", seq=1, started_ms=_T0 - 10_000, status="completed")
    _activation(store, entity_key="new", seq=1, started_ms=_T0, status="completed")

    view = _queries.overview(store.as_store(), window_ms=5_000)

    assert view.activations == 1


# --- Requirement: The console retains records for a bounded window -----------


def test_store_status_reports_counts_extent_and_retention(store: _Store) -> None:
    _seed_suspend_resume(store)
    _error(store, entity_key="key-a", seq=4, reason="activation_error", event_time_ms=_T0 + 2_000)
    _snapshot(
        store, entity_key="key-a", seq=4, snapshot_at_ms=_T0 + 3_000, pending_intent_ids=["i"]
    )
    handle = _Store(store.path, retention_hours=24.0).as_store()

    status = _queries.store_status(handle)

    assert status.row_counts == {"activations": 1, "errors": 1, "events": 9, "snapshots": 1}
    assert status.retention_hours == 24.0
    assert status.database_path == str(store.path)
    assert status.database_bytes is not None and status.database_bytes > 0
    assert status.oldest_record_ms == _T0
    assert status.newest_record_ms == _T0 + 3_000
    assert status.schema_version == 1


def test_an_activation_detail_carries_its_snapshot_and_replay_command(store: _Store) -> None:
    _seed_suspend_resume(store)
    _snapshot(
        store,
        entity_key="key-a",
        seq=4,
        snapshot_at_ms=_T0 + 3_000,
        state_schema_version=1,
        memory_entries=3,
        pending_intent_ids=["intent-1"],
        continuation_step_index=1,
    )

    detail = _queries.activation_detail(store.as_store(), entity_key="key-a", seq=4)

    assert detail is not None
    assert detail.snapshot is not None
    assert detail.snapshot.memory_entries == 3
    assert detail.snapshot.pending_intent_ids == ["intent-1"]
    assert detail.replay_command is not None
    assert "--seq 4" in detail.replay_command


def test_an_activation_without_a_snapshot_offers_no_replay_command(store: _Store) -> None:
    # Replay reconstructs an activation *from a StateSnapshot*; without one there
    # is no command to copy, and offering a broken one would waste the operator's
    # time at exactly the wrong moment.
    _seed_suspend_resume(store)

    detail = _queries.activation_detail(store.as_store(), entity_key="key-a", seq=4)

    assert detail is not None
    assert detail.snapshot is None
    assert detail.replay_command is None
