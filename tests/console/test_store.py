"""The console store: idempotent ingest, derived rollups, and retention.

Covers the `agent-console` scenarios "The same event ingested twice yields one
row", "A later copy carrying more attributes wins", "Opening a fresh path
creates a usable store", "A rollup is correct after a partial arrival", "A
rollup corrects itself when the rest arrives", "A suspend and resume are one
activation", and "Records outside the window are pruned".

Every event under test is a real `TraceEvent` built by the runtime's own
`ActivationTrace`, converted to an `EventRow` by the same hex-and-enum-name
mapping `_ingest.normalize` will apply. Hand-writing rows would let the store's
understanding of an event drift from the producer's without a test noticing.
"""

from __future__ import annotations

import dataclasses
import itertools
import sqlite3
from typing import TYPE_CHECKING, Any, cast

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.console import _schema
from beam_agents.console._records import (
    PROVENANCE_BUNDLE,
    PROVENANCE_NATIVE,
    PROVENANCE_OTLP,
    ErrorRow,
    EventRow,
    RecordBatch,
    SnapshotRow,
)
from beam_agents.console._schema import SCHEMA_VERSION, TABLES, apply_schema, schema_version_of
from beam_agents.console._store import ConsoleStore
from beam_agents.observability.traces import (
    ACTIVATION_KIND,
    ACTIVATION_STATUS,
    CACHE_HIT,
    OPERATION_CHAT,
    OPERATION_NAME,
    REQUEST_MODEL,
    ROLE_ACTIVATION,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
    ActivationTrace,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path

KEY = bytes.fromhex("0a0b0c0d")
KEY_HEX = KEY.hex()
SEQ = 4
T0 = 1_700_000_000_000
HOUR_MS = 3_600_000


# --- fixtures ----------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path) -> Iterator[ConsoleStore]:
    with ConsoleStore(tmp_path / "console.db") as opened:
        yield opened


def _row(event: TraceEvent, *, provenance: str = PROVENANCE_NATIVE) -> EventRow:
    """Normalize one `TraceEvent` exactly as the ingest layer will."""
    return EventRow(
        trace_id=event.trace_id.hex(),
        span_id=event.span_id.hex(),
        parent_span_id=event.parent_span_id.hex(),
        entity_key=event.entity_key.hex(),
        seq=event.seq,
        step_index=event.step_index,
        event_type=TraceEvent.EventType.Name(event.event_type),
        start_ms=event.start_ms,
        end_ms=event.end_ms,
        attributes=dict(event.attributes),
        provenance=provenance,
    )


def _batch(events: Iterable[TraceEvent], *, provenance: str = PROVENANCE_NATIVE) -> RecordBatch:
    return RecordBatch(events=tuple(_row(event, provenance=provenance) for event in events))


def _stamped(
    trace: ActivationTrace,
    event_type: TraceEvent.EventType,
    *,
    now_ms: int,
    step_index: int,
    attributes: dict[str, str],
    key: bytes = KEY,
    seq: int = SEQ,
) -> TraceEvent:
    """Build a producer-shaped event and let the trace stamp its correlation."""
    event = TraceEvent(
        entity_key=key,
        seq=seq,
        step_index=step_index,
        event_type=event_type,
        attributes=attributes,
        start_ms=now_ms,
        end_ms=now_ms,
    )
    return trace.stamp(event)


def _llm_call(
    trace: ActivationTrace,
    *,
    now_ms: int,
    step_index: int = 1,
    model: str = "gpt-4o-mini",
    prompt_tokens: int | None = 11,
    completion_tokens: int | None = 5,
    cache_hit: bool = False,
    key: bytes = KEY,
    seq: int = SEQ,
) -> TraceEvent:
    attributes = {
        OPERATION_NAME: OPERATION_CHAT,
        REQUEST_MODEL: model,
        CACHE_HIT: "true" if cache_hit else "false",
    }
    if prompt_tokens is not None:
        attributes[USAGE_INPUT_TOKENS] = str(prompt_tokens)
    if completion_tokens is not None:
        attributes[USAGE_OUTPUT_TOKENS] = str(completion_tokens)
    return _stamped(
        trace,
        TraceEvent.LLM_CALL,
        now_ms=now_ms,
        step_index=step_index,
        attributes=attributes,
        key=key,
        seq=seq,
    )


def _completed_activation(
    *, key: bytes = KEY, seq: int = SEQ, now_ms: int = T0
) -> list[TraceEvent]:
    """A whole successful activation: start, one LLM call, one tool, one intent, end."""
    trace = ActivationTrace(entity_key=key, seq=seq, now_ms=now_ms)
    return [
        trace.activation_start(),
        _llm_call(trace, now_ms=now_ms, key=key, seq=seq),
        trace.tool_call(step_index=1, tool_index=0, tool_name="lookup_account"),
        trace.intent_emitted(
            step_index=2,
            intent_id="intent-1",
            tool_name="charge_card",
            intent_kind="TOOL",
            expires_at_ms=now_ms + HOUR_MS,
        ),
        trace.activation_end(status="completed", step_index=2),
    ]


def _activation_row(store: ConsoleStore, *, entity_key: str = KEY_HEX, seq: int = SEQ) -> Any:
    with store.reader() as connection:
        return connection.execute(
            "SELECT * FROM activations WHERE entity_key = ? AND seq = ?", (entity_key, seq)
        ).fetchone()


def _scalar(store: ConsoleStore, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    with store.reader() as connection:
        row = connection.execute(sql, parameters).fetchone()
    return row[0]


# --- Requirement: the console stores telemetry records idempotently -----------


def test_opening_a_fresh_path_creates_a_usable_store(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "console.db"
    assert not path.exists()

    with ConsoleStore(path) as store:
        assert store.path == path
        assert store.write(_batch(_completed_activation())) > 0
        assert store.counts()["events"] == 5

    assert path.exists()


def test_the_same_event_ingested_twice_yields_one_row(store: ConsoleStore) -> None:
    events = _completed_activation()

    first = store.write(_batch(events))
    second = store.write(_batch(events))

    assert first > 0
    # Nothing to change: every record was already present and identical, which
    # is the expected outcome of a retried bundle or a replayed run.
    assert second == 0
    assert store.counts()["events"] == len(events)
    assert _scalar(store, "SELECT COUNT(*) FROM activations") == 1
    assert _activation_row(store)["llm_calls"] == 1


def test_a_later_copy_carrying_more_attributes_wins(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    sparse = _llm_call(trace, now_ms=T0, prompt_tokens=None, completion_tokens=None)
    rich = _llm_call(trace, now_ms=T0, prompt_tokens=11, completion_tokens=5)

    store.write(_batch([sparse]))
    store.write(_batch([rich]))

    assert store.counts()["events"] == 1
    with store.reader() as connection:
        stored = dict(
            connection.execute(
                "SELECT key, value FROM event_attributes WHERE span_id = ?", (rich.span_id.hex(),)
            ).fetchall()
        )
    assert stored == {
        OPERATION_NAME: OPERATION_CHAT,
        REQUEST_MODEL: "gpt-4o-mini",
        CACHE_HIT: "false",
        USAGE_INPUT_TOKENS: "11",
        USAGE_OUTPUT_TOKENS: "5",
    }
    assert _activation_row(store)["prompt_tokens"] == 11


def test_a_merge_never_drops_an_attribute_the_earlier_copy_carried(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    rich = _llm_call(trace, now_ms=T0)
    sparse = _llm_call(trace, now_ms=T0, prompt_tokens=None, completion_tokens=None)

    store.write(_batch([rich]))
    changed = store.write(_batch([sparse]))

    assert changed == 0
    assert _activation_row(store)["prompt_tokens"] == 11


def test_a_lossless_copy_survives_a_lossy_duplicate(store: ConsoleStore) -> None:
    events = _completed_activation()

    store.write(_batch(events))
    # The OTLP encoding cannot carry ACTIVATION_START at all, so a duplicate
    # arriving over it must not relabel a lossless record as lossy.
    store.write(_batch(events[1:], provenance=PROVENANCE_OTLP))

    assert (
        _scalar(store, "SELECT COUNT(*) FROM events WHERE provenance = ?", (PROVENANCE_OTLP,)) == 0
    )
    assert _activation_row(store)["complete_provenance"] == 1


def test_an_activation_seen_only_over_a_lossy_source_is_marked_incomplete(
    store: ConsoleStore,
) -> None:
    events = [
        event
        for event in _completed_activation()
        if event.event_type != TraceEvent.ACTIVATION_START
    ]

    store.write(_batch(events, provenance=PROVENANCE_OTLP))

    row = _activation_row(store)
    assert row["complete_provenance"] == 0
    assert row["provenance"] == f'["{PROVENANCE_OTLP}"]'


def test_reopening_an_existing_store_migrates_nothing_and_keeps_its_records(
    tmp_path: Path,
) -> None:
    path = tmp_path / "console.db"
    with ConsoleStore(path) as first:
        first.write(_batch(_completed_activation()))

    with ConsoleStore(path) as second:
        assert second.counts()["events"] == 5
        with second.reader() as connection:
            assert schema_version_of(connection) == SCHEMA_VERSION


def test_applying_the_schema_to_a_current_database_is_a_no_op(tmp_path: Path) -> None:
    path = tmp_path / "console.db"
    with ConsoleStore(path):
        pass

    connection = sqlite3.connect(path, isolation_level=None)
    try:
        assert apply_schema(connection) == SCHEMA_VERSION
        assert apply_schema(connection) == SCHEMA_VERSION
    finally:
        connection.close()


def test_a_database_from_a_newer_console_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "console.db"
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="newer"):
        ConsoleStore(path)


def test_errors_and_snapshots_are_idempotent(store: ConsoleStore) -> None:
    batch = RecordBatch(
        errors=(
            ErrorRow(
                entity_key=KEY_HEX,
                reason="activation_error",
                detail="ValueError: boom",
                event_time_ms=T0,
                seq=SEQ,
            ),
        ),
        snapshots=(
            SnapshotRow(
                entity_key=KEY_HEX,
                seq=SEQ,
                snapshot_at_ms=T0,
                state_schema_version=1,
                request_id="req-1",
                memory_entries=2,
                memory_bytes=64,
                pending_intent_ids=("intent-1",),
                raw=b"\x01\x02",
            ),
        ),
    )

    store.write(batch)
    assert store.write(batch) == 0

    counts = store.counts()
    assert counts["errors"] == 1
    assert counts["snapshots"] == 1
    assert _scalar(store, "SELECT raw FROM snapshots") == b"\x01\x02"


def test_two_errors_differing_only_in_detail_are_two_rows(store: ConsoleStore) -> None:
    def error(detail: str) -> RecordBatch:
        return RecordBatch(
            errors=(
                ErrorRow(
                    entity_key=KEY_HEX,
                    reason="intent_dead_letter",
                    detail=detail,
                    event_time_ms=T0,
                ),
            )
        )

    store.write(error('{"intent_id": "a"}'))
    store.write(error('{"intent_id": "b"}'))

    assert store.counts()["errors"] == 2


# --- Requirement: activation rollups are derived, never written ---------------


def test_a_single_attempt_reports_no_wall_time(store: ConsoleStore) -> None:
    # Scenario: Real measurements are shown as numbers. A single attempt has no
    # elapsed time to show. `ActivationTrace` holds one injected clock read and
    # stamps every event of the attempt with it, so START and END carry the
    # identical timestamp — their difference is that number subtracted from
    # itself, not a measured zero. Reporting `0` would tell an operator the
    # activation was instantaneous.
    store.write(_batch(_completed_activation()))

    row = _activation_row(store)
    assert row["status"] == "completed"
    assert row["ended_ms"] is not None
    assert row["wall_ms"] is None


def test_a_rollup_is_correct_after_a_partial_arrival(store: ConsoleStore) -> None:
    events = _completed_activation()

    store.write(_batch(events[:2]))

    row = _activation_row(store)
    assert row["status"] == "in_flight"
    assert row["ended_ms"] is None
    assert row["wall_ms"] is None
    assert row["llm_calls"] == 1
    assert row["tool_calls"] == 0
    assert row["intents"] == 0
    assert row["prompt_tokens"] == 11
    assert row["total_tokens"] == 16
    assert row["trace_id"] == events[0].trace_id.hex()


@pytest.mark.parametrize("order", list(itertools.permutations(range(5))))
def test_a_rollup_corrects_itself_when_the_rest_arrives(
    tmp_path: Path, order: tuple[int, ...]
) -> None:
    events = _completed_activation()
    with ConsoleStore(tmp_path / "all-at-once.db") as reference_store:
        reference_store.write(_batch(events))
        reference = dict(_activation_row(reference_store))

    with ConsoleStore(tmp_path / "one-at-a-time.db") as store:
        for index in order:
            store.write(_batch([events[index]]))
        assert dict(_activation_row(store)) == reference


def test_a_suspend_and_resume_are_one_activation(store: ConsoleStore) -> None:
    first = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    resumed = ActivationTrace(
        entity_key=KEY, seq=SEQ, now_ms=T0 + 5_000, entry_step_index=2, is_resume=True
    )
    events = [
        first.activation_start(),
        first.suspended(
            step_index=2, deadline_ms=T0 + HOUR_MS, adapter="langgraph", pending_intent_ids=("i-1",)
        ),
        first.activation_end(status="suspended", step_index=2),
        resumed.activation_start(),
        _llm_call(resumed, now_ms=T0 + 5_000, step_index=3),
        resumed.activation_end(status="completed", step_index=3),
    ]

    store.write(_batch(events))

    assert _scalar(store, "SELECT COUNT(*) FROM activations") == 1
    assert _scalar(store, "SELECT COUNT(*) FROM traces") == 1
    row = _activation_row(store)
    assert row["attempts"] == 2
    assert row["status"] == "completed"
    assert row["kind"] == "resume"
    assert row["started_ms"] == T0
    assert row["ended_ms"] == T0 + 5_000
    assert row["wall_ms"] == 5_000


def test_a_resumed_activation_still_running_is_in_flight(store: ConsoleStore) -> None:
    first = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    resumed = ActivationTrace(
        entity_key=KEY, seq=SEQ, now_ms=T0 + 5_000, entry_step_index=2, is_resume=True
    )

    store.write(
        _batch(
            [
                first.activation_start(),
                first.activation_end(status="suspended", step_index=2),
                resumed.activation_start(),
            ]
        )
    )

    row = _activation_row(store)
    assert row["status"] == "in_flight"
    assert row["attempts"] == 2


def test_attempts_are_counted_when_the_source_carries_no_start_event(
    store: ConsoleStore,
) -> None:
    first = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    resumed = ActivationTrace(
        entity_key=KEY, seq=SEQ, now_ms=T0 + 5_000, entry_step_index=2, is_resume=True
    )

    # What an OTLP-only ingest sees: both ACTIVATION_ENDs, neither START.
    store.write(
        _batch(
            [
                first.activation_end(status="suspended", step_index=2),
                resumed.activation_end(status="completed", step_index=3),
            ],
            provenance=PROVENANCE_OTLP,
        )
    )

    row = _activation_row(store)
    assert row["attempts"] == 2
    assert row["status"] == "completed"
    assert row["complete_provenance"] == 0
    assert row["wall_ms"] is None


def test_a_suspension_with_no_resume_reports_suspended(store: ConsoleStore) -> None:
    first = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)

    store.write(
        _batch(
            [
                first.activation_start(),
                first.activation_end(status="suspended", step_index=2),
            ]
        )
    )

    assert _activation_row(store)["status"] == "suspended"


def test_an_error_event_makes_the_activation_terminal(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0 + 900)

    store.write(_batch([trace.error(reason="activation_error", error_type="ValueError")]))

    row = _activation_row(store)
    assert row["status"] == "error"
    assert row["errors"] == 1
    assert row["reasons"] == '["activation_error"]'
    assert row["ended_ms"] == T0 + 900


def test_an_error_record_and_its_trace_event_are_one_failure(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    store.write(
        RecordBatch(
            events=(_row(trace.error(reason="activation_timeout")),),
            errors=(
                ErrorRow(
                    entity_key=KEY_HEX,
                    reason="activation_timeout",
                    detail="",
                    event_time_ms=T0,
                    seq=SEQ,
                ),
            ),
        )
    )

    row = _activation_row(store)
    # Both paths record the same failure; summing them would double-count it.
    assert row["errors"] == 1
    assert row["status"] == "error"


def test_an_error_record_alone_creates_the_activation_it_failed(store: ConsoleStore) -> None:
    store.write(
        RecordBatch(
            errors=(
                ErrorRow(
                    entity_key=KEY_HEX,
                    reason="budget_exceeded",
                    detail="",
                    event_time_ms=T0,
                    seq=SEQ,
                ),
            )
        )
    )

    row = _activation_row(store)
    assert row["status"] == "error"
    assert row["errors"] == 1
    assert row["started_ms"] == T0


def test_a_call_that_recorded_no_tokens_reports_none_not_zero(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)

    store.write(
        _batch(
            [
                trace.activation_start(),
                _llm_call(trace, now_ms=T0, prompt_tokens=None, completion_tokens=None),
            ]
        )
    )

    row = _activation_row(store)
    assert row["llm_calls"] == 1
    assert row["prompt_tokens"] is None
    assert row["completion_tokens"] is None
    assert row["total_tokens"] is None
    assert row["cache_hits"] == 0


def test_cache_hits_and_tools_are_rolled_up_from_attributes(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)

    store.write(
        _batch(
            [
                trace.activation_start(),
                _llm_call(trace, now_ms=T0, step_index=1, cache_hit=True),
                trace.tool_call(step_index=1, tool_index=0, tool_name="lookup_account"),
                trace.tool_call(step_index=1, tool_index=1, tool_name="fetch_balance"),
                trace.intent_emitted(
                    step_index=2,
                    intent_id="i-1",
                    tool_name="charge_card",
                    intent_kind="APPROVAL",
                    expires_at_ms=T0 + HOUR_MS,
                ),
            ]
        )
    )

    row = _activation_row(store)
    assert row["cache_hits"] == 1
    assert row["tool_calls"] == 2
    assert row["intents"] == 1
    assert row["model"] == "gpt-4o-mini"
    assert row["tools"] == '["charge_card", "fetch_balance", "lookup_account"]'
    assert (
        _scalar(store, "SELECT COUNT(*) FROM activation_tools WHERE entity_key = ?", (KEY_HEX,))
        == 3
    )


def test_the_span_tree_is_derived_from_the_events(store: ConsoleStore) -> None:
    events = _completed_activation()

    store.write(_batch(events))

    with store.reader() as connection:
        spans = connection.execute(
            "SELECT span_id, role, parent_span_id FROM spans ORDER BY first_ms, span_id"
        ).fetchall()
    roles = {row["role"] for row in spans}
    assert ROLE_ACTIVATION in roles
    assert roles == {ROLE_ACTIVATION, "LLM_CALL", "TOOL_CALL", "INTENT_EMITTED"}
    activation_span = events[0].span_id.hex()
    assert all(
        row["parent_span_id"] == activation_span
        for row in spans
        if row["span_id"] != activation_span
    )
    assert _scalar(store, "SELECT spans FROM traces") == 4
    assert _scalar(store, "SELECT events FROM traces") == 5


def test_an_entity_rolls_up_every_sequence_it_ran(store: ConsoleStore) -> None:
    store.write(_batch(_completed_activation(seq=1, now_ms=T0)))
    store.write(_batch(_completed_activation(seq=2, now_ms=T0 + HOUR_MS)))

    with store.reader() as connection:
        row = connection.execute(
            "SELECT * FROM entities WHERE entity_key = ?", (KEY_HEX,)
        ).fetchone()
    assert row["activations"] == 2
    assert row["first_seen_ms"] == T0
    assert row["last_seen_ms"] == T0 + HOUR_MS
    assert row["latest_seq"] == 2
    assert row["latest_status"] == "completed"
    assert row["total_tokens"] == 32


def test_an_unrecognized_terminal_status_is_not_invented(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    end = trace.activation_end(status="completed", step_index=1)
    end.attributes[ACTIVATION_STATUS] = "teleported"

    store.write(_batch([trace.activation_start(), end]))

    assert _activation_row(store)["status"] == "in_flight"
    assert _activation_row(store)["kind"] == "start"


def test_an_activation_with_no_kind_attribute_is_unknown(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    start = trace.activation_start()
    del start.attributes[ACTIVATION_KIND]

    store.write(_batch([start]))

    assert _activation_row(store)["kind"] == "unknown"


# --- Requirement: the console retains records for a bounded window ------------


def test_records_outside_the_window_are_pruned(tmp_path: Path) -> None:
    now_ms = T0 + 10 * HOUR_MS
    with ConsoleStore(tmp_path / "console.db", retention_hours=2.0) as store:
        store.write(_batch(_completed_activation(seq=1, now_ms=now_ms - 5 * HOUR_MS)))
        store.write(_batch(_completed_activation(seq=2, now_ms=now_ms - HOUR_MS)))
        store.write(
            RecordBatch(
                errors=(
                    ErrorRow(
                        entity_key=KEY_HEX,
                        reason="activation_error",
                        detail="old",
                        event_time_ms=now_ms - 5 * HOUR_MS,
                        seq=1,
                    ),
                ),
                snapshots=(
                    SnapshotRow(
                        entity_key=KEY_HEX,
                        seq=1,
                        snapshot_at_ms=now_ms - 5 * HOUR_MS,
                        state_schema_version=1,
                    ),
                ),
            )
        )

        removed = store.prune(now_ms=now_ms)

        assert removed > 0
        counts = store.counts()
        assert counts["events"] == 5
        assert counts["errors"] == 0
        assert counts["snapshots"] == 0
        assert counts["activations"] == 1
        assert counts["traces"] == 1
        assert counts["spans"] == 4
        assert counts["entities"] == 1
        assert _activation_row(store, seq=2)["status"] == "completed"
        assert _activation_row(store, seq=1) is None
        assert store.prune(now_ms=now_ms) == 0


def test_a_partially_pruned_activation_keeps_a_recomputed_rollup(tmp_path: Path) -> None:
    now_ms = T0 + 10 * HOUR_MS
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=now_ms - 5 * HOUR_MS)
    late = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=now_ms - 60_000)
    with ConsoleStore(tmp_path / "console.db", retention_hours=2.0) as store:
        store.write(
            _batch(
                [
                    trace.activation_start(),
                    _llm_call(late, now_ms=now_ms - 60_000, step_index=1),
                ]
            )
        )

        store.prune(now_ms=now_ms)

        row = _activation_row(store)
        assert row["llm_calls"] == 1
        assert row["attempts"] == 1
        assert row["started_ms"] == now_ms - 60_000
        assert store.counts()["events"] == 1


def test_an_unbounded_store_prunes_nothing(store: ConsoleStore) -> None:
    store.write(_batch(_completed_activation()))

    assert store.retention_hours is None
    assert store.prune(now_ms=T0 + 10_000 * HOUR_MS) == 0
    assert store.counts()["events"] == 5


# --- Store mechanics ---------------------------------------------------------


def test_the_store_is_a_wal_database_with_every_table(store: ConsoleStore) -> None:
    with store.reader() as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        present = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert set(TABLES) <= present
    assert set(store.counts()) == set(TABLES)


def test_the_reader_is_a_context_manager_that_cannot_write(store: ConsoleStore) -> None:
    with store.reader() as connection:
        assert connection.execute("SELECT 1").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("DELETE FROM events")


def test_an_empty_batch_writes_nothing(store: ConsoleStore) -> None:
    assert store.write(RecordBatch()) == 0


def test_a_record_the_database_refuses_leaves_no_partial_write(store: ConsoleStore) -> None:
    events = _completed_activation()
    unstorable = dataclasses.replace(
        _row(events[1]), attributes={"beam_agents.broken": cast("str", object())}
    )

    with pytest.raises(sqlite3.ProgrammingError):
        store.write(RecordBatch(events=(_row(events[0]), unstorable)))

    counts = store.counts()
    assert counts["events"] == 0
    assert counts["activations"] == 0


def test_an_unparseable_token_count_is_absent_rather_than_wrong(store: ConsoleStore) -> None:
    trace = ActivationTrace(entity_key=KEY, seq=SEQ, now_ms=T0)
    call = _llm_call(trace, now_ms=T0)
    call.attributes[USAGE_INPUT_TOKENS] = "lots"

    store.write(_batch([call]))

    row = _activation_row(store)
    assert row["prompt_tokens"] is None
    assert row["completion_tokens"] == 5
    assert row["total_tokens"] == 5


def test_an_error_against_an_unreadable_entity_key_still_records(store: ConsoleStore) -> None:
    store.write(
        RecordBatch(
            errors=(
                ErrorRow(
                    entity_key="not-hex",
                    reason="orphaned_result",
                    detail="",
                    event_time_ms=T0,
                    seq=SEQ,
                ),
            )
        )
    )

    row = _activation_row(store, entity_key="not-hex")
    assert row["status"] == "error"
    # No trace ID can be derived from a key that is not the hex the runtime
    # emits, and inventing one would point the UI at a trace that cannot exist.
    assert row["trace_id"] == ""


def test_pruning_the_last_record_for_an_entity_removes_it(tmp_path: Path) -> None:
    now_ms = T0 + 10 * HOUR_MS
    with ConsoleStore(tmp_path / "console.db", retention_hours=1.0) as store:
        store.write(_batch(_completed_activation(now_ms=now_ms - 5 * HOUR_MS)))

        assert store.prune(now_ms=now_ms) > 0

        assert store.counts() == dict.fromkeys(TABLES, 0)


def test_a_failing_prune_leaves_the_store_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def explode(*_args: Any, **_kwargs: Any) -> int:
        raise OSError("disk went away mid-prune")

    with ConsoleStore(tmp_path / "console.db", retention_hours=1.0) as store:
        store.write(_batch(_completed_activation()))
        monkeypatch.setattr(ConsoleStore, "_prune", explode)

        with pytest.raises(OSError, match="disk went away"):
            store.prune(now_ms=T0 + 100 * HOUR_MS)

        monkeypatch.undo()
        assert store.counts()["events"] == 5
        # The connection is still usable: the failed prune rolled its
        # transaction back rather than leaving one open.
        assert store.write(_batch(_completed_activation())) == 0


def test_a_migration_that_fails_leaves_the_version_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_schema, "_MIGRATIONS", (("CREATE TABLE ok (a TEXT)", "NOT VALID SQL"),))
    connection = sqlite3.connect(tmp_path / "console.db", isolation_level=None)
    try:
        with pytest.raises(sqlite3.OperationalError):
            apply_schema(connection)

        assert schema_version_of(connection) == 0
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert tables == set()
    finally:
        connection.close()


def test_the_schema_applies_inside_a_transaction_the_caller_opened(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "console.db", isolation_level=None)
    try:
        connection.execute("BEGIN IMMEDIATE")
        assert apply_schema(connection) == SCHEMA_VERSION
        assert connection.in_transaction
        connection.execute("COMMIT")
        assert schema_version_of(connection) == SCHEMA_VERSION
    finally:
        connection.close()


def test_closing_is_idempotent_and_a_closed_store_refuses_writes(tmp_path: Path) -> None:
    store = ConsoleStore(tmp_path / "console.db")
    store.close()
    store.close()

    with pytest.raises(RuntimeError, match="closed"):
        store.write(_batch(_completed_activation()))
    with pytest.raises(RuntimeError, match="closed"), store.reader():
        pass


def test_the_provenance_of_a_record_is_kept(store: ConsoleStore) -> None:
    store.write(_batch(_completed_activation(), provenance=PROVENANCE_BUNDLE))

    assert _activation_row(store)["provenance"] == f'["{PROVENANCE_BUNDLE}"]'
    assert _activation_row(store)["complete_provenance"] == 1


@pytest.mark.parametrize(
    ("sql", "parameters"),
    [
        ("SELECT * FROM activations ORDER BY started_ms DESC, entity_key DESC, seq DESC", ()),
        ("SELECT * FROM activations WHERE entity_key = ?", (KEY_HEX,)),
        ("SELECT * FROM activations WHERE status = ?", ("completed",)),
        ("SELECT * FROM activations WHERE kind = ?", ("start",)),
        ("SELECT * FROM activations WHERE model = ?", ("gpt-4o-mini",)),
        ("SELECT * FROM activations WHERE started_ms >= ?", (T0,)),
        ("SELECT * FROM activations WHERE trace_id = ?", ("abc",)),
        ("SELECT entity_key, seq FROM activation_tools WHERE tool_name = ?", ("lookup",)),
        ("SELECT entity_key, seq FROM activation_reasons WHERE reason = ?", ("hitl_timeout",)),
        ("SELECT * FROM errors WHERE reason = ?", ("hitl_timeout",)),
        ("SELECT * FROM errors WHERE entity_key = ? AND seq = ?", (KEY_HEX, SEQ)),
        ("SELECT * FROM errors WHERE event_time_ms >= ?", (T0,)),
        ("SELECT * FROM events WHERE entity_key = ? AND seq = ?", (KEY_HEX, SEQ)),
        ("SELECT * FROM events WHERE trace_id = ?", ("abc",)),
        ("SELECT * FROM events WHERE event_type = ? AND start_ms >= ?", ("LLM_CALL", T0)),
        ("SELECT * FROM event_attributes WHERE key = ? AND value = ?", ("k", "v")),
        ("SELECT * FROM snapshots WHERE entity_key = ? AND seq = ?", (KEY_HEX, SEQ)),
        ("SELECT * FROM entities ORDER BY last_seen_ms DESC", ()),
        ("SELECT * FROM spans WHERE trace_id = ? AND parent_span_id = ?", ("abc", "def")),
    ],
)
def test_every_filter_the_query_layer_exposes_has_an_index(
    store: ConsoleStore, sql: str, parameters: tuple[Any, ...]
) -> None:
    with store.reader() as connection:
        plan = " ".join(
            str(row[3]) for row in connection.execute(f"EXPLAIN QUERY PLAN {sql}", parameters)
        )
    # "SCAN <table> USING INDEX" is an index-ordered walk, which is what an
    # unfiltered keyset ORDER BY should produce; only a bare "SCAN <table>" —
    # naming no index and no primary key — is the full-table read this asserts
    # against. A `WITHOUT ROWID` table's primary key *is* its clustered index,
    # so the planner reporting it is a hit, not a miss.
    assert "INDEX" in plan or "USING PRIMARY KEY" in plan, plan
