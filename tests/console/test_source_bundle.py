"""Importing a run captured for the replay CLI.

Covers the `console-ingest` scenarios "A captured run is inspectable offline"
and "A truncated stream reports what it read".

`_store` and `_ingest` are still placeholders while their own units land, so the
store is a fake that records the batches written to it and the four `_ingest`
entry points are monkeypatched with thin real implementations built on
`replay.bundle` — the same parser production goes through. Every test here fails
if `_bundle` stops routing its decoding through `_ingest`, which is the property
design D7 exists to protect.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from beam_agents._protos import ActivationErrorRecord, StateSnapshot, TraceEvent
from beam_agents.console import _ingest
from beam_agents.console._ingest import TruncatedStreamError
from beam_agents.console._records import (
    PROVENANCE_BUNDLE,
    ErrorRow,
    EventRow,
    RecordBatch,
    SnapshotRow,
)
from beam_agents.console._sources._bundle import BundleImportResult, import_bundle, import_bytes
from beam_agents.core.snapshot import serialize_snapshot
from beam_agents.replay.bundle import ReplayUsageError, frame_trace_events, parse_trace_stream
from tests.replay._fixtures import KEY, NOW_MS, SEQ, Original, run_original

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from beam_agents.console._store import ConsoleStore


# --- the doubles standing in for units 1 and 2 --------------------------------


class FakeStore:
    """A store that records what it was asked to write."""

    def __init__(self) -> None:
        self.batches: list[RecordBatch] = []

    def write(self, batch: RecordBatch) -> int:
        """Record the batch and report every record in it as a changed row."""
        self.batches.append(batch)
        return len(batch)

    @property
    def events(self) -> tuple[EventRow, ...]:
        """Every event row written, in write order."""
        return tuple(row for batch in self.batches for row in batch.events)

    @property
    def snapshots(self) -> tuple[SnapshotRow, ...]:
        """Every snapshot row written, in write order."""
        return tuple(row for batch in self.batches for row in batch.snapshots)

    @property
    def errors(self) -> tuple[ErrorRow, ...]:
        """Every error row written, in write order."""
        return tuple(row for batch in self.batches for row in batch.errors)


def _store() -> tuple[FakeStore, ConsoleStore]:
    """A fake store and the same object under the type the importer declares."""
    fake = FakeStore()
    return fake, cast("ConsoleStore", fake)


def _complete_records(payload: bytes) -> int:
    """How many whole frames a payload holds, by the real parser's reckoning.

    A prefix parses cleanly only when it ends on a frame boundary, so the
    largest record count any prefix yields is the number of complete records the
    payload carries. Quadratic, and deliberately so: it needs no framing
    knowledge of its own, which is what keeps this double honest.
    """
    read = 0
    for end in range(len(payload) + 1):
        try:
            read = max(read, len(parse_trace_stream(payload[:end])))
        except ReplayUsageError:
            continue
    return read


def fake_decode_trace_stream(payload: bytes) -> tuple[TraceEvent, ...]:
    """Decode a framed trace stream, reporting truncation with its record count.

    The real decoder wraps this same parser (design D7). Only truncation is
    modelled here; a frame whose payload is not a `TraceEvent` is out of scope
    for the bundle importer.
    """
    try:
        return tuple(parse_trace_stream(payload))
    except ReplayUsageError as exc:
        raise TruncatedStreamError(str(exc), records_read=_complete_records(payload)) from exc


def fake_decode_snapshot_payload(payload: bytes) -> tuple[StateSnapshot, ...]:
    """Parse one serialized `StateSnapshot`."""
    snapshot = StateSnapshot()
    snapshot.ParseFromString(payload)
    return (snapshot,)


def fake_decode_error_payload(payload: bytes) -> tuple[ActivationErrorRecord, ...]:
    """Parse one bare `ActivationErrorRecord`."""
    record = ActivationErrorRecord()
    record.ParseFromString(payload)
    return (record,)


def fake_normalize(
    *,
    events: Sequence[TraceEvent] = (),
    errors: Sequence[ActivationErrorRecord] = (),
    snapshots: Sequence[StateSnapshot] = (),
    provenance: str,
) -> RecordBatch:
    """Turn protos into rows, stamping provenance the way the real one does."""
    return RecordBatch(
        events=tuple(
            EventRow(
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
            for event in events
        ),
        errors=tuple(
            ErrorRow(
                entity_key=error.entity_key.hex(),
                reason=error.reason,
                detail=error.detail,
                event_time_ms=error.event_time_ms,
                provenance=provenance,
            )
            for error in errors
        ),
        snapshots=tuple(
            SnapshotRow(
                entity_key=snapshot.entity_key.hex(),
                seq=snapshot.seq,
                snapshot_at_ms=snapshot.snapshot_at_ms,
                state_schema_version=snapshot.state_schema_version,
                request_id=snapshot.request_id,
                memory_entries=len(snapshot.memory.entries),
                llm_cache_entries=len(snapshot.llm_cache.entries),
                raw=snapshot.SerializeToString(deterministic=True),
                provenance=provenance,
            )
            for snapshot in snapshots
        ),
    )


@pytest.fixture(autouse=True)
def ingest_doubles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install the thin `_ingest` implementations for the length of a test."""
    monkeypatch.setattr(_ingest, "decode_trace_stream", fake_decode_trace_stream)
    monkeypatch.setattr(_ingest, "decode_snapshot_payload", fake_decode_snapshot_payload)
    monkeypatch.setattr(_ingest, "decode_error_payload", fake_decode_error_payload)
    monkeypatch.setattr(_ingest, "normalize", fake_normalize)


# --- a real capture on disk ---------------------------------------------------


def _capture(tmp_path: Path) -> tuple[Path, Path, Original]:
    """Write a real activation's trace stream and snapshot the way an operator would."""
    original = run_original()
    traces = tmp_path / "traces.pb"
    traces.write_bytes(frame_trace_events(original.traces))
    snapshot = tmp_path / "snapshot.pb"
    snapshot.write_bytes(serialize_snapshot(original.snapshot)[1])
    return traces, snapshot, original


# --- Scenario: A captured run is inspectable offline --------------------------


def test_a_captured_run_is_inspectable_offline(tmp_path: Path) -> None:
    # The two files `beam-agents-replay` consumes, produced by a real
    # activation, imported with no pipeline and no network.
    traces, snapshot, original = _capture(tmp_path)
    fake, store = _store()

    result = import_bundle(store, traces=traces, snapshot=snapshot)

    assert result.events == len(original.traces)
    assert result.snapshots == 1
    assert result.errors == 0
    assert result.activations == 1
    assert result.truncated is False
    # Every stored event is the traced one, field for field.
    assert [row.event_type for row in fake.events] == [
        TraceEvent.EventType.Name(event.event_type) for event in original.traces
    ]
    assert {row.trace_id for row in fake.events} == {
        event.trace_id.hex() for event in original.traces
    }
    assert fake.snapshots[0].raw == serialize_snapshot(original.snapshot)[1]


def test_an_imported_run_is_stamped_with_bundle_provenance(tmp_path: Path) -> None:
    # Provenance is how the UI knows how complete a record can possibly be, and
    # a bundle carries the full native vocabulary, ACTIVATION_START included.
    traces, snapshot, _ = _capture(tmp_path)
    fake, store = _store()

    import_bundle(store, traces=traces, snapshot=snapshot)

    assert {row.provenance for row in fake.events} == {PROVENANCE_BUNDLE}
    assert {row.provenance for row in fake.snapshots} == {PROVENANCE_BUNDLE}
    assert "ACTIVATION_START" in {row.event_type for row in fake.events}


def test_the_activations_the_import_touched_are_counted(tmp_path: Path) -> None:
    # The console's primary list object is an activation scope, so that is what
    # an import reports having touched — deduplicated across records.
    events = [
        TraceEvent(entity_key=KEY, seq=1, event_type=TraceEvent.ACTIVATION_START),
        TraceEvent(entity_key=KEY, seq=1, event_type=TraceEvent.ACTIVATION_END),
        TraceEvent(entity_key=KEY, seq=2, event_type=TraceEvent.ACTIVATION_START),
        TraceEvent(entity_key=b"other", seq=1, event_type=TraceEvent.ACTIVATION_START),
    ]
    traces = tmp_path / "traces.pb"
    traces.write_bytes(frame_trace_events(events))
    _, store = _store()

    result = import_bundle(store, traces=traces)

    assert result.events == 4
    assert result.activations == 3


def test_a_trace_stream_alone_is_a_usable_import(tmp_path: Path) -> None:
    # An operator who captured traces and no snapshot still has an incident.
    traces, _, original = _capture(tmp_path)
    fake, store = _store()

    result = import_bundle(store, traces=traces)

    assert result.events == len(original.traces)
    assert result.snapshots == 0
    assert fake.snapshots == ()


def test_a_snapshot_alone_is_a_usable_import(tmp_path: Path) -> None:
    _, snapshot, original = _capture(tmp_path)
    fake, store = _store()

    result = import_bundle(store, snapshot=snapshot)

    assert result.events == 0
    assert result.snapshots == 1
    # A snapshot's seq is the counter at export — the seq the *next* activation
    # would run at — so it names no activation that ran.
    assert result.activations == 0
    assert fake.snapshots[0].entity_key == original.snapshot.entity_key.hex()


def test_a_captured_error_record_is_imported(tmp_path: Path) -> None:
    record = ActivationErrorRecord(
        entity_key=KEY, reason="activation_timeout", detail="seq=3", event_time_ms=NOW_MS
    )
    errors = tmp_path / "errors.pb"
    errors.write_bytes(record.SerializeToString(deterministic=True))
    fake, store = _store()

    result = import_bundle(store, errors=errors)

    assert result.errors == 1
    assert fake.errors[0].reason == "activation_timeout"


def test_an_import_of_nothing_writes_nothing() -> None:
    # Every argument is optional, so "no files supplied" is an empty answer with
    # a stated reason rather than a crash.
    fake, store = _store()

    result = import_bytes(store)

    assert result == BundleImportResult(detail=result.detail)
    assert fake.batches == []
    assert "nothing" in result.detail


def test_an_uploaded_payload_imports_what_the_same_bytes_on_disk_do(tmp_path: Path) -> None:
    # The upload endpoint and the CLI must not be two importers.
    traces, snapshot, _ = _capture(tmp_path)
    from_path_fake, from_path_store = _store()
    from_bytes_fake, from_bytes_store = _store()

    from_path = import_bundle(from_path_store, traces=traces, snapshot=snapshot)
    from_bytes = import_bytes(
        from_bytes_store, traces=traces.read_bytes(), snapshot=snapshot.read_bytes()
    )

    assert from_bytes == from_path
    assert from_bytes_fake.events == from_path_fake.events
    assert from_bytes_fake.snapshots == from_path_fake.snapshots


def test_re_importing_a_bundle_reports_the_same_records(tmp_path: Path) -> None:
    # Ingest is idempotent (design D5), so a retried import is a supported
    # operation whose report does not change under the retry.
    traces, snapshot, _ = _capture(tmp_path)
    _, store = _store()

    first = import_bundle(store, traces=traces, snapshot=snapshot)
    second = import_bundle(store, traces=traces, snapshot=snapshot)

    assert second == first


def test_a_missing_file_names_the_path(tmp_path: Path) -> None:
    _, store = _store()

    with pytest.raises(OSError, match=r"absent\.pb"):
        import_bundle(store, traces=tmp_path / "absent.pb")


# --- Scenario: A truncated stream reports what it read ------------------------


def test_a_truncated_stream_reports_what_it_read(tmp_path: Path) -> None:
    # What a crash leaves behind: the last frame never finished being written.
    traces, snapshot, original = _capture(tmp_path)
    # Every complete frame, plus two bytes of the one that never finished.
    complete = len(frame_trace_events(original.traces[:-1]))
    traces.write_bytes(traces.read_bytes()[: complete + 2])
    fake, store = _store()

    result = import_bundle(store, traces=traces, snapshot=snapshot)

    assert result.truncated is True
    assert result.events == len(original.traces) - 1
    # The records that were read remain stored, and the snapshot beside them
    # imports normally: a partial capture is still evidence.
    assert len(fake.events) == len(original.traces) - 1
    assert [row.event_type for row in fake.events] == [
        TraceEvent.EventType.Name(event.event_type) for event in original.traces[:-1]
    ]
    assert result.snapshots == 1


def test_a_truncated_stream_says_so_in_its_detail(tmp_path: Path) -> None:
    traces, _, original = _capture(tmp_path)
    traces.write_bytes(traces.read_bytes()[:-2])
    _, store = _store()

    result = import_bundle(store, traces=traces)

    assert "truncated" in result.detail
    assert str(len(original.traces) - 1) in result.detail


def test_a_stream_truncated_before_its_first_record_imports_nothing(tmp_path: Path) -> None:
    # The degenerate case: a capture that caught only the beginning of one
    # frame. Truncation is still reported rather than read as an empty stream.
    traces = tmp_path / "traces.pb"
    traces.write_bytes(frame_trace_events(run_original().traces)[:3])
    fake, store = _store()

    result = import_bundle(store, traces=traces)

    assert result.truncated is True
    assert result.events == 0
    assert fake.batches == []


def test_an_empty_trace_stream_is_not_truncated(tmp_path: Path) -> None:
    # End-of-file at offset zero is a clean end, not a break mid-record.
    traces = tmp_path / "traces.pb"
    traces.write_bytes(b"")
    _, store = _store()

    result = import_bundle(store, traces=traces)

    assert result.truncated is False
    assert result.events == 0


def test_a_truncated_stream_keeps_every_record_before_the_break(tmp_path: Path) -> None:
    # One byte short of complete, over a stream long enough that recovering the
    # prefix cannot be a lucky guess about where the break landed.
    events = [
        TraceEvent(entity_key=KEY, seq=SEQ, step_index=i, event_type=TraceEvent.LLM_CALL)
        for i in range(64)
    ]
    traces = tmp_path / "traces.pb"
    traces.write_bytes(frame_trace_events(events)[:-1])
    fake, store = _store()

    result = import_bundle(store, traces=traces)

    assert result.truncated is True
    assert result.events == 63
    assert [row.step_index for row in fake.events] == list(range(63))
