"""Import a run captured for the replay CLI.

``beam-agents-replay`` already reads local files by design: a varint-length-
delimited ``TraceEvent`` stream and a serialized ``StateSnapshot``. Those are
the artifacts an operator captures when something goes wrong, and until now the
only thing that could read them was the replay CLI's diff output.

This imports the same files, unchanged, so a captured incident is inspectable in
the console with no pipeline running and no network access. It reuses
``replay.bundle.parse_trace_stream`` through ``_ingest.decode_trace_stream``
rather than reimplementing framing — reading the same files a different way is
how the two drift.

A stream that ends mid-record is imported up to the break and the truncation is
reported. A partially-flushed capture is precisely what a crash leaves behind,
and discarding it would throw away the evidence at the moment it is wanted.

Importing this module has no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from beam_agents.console import _ingest
from beam_agents.console._ingest import TruncatedStreamError
from beam_agents.console._records import PROVENANCE_BUNDLE

if TYPE_CHECKING:
    from beam_agents._protos import ActivationErrorRecord, StateSnapshot, TraceEvent
    from beam_agents.console._records import RecordBatch
    from beam_agents.console._store import ConsoleStore

__all__ = ["BundleImportResult", "import_bundle", "import_bytes"]


@dataclass(frozen=True, slots=True)
class BundleImportResult:
    """What an import read, including what it could not.

    ``truncated`` distinguishes a clean end-of-file from a stream that stopped
    mid-record, which is the difference between a complete capture and a crash
    artifact — and the operator needs to know which one they are looking at.
    """

    events: int = 0
    snapshots: int = 0
    errors: int = 0
    activations: int = 0
    truncated: bool = False
    detail: str = ""


def import_bundle(
    store: ConsoleStore,
    *,
    traces: str | Path | None = None,
    snapshot: str | Path | None = None,
    errors: str | Path | None = None,
) -> BundleImportResult:
    """Import capture files from local paths into ``store``.

    Every argument is optional: a trace stream alone is a usable import, and so
    is a snapshot alone.

    An unreadable path raises the underlying :class:`OSError`, which already
    names the file — the caller (a CLI flag, an upload handler) is the layer
    that knows how to phrase that for its user.
    """
    return import_bytes(
        store,
        traces=_read(traces),
        snapshot=_read(snapshot),
        errors=_read(errors),
    )


def import_bytes(
    store: ConsoleStore,
    *,
    traces: bytes | None = None,
    snapshot: bytes | None = None,
    errors: bytes | None = None,
) -> BundleImportResult:
    """Import capture payloads already in memory, for the upload endpoint."""
    if traces is None and snapshot is None and errors is None:
        return BundleImportResult(detail="nothing to import: no capture files were supplied")

    events: tuple[TraceEvent, ...] = ()
    truncated = False
    if traces is not None:
        events, truncated = _decode_events(traces)

    snapshots: tuple[StateSnapshot, ...] = ()
    if snapshot is not None:
        snapshots = _ingest.decode_snapshot_payload(snapshot)

    records: tuple[ActivationErrorRecord, ...] = ()
    if errors is not None:
        records = _ingest.decode_error_payload(errors)

    batch = _ingest.normalize(
        events=events,
        errors=records,
        snapshots=snapshots,
        provenance=PROVENANCE_BUNDLE,
    )
    # An empty batch is not written: a capture truncated before its first
    # complete record has nothing to store, and the report is the whole answer.
    if batch:
        store.write(batch)

    activations = len(_activations(batch))
    return BundleImportResult(
        events=len(batch.events),
        snapshots=len(batch.snapshots),
        errors=len(batch.errors),
        activations=activations,
        truncated=truncated,
        detail=_detail(batch, activations=activations, truncated=truncated),
    )


def _read(path: str | Path | None) -> bytes | None:
    """Read a capture file, or return ``None`` for an argument not supplied."""
    return None if path is None else Path(path).read_bytes()


def _decode_events(payload: bytes) -> tuple[tuple[TraceEvent, ...], bool]:
    """Decode a framed stream, recovering the prefix when it ends mid-record."""
    try:
        return _ingest.decode_trace_stream(payload), False
    except TruncatedStreamError as exc:
        return _complete_prefix(payload, records=exc.records_read), True


def _complete_prefix(payload: bytes, *, records: int) -> tuple[TraceEvent, ...]:
    """The records a truncated stream held before the break.

    The decoder reports *how many* records it read, not which — it raises rather
    than returning a partial result, because every other caller wants the
    all-or-nothing contract. Recovering them here without reimplementing framing
    means finding the byte offset the last complete frame ends at, and the
    decoder is the only thing that knows where that is.

    So: binary search on the prefix length. The number of complete records in
    ``payload[:end]`` is non-decreasing in ``end`` — a longer prefix can only
    add frames — which makes the smallest ``end`` whose count reaches
    ``records`` exactly the boundary the last complete frame ends at, and a
    prefix ending on a frame boundary decodes cleanly. Logarithmically many
    decodes, rather than a byte-at-a-time walk back from the break.
    """
    found: tuple[TraceEvent, ...] = ()
    low, high = 0, len(payload)
    while low <= high:
        middle = (low + high) // 2
        decoded, count = _count_at(payload, middle)
        if count < records:
            low = middle + 1
            continue
        if decoded is not None:
            found = decoded
        high = middle - 1
    return found


def _count_at(payload: bytes, end: int) -> tuple[tuple[TraceEvent, ...] | None, int]:
    """Decode ``payload[:end]``: the records if it is whole, and how many either way."""
    try:
        decoded = _ingest.decode_trace_stream(payload[:end])
    except TruncatedStreamError as exc:
        return None, exc.records_read
    return decoded, len(decoded)


def _activations(batch: RecordBatch) -> set[tuple[str, int]]:
    """The distinct ``(entity_key, seq)`` scopes the batch's records describe.

    An activation scope is the console's primary object (design D1 of
    ``add-trace-events``: one trace per scope, spanning any suspend/resume
    cycle), so "what did this import touch" is counted in scopes rather than in
    records. An error carrying no ``seq`` — several dead-letter reasons fire
    from timer callbacks that never ran an activation — names no scope and is
    counted only in ``errors``.

    Snapshots are deliberately not counted. A ``StateSnapshot``'s ``seq`` is the
    key's SEQ *counter* at export — the seq the next activation would run at,
    not one that ran — so a capture of one activation plus its post-hoc snapshot
    would otherwise report two.
    """
    scopes = {(row.entity_key, row.seq) for row in batch.events}
    scopes |= {(row.entity_key, row.seq) for row in batch.errors if row.seq is not None}
    return scopes


def _detail(batch: RecordBatch, *, activations: int, truncated: bool) -> str:
    """One line an operator can read: what landed, and what was missing."""
    summary = (
        f"imported {_count(len(batch.events), 'trace event')}, "
        f"{_count(len(batch.snapshots), 'snapshot')}, and "
        f"{_count(len(batch.errors), 'error record')} across "
        f"{_count(activations, 'activation')}"
    )
    if not truncated:
        return summary
    return (
        f"{summary}; the trace stream was truncated mid-record after "
        f"{_count(len(batch.events), 'complete record')}, and the remainder was not captured"
    )


def _count(number: int, noun: str) -> str:
    """``number`` with ``noun``, pluralized. The detail line is read by a human."""
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"
