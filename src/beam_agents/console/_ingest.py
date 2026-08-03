"""The single path from bytes to store rows (design D7).

Five sources deliver the same three protos in five encodings. Each decoder here
reverses exactly one of them and returns protos; :func:`normalize` is the only
function that turns protos into rows. Nothing in this module touches the store,
so a decoder cannot drift from the store's understanding of a record.

Each reversal has a surprising part, and they are all here rather than scattered
across the sources:

- **Framed streams** reuse ``replay.bundle.parse_trace_stream`` rather than
  reimplementing varint framing — the console reads the same files the replay
  CLI writes, so it must read them the same way.
- **BigQuery rows** must reverse ``observability/exporters.trace_event_to_row``:
  hex identifiers, the enum *name* for ``event_type``, attributes as a sorted
  list of key/value records, and an ``event_time`` derived from ``start_ms``.
- **OTLP requests** must reverse the span-name-and-attribute mapping in
  ``observability/otlp.py``, and must accept that ``ACTIVATION_START`` will
  never arrive: the exporter drops it because it shares a span ID with
  ``ACTIVATION_END`` and OTLP cannot represent two events on one span. The OTLP
  span carries no ``entity_key``, ``seq``, or ``step_index`` either — the
  mapping never put them on a span — so those come back empty, which is part of
  what ``PROVENANCE_OTLP`` exists to record.

Two decoding decisions are worth stating where they are made:

**A native payload is one record or a framed batch of them.** The batch framing
is the varint-length-delimited framing ``replay.bundle.frame_trace_events``
writes — one framing for the whole project, not a second one for the console.
``parse_trace_stream`` is typed to ``TraceEvent`` and so cannot serve the error
and snapshot paths, which is the only reason :func:`_split_frames` exists; a
test pins :func:`_frame_records` byte-identical to ``frame_trace_events`` so the
two cannot drift apart. The trace path still decodes through
``parse_trace_stream`` itself, and uses the splitter only to find where a
truncated stream stopped.

**Bare and enveloped errors overlap on the wire**, so the two are told apart
structurally rather than by trying one and hoping. An ``ActivationErrorRecord``'s
``detail`` occupies the same field number and wire type as an ``AgentEnvelope``'s
``external_event``, and protobuf treats a field number carrying the wrong wire
type as an unknown field rather than as an error, so each form parses as the
other. The envelope is accepted only when its payload really is an error record
naming a reason, and the closed reason vocabulary is what makes that check
meaningful.

``opentelemetry-proto`` is imported inside :func:`decode_otlp_request` only, so
this module imports with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, TypeVar, cast

from google.protobuf.message import DecodeError

from beam_agents._protos import ActivationErrorRecord, AgentEnvelope, StateSnapshot, TraceEvent
from beam_agents.console._records import PROVENANCE, ErrorRow, EventRow, RecordBatch, SnapshotRow
from beam_agents.core.dofn import (
    REASON_HITL_TIMEOUT,
    REASON_INTENT_DEAD_LETTER,
    REASON_TTL_WIPED_SUSPENSION,
)
from beam_agents.replay.bundle import ReplayUsageError, parse_trace_stream

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from google.protobuf.message import Message

__all__ = [
    "TruncatedStreamError",
    "decode_bigquery_rows",
    "decode_error_payload",
    "decode_otlp_request",
    "decode_snapshot_payload",
    "decode_trace_stream",
    "normalize",
]

_MessageT = TypeVar("_MessageT", bound="Message")

_VARINT_CONTINUATION_BIT = 0x80
_VARINT_PAYLOAD_MASK = 0x7F
_VARINT_SHIFT = 7

_NANOS_PER_MS = 1_000_000

# The two shapes `core/dofn.py` writes an activation's `seq` into a dead
# letter's free-form `detail`: `seq=<n>` for the timer routes that know the
# suspended activation, and JSON for the intent dead letter, whose detail is
# built by `core/error_records.intent_dead_letter_to_error`. Keyed by reason
# rather than matched everywhere, because an `activation_error`'s detail leads
# with `repr(exc)` and an exception message is free to contain the text `seq=`.
_SEQ_IN_DETAIL = re.compile(r"(?:\A|[\s,;])seq=(-?\d+)")
_REASONS_WITH_SCALAR_SEQ = frozenset({REASON_TTL_WIPED_SUSPENSION, REASON_HITL_TIMEOUT})


class TruncatedStreamError(ValueError):
    """A framed stream ended mid-record.

    Carries the number of complete records read before the truncation, because
    a partially-written capture is still worth importing: the import reports
    what it read rather than discarding it.
    """

    def __init__(
        self, message: str, *, records_read: int, records: tuple[Message, ...] = ()
    ) -> None:
        """Record how many complete records were decoded before the break."""
        super().__init__(message)
        self.records_read = records_read
        # The records themselves, so an importer can store what it read in the
        # same pass that discovers the truncation — otherwise the only way to
        # honour "the records that were read remain stored" would be to decode
        # the payload twice. `records_read` stays the reported count.
        self.records = records


def decode_trace_stream(payload: bytes) -> tuple[TraceEvent, ...]:
    """Decode a varint-length-delimited ``TraceEvent`` stream.

    The on-disk trace format the replay CLI consumes. Raises
    :class:`TruncatedStreamError` naming how many records were read if the
    payload ends mid-record.
    """
    consumed = _frames_end(_frame_bounds(payload))
    try:
        events = tuple(parse_trace_stream(payload[:consumed]))
    except ReplayUsageError as exc:
        # A complete frame whose body is not a TraceEvent is corruption, not a
        # short write. Re-raised as a ValueError so every decoder here fails the
        # same way and one `except ValueError` at the endpoint covers all of
        # them.
        raise ValueError(f"malformed TraceEvent stream: {exc}") from exc
    if consumed != len(payload):
        raise TruncatedStreamError(
            f"truncated trace stream: {len(events)} complete records read, "
            f"{len(payload) - consumed} trailing bytes are an incomplete record",
            records_read=len(events),
            records=events,
        )
    return events


def decode_otlp_request(payload: bytes) -> tuple[TraceEvent, ...]:
    """Reverse an OTLP ``ExportTraceServiceRequest`` back into trace events.

    Lossy by construction, not by implementation: the OTLP encoding carries no
    ``ACTIVATION_START``, so an activation assembled from this decoder alone is
    missing the event that distinguishes a start from a resume. Records decoded
    here are marked with OTLP provenance so the UI can say so.
    """
    # Imported here, not at module scope: `opentelemetry-proto` is an optional
    # extra and `import beam_agents.console` must work without it. Same
    # rationale — and the same actionable ImportError — as `_require_otlp` in
    # `observability/otlp.py`.
    try:
        from opentelemetry.proto.collector.trace.v1 import trace_service_pb2  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - the extra is a test dependency
        raise ImportError(
            "decoding an OTLP export request requires opentelemetry-proto; "
            "install the extra: pip install 'beam-agents[console]'"
        ) from exc

    request = trace_service_pb2.ExportTraceServiceRequest()
    try:
        request.ParseFromString(payload)
    except DecodeError as exc:
        raise ValueError(f"not an OTLP ExportTraceServiceRequest: {exc}") from exc

    return tuple(
        _span_to_event(span)
        for resource_spans in request.resource_spans
        for scope_spans in resource_spans.scope_spans
        for span in scope_spans.spans
    )


def decode_error_payload(payload: bytes) -> tuple[ActivationErrorRecord, ...]:
    """Decode an error payload, accepting both the bare and enveloped forms.

    ``DefaultSinkResolver``'s bus encoding wraps an ``ActivationErrorRecord`` in
    an ``AgentEnvelope`` so that an errors topic is itself a valid ``RunAgent``
    input; the BigQuery encoding does not. Both reach the console.
    """
    return _decode_batch(payload, _decode_one_error, "error record")


def decode_snapshot_payload(payload: bytes) -> tuple[StateSnapshot, ...]:
    """Decode one or more serialized ``StateSnapshot`` messages."""
    return _decode_batch(payload, _decode_one_snapshot, "StateSnapshot")


def decode_bigquery_rows(rows: Iterable[dict[str, Any]]) -> tuple[TraceEvent, ...]:
    """Reverse rows produced by the runtime's BigQuery trace encoder.

    The inverse of ``observability/exporters.trace_event_to_row``. ``event_time``
    is ignored on the way back: it is a pure derivation of ``start_ms``, so
    reading it would be reading the same number twice.
    """
    return tuple(_row_to_event(row, index) for index, row in enumerate(rows))


def normalize(
    *,
    events: Sequence[TraceEvent] = (),
    errors: Sequence[ActivationErrorRecord] = (),
    snapshots: Sequence[StateSnapshot] = (),
    provenance: str,
) -> RecordBatch:
    """Turn decoded protos into the rows the store writes.

    The only proto-to-row conversion in the package. ``provenance`` is stamped
    on every row so a record's delivery path — and therefore how complete it can
    possibly be — stays attached to it.
    """
    if provenance not in PROVENANCE:
        raise ValueError(
            f"unknown provenance {provenance!r}; expected one of {', '.join(PROVENANCE)}"
        )
    return RecordBatch(
        events=tuple(_event_row(event, provenance) for event in events),
        errors=tuple(_error_row(error, provenance) for error in errors),
        snapshots=tuple(_snapshot_row(snapshot, provenance) for snapshot in snapshots),
    )


# --- proto -> row -------------------------------------------------------------


def _event_row(event: TraceEvent, provenance: str) -> EventRow:
    return EventRow(
        trace_id=event.trace_id.hex(),
        span_id=event.span_id.hex(),
        parent_span_id=event.parent_span_id.hex(),
        entity_key=event.entity_key.hex(),
        seq=event.seq,
        step_index=event.step_index,
        event_type=_event_type_name(event.event_type),
        start_ms=event.start_ms,
        end_ms=event.end_ms,
        # Sorted by key, matching every encoder the runtime ships: two copies of
        # one event that arrived by different routes must compare equal, and a
        # proto map has no inherent order.
        attributes={key: event.attributes[key] for key in sorted(event.attributes)},
        provenance=provenance,
    )


def _error_row(error: ActivationErrorRecord, provenance: str) -> ErrorRow:
    return ErrorRow(
        entity_key=error.entity_key.hex(),
        reason=error.reason,
        detail=error.detail,
        event_time_ms=error.event_time_ms,
        seq=_seq_from_detail(error.reason, error.detail),
        provenance=provenance,
    )


def _snapshot_row(snapshot: StateSnapshot, provenance: str) -> SnapshotRow:
    suspended = snapshot.HasField("continuation")
    continuation = snapshot.continuation
    # The key's pending intents are the durable record of what it is waiting on;
    # a snapshot taken while the key is suspended lists the same ids on its
    # continuation, which is the fallback for a capture that carried the
    # suspension without the intents themselves.
    pending_intent_ids = tuple(intent.intent_id for intent in snapshot.pending) or (
        tuple(continuation.pending_intent_ids) if suspended else ()
    )
    return SnapshotRow(
        entity_key=snapshot.entity_key.hex(),
        seq=snapshot.seq,
        snapshot_at_ms=snapshot.snapshot_at_ms,
        state_schema_version=snapshot.state_schema_version,
        request_id=snapshot.request_id,
        memory_entries=len(snapshot.memory.entries),
        memory_bytes=snapshot.memory.total_value_bytes,
        llm_cache_entries=len(snapshot.llm_cache.entries),
        pending_intent_ids=pending_intent_ids,
        # Absent, not zero: presence of the continuation is what distinguishes
        # "not suspended" from "suspended at step 0".
        continuation_step_index=continuation.step_index if suspended else None,
        continuation_deadline_ms=continuation.deadline_ms if suspended else None,
        continuation_adapter=continuation.adapter if suspended else "",
        # Re-serialized rather than carried through as the received bytes: every
        # producer in the runtime serializes deterministically, so this is the
        # same image `beam-agents-replay --snapshot` reads, and taking only the
        # proto keeps this the one proto-to-row conversion.
        raw=snapshot.SerializeToString(deterministic=True),
        provenance=provenance,
    )


def _seq_from_detail(reason: str, detail: str) -> int | None:
    """The activation ``seq`` a dead letter's ``detail`` carries, when it does.

    ``seq`` is not a field on ``ActivationErrorRecord`` because several reasons
    fire from timer callbacks that have no activation. The reasons that do know
    their activation write it into ``detail``, and only those are read here.
    """
    if reason == REASON_INTENT_DEAD_LETTER:
        try:
            decoded = json.loads(detail)
        except ValueError:
            return None
        value = decoded.get("seq") if isinstance(decoded, dict) else None
        return value if isinstance(value, int) and not isinstance(value, bool) else None
    if reason in _REASONS_WITH_SCALAR_SEQ:
        match = _SEQ_IN_DETAIL.search(detail)
        return int(match.group(1)) if match is not None else None
    return None


# --- encoding reversals -------------------------------------------------------


def _span_to_event(span: Any) -> TraceEvent:
    """Reverse ``observability/otlp._event_to_span`` for one span."""
    return TraceEvent(
        trace_id=span.trace_id,
        span_id=span.span_id,
        parent_span_id=span.parent_span_id,
        event_type=_event_type_for_span_name(span.name),
        # The exporter multiplies by 10^6, and nothing in the runtime measures
        # below a millisecond, so the division is exact for anything it wrote.
        start_ms=span.start_time_unix_nano // _NANOS_PER_MS,
        end_ms=span.end_time_unix_nano // _NANOS_PER_MS,
        attributes={
            attribute.key: _any_value_to_str(attribute.value) for attribute in span.attributes
        },
    )


def _row_to_event(row: dict[str, Any], index: int) -> TraceEvent:
    """Reverse ``observability/exporters.trace_event_to_row`` for one row."""
    name = str(row.get("event_type") or "EVENT_TYPE_UNSPECIFIED")
    try:
        event_type = cast("TraceEvent.EventType", TraceEvent.EventType.Value(name))
    except ValueError as exc:
        # Version skew, not a corrupt row: the table holds an event type this
        # package has no number for. Loud, because decoding it to
        # EVENT_TYPE_UNSPECIFIED would put a plausible-looking record in the
        # store that is not the one the pipeline wrote.
        raise ValueError(
            f"row {index} carries event_type {name!r}, which is not a "
            "TraceEvent.EventType this package knows"
        ) from exc
    return TraceEvent(
        trace_id=_from_hex(row.get("trace_id")),
        span_id=_from_hex(row.get("span_id")),
        parent_span_id=_from_hex(row.get("parent_span_id")),
        entity_key=_from_hex(row.get("entity_key")),
        # Every column in TRACE_TABLE_SCHEMA is NULLABLE, so a row read back can
        # carry SQL NULL where the encoder wrote a zero-valued default.
        seq=int(row.get("seq") or 0),
        step_index=int(row.get("step_index") or 0),
        event_type=event_type,
        start_ms=int(row.get("start_ms") or 0),
        end_ms=int(row.get("end_ms") or 0),
        attributes={
            str(attribute["key"]): str(attribute.get("value") or "")
            for attribute in (row.get("attributes") or ())
        },
    )


def _event_type_name(event_type: int) -> str:
    """The enum name for ``event_type``, or its number when unrecognized.

    proto3 enums are open: a stream written by a newer package can carry an
    event type this one has no name for. Recording the number keeps the record
    rather than collapsing it onto ``EVENT_TYPE_UNSPECIFIED``, which would be a
    claim about the pipeline that is not true.
    """
    try:
        return str(TraceEvent.EventType.Name(event_type))
    except ValueError:
        return f"EVENT_TYPE_{event_type}"


def _event_type_for_span_name(name: str) -> TraceEvent.EventType:
    """Reverse the exporter's span name — the lowercased enum name.

    A span whose name is not in this runtime's vocabulary comes from some other
    OTLP producer pointed at the same endpoint. It decodes as unspecified rather
    than being dropped: this endpoint's loss is documented as "no
    ``ACTIVATION_START``", and silently discarding spans would be a second,
    undocumented one.
    """
    try:
        return cast("TraceEvent.EventType", TraceEvent.EventType.Value(name.upper()))
    except ValueError:
        return TraceEvent.EVENT_TYPE_UNSPECIFIED


def _any_value_to_str(value: Any) -> str:
    """Render an OTLP ``AnyValue`` as the string a ``TraceEvent`` attribute is.

    The exporter only ever writes ``string_value``, so that is the only branch a
    round trip exercises; the others exist so a third-party OTLP producer's
    attributes are kept rather than silently blanked.
    """
    which = value.WhichOneof("value")
    if which == "string_value":
        return str(value.string_value)
    if which == "bool_value":
        return "true" if value.bool_value else "false"
    if which == "int_value":
        return str(value.int_value)
    if which == "double_value":
        return str(value.double_value)
    if which == "bytes_value":
        return bytes(value.bytes_value).hex()
    return ""


def _from_hex(value: Any) -> bytes:
    """Reverse a hex identifier column, tolerating the schema's NULLs."""
    if not value:
        return b""
    try:
        return bytes.fromhex(str(value))
    except ValueError as exc:
        raise ValueError(f"not a hexadecimal identifier: {value!r}") from exc


# --- framing ------------------------------------------------------------------


def _frame_records(messages: Iterable[Message]) -> bytes:
    """Frame messages exactly as ``replay.bundle.frame_trace_events`` does.

    The console's native batch encoding, kept beside its decoder so the ingest
    endpoint and the sink cannot disagree about it. A test pins this
    byte-identical to ``frame_trace_events`` for trace events, which is what
    makes "one framing" a checked claim rather than a comment.
    """
    out = bytearray()
    for message in messages:
        payload = message.SerializeToString(deterministic=True)
        out += _varint(len(payload)) + payload
    return bytes(out)


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & _VARINT_PAYLOAD_MASK
        value >>= _VARINT_SHIFT
        if value:
            out.append(byte | _VARINT_CONTINUATION_BIT)
        else:
            out.append(byte)
            return bytes(out)


def _frame_bounds(payload: bytes) -> tuple[tuple[int, int], ...]:
    """Locate every complete frame body in a framed payload, as ``(start, end)``.

    Bounds rather than the bodies themselves: the trace path only needs to know
    where the complete frames stop, and hands the prefix to
    ``parse_trace_stream`` to do the actual decoding, so materializing slices
    here would copy the whole payload for nothing.

    The last bound's ``end`` is where the complete frames stop, which is what
    tells a clean end of stream from a truncated tail. Never raises: a partial
    frame is a fact about the payload, and every caller wants the complete
    prefix either way.
    """
    bounds: list[tuple[int, int]] = []
    offset = 0
    size = len(payload)
    while offset < size:
        length, body_start = _read_varint(payload, offset)
        if body_start is None:
            break
        end = body_start + length
        if end > size:
            break
        bounds.append((body_start, end))
        offset = end
    return tuple(bounds)


def _frames_end(bounds: tuple[tuple[int, int], ...]) -> int:
    """The offset just past the last complete frame, or ``0`` when there is none."""
    return bounds[-1][1] if bounds else 0


def _read_varint(data: bytes, offset: int) -> tuple[int, int | None]:
    """Read one varint; the offset is ``None`` when the varint is itself cut short."""
    value = 0
    shift = 0
    size = len(data)
    while offset < size:
        byte = data[offset]
        offset += 1
        value |= (byte & _VARINT_PAYLOAD_MASK) << shift
        if not byte & _VARINT_CONTINUATION_BIT:
            return value, offset
        shift += _VARINT_SHIFT
    return 0, None


def _decode_batch(
    payload: bytes,
    decode_one: Callable[[bytes], _MessageT | None],
    what: str,
) -> tuple[_MessageT, ...]:
    """Decode one record, or a framed batch of them.

    Single message first: a lone record is what a Kafka message and a bare POST
    carry, and the per-kind recognizers reject the misreadings that make the
    shapes ambiguous, so a framed batch falls through to the split rather than
    being mistaken for one oversized record.
    """
    if not payload:
        return ()
    single = decode_one(payload)
    if single is not None:
        return (single,)
    bounds = _frame_bounds(payload)
    decoded: list[_MessageT] = []
    for start, end in bounds:
        record = decode_one(payload[start:end])
        if record is None:
            raise ValueError(f"payload is neither one {what} nor a framed batch of them")
        decoded.append(record)
    if not decoded:
        raise ValueError(f"payload is neither one {what} nor a framed batch of them")
    records = tuple(decoded)
    consumed = _frames_end(bounds)
    if consumed != len(payload):
        raise TruncatedStreamError(
            f"truncated {what} stream: {len(records)} complete records read, "
            f"{len(payload) - consumed} trailing bytes are an incomplete record",
            records_read=len(records),
            records=records,
        )
    return records


def _decode_one_error(payload: bytes) -> ActivationErrorRecord | None:
    """One error record, bare or wrapped in the errors sink's bus envelope.

    The envelope is tried first and accepted only when its ``external_event``
    really is an error record: the two messages share the field number and wire
    type that ``detail`` and ``external_event`` occupy, so each parses as the
    other and only the payload can tell them apart.
    """
    envelope = AgentEnvelope()
    try:
        envelope.ParseFromString(payload)
    except DecodeError:
        pass
    else:
        if envelope.WhichOneof("payload") == "external_event":
            wrapped = _parse_error_record(envelope.external_event)
            if wrapped is not None:
                return wrapped
    return _parse_error_record(payload)


def _parse_error_record(payload: bytes) -> ActivationErrorRecord | None:
    """An error record, or ``None`` when these bytes are not one.

    A dead letter always names a reason from the runtime's closed vocabulary
    (``core/dofn.py``, ``core/hitl.py``), so an empty ``reason`` means these
    bytes are some other message that happened to parse — which is exactly how
    the bare and enveloped forms shadow each other, and how a framed batch
    shadows a single record.
    """
    record = ActivationErrorRecord()
    try:
        record.ParseFromString(payload)
    except DecodeError:
        return None
    return record if record.reason else None


def _decode_one_snapshot(payload: bytes) -> StateSnapshot | None:
    """One state snapshot, or ``None`` when these bytes are not one.

    A framed batch of snapshots can parse as a single snapshot — its leading
    length byte reads as a field tag — so acceptance requires that the message
    re-serializes to the bytes it came from. Every producer in the runtime
    serializes deterministically, so a real snapshot round-trips and a misread
    batch does not.
    """
    snapshot = StateSnapshot()
    try:
        snapshot.ParseFromString(payload)
    except DecodeError:
        return None
    return snapshot if snapshot.SerializeToString(deterministic=True) == payload else None
