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
  ``ACTIVATION_END`` and OTLP cannot represent two events on one span.

``opentelemetry-proto`` is imported inside :func:`decode_otlp_request` only, so
this module imports with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from beam_agents._protos import ActivationErrorRecord, StateSnapshot, TraceEvent
    from beam_agents.console._records import RecordBatch

__all__ = [
    "TruncatedStreamError",
    "decode_bigquery_rows",
    "decode_error_payload",
    "decode_otlp_request",
    "decode_snapshot_payload",
    "decode_trace_stream",
    "normalize",
]


class TruncatedStreamError(ValueError):
    """A framed stream ended mid-record.

    Carries the number of complete records read before the truncation, because
    a partially-written capture is still worth importing: the import reports
    what it read rather than discarding it.
    """

    def __init__(self, message: str, *, records_read: int) -> None:
        """Record how many complete records were decoded before the break."""
        super().__init__(message)
        self.records_read = records_read


def decode_trace_stream(payload: bytes) -> tuple[TraceEvent, ...]:
    """Decode a varint-length-delimited ``TraceEvent`` stream.

    The on-disk trace format the replay CLI consumes. Raises
    :class:`TruncatedStreamError` naming how many records were read if the
    payload ends mid-record.
    """
    raise NotImplementedError


def decode_otlp_request(payload: bytes) -> tuple[TraceEvent, ...]:
    """Reverse an OTLP ``ExportTraceServiceRequest`` back into trace events.

    Lossy by construction, not by implementation: the OTLP encoding carries no
    ``ACTIVATION_START``, so an activation assembled from this decoder alone is
    missing the event that distinguishes a start from a resume. Records decoded
    here are marked with OTLP provenance so the UI can say so.
    """
    raise NotImplementedError


def decode_error_payload(payload: bytes) -> tuple[ActivationErrorRecord, ...]:
    """Decode an error payload, accepting both the bare and enveloped forms.

    ``DefaultSinkResolver``'s bus encoding wraps an ``ActivationErrorRecord`` in
    an ``AgentEnvelope`` so that an errors topic is itself a valid ``RunAgent``
    input; the BigQuery encoding does not. Both reach the console.
    """
    raise NotImplementedError


def decode_snapshot_payload(payload: bytes) -> tuple[StateSnapshot, ...]:
    """Decode one or more serialized ``StateSnapshot`` messages."""
    raise NotImplementedError


def decode_bigquery_rows(rows: Iterable[dict[str, Any]]) -> tuple[TraceEvent, ...]:
    """Reverse rows produced by the runtime's BigQuery trace encoder.

    The inverse of ``observability/exporters.trace_event_to_row``. ``event_time``
    is ignored on the way back: it is a pure derivation of ``start_ms``, so
    reading it would be reading the same number twice.
    """
    raise NotImplementedError


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
    raise NotImplementedError
