"""Read a trace table a pipeline is already writing.

The counterpart to the Kafka source for deployments that export with
``traces_to="bigquery://…"``, and the documented answer for volume the console's
SQLite store cannot hold: keep BigQuery as the system of record and let the
console read a window of it.

The work is reversing ``observability/exporters.trace_event_to_row`` — hex
identifiers, the enum *name* rather than the number for ``event_type``, and
attributes as a list of key/value records sorted by key. ``event_time`` is
ignored on the way back: it is a pure derivation of ``start_ms``, so reading it
would be reading the same number twice, and it exists in the table only because
BigQuery cannot day-partition on an INT64 epoch-millis column. The projection
below therefore does not even select it, while the ``WHERE`` clause filters on
nothing else.

Pulls are incremental by ``event_time`` — the table's partition column, so a
windowed read prunes partitions instead of scanning. Re-reading an overlapping
window is harmless because ingest is idempotent, which means the watermark can
be conservative rather than exactly-once: each pull resumes
:data:`_WINDOW_OVERLAP_MS` *behind* where the last one ended, so a row that
became queryable slightly after its event time is picked up by the next pull
rather than missed forever. Overshooting backwards costs a repeated
partition-local read; undershooting costs a record.

Decoding lives here rather than in ``_ingest`` because these rows never exist as
bytes on a wire — they are the client library's own return values, produced by
the projection a few lines below, and the two only make sense read together.
Design D7's invariant is untouched: this module hands *protos* to
``_ingest.normalize``, which remains the only thing in the package that builds a
store row.

``google-cloud-bigquery`` is imported inside the constructor, so importing this
module works with no extras installed. The ``client`` argument short-circuits
that import entirely, which is what lets the reader be driven offline.

Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from beam_agents._protos import TraceEvent
from beam_agents.console import _ingest
from beam_agents.console._records import PROVENANCE_BIGQUERY
from beam_agents.core.transform import DefaultSinkResolver

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from beam_agents.console._store import ConsoleStore

__all__ = ["EXTRA_NAME", "BigQueryTraceSource"]

# Named in the error a missing client raises, so the fix is in the message.
EXTRA_NAME = "console-ingest"

_LOG = logging.getLogger(__name__)

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

_MS_PER_HOUR = 3_600_000

# Every column `trace_event_to_row` writes except `event_time`. Reading it back
# would be reading `start_ms` twice; it earns its place in the table as the
# partition key and nowhere else.
_COLUMNS = (
    "trace_id",
    "span_id",
    "parent_span_id",
    "entity_key",
    "seq",
    "step_index",
    "event_type",
    "start_ms",
    "end_ms",
    "attributes",
)

# How far behind the last window's end the next one starts. A row is queryable
# some time after the event it describes happened, so a watermark that resumed
# exactly where it stopped would drop whatever landed in between. Ingest is
# idempotent (design D5, keyed on `(trace_id, span_id, event_type)`), so the
# repeated minute is a repeated read and nothing else.
_WINDOW_OVERLAP_MS = 60_000

# BigQuery cannot parameterize an identifier, so the table path is interpolated
# into the SQL. These refuse anything that could end the backtick quoting before
# a query is ever built. Projects allow the domain-scoped `domain.com:id` form;
# datasets and tables do not.
_PROJECT_PATTERN = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.:-]*\Z")
_NAME_PATTERN = re.compile(r"\A[A-Za-z0-9_-]+\Z")

# The field a `bigquery://` trace URI is copied from, used verbatim in grammar
# errors so the message points at the config key the user would edit.
_URI_FIELD = "traces_to"


def _missing_client_error() -> ImportError:
    """The actionable constructor-time error for an absent BigQuery client."""
    return ImportError(
        "BigQueryTraceSource requires the 'google-cloud-bigquery' client library, "
        f"which is not installed; install the {EXTRA_NAME!r} extra "
        f"(pip install 'beam-agents[{EXTRA_NAME}]') or add the client to your environment"
    )


def _parse_uri(uri: str) -> tuple[str, str, str]:
    """Split a ``bigquery://<project>/<dataset>/<table>`` URI into its three parts.

    The grammar is the runtime's, not a second one: ``DefaultSinkResolver``
    validates it, so a URI a pipeline's ``traces_to`` accepts is a URI this
    accepts and vice versa. Only the scheme check is local, because the resolver
    accepts every sink scheme and this reader accepts exactly one.
    """
    parsed = urlparse(uri)
    if parsed.scheme != "bigquery":
        raise ValueError(
            f"BigQueryTraceSource: {uri!r} is not a bigquery:// URI; "
            "expected bigquery://<project>/<dataset>/<table>"
        )
    # Raises `UnknownSinkSchemeError` (a `ValueError`) naming the grammar for a
    # missing project or the wrong number of path segments, so the two segments
    # below are guaranteed to be there.
    DefaultSinkResolver().validate(_URI_FIELD, uri)
    project = parsed.netloc
    dataset, table = [segment for segment in parsed.path.split("/") if segment]
    for pattern, name, value in (
        (_PROJECT_PATTERN, "project", project),
        (_NAME_PATTERN, "dataset", dataset),
        (_NAME_PATTERN, "table", table),
    ):
        if not pattern.match(value):
            raise ValueError(
                f"BigQueryTraceSource: {value!r} is not a valid BigQuery {name} identifier "
                f"in {uri!r}"
            )
    return project, dataset, table


def _require_positive(name: str, value: float) -> None:
    if value <= 0:
        raise ValueError(f"BigQueryTraceSource: {name} must be positive, got {value!r}")


def _timestamp_literal(ms: int) -> str:
    """Render epoch millis as the RFC 3339 form ``trace_event_to_row`` writes."""
    return (_EPOCH + datetime.timedelta(milliseconds=ms)).isoformat()


def _row_to_event(row: Mapping[str, Any]) -> TraceEvent:
    """Reverse one row of ``trace_event_to_row`` back into the event it encoded.

    Every column is NULLABLE, so a missing value decodes to the proto's own
    default rather than raising — the encoder never writes one, but a table
    written by an older schema or hand-edited should not take the reader down.
    An unknown ``event_type`` name *does* raise: an event whose type cannot be
    named is not a trace event, and the caller counts it as a decode failure.
    """
    event = TraceEvent(
        trace_id=bytes.fromhex(row["trace_id"] or ""),
        span_id=bytes.fromhex(row["span_id"] or ""),
        parent_span_id=bytes.fromhex(row["parent_span_id"] or ""),
        entity_key=bytes.fromhex(row["entity_key"] or ""),
        seq=int(row["seq"] or 0),
        step_index=int(row["step_index"] or 0),
        # The row carries the enum *name*, which is what protobuf's own
        # constructor takes; it raises `ValueError` for a name it does not know,
        # and the caller counts that row as a decode failure.
        event_type=row["event_type"],
        start_ms=int(row["start_ms"] or 0),
        end_ms=int(row["end_ms"] or 0),
    )
    # The encoder sorts by key; a proto map is unordered, so the sort is
    # information the round trip neither needs nor can carry.
    for attribute in row["attributes"] or ():
        event.attributes[attribute["key"]] = attribute["value"] or ""
    return event


class BigQueryTraceSource:
    """An incremental reader over a BigQuery trace table.

    ``uri`` uses the same ``bigquery://<project>/<dataset>/<table>`` grammar the
    runtime's sink resolver parses, so the value can be copied verbatim from the
    pipeline's ``traces_to``.
    """

    def __init__(
        self,
        uri: str,
        store: ConsoleStore,
        *,
        lookback_hours: float = 24.0,
        poll_interval_s: float = 30.0,
        client: Any = None,
        **options: Any,
    ) -> None:
        """Configure the reader; raise naming the extra if the client is absent.

        ``client`` is an injection seam for tests: a fake standing in for a
        BigQuery client is what lets this be driven offline.
        """
        self._project, self._dataset, self._table = _parse_uri(uri)
        _require_positive("lookback_hours", lookback_hours)
        _require_positive("poll_interval_s", poll_interval_s)
        self._uri = uri
        self._store = store
        self._lookback_ms = int(lookback_hours * _MS_PER_HOUR)
        self._poll_interval_s = poll_interval_s
        self._client: Any = client if client is not None else self._build_client(options)
        self._watermark: int | None = None
        self._records_stored = 0
        self._decode_failures = 0
        self._stopped = asyncio.Event()
        self._released = False

    def _build_client(self, options: dict[str, Any]) -> Any:
        try:
            # PLC0415 is the point: the client is the optional `console-ingest`
            # extra, and importing this module must work without it.
            from google.cloud import bigquery  # noqa: PLC0415
        except ImportError as exc:
            raise _missing_client_error() from exc
        return bigquery.Client(project=self._project, **options)

    @property
    def watermark_ms(self) -> int | None:
        """The event time the next pull resumes from, or ``None`` before the first."""
        return self._watermark

    @property
    def records_stored(self) -> int:
        """Trace events successfully handed to the store."""
        return self._records_stored

    @property
    def decode_failures(self) -> int:
        """Rows that were not valid trace events, counted and skipped."""
        return self._decode_failures

    def pull_once(self, *, now_ms: int) -> int:
        """Read one window forward from the watermark; return records stored."""
        start_ms, end_ms = self._window(now_ms)
        if start_ms >= end_ms:
            # `now_ms` has not moved past the watermark. An empty window is not a
            # backwards one: leave the watermark where it is and read nothing.
            return 0
        events = self._decode(self._read(start_ms, end_ms))
        if events:
            self._store.write(_ingest.normalize(events=events, provenance=PROVENANCE_BIGQUERY))
            self._records_stored += len(events)
        self._watermark = end_ms
        return len(events)

    async def run(self) -> None:
        """Poll until cancelled, advancing the watermark after each pull."""
        while not self._stopped.is_set():
            try:
                # The BigQuery client is synchronous; keeping it off the event
                # loop is what lets the console serve requests while it reads.
                await asyncio.to_thread(self.pull_once, now_ms=_now_ms())
            except Exception:
                # A viewer that stops viewing because one query failed is worse
                # than a viewer that is briefly behind: log and poll again.
                _LOG.warning("BigQuery trace pull failed for %s", self._uri, exc_info=True)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._poll_interval_s)

    async def stop(self) -> None:
        """Stop polling and release the client. Idempotent."""
        self._stopped.set()
        if self._released:
            return
        self._released = True
        close = getattr(self._client, "close", None)
        if callable(close):
            await asyncio.to_thread(close)

    def _window(self, now_ms: int) -> tuple[int, int]:
        if self._watermark is None:
            start = now_ms - self._lookback_ms
        else:
            start = self._watermark - _WINDOW_OVERLAP_MS
        return max(start, 0), now_ms

    def _read(self, start_ms: int, end_ms: int) -> Iterable[Any]:
        sql = (
            f"SELECT {', '.join(_COLUMNS)}\n"
            f"FROM `{self._project}.{self._dataset}.{self._table}`\n"
            f"WHERE event_time >= TIMESTAMP '{_timestamp_literal(start_ms)}'\n"
            f"  AND event_time < TIMESTAMP '{_timestamp_literal(end_ms)}'\n"
            "ORDER BY start_ms, seq"
        )
        result: Iterable[Any] = self._client.query(sql).result()
        return result

    def _decode(self, rows: Iterable[Any]) -> tuple[TraceEvent, ...]:
        events: list[TraceEvent] = []
        for row in rows:
            try:
                events.append(_row_to_event(dict(row)))
            except (KeyError, TypeError, ValueError):
                # Third rule of `_sources/`: one malformed record is not a reason
                # for a viewer to stop viewing.
                self._decode_failures += 1
                _LOG.warning("undecodable BigQuery trace row in %s", self._uri, exc_info=True)
        return tuple(events)


def _now_ms() -> int:
    return int((datetime.datetime.now(tz=datetime.UTC) - _EPOCH).total_seconds() * 1000)
