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
BigQuery cannot day-partition on an INT64 epoch-millis column.

Pulls are incremental by ``event_time`` — the table's partition column, so a
windowed read prunes partitions instead of scanning. Re-reading an overlapping
window is harmless because ingest is idempotent, which means the watermark can
be conservative rather than exactly-once.

``google-cloud-bigquery`` is imported inside the constructor, so importing this
module works with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from beam_agents.console._store import ConsoleStore

__all__ = ["EXTRA_NAME", "BigQueryTraceSource"]

EXTRA_NAME = "console-ingest"


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
        raise NotImplementedError

    @property
    def watermark_ms(self) -> int | None:
        """The event time the next pull resumes from, or ``None`` before the first."""
        raise NotImplementedError

    @property
    def records_stored(self) -> int:
        """Trace events successfully handed to the store."""
        raise NotImplementedError

    def pull_once(self, *, now_ms: int) -> int:
        """Read one window forward from the watermark; return records stored."""
        raise NotImplementedError

    async def run(self) -> None:
        """Poll until cancelled, advancing the watermark after each pull."""
        raise NotImplementedError

    async def stop(self) -> None:
        """Stop polling and release the client. Idempotent."""
        raise NotImplementedError
