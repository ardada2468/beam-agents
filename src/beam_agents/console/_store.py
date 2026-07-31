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

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path
    from types import TracebackType

    from beam_agents.console._records import RecordBatch

__all__ = ["ConsoleStore"]


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
        raise NotImplementedError

    def __enter__(self) -> Self:
        """Return the open store."""
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the store."""
        raise NotImplementedError

    def close(self) -> None:
        """Close every connection the store holds. Idempotent."""
        raise NotImplementedError

    @property
    def path(self) -> Path:
        """The filesystem path this store was opened on."""
        raise NotImplementedError

    @property
    def retention_hours(self) -> float | None:
        """The configured retention window, or ``None`` when unbounded."""
        raise NotImplementedError

    def write(self, batch: RecordBatch) -> int:
        """Write ``batch`` idempotently; return the number of rows it changed.

        A return of ``0`` means every record in the batch was already present
        and identical — the expected outcome when a bundle is retried or a
        replayed run is re-ingested.
        """
        raise NotImplementedError

    def counts(self) -> dict[str, int]:
        """Return row counts per table, for the API's store-status report."""
        raise NotImplementedError

    def prune(self, *, now_ms: int) -> int:
        """Delete records older than the retention window; return rows removed.

        ``now_ms`` is passed in rather than read from a clock so pruning is
        testable and so a caller replaying history can prune against the time it
        is replaying, not the time it is running.
        """
        raise NotImplementedError

    def reader(self) -> Iterator[sqlite3.Connection]:
        """Yield a read-only connection, as a context manager.

        The query layer's single entry point to the database. Separate from the
        writer connection so a long read cannot delay ingest.
        """
        raise NotImplementedError
