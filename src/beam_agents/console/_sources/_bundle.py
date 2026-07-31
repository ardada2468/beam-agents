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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

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
    """
    raise NotImplementedError


def import_bytes(
    store: ConsoleStore,
    *,
    traces: bytes | None = None,
    snapshot: bytes | None = None,
    errors: bytes | None = None,
) -> BundleImportResult:
    """Import capture payloads already in memory, for the upload endpoint."""
    raise NotImplementedError
