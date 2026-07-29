"""The e2e gate's ingest spool: durable segment files between Kafka and Beam.

Cross-language Kafka IO does not work on the compose Flink stack (no Java SDK
environment on the TaskManager; see docker/README.md), so the pipeline cannot
read its topics directly. Ingest is split in two instead (design D9):

- A **drainer** (plain aiokafka consumer, runs in the test process) tails the
  events + results + approvals topics for the whole run and appends each record
  to sequence-numbered segment files on a directory bind-mounted into the
  beam-sdk-harness container.
- A **spool source** (Splittable DoFn, runs inside the harness) replays sealed
  segments with its position held in restriction state, tailing for new
  segments until an EOF sentinel appears.

Segments are written to a ``.tmp`` name and sealed by atomic rename; a sealed
segment is immutable forever after. That immutability is the replay argument:
a checkpoint-restored restriction re-reads byte-identical records, which is
what the gate's intent-determinism assertion rests on.

Record format: 4-byte big-endian length prefix + serialized ``AgentEnvelope``.
No index, no locks — one writer, any number of readers, safe across the bind
mount (virtiofs locally, overlay on CI) where SQLite locking would not be.

This module runs on both sides of the container boundary, so it imports only
stdlib + beam + beam_agents protos.
"""

from __future__ import annotations

import logging
import os
import struct
import time
from collections.abc import Iterator
from pathlib import Path
from typing import IO, Any

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.utils import timestamp

from beam_agents._protos import AgentEnvelope

_LOG = logging.getLogger("beam_agents.e2e.spool")

_HEADER = struct.Struct(">I")

# Restriction upper bound: effectively "unbounded" in segment count while still
# giving OffsetRange a finite stop. A run producing this many segments has
# already failed on wall clock.
_MAX_SEGMENTS = 2**31

_EOF_NAME = "EOF"
_SEG_SUFFIX = ".seg"

# How long the source sleeps (via defer_remainder) when it has caught up with
# the drainer and the EOF sentinel has not appeared yet. Deferral uses an
# absolute wall-clock Timestamp, matching Beam's own PeriodicImpulse
# (ImpulseSeqGenDoFn) — the one defer_remainder pattern verified to actually
# resume on the portable Flink runner.
_TAIL_POLL_S = 0.5


def _segment_name(index: int) -> str:
    return f"{index:08d}{_SEG_SUFFIX}"


class SpoolWriter:
    """Single-writer append side of the spool (runs in the test process).

    ``append`` buffers into the current open segment; ``seal`` makes everything
    appended so far visible to readers, atomically. ``close`` seals the tail
    and writes the EOF sentinel recording the total segment count, which is
    what tells the source to stop tailing.
    """

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)
        self._index = 0
        self._tmp: Path | None = None
        self._handle: IO[bytes] | None = None
        self._closed = False

    def append(self, envelope: AgentEnvelope) -> None:
        if self._closed:
            raise RuntimeError("spool writer is closed")
        if self._handle is None:
            self._tmp = self._root / (_segment_name(self._index) + ".tmp")
            self._handle = self._tmp.open("wb")
        payload = envelope.SerializeToString()
        self._handle.write(_HEADER.pack(len(payload)))
        self._handle.write(payload)

    def seal(self) -> None:
        """Seal the open segment, if any: flush, fsync, atomic rename."""
        if self._handle is None:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        self._handle = None
        assert self._tmp is not None
        self._tmp.rename(self._root / _segment_name(self._index))
        self._tmp = None
        self._index += 1

    def close(self) -> None:
        """Seal the tail segment and write the EOF sentinel."""
        if self._closed:
            return
        self.seal()
        eof_tmp = self._root / (_EOF_NAME + ".tmp")
        eof_tmp.write_text(str(self._index))
        eof_tmp.rename(self._root / _EOF_NAME)
        self._closed = True

    @property
    def sealed_count(self) -> int:
        return self._index


def read_segment(path: Path) -> Iterator[bytes]:
    """Yield the raw record payloads of one sealed segment, in order."""
    with path.open("rb") as fh:
        while True:
            header = fh.read(_HEADER.size)
            if not header:
                return
            (length,) = _HEADER.unpack(header)
            payload = fh.read(length)
            if len(payload) != length:
                raise ValueError(
                    f"truncated record in {path}: wanted {length} bytes, got {len(payload)}"
                )
            yield payload


def eof_total(root: Path) -> int | None:
    """The total segment count from the EOF sentinel, or None if still open."""
    eof = root / _EOF_NAME
    try:
        return int(eof.read_text())
    except FileNotFoundError:
        return None


class _SegmentRestrictions(beam.transforms.core.RestrictionProvider):
    def initial_restriction(self, element: str) -> OffsetRange:
        return OffsetRange(0, _MAX_SEGMENTS)

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element: str, restriction: OffsetRange) -> int:
        return restriction.size()


@beam.DoFn.unbounded_per_element()
class SpoolSourceDoFn(beam.DoFn):
    """Replay a spool directory as a stream of ``AgentEnvelope``s.

    The element is the spool directory path *as seen by this process* — the
    container path when running on the Flink stack. One segment index is one
    claimable position; a claimed segment is emitted whole. When the next
    segment is not sealed yet, the source defers and polls; when the EOF
    sentinel says all segments are consumed, it finishes, letting the pipeline
    drain and terminate.
    """

    def process(
        self,
        spool_dir: str,
        tracker: Any = beam.DoFn.RestrictionParam(_SegmentRestrictions()),
        watermark_estimator: Any = beam.DoFn.WatermarkEstimatorParam(
            ManualWatermarkEstimator.default_provider()
        ),
    ) -> Iterator[AgentEnvelope]:
        root = Path(spool_dir)
        index = tracker.current_restriction().start
        # Worker-side breadcrumb (reaches the TaskManager log via FnAPI): what
        # this invocation actually sees. Cheap at one line per SDF invocation
        # on the claim path; the tail-poll path logs only every ~40th poll.
        if index == 0 or index % 20 == 0:
            _LOG.info(
                "spool source at %s: restriction start=%d, sealed=%d, eof=%s",
                spool_dir,
                index,
                len(list(root.glob("*.seg"))),
                eof_total(root),
            )
        while True:
            segment = root / _segment_name(index)
            if segment.exists():
                if not tracker.try_claim(index):
                    return
                for payload in read_segment(segment):
                    envelope = AgentEnvelope.FromString(payload)
                    # Monotonic event-time watermark, PeriodicImpulse-style.
                    event_ts = timestamp.Timestamp(micros=envelope.event_time_ms * 1000)
                    current = watermark_estimator.current_watermark()
                    if current is None or event_ts > current:
                        watermark_estimator.set_watermark(event_ts)
                    yield envelope
                index += 1
                continue
            total = eof_total(root)
            if total is not None and index >= total:
                # All segments consumed: claim past the end to mark the
                # restriction done instead of deferring forever.
                tracker.try_claim(tracker.current_restriction().stop)
                return
            tracker.defer_remainder(timestamp.Timestamp(time.time() + _TAIL_POLL_S))
            return


def read_spool(pipeline: beam.Pipeline, spool_dir: str) -> beam.pvalue.PCollection:
    """The ingest transform: one impulse element fanned into the spool replay."""
    return (
        pipeline
        | "SpoolImpulse" >> beam.Create([spool_dir])
        | "SpoolRead" >> beam.ParDo(SpoolSourceDoFn())
    )
