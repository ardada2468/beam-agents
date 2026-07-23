"""Soft- and hard-cap enforcement for the memory-facade capability.

Covers the "Soft cap warns and triggers compaction at 75%" and "Hard cap raises
MemoryOverflow at 1 MiB" requirements.
"""

from __future__ import annotations

import logging
from unittest import mock

import pytest

from beam_agents._protos import MemoryBlob
from beam_agents.memory import Memory, MemoryOverflow
from beam_agents.memory.facade import _SOFT_CAP_BYTES, HARD_CAP_BYTES

NOW = 11_000


class _RecordingCompactor:
    def __init__(self) -> None:
        self.calls = 0

    def compact(self, memory: Memory) -> None:
        self.calls += 1


class _DeletingCompactor:
    """Deletes the named keys when invoked (frees exactly their stored bytes)."""

    def __init__(self, *keys: str) -> None:
        self._keys = keys
        self.calls = 0

    def compact(self, memory: Memory) -> None:
        self.calls += 1
        for key in self._keys:
            memory.delete(key)


def _scalar_blob(*pairs: tuple[str, bytes]) -> MemoryBlob:
    """Build a loaded blob with scalar-encoded entries (tag 0x00), no mutation."""
    blob = MemoryBlob(state_schema_version=1)
    total = 0
    for key, value in pairs:
        encoded = b"\x00" + value
        blob.entries.add(key=key, value=encoded, last_access_ms=0)
        total += len(encoded)
    blob.total_value_bytes = total
    return blob


def _fill_to(mem: Memory, key: str, target: int) -> None:
    """Set ``key`` so total is exactly ``target`` bytes (tag byte included)."""
    mem.set(key, b"x" * (target - 1))


def test_crossing_the_soft_cap_warns_once_and_compacts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Scenario: Crossing the soft cap warns once and compacts.
    compactor = _RecordingCompactor()
    mem = Memory(now_ms=NOW, compactor=compactor)

    with mock.patch("beam_agents.memory.facade.Metrics") as metrics:
        counter = metrics.counter.return_value
        with caplog.at_level(logging.WARNING, logger="beam_agents.memory.facade"):
            _fill_to(mem, "a", _SOFT_CAP_BYTES)  # crosses the soft cap
            mem.set("b", b"more")  # still above, must not re-warn
            mem.set("c", b"evenmore")

    assert sum("soft cap" in r.getMessage().lower() for r in caplog.records) == 1
    assert counter.inc.call_count == 1
    assert compactor.calls == 1
    assert mem.get("b") == b"more"


def test_no_compactor_configured_is_not_an_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Scenario: No compactor configured is not an error.
    mem = Memory(now_ms=NOW)

    with mock.patch("beam_agents.memory.facade.Metrics") as metrics:
        counter = metrics.counter.return_value
        with caplog.at_level(logging.WARNING, logger="beam_agents.memory.facade"):
            _fill_to(mem, "a", _SOFT_CAP_BYTES)

    assert counter.inc.call_count == 1
    assert any("soft cap" in r.getMessage().lower() for r in caplog.records)
    assert mem.size_bytes == _SOFT_CAP_BYTES


def test_overflowing_write_is_rejected_atomically() -> None:
    # Scenario: Overflowing write is rejected atomically.
    mem = Memory(now_ms=NOW)
    mem.set("k", b"keep")
    before = mem.size_bytes

    with pytest.raises(MemoryOverflow) as exc:
        mem.set("k", b"x" * HARD_CAP_BYTES)

    assert exc.value.key == "k"
    assert exc.value.cap_bytes == HARD_CAP_BYTES
    assert exc.value.attempted_bytes > HARD_CAP_BYTES
    assert mem.get("k") == b"keep"
    assert mem.size_bytes == before


def test_compaction_that_frees_space_lets_the_write_succeed() -> None:
    # Scenario: Compaction that frees space lets the write succeed.
    # Load a near-full blob (loading is not a mutation, so no soft-cap fires);
    # the overflowing set then exercises the hard-cap compaction path directly.
    blob = _scalar_blob(("filler", b"f" * (HARD_CAP_BYTES - 200)))
    mem = Memory(blob, now_ms=NOW, compactor=_DeletingCompactor("filler"))

    mem.set("payload", b"y" * 200)  # would overflow before compaction frees "filler"

    assert mem.get("filler") is None
    assert mem.get("payload") == b"y" * 200


def test_compaction_that_frees_too_little_still_rejects() -> None:
    # Scenario: Compaction that frees too little still rejects.
    blob = _scalar_blob(
        ("small", b"z" * 50),
        ("bulk", b"b" * (HARD_CAP_BYTES - 100)),
    )
    mem = Memory(blob, now_ms=NOW, compactor=_DeletingCompactor("small"))

    with pytest.raises(MemoryOverflow):
        mem.set("payload", b"y" * 500)

    # Compaction's deletion of "small" persists even though the write was rejected.
    assert mem.get("small") is None
    assert mem.get("payload") is None
