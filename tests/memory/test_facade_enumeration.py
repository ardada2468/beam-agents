"""Read-only LRU-order enumeration for the memory-facade capability.

Covers the "Read-only LRU-order enumeration for compaction strategies"
requirement: ``keys()`` reports stored keys least-recently-used first (the
order ``to_blob()`` persists) and ``entry_size(key)`` reports the stored
encoded value size, with neither call re-stamping access order or setting
``dirty`` — a compaction strategy has to be able to iterate candidates without
perturbing the eviction order it is iterating.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import MemoryBlob
from beam_agents.memory import Memory

NOW = 21_000


def _scalar_blob(*pairs: tuple[str, bytes]) -> MemoryBlob:
    blob = MemoryBlob(state_schema_version=1)
    total = 0
    for index, (key, value) in enumerate(pairs):
        encoded = b"\x00" + value
        blob.entries.add(key=key, value=encoded, last_access_ms=index)
        total += len(encoded)
    blob.total_value_bytes = total
    return blob


def _loaded() -> Memory:
    return Memory(_scalar_blob(("a", b"aaa"), ("b", b"bb"), ("c", b"c")), now_ms=NOW)


def test_keys_reports_lru_order_without_dirtying_the_facade() -> None:
    # Scenario: keys() reports LRU order without dirtying the facade.
    fresh = _loaded()
    mutated = _loaded()
    mutated.get("b")  # a read re-stamps LRU order, by design (facade D6)

    assert fresh.keys() == ("a", "b", "c")
    assert fresh.dirty is False
    assert mutated.keys() == ("a", "c", "b")

    # Enumerating is itself inert: calling again returns the same order and
    # still leaves the fresh facade clean.
    assert fresh.keys() == ("a", "b", "c")
    assert fresh.dirty is False
    assert mutated.keys() == ("a", "c", "b")


def test_keys_matches_the_order_to_blob_persists() -> None:
    # The order is not merely "some order": it is the persisted one, which is
    # what makes a compactor's eviction set reproducible from a committed blob.
    mem = _loaded()
    mem.get("a")
    assert mem.keys() == tuple(entry.key for entry in mem.to_blob().entries)


def test_entry_size_does_not_perturb_eviction_order() -> None:
    # Scenario: entry_size does not perturb eviction order.
    mem = _loaded()
    lru_key = mem.keys()[0]

    assert mem.entry_size(lru_key) == 4  # kind tag + b"aaa"
    assert mem.dirty is False
    stored = mem.keys()
    assert sum(mem.entry_size(key) for key in stored) == mem.size_bytes

    # The inspected key is still the one an LRU-order pass evicts first.
    assert mem.keys()[0] == lru_key
    mem.delete(mem.keys()[0])
    assert mem.keys() == ("b", "c")


def test_entry_size_raises_key_error_for_an_absent_key() -> None:
    mem = _loaded()
    with pytest.raises(KeyError):
        mem.entry_size("nope")


def test_enumeration_reflects_staged_mutations() -> None:
    # Both surfaces read the staged (uncommitted) entries: a compactor runs
    # mid-activation, so anything else would enumerate stale candidates.
    mem = _loaded()
    mem.set("d", b"dddd")
    mem.delete("a")

    assert mem.keys() == ("b", "c", "d")
    assert mem.entry_size("d") == 5
    stored = mem.keys()
    assert sum(mem.entry_size(key) for key in stored) == mem.size_bytes


def test_entry_size_counts_ring_framing() -> None:
    # Ring entries are stored self-describing (kind tag + length-prefixed
    # items); the compaction surface reports the stored bytes, framing included,
    # because that is what eviction actually reclaims.
    mem = Memory(now_ms=NOW)
    mem.append("log", b"xy", max_items=8)
    mem.append("log", b"z", max_items=8)

    # 1 kind tag + (4 + 2) + (4 + 1)
    assert mem.entry_size("log") == 12
    assert mem.entry_size("log") == mem.size_bytes
