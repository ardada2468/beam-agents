"""Staging semantics for the memory-facade capability.

Covers the "Facade stages mutations over an in-memory MemoryBlob" requirement:
blob round-trip without access, fresh versioned empty blob, rejected mutations
leaving staged state unchanged, and the ``dirty`` flag.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import MemoryBlob
from beam_agents.memory import Memory, MemoryOverflow
from beam_agents.memory.facade import HARD_CAP_BYTES

NOW = 1_000


def _scalar_blob(*pairs: tuple[str, bytes, int]) -> MemoryBlob:
    """Build a MemoryBlob whose entries hold scalar-encoded values (tag 0x00)."""
    blob = MemoryBlob(state_schema_version=1)
    total = 0
    for key, value, last_access_ms in pairs:
        encoded = b"\x00" + value
        blob.entries.add(key=key, value=encoded, last_access_ms=last_access_ms)
        total += len(encoded)
    blob.total_value_bytes = total
    return blob


def test_blob_round_trips_through_the_facade() -> None:
    # Scenario: Blob round-trips through the facade.
    blob = _scalar_blob(("a", b"1", 10), ("b", b"22", 20))
    mem = Memory(blob, now_ms=NOW)

    out = mem.to_blob()

    assert mem.dirty is False
    assert out.state_schema_version == 1
    assert out.total_value_bytes == blob.total_value_bytes
    assert [(e.key, e.value, e.last_access_ms) for e in out.entries] == [
        (e.key, e.value, e.last_access_ms) for e in blob.entries
    ]


def test_fresh_facade_produces_a_versioned_empty_blob() -> None:
    # Scenario: Fresh facade produces a versioned empty blob.
    out = Memory(now_ms=NOW).to_blob()

    assert out.state_schema_version == 1
    assert len(out.entries) == 0
    assert out.total_value_bytes == 0


def test_rejected_mutation_leaves_staged_state_unchanged() -> None:
    # Scenario: Rejected mutation leaves staged state unchanged.
    mem = Memory(now_ms=NOW)
    mem.set("k", b"v")
    before = mem.size_bytes

    with pytest.raises(MemoryOverflow):
        mem.set("big", b"x" * (HARD_CAP_BYTES + 1))

    assert mem.get("k") == b"v"
    assert mem.size_bytes == before


def test_dirty_flag_transitions_on_first_mutation() -> None:
    # Scenario support: dirty starts False, flips on the first mutation.
    mem = Memory(_scalar_blob(("a", b"1", 10)), now_ms=NOW)
    assert mem.dirty is False

    mem.set("b", b"2")
    assert mem.dirty is True
