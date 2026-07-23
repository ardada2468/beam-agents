"""Ring-buffer append semantics for the memory-facade capability.

Covers the "Append maintains a bounded ring per key" requirement.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import MemoryBlob
from beam_agents.memory import Memory

NOW = 7_000


def test_appends_preserve_order_and_survive_blob_round_trip() -> None:
    # Scenario: Appends preserve order and survive blob round-trip.
    mem = Memory(now_ms=NOW)
    mem.append("log", b"1")
    mem.append("log", b"2")
    mem.append("log", b"3")

    reloaded = Memory(mem.to_blob(), now_ms=NOW)
    assert reloaded.ring("log") == (b"1", b"2", b"3")


def test_ring_of_absent_key_is_empty() -> None:
    # Requirement: ring returns empty tuple for absent keys.
    assert Memory(now_ms=NOW).ring("nope") == ()


def test_ring_drops_oldest_at_capacity() -> None:
    # Scenario: Ring drops oldest at capacity.
    mem = Memory(now_ms=NOW)
    for item in (b"1", b"2", b"3"):
        mem.append("r", item, max_items=3)
    full = mem.size_bytes

    mem.append("r", b"4", max_items=3)

    assert mem.ring("r") == (b"2", b"3", b"4")
    # b"1" (same size as b"4") was dropped, so total is unchanged, not grown.
    assert mem.size_bytes == full


def test_append_on_scalar_key_raises() -> None:
    # Scenario: Kind mixing raises (append on a scalar key).
    mem = Memory(now_ms=NOW)
    mem.set("k", b"v")

    with pytest.raises(TypeError):
        mem.append("k", b"x")

    assert mem.get("k") == b"v"


def test_get_on_ring_key_raises() -> None:
    # Scenario: Kind mixing raises (get on a ring key).
    mem = Memory(now_ms=NOW)
    mem.append("r", b"x")

    with pytest.raises(TypeError):
        mem.get("r")

    assert mem.ring("r") == (b"x",)


def test_ring_on_scalar_key_raises() -> None:
    # Scenario: Kind mixing raises (ring on a scalar key).
    mem = Memory(now_ms=NOW)
    mem.set("k", b"v")

    with pytest.raises(TypeError):
        mem.ring("k")

    assert mem.get("k") == b"v"


@pytest.mark.parametrize(
    "corrupt",
    [
        b"\x01\x00\x00",  # ring tag + truncated (<4 byte) length prefix
        b"\x01" + (9).to_bytes(4, "big"),  # length prefix claims 9 bytes, none follow
    ],
)
def test_corrupt_ring_bytes_raise_valueerror(corrupt: bytes) -> None:
    # Design risk mitigation: a truncated ring entry raises a clear ValueError
    # rather than yielding garbage items.
    blob = MemoryBlob(state_schema_version=1)
    blob.entries.add(key="r", value=corrupt, last_access_ms=0)
    blob.total_value_bytes = len(corrupt)

    mem = Memory(blob, now_ms=NOW)
    with pytest.raises(ValueError, match="corrupt ring"):
        mem.ring("r")
