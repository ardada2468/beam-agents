"""Scalar get/set/delete with LRU stamping for the memory-facade capability.

Covers the "Scalar get/set/delete with LRU stamping" requirement.
"""

from __future__ import annotations

from beam_agents.memory import Memory

NOW = 5_000


def test_set_then_get_round_trips_bytes() -> None:
    # Scenario: Set then get round-trips bytes.
    mem = Memory(now_ms=NOW)
    mem.set("k", b"v")

    assert mem.get("k") == b"v"
    assert mem.get("missing") is None


def test_empty_value_round_trips() -> None:
    # Requirement: set stores arbitrary bytes including empty.
    mem = Memory(now_ms=NOW)
    mem.set("k", b"")
    assert mem.get("k") == b""


def test_access_order_is_persisted_for_lru() -> None:
    # Scenario: Access order is persisted for LRU.
    mem = Memory(now_ms=100)
    mem.set("a", b"1")
    mem.set("b", b"2")
    mem.set("c", b"3")

    mem = Memory(mem.to_blob(), now_ms=NOW)
    assert mem.get("a") == b"1"

    out = mem.to_blob()
    assert [e.key for e in out.entries] == ["b", "c", "a"]
    a_entry = next(e for e in out.entries if e.key == "a")
    assert a_entry.last_access_ms == NOW


def test_delete_removes_and_is_idempotent() -> None:
    # Scenario: Delete removes and is idempotent.
    mem = Memory(now_ms=NOW)
    mem.set("k", b"v")
    filled = mem.size_bytes

    mem.delete("k")
    mem.delete("k")

    assert mem.get("k") is None
    assert mem.size_bytes < filled
    assert mem.size_bytes == 0
