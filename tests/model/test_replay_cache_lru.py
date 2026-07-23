"""LRU-bound tests for the `llm-replay-cache` capability.

Covers the 64-entry bound: the 65th distinct insert evicts the least-recently
-used entry, a recently read entry survives, and digest-only entries count
toward the bound.
"""

from __future__ import annotations

from beam_agents.model import MAX_ENTRIES, ReplayCache

_NOW = 1_700_000_000_000


def _key(i: int) -> str:
    return f"{i:064x}"


# --- Requirement: LRU bound of 64 entries ------------------------------------


def test_sixty_fifth_insert_evicts_least_recently_used() -> None:
    # Scenario: The 65th insert evicts the least-recently-used entry.
    cache = ReplayCache(now_ms=_NOW)
    for i in range(MAX_ENTRIES + 1):
        cache.put(_key(i), b"r")

    blob = cache.to_blob()
    keys = {e.cache_key for e in blob.entries}
    assert len(blob.entries) == MAX_ENTRIES
    assert _key(0) not in keys  # first-staged, never touched -> evicted
    assert _key(MAX_ENTRIES) in keys  # newest survives


def test_recently_read_entry_survives_eviction() -> None:
    # Scenario: A recently read entry survives eviction.
    cache = ReplayCache(now_ms=_NOW)
    for i in range(MAX_ENTRIES):
        cache.put(_key(i), b"r")

    # Read the first-staged key so it becomes most-recently-used.
    assert cache.get(_key(0)) is not None
    # A fresh insert must now evict the *second*-staged key, not the first.
    cache.put(_key(MAX_ENTRIES), b"r")

    keys = {e.cache_key for e in cache.to_blob().entries}
    assert _key(0) in keys
    assert _key(1) not in keys


def test_digest_only_entries_count_toward_the_bound() -> None:
    # Scenario: Digest-only entries count toward the 64-entry bound.
    cache = ReplayCache(now_ms=_NOW)
    # Fill with 64 oversized responses -> all stored digest-only.
    oversized = b"x" * (200_000)
    for i in range(MAX_ENTRIES):
        cache.put(_key(i), oversized)

    blob = cache.to_blob()
    assert len(blob.entries) == MAX_ENTRIES
    assert all(e.digest_only for e in blob.entries)

    # One more digest-only insert still evicts to hold the bound.
    cache.put(_key(MAX_ENTRIES), oversized)
    assert len(cache.to_blob().entries) == MAX_ENTRIES
