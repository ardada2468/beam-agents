"""Hit/miss and LRU-touch tests for the `llm-replay-cache` capability.

Covers: put-then-get replays identical bytes with the right digest, a hit moves
the entry to most-recently-used and re-stamps its access time, and a miss
returns ``None`` without dirtying the facade.
"""

from __future__ import annotations

import hashlib

from beam_agents.model import ReplayCache

_NOW = 1_700_000_000_000
_KEY_A = "a" * 64
_KEY_B = "b" * 64
_KEY_C = "c" * 64


# --- Requirement: Cache hits return the staged response and update LRU order --


def test_put_then_get_replays_identical_bytes() -> None:
    # Scenario: Put-then-get replays the identical bytes.
    cache = ReplayCache(now_ms=_NOW)
    response = b"the provider response bytes"
    cache.put(_KEY_A, response)

    entry = cache.get(_KEY_A)

    assert entry is not None
    assert entry.response == response
    assert entry.digest_only is False
    assert entry.response_digest == hashlib.sha256(response).digest()


def test_hit_moves_entry_to_most_recently_used() -> None:
    # Scenario: A hit moves the entry to most-recently-used.
    # Stage three entries at an earlier clock, then read the oldest at _NOW.
    cache = ReplayCache(now_ms=_NOW - 100)
    cache.put(_KEY_A, b"ra")
    cache.put(_KEY_B, b"rb")
    cache.put(_KEY_C, b"rc")

    reader = ReplayCache(cache.to_blob(), now_ms=_NOW)
    hit = reader.get(_KEY_A)
    assert hit is not None

    blob = reader.to_blob()
    assert [e.cache_key for e in blob.entries] == [_KEY_B, _KEY_C, _KEY_A]
    moved = next(e for e in blob.entries if e.cache_key == _KEY_A)
    assert moved.last_access_ms == _NOW


def test_miss_returns_none_and_leaves_facade_clean() -> None:
    # Scenario: A miss leaves the facade clean.
    cache = ReplayCache(now_ms=_NOW)
    assert cache.get("f" * 64) is None
    assert cache.dirty is False
