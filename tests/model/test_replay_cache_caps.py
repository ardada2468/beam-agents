"""Blob-cap and digest-only-fallback tests for the `llm-replay-cache` capability.

Covers: the 100 KiB serialized-blob cap holds under arbitrary operation
sequences with the response-bytes accounting staying exact, large inserts evict
until the blob fits, an oversized response degrades to digest-only without
evicting live entries, and digest-only hits identify themselves.
"""

from __future__ import annotations

import hashlib

from hypothesis import given, settings
from hypothesis import strategies as st

from beam_agents.model import BLOB_CAP_BYTES, MAX_ENTRIES, ReplayCache

_NOW = 1_700_000_000_000

# A small key pool so sequences exercise both fresh inserts and overwrites.
_keys = st.integers(min_value=0, max_value=20).map(lambda i: f"{i:064x}")
# Response sizes spanning "many fit", "few fit", and occasional "never fits".
_sizes = st.integers(min_value=0, max_value=120_000)


# --- Requirement: Serialized blob never exceeds 100 KiB ----------------------


@settings(max_examples=200, deadline=None)
@given(ops=st.lists(st.tuples(_keys, _sizes), max_size=60))
def test_cap_holds_under_arbitrary_operation_sequences(ops: list[tuple[str, int]]) -> None:
    # Scenario: The cap holds under arbitrary operation sequences.
    cache = ReplayCache(now_ms=_NOW)
    for key, size in ops:
        cache.put(key, b"x" * size)
        blob = cache.to_blob()
        # Cap invariant against the ground-truth serialized size.
        assert blob.ByteSize() <= BLOB_CAP_BYTES
        # LRU bound holds jointly with the byte cap.
        assert len(blob.entries) <= MAX_ENTRIES
        # Response-bytes accounting is exact: sum of *stored* responses.
        assert blob.total_response_bytes == sum(len(e.response) for e in blob.entries)


def test_large_inserts_evict_until_the_blob_fits() -> None:
    # Scenario: Large inserts evict until the blob fits.
    cache = ReplayCache(now_ms=_NOW)
    chunk = b"y" * 30_000  # ~30 KiB each; only three fit under 100 KiB
    for i in range(6):
        cache.put(f"{i:064x}", chunk)

    blob = cache.to_blob()
    assert blob.ByteSize() <= BLOB_CAP_BYTES
    # Newest entry is fully stored (not digest-only).
    newest = next(e for e in blob.entries if e.cache_key == f"{5:064x}")
    assert newest.digest_only is False
    assert newest.response == chunk
    # Oldest entries were evicted to make room.
    assert f"{0:064x}" not in {e.cache_key for e in blob.entries}


def test_oversized_response_becomes_digest_only_without_collateral_eviction() -> None:
    # Scenario: An oversized response becomes digest-only without collateral eviction.
    cache = ReplayCache(now_ms=_NOW)
    cache.put("a" * 64, b"small-a")
    cache.put("b" * 64, b"small-b")

    huge = b"z" * (BLOB_CAP_BYTES * 2)
    cache.put("c" * 64, huge)

    blob = cache.to_blob()
    keys = {e.cache_key for e in blob.entries}
    # Pre-existing entries all survive.
    assert "a" * 64 in keys
    assert "b" * 64 in keys
    # The oversized entry is stored digest-only with the full-response digest.
    oversized = next(e for e in blob.entries if e.cache_key == "c" * 64)
    assert oversized.digest_only is True
    assert oversized.response == b""
    assert oversized.response_digest == hashlib.sha256(huge).digest()
    assert blob.ByteSize() <= BLOB_CAP_BYTES


def test_digest_only_hits_identify_themselves() -> None:
    # Scenario: Digest-only hits identify themselves.
    cache = ReplayCache(now_ms=_NOW)
    huge = b"w" * (BLOB_CAP_BYTES * 2)
    cache.put("d" * 64, huge)

    entry = cache.get("d" * 64)
    assert entry is not None
    assert entry.digest_only is True
    assert entry.response == b""
    assert entry.response_digest == hashlib.sha256(huge).digest()
