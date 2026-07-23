"""TTL-expiry tests for the `llm-replay-cache` capability.

Covers the 6h TTL anchored on ``created_at_ms``: expired entries are misses and
are purged, the boundary is inclusive, access never refreshes the TTL, and a
re-put resets creation time.
"""

from __future__ import annotations

import hashlib

from beam_agents._protos import LlmCacheBlob
from beam_agents.model import TTL_MS, ReplayCache

_CREATED = 1_700_000_000_000
_KEY = "a" * 64


def _blob_with_entry(created_ms: int, accessed_ms: int) -> LlmCacheBlob:
    response = b"cached"
    blob = LlmCacheBlob(state_schema_version=1, total_response_bytes=len(response))
    blob.entries.add(
        cache_key=_KEY,
        response=response,
        response_digest=hashlib.sha256(response).digest(),
        created_at_ms=created_ms,
        last_access_ms=accessed_ms,
        digest_only=False,
    )
    return blob


# --- Requirement: Entries expire 6 hours after creation ----------------------


def test_expired_entry_is_a_miss_and_is_purged() -> None:
    # Scenario: An expired entry is a miss and is purged.
    blob = _blob_with_entry(_CREATED, _CREATED)
    cache = ReplayCache(blob, now_ms=_CREATED + TTL_MS + 1)

    assert cache.get(_KEY) is None
    assert all(e.cache_key != _KEY for e in cache.to_blob().entries)


def test_ttl_boundary_is_inclusive() -> None:
    # Scenario: The TTL boundary is inclusive.
    blob = _blob_with_entry(_CREATED, _CREATED)
    cache = ReplayCache(blob, now_ms=_CREATED + TTL_MS)

    entry = cache.get(_KEY)
    assert entry is not None
    assert entry.response == b"cached"


def test_put_purges_expired_entries() -> None:
    # Scenario: An expired entry is a miss and is purged — via put, which purges
    # all expired entries before enforcing capacity bounds.
    blob = _blob_with_entry(_CREATED, _CREATED)
    cache = ReplayCache(blob, now_ms=_CREATED + TTL_MS + 1)

    cache.put("b" * 64, b"new")

    keys = {e.cache_key for e in cache.to_blob().entries}
    assert _KEY not in keys  # the stale entry was purged by the put
    assert "b" * 64 in keys


def test_access_does_not_refresh_ttl() -> None:
    # Scenario: Access does not refresh TTL.
    # Read once just before expiry (which re-stamps last_access_ms), then a new
    # facade at >6h from creation must still treat it as expired.
    blob = _blob_with_entry(_CREATED, _CREATED)
    near = ReplayCache(blob, now_ms=_CREATED + TTL_MS - 1)
    assert near.get(_KEY) is not None  # touches last_access_ms

    later = ReplayCache(near.to_blob(), now_ms=_CREATED + TTL_MS + 1)
    assert later.get(_KEY) is None


def test_re_put_resets_created_at() -> None:
    # Scenario: Re-put of an existing key resets created_at.
    blob = _blob_with_entry(_CREATED, _CREATED)
    # Re-put well after the original creation but before it would expire.
    later = _CREATED + TTL_MS - 1
    cache = ReplayCache(blob, now_ms=later)
    cache.put(_KEY, b"fresh")

    entry = next(e for e in cache.to_blob().entries if e.cache_key == _KEY)
    assert entry.created_at_ms == later
    # And it now survives until 6h past the *new* creation time.
    revived = ReplayCache(cache.to_blob(), now_ms=later + TTL_MS)
    assert revived.get(_KEY) is not None
