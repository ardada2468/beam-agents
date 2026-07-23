"""Staging/serialization tests for the `llm-replay-cache` capability.

Covers the in-memory facade contract: untouched round-trip stays clean, a fresh
facade emits a versioned empty blob, and the clock is injected (never read from
wall time).
"""

from __future__ import annotations

import hashlib

from beam_agents._protos import LlmCacheBlob
from beam_agents.model import ReplayCache

_NOW = 1_700_000_000_000


def _blob_with(entries: list[tuple[str, bytes, int, int, bool]]) -> LlmCacheBlob:
    blob = LlmCacheBlob(state_schema_version=1)
    total = 0
    for cache_key, response, created, accessed, digest_only in entries:
        blob.entries.add(
            cache_key=cache_key,
            response=response,
            response_digest=hashlib.sha256(response).digest(),
            created_at_ms=created,
            last_access_ms=accessed,
            digest_only=digest_only,
        )
        total += len(response)
    blob.total_response_bytes = total
    return blob


# --- Requirement: Facade stages lookups and inserts over an in-memory blob ----


def test_untouched_blob_round_trips_clean() -> None:
    # Scenario: Blob round-trips through an untouched facade.
    source = _blob_with(
        [
            ("a" * 64, b"resp-a", _NOW - 10, _NOW - 5, False),
            ("b" * 64, b"resp-bb", _NOW - 8, _NOW - 3, False),
        ]
    )
    cache = ReplayCache(source, now_ms=_NOW)
    result = cache.to_blob()

    assert cache.dirty is False
    assert result.state_schema_version == 1
    assert [e.cache_key for e in result.entries] == [e.cache_key for e in source.entries]
    assert [e.response for e in result.entries] == [e.response for e in source.entries]
    assert result.total_response_bytes == source.total_response_bytes


def test_fresh_facade_emits_versioned_empty_blob() -> None:
    # Scenario: Fresh facade produces a versioned empty blob.
    cache = ReplayCache(now_ms=_NOW)
    result = cache.to_blob()

    assert result.state_schema_version == 1
    assert len(result.entries) == 0
    assert result.total_response_bytes == 0
    assert cache.dirty is False


def test_facade_uses_injected_clock_only() -> None:
    # Scenario (design D3): no wall-clock reads — stamps come from now_ms.
    cache = ReplayCache(now_ms=_NOW)
    cache.put("c" * 64, b"resp")
    blob = cache.to_blob()

    entry = blob.entries[0]
    assert entry.created_at_ms == _NOW
    assert entry.last_access_ms == _NOW
