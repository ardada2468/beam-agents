"""In-memory LLM replay cache with content-hash keying and cap enforcement.

See :mod:`beam_agents.model` for the capability overview and the change design
(``openspec/changes/add-llm-replay-cache/design.md``) for the load-bearing
decisions: single-document canonical-JSON cache key (D1), ``LlmCacheBlob``
mirroring ``MemoryBlob`` (D2), injected-and-frozen clock over an insertion
-ordered dict (D3), TTL anchored on creation with lazy purge (D4),
purge->count-evict->byte-evict->digest-fallback cap order (D5), digest-only
fallback preserving divergence detection (D6), and the typed miss-is-``None``
read view (D7).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from beam_agents._protos import LlmCacheBlob

# Content-hash cache stays useful only for recent activations; these three
# bounds keep per-key cache state small and predictable (correctness invariant
# 3, project constraints).
MAX_ENTRIES = 64
TTL_MS = 21_600_000  # 6 hours
BLOB_CAP_BYTES = 102_400  # 100 KiB

# Wire-format constant: the repeated `entries` field is number 2, so each
# element carries a single-byte tag (field 2, wire type 2) before its
# length-delimited payload.
_ENTRY_TAG_BYTES = 1
# A base-128 varint uses the high bit of each byte as a continuation flag.
_VARINT_CONTINUATION_BIT = 0x80


@dataclass(frozen=True, slots=True)
class ReplayEntry:
    """Immutable view of one cache hit returned by :meth:`ReplayCache.get`.

    ``response`` is the cached provider bytes, empty when ``digest_only`` is set
    (the payload was too large to store). ``response_digest`` is the sha256 of
    the full original response and is always populated, so a caller that must
    re-call the provider on a digest-only hit can compare digests to detect
    provider nondeterminism.
    """

    response: bytes
    response_digest: bytes
    digest_only: bool


@dataclass(slots=True)
class _Entry:
    """One staged cache entry plus its cached serialized-blob contribution."""

    response: bytes
    response_digest: bytes
    created_at_ms: int
    last_access_ms: int
    digest_only: bool
    wire_size: int


def _varint_len(value: int) -> int:
    """Byte length of the base-128 varint encoding of a non-negative int."""
    length = 1
    while value >= _VARINT_CONTINUATION_BIT:
        value >>= 7
        length += 1
    return length


def compute_cache_key(
    model_id: str,
    messages: object,
    tools_schema: object,
    sampling_params: object,
    entity_key: bytes,
    seq: int,
) -> str:
    """Return the lowercase-hex sha256 of the canonical request material.

    All six components are packed into one JSON document with fixed field names
    and serialized canonically (sorted keys, compact separators, non-ASCII
    preserved, NaN/Infinity rejected) so logically equal requests hash
    identically regardless of dict insertion order. Non-JSON-serializable input
    raises ``TypeError``; a NaN/Infinity float raises ``ValueError``.

    ``messages``/``tools_schema``/``sampling_params`` are typed ``object`` (not
    a recursive JSON alias): callers pass provider-shaped structures of any
    concrete container type, and canonical-JSON serialization is the single
    validation point — anything non-serializable fails loudly here.
    """
    document = {
        "model": model_id,
        "messages": messages,
        "tools": tools_schema,
        "params": sampling_params,
        "key": entity_key.hex(),
        "seq": seq,
    }
    canonical = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ReplayCache:
    """Facade over a single keyed ``LlmCacheBlob``, staging lookups and inserts.

    Constructed once per activation from the loaded blob and the activation's
    ``now_ms`` clock. Mutates only in-memory data — no Beam state I/O, no
    wall-clock reads — so bundle retries replay byte-identically. The stateful
    DoFn commits :meth:`to_blob` back to keyed state after a successful
    activation.
    """

    def __init__(self, blob: LlmCacheBlob | None = None, *, now_ms: int) -> None:
        self._now_ms = now_ms
        self._entries: dict[str, _Entry] = {}
        self._response_total = 0
        self._entries_wire_total = 0
        self._dirty = False
        if blob is not None:
            for proto in blob.entries:
                entry = _Entry(
                    response=proto.response,
                    response_digest=proto.response_digest,
                    created_at_ms=proto.created_at_ms,
                    last_access_ms=proto.last_access_ms,
                    digest_only=proto.digest_only,
                    wire_size=0,
                )
                entry.wire_size = _entry_wire_size(proto.cache_key, entry)
                self._entries[proto.cache_key] = entry
                self._response_total += len(entry.response)
                self._entries_wire_total += entry.wire_size

    # -- properties -----------------------------------------------------------

    @property
    def dirty(self) -> bool:
        return self._dirty

    # -- access ---------------------------------------------------------------

    def get(self, cache_key: str) -> ReplayEntry | None:
        entry = self._entries.get(cache_key)
        if entry is None:
            return None  # never stored: a clean miss, no state change
        if self._is_expired(entry):
            self._remove(cache_key)  # lazy purge of the expired entry
            self._dirty = True
            return None
        self._touch(cache_key, entry)
        return ReplayEntry(entry.response, entry.response_digest, entry.digest_only)

    def put(self, cache_key: str, response: bytes) -> None:
        self._purge_expired()

        digest = hashlib.sha256(response).digest()
        full = _Entry(
            response=response,
            response_digest=digest,
            created_at_ms=self._now_ms,
            last_access_ms=self._now_ms,
            digest_only=False,
            wire_size=0,
        )
        full.wire_size = _entry_wire_size(cache_key, full)

        # A response that could never fit even in an otherwise empty blob is
        # stored digest-only, without flushing the rest of the cache (D5/D6).
        if _blob_header_size(len(response)) + full.wire_size > BLOB_CAP_BYTES:
            entry = _Entry(
                response=b"",
                response_digest=digest,
                created_at_ms=self._now_ms,
                last_access_ms=self._now_ms,
                digest_only=True,
                wire_size=0,
            )
            entry.wire_size = _entry_wire_size(cache_key, entry)
        else:
            entry = full

        self._remove(cache_key)  # replace: drop any prior entry for this key
        self._entries[cache_key] = entry  # inserted at MRU (dict tail)
        self._response_total += len(entry.response)
        self._entries_wire_total += entry.wire_size
        self._dirty = True

        self._evict_over_count()
        self._evict_over_bytes()

    # -- serialization --------------------------------------------------------

    def to_blob(self) -> LlmCacheBlob:
        # Imported lazily: `beam_agents.core`'s package init imports the
        # context, which imports this package — a top-level import here would
        # make `import beam_agents.model` order-dependent. The call-time read
        # also means a version bump in core/migration.py moves this stamp with
        # no edit here.
        from beam_agents.core import migration

        blob = LlmCacheBlob(state_schema_version=migration.CURRENT_STATE_SCHEMA_VERSION)
        for cache_key, entry in self._entries.items():
            blob.entries.add(
                cache_key=cache_key,
                response=entry.response,
                response_digest=entry.response_digest,
                created_at_ms=entry.created_at_ms,
                last_access_ms=entry.last_access_ms,
                digest_only=entry.digest_only,
            )
        blob.total_response_bytes = self._response_total
        return blob

    # -- internals ------------------------------------------------------------

    def _is_expired(self, entry: _Entry) -> bool:
        # Anchored on creation, inclusive boundary: age == TTL is still live.
        return self._now_ms - entry.created_at_ms > TTL_MS

    def _touch(self, cache_key: str, entry: _Entry) -> None:
        """Re-stamp access time and move the entry to most-recently-used."""
        del self._entries[cache_key]
        entry.last_access_ms = self._now_ms
        self._entries[cache_key] = entry
        self._dirty = True

    def _remove(self, cache_key: str) -> None:
        existing = self._entries.pop(cache_key, None)
        if existing is not None:
            self._response_total -= len(existing.response)
            self._entries_wire_total -= existing.wire_size

    def _purge_expired(self) -> None:
        expired = [key for key, entry in self._entries.items() if self._is_expired(entry)]
        for key in expired:
            self._remove(key)
            self._dirty = True

    def _prospective_blob_size(self) -> int:
        return _blob_header_size(self._response_total) + self._entries_wire_total

    def _evict_over_count(self) -> None:
        while len(self._entries) > MAX_ENTRIES:
            self._evict_least_recently_used()

    def _evict_over_bytes(self) -> None:
        # A single stored entry always fits alone (oversized responses are
        # stored digest-only), so this terminates with the newest entry intact.
        while len(self._entries) > 1 and self._prospective_blob_size() > BLOB_CAP_BYTES:
            self._evict_least_recently_used()

    def _evict_least_recently_used(self) -> None:
        lru_key = next(iter(self._entries))  # dict head = least-recently-used
        self._remove(lru_key)


def _entry_wire_size(cache_key: str, entry: _Entry) -> int:
    """Serialized contribution of one entry to the blob's ``entries`` field.

    Equal to the repeated-field element encoding: a one-byte tag, the varint
    length prefix, and the entry message's own ``ByteSize()``. Summing these
    with :func:`_blob_header_size` reproduces ``to_blob().ByteSize()`` exactly,
    so the byte-cap check needs no full re-serialize on the hot path (D5).
    """
    proto = LlmCacheBlob.LlmCacheEntry(
        cache_key=cache_key,
        response=entry.response,
        response_digest=entry.response_digest,
        created_at_ms=entry.created_at_ms,
        last_access_ms=entry.last_access_ms,
        digest_only=entry.digest_only,
    )
    payload = proto.ByteSize()
    return _ENTRY_TAG_BYTES + _varint_len(payload) + payload


def _blob_header_size(total_response_bytes: int) -> int:
    """Serialized size of the blob's non-repeated fields.

    ``state_schema_version`` is always set to ``CURRENT_STATE_SCHEMA_VERSION``
    (tag + one varint byte while the version stays below 128).
    ``total_response_bytes`` (field 3, int64) is omitted by proto3 when zero,
    else a tag byte plus its varint.
    """
    size = 2  # state_schema_version: tag + single-byte varint
    if total_response_bytes != 0:
        size += 1 + _varint_len(total_response_bytes)
    return size
