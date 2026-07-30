"""`MemoryStore` over Redis: per-entity hashes, Lua compare-and-set upserts.

Each entity's records live in one hash keyed by a prefixed ``hex(entity_key)``,
field = record ``key``, value framed as the 8-byte big-endian seq followed by
the envelope bytes (design D8). ``save`` runs server-side as a compare-and-set
script — the same conditional-write-needs-a-script reasoning as the dedup
store's ``complete`` — so the guard holds without a client-side
read-modify-write race. ``search`` scans the hash with the prefix escaped to a
literal ``HSCAN`` match and assembles the bounded, ordered result client-side,
acceptable because the namespace is one entity's rows, not the keyspace (D7).

The client library is imported inside the constructor: it belongs to the
optional ``memory-stores`` extra.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from beam_agents.memory.stores.base import (
    MemoryRecord,
    MemoryStore,
    decode_envelope,
    encode_envelope,
    encode_seq,
    missing_client_error,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Lua strings compare lexicographically byte-by-byte, and the 8-byte big-endian
# seq framing makes that agree with numeric order — so "incoming >= stored" is
# one string compare on the value's fixed-width prefix. The write and the
# compare execute as one script: atomic by Redis's execution model.
_SAVE_SCRIPT = """
local stored = redis.call('HGET', KEYS[1], ARGV[1])
if stored == false or ARGV[2] >= string.sub(stored, 1, 8) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. ARGV[3])
  return 1
end
return 0
"""

# HSCAN MATCH is a glob grammar; every metacharacter is escaped so the prefix
# is always a literal (the requirement's "prefix metacharacters are literal").
_GLOB_SPECIALS = "\\?*[]"


def _literal_glob_prefix(prefix: str) -> str:
    escaped = []
    for ch in prefix:
        if ch in _GLOB_SPECIALS:
            escaped.append("\\" + ch)
        else:
            escaped.append(ch)
    return "".join(escaped) + "*"


class RedisMemoryStore(MemoryStore):
    """`MemoryStore` over Redis; see the module docstring for the layout."""

    def __init__(self, uri: str, *, key_prefix: str = "beam-agents:ltm:") -> None:
        try:
            from redis import asyncio as redis_asyncio
        except ImportError as exc:
            raise missing_client_error("RedisMemoryStore", "redis", exc) from exc

        self._redis = redis_asyncio.from_url(uri)
        self._prefix = key_prefix
        self._save_script = self._redis.register_script(_SAVE_SCRIPT)

    def _hash_key(self, entity_key: bytes) -> str:
        return f"{self._prefix}{entity_key.hex()}"

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        framed = cast("bytes | None", await self._redis.hget(self._hash_key(entity_key), key))
        if framed is None:
            return None
        return decode_envelope(entity_key, framed[8:])

    async def _save(self, record: MemoryRecord) -> bool:
        applied = await self._save_script(
            keys=[self._hash_key(record.entity_key)],
            args=[record.key, encode_seq(record.seq), encode_envelope(record)],
        )
        return bool(applied)

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        matched: list[tuple[str, bytes]] = []
        scan = cast(
            "AsyncIterator[tuple[bytes, bytes]]",
            self._redis.hscan_iter(self._hash_key(entity_key), match=_literal_glob_prefix(prefix)),
        )
        async for field, framed in scan:
            key = field.decode("utf-8")
            # Belt over the glob's braces: the match pattern is server-side
            # glob semantics; the contract is plain startswith.
            if key.startswith(prefix):
                matched.append((key, framed))
        matched.sort(key=lambda item: item[0])
        return [decode_envelope(entity_key, framed[8:]) for _, framed in matched[:limit]]

    async def close(self) -> None:
        await self._redis.aclose()
