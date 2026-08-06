"""Offline call-shape tests for the Redis `MemoryStore`.

Covers the client-side half of "The Redis store guards upserts with a
server-side script" without a server, through a faked `redis` module injected
at the constructor's lazy-import seam — so the suite runs (and is
coverage-counted) in the `ci`/`quality` lanes, where the real client is not
installed. Pinned here: the prefixed entity hash key, the script's argument
vector and framed value layout, the frame slicing on load, the literal-glob
escaping and client-side assembly of `search`, and `aclose`. The fakes never
evaluate the Lua compare-and-set — the script's atomicity is the live suite's
to verify (`test_redis_live.py`, `-m integration`), which stays the
interchangeability authority.
"""

from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING

import pytest

from beam_agents.memory.stores import MemoryRecord
from beam_agents.memory.stores.base import _encode_envelope, _encode_seq
from beam_agents.memory.stores.redis import RedisMemoryStore, _literal_glob_prefix

from ._conformance import ENTITY_A, a_record

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

URI = "redis://localhost:6379/0"


def _frame(record: MemoryRecord) -> bytes:
    """The stored value layout the requirement pins: 8-byte seq + envelope."""
    return _encode_seq(record.seq) + _encode_envelope(record)


# -- The faked client surface --------------------------------------------------


class _FakeScript:
    """Records invocations of the registered compare-and-set script."""

    def __init__(self, source: str) -> None:
        self.source = source
        self.calls: list[tuple[list[str], list[object]]] = []
        self.reply = 1

    async def __call__(self, *, keys: list[str], args: list[object]) -> int:
        self.calls.append((keys, args))
        return self.reply


class _FakeRedis:
    """Records the store's client calls; scripted replies, no server."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.scripts: list[_FakeScript] = []
        self.hgets: list[tuple[str, str]] = []
        self.hget_reply: bytes | None = None
        self.hscans: list[tuple[str, str | None]] = []
        # (field, framed value) pairs hscan_iter will yield, in this order.
        self.scan_fields: list[tuple[bytes, bytes]] = []
        self.closed = False

    def register_script(self, script: str) -> _FakeScript:
        registered = _FakeScript(script)
        self.scripts.append(registered)
        return registered

    async def hget(self, name: str, key: str) -> bytes | None:
        self.hgets.append((name, key))
        return self.hget_reply

    def hscan_iter(self, name: str, match: str | None = None) -> AsyncIterator[tuple[bytes, bytes]]:
        self.hscans.append((name, match))

        async def _iter() -> AsyncIterator[tuple[bytes, bytes]]:
            for field, framed in self.scan_fields:
                yield field, framed

        return _iter()

    async def aclose(self) -> None:
        self.closed = True


def _install_fake_redis(monkeypatch: pytest.MonkeyPatch) -> list[_FakeRedis]:
    """Satisfy `from redis import asyncio as redis_asyncio` with the fake."""
    clients: list[_FakeRedis] = []

    def _from_url(url: str) -> _FakeRedis:
        client = _FakeRedis(url)
        clients.append(client)
        return client

    fake_asyncio = types.ModuleType("redis.asyncio")
    setattr(fake_asyncio, "from_url", _from_url)  # noqa: B010
    fake_redis = types.ModuleType("redis")
    setattr(fake_redis, "asyncio", fake_asyncio)  # noqa: B010
    monkeypatch.setitem(sys.modules, "redis", fake_redis)
    monkeypatch.setitem(sys.modules, "redis.asyncio", fake_asyncio)
    return clients


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> tuple[RedisMemoryStore, _FakeRedis]:
    clients = _install_fake_redis(monkeypatch)
    built = RedisMemoryStore(URI)
    (client,) = clients
    return built, client


HASH_KEY = "beam-agents:ltm:" + ENTITY_A.hex()


# -- Requirement: The Redis store guards upserts with a server-side script ----


def test_the_client_is_built_from_the_configured_url_and_registers_one_script(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    _, client = store

    assert client.url == URI
    # One compare-and-set script, registered once at construction: the guard
    # lives server-side, never as a client-side read-modify-write.
    (script,) = client.scripts
    assert "HGET" in script.source and "HSET" in script.source


async def test_save_sends_the_entity_hash_key_and_the_framed_argument_vector(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    rs, client = store
    record = a_record("case/2", seq=7)

    applied = await rs.save(record)

    assert applied
    (script,) = client.scripts
    ((keys, args),) = script.calls
    assert keys == [HASH_KEY]
    # ARGV: the field, the 8-byte big-endian seq, the envelope — the script
    # concatenates ARGV[2]..ARGV[3] into exactly the `_frame` layout.
    assert args == ["case/2", _encode_seq(7), _encode_envelope(record)]


async def test_the_script_reply_maps_to_applied_and_not_applied(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    # The applied verdict is the backend's reply, never a client-side
    # re-check: 1 -> True, 0 (stale seq refused server-side) -> False.
    rs, client = store
    (script,) = client.scripts

    script.reply = 1
    assert await rs.save(a_record("profile", seq=5))
    script.reply = 0
    assert not await rs.save(a_record("profile", seq=4))


# -- Scenario: Load returns the saved record or None --------------------------


async def test_load_returns_none_for_an_absent_field(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    rs, client = store

    assert await rs.load(ENTITY_A, "missing") is None
    assert client.hgets == [(HASH_KEY, "missing")]


async def test_load_decodes_the_envelope_from_byte_eight_on(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    rs, client = store
    record = a_record("profile", seq=9)
    client.hget_reply = _frame(record)

    loaded = await rs.load(ENTITY_A, "profile")

    assert loaded == record


# -- Requirement: Search is a bounded per-entity key-prefix scan --------------


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("case/", "case/*"),
        ("", "*"),
        ("a?b", "a\\?b*"),
        ("a*", "a\\**"),
        ("a[b]", "a\\[b\\]*"),
        ("a\\b", "a\\\\b*"),
    ],
)
def test_the_hscan_match_is_the_escaped_literal_glob(prefix: str, expected: str) -> None:
    # Scenario: Prefix metacharacters are literal — HSCAN MATCH is a glob
    # grammar, so every metacharacter is escaped and `*` appended.
    assert _literal_glob_prefix(prefix) == expected


async def test_search_scans_the_entity_hash_with_the_literal_match(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    rs, client = store

    await rs.search(ENTITY_A, "a[b", limit=10)

    assert client.hscans == [(HASH_KEY, "a\\[b*")]


async def test_search_sorts_bounds_and_belts_the_scanned_fields(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    # Scenario: Prefix search returns ordered, bounded, entity-scoped results
    # — the client-side half: HSCAN yields in hash order, so the store sorts,
    # truncates to `limit`, and drops any field the server-side glob matched
    # that is not a plain startswith match (the belt over the glob's braces).
    rs, client = store
    records = {key: a_record(key, value=key.encode()) for key in ("case/1", "case/2", "case/3")}
    client.scan_fields = [
        (b"case/2", _frame(records["case/2"])),
        (b"note/9", _frame(a_record("note/9"))),  # glob artifact: not a startswith match
        (b"case/1", _frame(records["case/1"])),
        (b"case/3", _frame(records["case/3"])),
    ]

    results = await rs.search(ENTITY_A, "case/", limit=2)

    assert [r.key for r in results] == ["case/1", "case/2"]
    assert results == [records["case/1"], records["case/2"]]


async def test_search_decodes_unicode_fields_as_utf8(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    rs, client = store
    record = a_record("café/☕", value=b"latte")
    client.scan_fields = [("café/☕".encode(), _frame(record))]

    results = await rs.search(ENTITY_A, "café/", limit=1)

    assert results == [record]


# -- Close releases the client pool -------------------------------------------


async def test_close_closes_the_client_pool(
    store: tuple[RedisMemoryStore, _FakeRedis],
) -> None:
    rs, client = store

    await rs.close()

    assert client.closed
