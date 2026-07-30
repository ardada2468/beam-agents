"""The explicit `memory.longterm` accessor for the memory-facade capability.

Covers "Long-term store access is explicit via memory.longterm" and the
read-side of "Long-term reads are point-in-time with a read-your-writes
overlay": unconfigured access raises an actionable error naming
`AgentConfig.longterm_memory`, working-tier operations never reach the store,
and the handle's staged saves are visible to its own reads before any flush —
all against a recording in-memory store, so "the store performed no write" is
an assertion, not an assumption.
"""

from __future__ import annotations

import pytest

from beam_agents.memory.facade import Compactor, LongtermMemory, Memory
from beam_agents.memory.stores import InMemoryMemoryStore, MemoryRecord

_ENTITY = b"entity-a"
_SEQ = 4
_NOW_MS = 1_700_000_000_000


class _RecordingStore(InMemoryMemoryStore):
    """In-memory store that logs every operation by name."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        self.calls.append(f"load:{key}")
        return await super()._load(entity_key, key)

    async def _save(self, record: MemoryRecord) -> bool:
        self.calls.append(f"save:{record.key}")
        return await super()._save(record)

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        self.calls.append(f"search:{prefix}")
        return await super()._search(entity_key, prefix, limit)


def _handle(store: InMemoryMemoryStore) -> LongtermMemory:
    return LongtermMemory(store, entity_key=_ENTITY, seq=_SEQ, now_ms=_NOW_MS)


# -- Requirement: Long-term store access is explicit via memory.longterm ------


def test_unconfigured_access_raises_an_error_naming_the_config_field() -> None:
    # Scenario: Unconfigured pipelines behave exactly as today (the accessor
    # half: touching `longterm` with no store raises, actionably).
    memory = Memory(None, now_ms=_NOW_MS)

    with pytest.raises(RuntimeError, match=r"AgentConfig\.longterm_memory"):
        _ = memory.longterm


def test_working_tier_operations_never_reach_the_store() -> None:
    # Scenario: Working-tier operations never reach the store.
    store = _RecordingStore()

    class _EvictAll:
        def compact(self, memory: Memory) -> None:
            memory.delete("a")

    compactor: Compactor = _EvictAll()
    memory = Memory(None, now_ms=_NOW_MS, compactor=compactor, longterm=_handle(store))

    memory.set("a", b"1")
    memory.get("a")
    memory.append("ring", b"x", max_items=4)
    memory.ring("ring")
    memory.delete("a")
    memory.to_blob()

    assert store.calls == []


async def test_only_explicit_longterm_calls_reach_the_store() -> None:
    store = _RecordingStore()
    memory = Memory(None, now_ms=_NOW_MS, longterm=_handle(store))

    await memory.longterm.load("profile")
    await memory.longterm.search("case/", limit=3)

    assert store.calls == ["load:profile", "search:case/"]


def test_longterm_staged_is_empty_without_a_handle() -> None:
    memory = Memory(None, now_ms=_NOW_MS)
    assert memory.longterm_staged() == ()


# -- Requirement: Long-term saves stage in the activation ---------------------


def test_save_stages_without_store_io_and_stamps_seq_and_now() -> None:
    # `save` is pure staging: no store call, and the record is stamped with the
    # activation's frozen seq and clock.
    store = _RecordingStore()
    handle = _handle(store)

    handle.save("profile", b"v1")

    assert store.calls == []
    assert handle.staged_upserts == (
        MemoryRecord(
            entity_key=_ENTITY, key="profile", value=b"v1", seq=_SEQ, updated_at_ms=_NOW_MS
        ),
    )


def test_a_re_save_of_one_key_keeps_the_last_value() -> None:
    handle = _handle(_RecordingStore())

    handle.save("profile", b"v1")
    handle.save("profile", b"v2")
    handle.save("other", b"x")

    assert [(r.key, r.value) for r in handle.staged_upserts] == [
        ("profile", b"v2"),
        ("other", b"x"),
    ]


def test_save_rejects_an_empty_key() -> None:
    handle = _handle(_RecordingStore())
    with pytest.raises(ValueError, match="key"):
        handle.save("", b"v")


# -- Requirement: Long-term reads are point-in-time with an overlay -----------


async def test_staged_saves_are_visible_to_reads_before_any_flush() -> None:
    # Scenario: Staged saves are visible to reads before any flush.
    store = _RecordingStore()
    handle = _handle(store)

    handle.save("case/7", b"staged")
    loaded = await handle.load("case/7")
    searched = await handle.search("case/", limit=10)

    assert loaded is not None and loaded.value == b"staged"
    assert [r.value for r in searched] == [b"staged"]
    # The store has still performed no write.
    assert all(not call.startswith("save:") for call in store.calls)
    assert await store.load(_ENTITY, "case/7") is None


async def test_a_staged_save_shadows_the_stored_row() -> None:
    store = _RecordingStore()
    assert await store.save(
        MemoryRecord(entity_key=_ENTITY, key="case/7", value=b"old", seq=1, updated_at_ms=1)
    )
    handle = _handle(store)

    handle.save("case/7", b"new")

    loaded = await handle.load("case/7")
    assert loaded is not None and loaded.value == b"new"
    results = await handle.search("case/", limit=10)
    assert [r.value for r in results] == [b"new"]


async def test_search_merges_staged_and_stored_rows_in_key_order() -> None:
    store = _RecordingStore()
    assert await store.save(
        MemoryRecord(entity_key=_ENTITY, key="case/2", value=b"stored", seq=1, updated_at_ms=1)
    )
    handle = _handle(store)
    handle.save("case/1", b"staged")
    handle.save("note/1", b"other-prefix")

    results = await handle.search("case/", limit=10)

    assert [(r.key, r.value) for r in results] == [("case/1", b"staged"), ("case/2", b"stored")]


async def test_search_stays_bounded_after_the_merge() -> None:
    store = _RecordingStore()
    for i in (2, 3):
        assert await store.save(
            MemoryRecord(entity_key=_ENTITY, key=f"case/{i}", value=b"s", seq=1, updated_at_ms=1)
        )
    handle = _handle(store)
    handle.save("case/1", b"staged")

    results = await handle.search("case/", limit=2)

    assert [r.key for r in results] == ["case/1", "case/2"]


async def test_a_read_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    # A failing store read must raise through the handle (and so fail the
    # activation closed); it must not be swallowed into an empty result.
    store = _RecordingStore()

    async def _boom(entity_key: bytes, key: str) -> MemoryRecord | None:
        raise ConnectionError("store unreachable")

    monkeypatch.setattr(store, "_load", _boom)
    handle = _handle(store)

    with pytest.raises(ConnectionError):
        await handle.load("profile")
