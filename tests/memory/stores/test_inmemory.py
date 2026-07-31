"""The in-memory `MemoryStore` for the memory-stores capability.

Runs the shared conformance suite (`_conformance.py`) against
`InMemoryMemoryStore` offline — the reference the offline lane gates on — and
pins the ABC-owned behavior the backends inherit rather than re-implement:
argument validation happens before any storage primitive is reached, and the
seq-guard comparison rule has exactly one definition.
"""

from __future__ import annotations

import pytest

from beam_agents.memory.stores import InMemoryMemoryStore, MemoryRecord, MemoryStore
from beam_agents.memory.stores.base import _seq_guard_applies

from ._conformance import ENTITY_A, MemoryStoreConformance, a_record


class _FakeClock:
    """Injectable millisecond clock, so time never has to pass for real."""

    def __init__(self, now_ms: int = 1_700_000_000_000) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


class TestInMemoryMemoryStoreConformance(MemoryStoreConformance):
    @pytest.fixture
    def store(self) -> MemoryStore:
        return InMemoryMemoryStore(clock=_FakeClock())


class _RecordingStore(InMemoryMemoryStore):
    """In-memory store that counts every storage-primitive invocation."""

    def __init__(self) -> None:
        super().__init__()
        self.operations = 0

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        self.operations += 1
        return await super()._load(entity_key, key)

    async def _save(self, record: MemoryRecord) -> bool:
        self.operations += 1
        return await super()._save(record)

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        self.operations += 1
        return await super()._search(entity_key, prefix, limit)


async def test_invalid_arguments_never_reach_the_storage_primitive() -> None:
    # Scenario: Invalid arguments are rejected before any I/O — the "before"
    # half, provable only against a store that records its primitive calls.
    store = _RecordingStore()

    with pytest.raises(ValueError, match="key"):
        await store.save(a_record(""))
    with pytest.raises(ValueError, match="seq"):
        await store.save(a_record("k", seq=-1))
    with pytest.raises(ValueError, match="limit"):
        await store.search(ENTITY_A, "p", limit=0)
    with pytest.raises(ValueError, match="key"):
        await store.load(ENTITY_A, "")

    assert store.operations == 0


async def test_the_store_is_a_memorystore() -> None:
    assert isinstance(InMemoryMemoryStore(), MemoryStore)


async def test_close_is_a_no_op() -> None:
    store = InMemoryMemoryStore()
    assert await store.save(a_record())
    await store.close()


def test_the_seq_guard_rule_has_one_definition() -> None:
    # The base class owns "applies iff incoming >= stored (or nothing stored)";
    # backends encode it into their primitives but tests pin the reference.
    assert _seq_guard_applies(0, None)
    assert _seq_guard_applies(5, 5)
    assert _seq_guard_applies(6, 5)
    assert not _seq_guard_applies(4, 5)
