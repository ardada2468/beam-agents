"""The shared `MemoryStore` behavioral suite for the memory-stores capability.

Every backend-independent requirement — load/save round-trip, argument
validation, the seq-guarded idempotent upsert (including the full seq-pair
matrix), and the bounded per-entity prefix search — is expressed once, here,
and run against every implementation: the in-memory store (offline), sqlite
via SQLAlchemy (offline), and Redis / the Bigtable and Firestore emulators
(`-m integration`). A backend that passes this suite is substitutable for the
others, which is the whole point of the ABC.

Subclasses supply a ``store`` fixture; the same scenarios then run against
each backend's own atomic guard primitive without special-casing.
"""

from __future__ import annotations

import pytest

from beam_agents.memory.stores import MemoryRecord, MemoryStore

ENTITY_A = b"entity-a"
ENTITY_B = b"entity-b"


def a_record(
    key: str = "profile",
    *,
    entity_key: bytes = ENTITY_A,
    value: bytes = b'{"name":"ada"}',
    seq: int = 7,
    updated_at_ms: int = 1_700_000_000_000,
) -> MemoryRecord:
    return MemoryRecord(
        entity_key=entity_key, key=key, value=value, seq=seq, updated_at_ms=updated_at_ms
    )


class MemoryStoreConformance:
    """Behavioral contract every `MemoryStore` implementation must satisfy."""

    @pytest.fixture
    def store(self) -> MemoryStore:
        raise NotImplementedError("subclasses provide a store fixture")

    # -- Requirement: MemoryStore is an async ABC with load, save, and search --

    async def test_load_returns_the_saved_record_or_none(self, store: MemoryStore) -> None:
        # Scenario: Load returns the saved record or None.
        saved = a_record("profile")
        assert await store.save(saved)

        loaded = await store.load(ENTITY_A, "profile")
        absent = await store.load(ENTITY_A, "no-such-key")

        assert loaded == saved
        assert absent is None

    async def test_invalid_arguments_are_rejected_before_any_io(self, store: MemoryStore) -> None:
        # Scenario: Invalid arguments are rejected before any I/O.
        with pytest.raises(ValueError, match="key"):
            await store.save(a_record(""))
        with pytest.raises(ValueError, match="seq"):
            await store.save(a_record("profile", seq=-1))
        with pytest.raises(ValueError, match="limit"):
            await store.search(ENTITY_A, "p", limit=0)
        with pytest.raises(ValueError, match="limit"):
            await store.search(ENTITY_A, "p", limit=-3)
        with pytest.raises(ValueError, match="key"):
            await store.load(ENTITY_A, "")

    # -- Requirement: Save is an idempotent upsert guarded by seq -------------

    async def test_replayed_flush_converges_on_the_identical_row(self, store: MemoryStore) -> None:
        # Scenario: Replayed flush converges on the identical row.
        record = a_record("profile", seq=7)

        first = await store.save(record)
        after_first = await store.load(ENTITY_A, "profile")
        second = await store.save(record)
        after_second = await store.load(ENTITY_A, "profile")

        assert first and second
        assert after_first == after_second == record

    async def test_a_stale_seq_cannot_regress_a_newer_row(self, store: MemoryStore) -> None:
        # Scenario: A stale seq cannot regress a newer row.
        newer = a_record("profile", seq=7, value=b"new")
        assert await store.save(newer)

        applied = await store.save(a_record("profile", seq=5, value=b"stale"))

        assert not applied
        assert await store.load(ENTITY_A, "profile") == newer

    async def test_a_newer_seq_overwrites(self, store: MemoryStore) -> None:
        # Scenario: A newer seq overwrites.
        assert await store.save(a_record("profile", seq=5, value=b"old"))
        newer = a_record("profile", seq=7, value=b"new")

        assert await store.save(newer)

        assert await store.load(ENTITY_A, "profile") == newer

    async def test_the_seq_pair_matrix_holds_against_absent_and_present_rows(
        self, store: MemoryStore
    ) -> None:
        # The full guard matrix the requirement demands: lower / equal / higher
        # incoming seq against an absent row and a present one.
        # Absent row: every non-negative seq applies (there is nothing stored).
        assert await store.save(a_record("m-absent-low", seq=0))
        assert await store.save(a_record("m-absent-high", seq=9))

        # Present row at seq 5.
        base = a_record("m-present", seq=5, value=b"base")
        assert await store.save(base)
        # Lower: refused, row untouched.
        assert not await store.save(a_record("m-present", seq=4, value=b"lower"))
        assert await store.load(ENTITY_A, "m-present") == base
        # Equal: accepted (a replayed activation rewrites its own row).
        equal = a_record("m-present", seq=5, value=b"base")
        assert await store.save(equal)
        assert await store.load(ENTITY_A, "m-present") == equal
        # Higher: accepted, row replaced.
        higher = a_record("m-present", seq=6, value=b"higher")
        assert await store.save(higher)
        assert await store.load(ENTITY_A, "m-present") == higher

    # -- Requirement: Search is a bounded per-entity key-prefix scan ----------

    async def test_prefix_search_returns_ordered_bounded_entity_scoped_results(
        self, store: MemoryStore
    ) -> None:
        # Scenario: Prefix search returns ordered, bounded, entity-scoped results.
        for key in ("case/2", "note/1", "case/1", "case/3"):
            assert await store.save(a_record(key, value=key.encode()))
        assert await store.save(a_record("case/9", entity_key=ENTITY_B))

        results = await store.search(ENTITY_A, "case/", limit=2)

        assert [r.key for r in results] == ["case/1", "case/2"]
        assert all(r.entity_key == ENTITY_A for r in results)

    async def test_an_empty_prefix_matches_all_of_the_entitys_records(
        self, store: MemoryStore
    ) -> None:
        # The requirement's empty-prefix clause: match everything, still bounded.
        for key in ("b", "a", "c"):
            assert await store.save(a_record(key))

        results = await store.search(ENTITY_A, "", limit=2)

        assert [r.key for r in results] == ["a", "b"]

    async def test_prefix_metacharacters_are_literal(self, store: MemoryStore) -> None:
        # Scenario: Prefix metacharacters are literal.
        assert await store.save(a_record("a%b", value=b"percent"))
        assert await store.save(a_record("axb", value=b"literal-x"))
        assert await store.save(a_record("a_b", value=b"underscore"))

        percent = await store.search(ENTITY_A, "a%", limit=10)
        underscore = await store.search(ENTITY_A, "a_", limit=10)

        assert [r.key for r in percent] == ["a%b"]
        assert [r.key for r in underscore] == ["a_b"]

    async def test_search_round_trips_the_full_record(self, store: MemoryStore) -> None:
        # Search results carry the same fields a load would return.
        record = a_record("case/1", seq=3, value=b"v", updated_at_ms=1_700_000_000_123)
        assert await store.save(record)

        results = await store.search(ENTITY_A, "case/", limit=1)

        assert results == [record]
