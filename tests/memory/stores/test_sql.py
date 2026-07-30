"""The SQLAlchemy `MemoryStore` for the memory-stores capability.

Runs the shared conformance suite offline against ``sqlite+aiosqlite`` — no
docker — covering "The conformance suite passes offline on sqlite", plus the
SQL-specific requirement details: the escaped-`LIKE` literal prefix and the
documented (never implicitly executed) DDL. The async-engine-only
implementation is what satisfies "Store operations never block the event
loop", enforced by the ruff ASYNC rules over the store module itself.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from beam_agents.memory.stores.base import encode_envelope
from tests.memory.stores._conformance import ENTITY_A, MemoryStoreConformance, a_record

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from beam_agents.memory.stores import MemoryStore

# Offline leg of the SQL backend: sqlalchemy+aiosqlite ride the `test`
# dependency group (mirroring the `memory-stores` extra), but skip cleanly in
# an environment synced before that addition.
sqlalchemy = pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from beam_agents.memory.stores.sql import DDL, SqlMemoryStore  # noqa: E402


async def _make_store() -> SqlMemoryStore:
    store = SqlMemoryStore("sqlite+aiosqlite://")
    await store.ensure_schema()
    return store


class TestSqlMemoryStoreConformance(MemoryStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryStore]:
        store = await _make_store()
        yield store
        await store.close()


async def test_the_ddl_is_documented_not_implicit() -> None:
    # Scenario (requirement clause): the DDL ships as a documented statement;
    # a store pointed at an unprovisioned database does not create the table
    # behind the operator's back.
    assert "CREATE TABLE" in DDL
    assert "beam_agents_longterm" in DDL
    assert "PRIMARY KEY (entity_key, key)" in DDL

    store = SqlMemoryStore("sqlite+aiosqlite://")
    try:
        with pytest.raises(Exception, match=r"(?i)no such table"):
            await store.save(a_record())
    finally:
        await store.close()


async def test_like_metacharacters_in_the_prefix_are_escaped() -> None:
    # Scenario: Prefix metacharacters are literal — the SQL-specific risk this
    # requirement exists for: an unescaped `%` widens the scan silently.
    store = await _make_store()
    try:
        for key in ("a%b", "axb", "a_b", "aab", "a\\b", "a\\x"):
            assert await store.save(a_record(key, value=key.encode()))

        percent = await store.search(ENTITY_A, "a%", limit=10)
        underscore = await store.search(ENTITY_A, "a_", limit=10)
        backslash = await store.search(ENTITY_A, "a\\", limit=10)

        assert [r.key for r in percent] == ["a%b"]
        assert [r.key for r in underscore] == ["a_b"]
        assert [r.key for r in backslash] == ["a\\b", "a\\x"]
    finally:
        await store.close()


async def test_rows_store_the_envelope_byte_identically() -> None:
    # The cross-backend contract: what SQL stores in `rec` is the same
    # deterministic envelope every other backend stores.
    store = await _make_store()
    try:
        record = a_record("profile", seq=7)
        assert await store.save(record)

        async with store._engine.connect() as conn:
            table = store._table
            row = (
                await conn.execute(
                    sqlalchemy.select(table.c.rec).where(
                        table.c.entity_key == ENTITY_A, table.c.key == "profile"
                    )
                )
            ).scalar_one()

        assert row == encode_envelope(record)
    finally:
        await store.close()
