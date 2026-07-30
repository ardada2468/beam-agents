"""`MemoryStore` over SQLAlchemy: portable transactional seq-guarded upserts.

Async engine throughout (the store runs on the bridge loop and must never
block it — ruff ASYNC rules apply); table ``beam_agents_longterm`` with
primary key ``(entity_key, key)`` plus ``seq``, ``rec`` (envelope bytes), and
``updated_at_ms`` columns. ``save`` is a transactional read-compare-write —
deliberately portable across dialects rather than ``ON CONFLICT DO UPDATE …
WHERE``, which is dialect-specific — acquiring a row lock where the dialect
supports it (``SELECT … FOR UPDATE``; SQLite's compiler drops the clause).
``search`` is an escaped-``LIKE`` prefix with ``ORDER BY key`` and ``LIMIT``,
so ``%``/``_`` in a prefix are always literal (design D8, D7).

The required DDL ships as the documented :data:`DDL` statement (and the
explicit :meth:`SqlMemoryStore.ensure_schema` helper for tests/provisioning);
it is never executed implicitly at runtime.

SQLAlchemy is imported inside the constructor: it belongs to the optional
``memory-stores`` extra.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from beam_agents.memory.stores.base import (
    MemoryRecord,
    MemoryStore,
    decode_envelope,
    encode_envelope,
    missing_client_error,
    seq_guard_applies,
)

if TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine

_TABLE_NAME = "beam_agents_longterm"

#: The documented provisioning statement (PostgreSQL types shown; other
#: dialects map equivalently — sqlite stores both as BLOB/INTEGER). Run it —
#: or `ensure_schema()` — explicitly before pointing a pipeline at the store;
#: the store never auto-migrates.
DDL = """\
CREATE TABLE beam_agents_longterm (
  entity_key BYTEA NOT NULL,
  key TEXT NOT NULL,
  seq BIGINT NOT NULL,
  rec BYTEA NOT NULL,
  updated_at_ms BIGINT NOT NULL,
  PRIMARY KEY (entity_key, key)
)
"""

_LIKE_ESCAPE = "\\"


def _escape_like_prefix(prefix: str) -> str:
    """Escape ``LIKE`` metacharacters so the prefix is always a literal."""
    return (
        prefix.replace(_LIKE_ESCAPE, _LIKE_ESCAPE + _LIKE_ESCAPE)
        .replace("%", _LIKE_ESCAPE + "%")
        .replace("_", _LIKE_ESCAPE + "_")
    )


class SqlMemoryStore(MemoryStore):
    """`MemoryStore` over any SQLAlchemy async URL; see the module docstring."""

    def __init__(self, url: str) -> None:
        try:
            import sqlalchemy
            from sqlalchemy.ext.asyncio import create_async_engine
        except ImportError as exc:
            raise missing_client_error("SqlMemoryStore", "sqlalchemy", exc) from exc

        self._sa = sqlalchemy
        self._engine: AsyncEngine = create_async_engine(url)
        metadata = sqlalchemy.MetaData()
        self._table: Table = sqlalchemy.Table(
            _TABLE_NAME,
            metadata,
            sqlalchemy.Column("entity_key", sqlalchemy.LargeBinary, primary_key=True),
            sqlalchemy.Column("key", sqlalchemy.String, primary_key=True),
            sqlalchemy.Column("seq", sqlalchemy.BigInteger, nullable=False),
            sqlalchemy.Column("rec", sqlalchemy.LargeBinary, nullable=False),
            sqlalchemy.Column("updated_at_ms", sqlalchemy.BigInteger, nullable=False),
        )
        self._metadata = metadata

    async def ensure_schema(self) -> None:
        """Create the table if absent. Explicit provisioning for tests and
        operators — never called implicitly by any store operation.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(self._metadata.create_all)

    async def _load(self, entity_key: bytes, key: str) -> MemoryRecord | None:
        table = self._table
        stmt = self._sa.select(table.c.rec).where(
            table.c.entity_key == entity_key, table.c.key == key
        )
        async with self._engine.connect() as conn:
            envelope: bytes | None = (await conn.execute(stmt)).scalar_one_or_none()
        if envelope is None:
            return None
        return decode_envelope(entity_key, envelope)

    async def _save(self, record: MemoryRecord) -> bool:
        sa = self._sa
        table = self._table
        async with self._engine.begin() as conn:
            select = (
                sa.select(table.c.seq)
                .where(
                    table.c.entity_key == record.entity_key,
                    table.c.key == record.key,
                )
                .with_for_update()
            )
            stored_seq: int | None = (await conn.execute(select)).scalar_one_or_none()
            if not seq_guard_applies(record.seq, stored_seq):
                return False
            values: dict[str, Any] = {
                "seq": record.seq,
                "rec": encode_envelope(record),
                "updated_at_ms": record.updated_at_ms,
            }
            if stored_seq is None:
                await conn.execute(
                    sa.insert(table).values(entity_key=record.entity_key, key=record.key, **values)
                )
            else:
                await conn.execute(
                    sa.update(table)
                    .where(
                        table.c.entity_key == record.entity_key,
                        table.c.key == record.key,
                    )
                    .values(**values)
                )
            return True

    async def _search(self, entity_key: bytes, prefix: str, limit: int) -> list[MemoryRecord]:
        sa = self._sa
        table = self._table
        stmt = (
            sa.select(table.c.rec)
            .where(
                table.c.entity_key == entity_key,
                table.c.key.like(_escape_like_prefix(prefix) + "%", escape=_LIKE_ESCAPE),
            )
            .order_by(table.c.key.asc())
            .limit(limit)
        )
        async with self._engine.connect() as conn:
            envelopes = [row[0] for row in (await conn.execute(stmt)).all()]
        return [decode_envelope(entity_key, envelope) for envelope in envelopes]

    async def close(self) -> None:
        await self._engine.dispose()
