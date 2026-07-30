"""The Redis `MemoryStore` against a live Redis (memory-stores capability).

Runs the shared conformance suite plus the Redis-specific requirement — "The
script applies the seq-pair matrix atomically" — against a real server, which
is the point of the Lua compare-and-set: the guard holds server-side, with no
client-side read-modify-write race. Requires `make compose-up` (Redis on
localhost:16379); override with `BEAM_AGENTS_REDIS_URL`.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from beam_agents.memory.stores import MemoryStore
from beam_agents.memory.stores.redis import RedisMemoryStore

from ._conformance import ENTITY_A, MemoryStoreConformance, a_record

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.slow]

REDIS_URL = os.environ.get("BEAM_AGENTS_REDIS_URL", "redis://localhost:16379")


def _store() -> RedisMemoryStore:
    # A per-test key prefix keeps parallel runs and reruns from colliding on
    # the fixed entity keys the conformance suite uses.
    return RedisMemoryStore(REDIS_URL, key_prefix=f"beam-agents-test:ltm:{uuid.uuid4().hex}:")


class TestRedisMemoryStoreConformance(MemoryStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryStore]:
        store = _store()
        yield store
        await store.close()


async def test_the_script_applies_the_seq_pair_matrix_atomically() -> None:
    # Scenario: The script applies the seq-pair matrix atomically.
    store = _store()
    try:
        # Absent row: applies.
        assert await store.save(a_record("m", seq=5, value=b"v5"))
        # Lower: not applied, framed value untouched.
        assert not await store.save(a_record("m", seq=4, value=b"v4"))
        loaded = await store.load(ENTITY_A, "m")
        assert loaded is not None and loaded.seq == 5 and loaded.value == b"v5"
        # Equal: replaced in a single scripted operation.
        assert await store.save(a_record("m", seq=5, value=b"v5b"))
        loaded = await store.load(ENTITY_A, "m")
        assert loaded is not None and loaded.value == b"v5b"
        # Higher: replaced.
        assert await store.save(a_record("m", seq=6, value=b"v6"))
        loaded = await store.load(ENTITY_A, "m")
        assert loaded is not None and loaded.seq == 6 and loaded.value == b"v6"
    finally:
        await store.close()
