"""The Bigtable `MemoryStore` against the emulator (memory-stores capability).

Runs the shared conformance suite against the compose Bigtable emulator, which
implements `CheckAndMutateRow` and the filter set the seq guard depends on —
so the conditional-upsert semantics are exercised for real rather than mocked
(the offline call-shape tests live in `test_bigtable.py`). Requires
`make compose-up` (the `bigtable-emulator` service on localhost:8086);
override with `BIGTABLE_EMULATOR_HOST`.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from beam_agents.memory.stores import MemoryStore
from beam_agents.memory.stores.bigtable import BigtableMemoryStore

from ._conformance import ENTITY_A, MemoryStoreConformance, a_record

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Installed in the integration lane; also present in the unit lane via
# apache-beam[gcp], but the emulator is not.
Client = pytest.importorskip("google.cloud.bigtable").Client

pytestmark = [pytest.mark.integration, pytest.mark.slow]

EMULATOR_HOST = os.environ.get("BIGTABLE_EMULATOR_HOST", "localhost:8086")
PROJECT = "beam-agents-test"
INSTANCE = "memory-stores"


def _create_table(table_id: str) -> None:
    """Provision a table with the store's column family on the emulator."""
    client = Client(project=PROJECT, admin=True)
    instance = client.instance(INSTANCE)
    table = instance.table(table_id)
    table.create(column_families={BigtableMemoryStore.COLUMN_FAMILY: None})
    client.close()


@pytest.fixture(autouse=True, scope="module")
def _emulator_env() -> None:
    os.environ.setdefault("BIGTABLE_EMULATOR_HOST", EMULATOR_HOST)
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", PROJECT)


def _store() -> BigtableMemoryStore:
    table_id = f"ltm-{uuid.uuid4().hex[:12]}"
    _create_table(table_id)
    return BigtableMemoryStore(PROJECT, INSTANCE, table_id)


class TestBigtableMemoryStoreConformance(MemoryStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryStore]:
        store = _store()
        yield store
        await store.close()


async def test_a_superseded_seq_cell_never_decides_the_predicate() -> None:
    # Scenario: Only the latest seq cell decides the predicate — for real: the
    # seq column accumulates superseded cell versions as rows are rewritten,
    # and a stale older cell must neither satisfy nor defeat the guard.
    store = _store()
    try:
        # Build up cell-version history: seq 3, then 9, then back-fill attempts.
        assert await store.save(a_record("m", seq=3, value=b"v3"))
        assert await store.save(a_record("m", seq=9, value=b"v9"))
        # Incoming 5: the *latest* stored cell (9) is greater -> refused, even
        # though the superseded cell (3) is smaller.
        assert not await store.save(a_record("m", seq=5, value=b"v5"))
        loaded = await store.load(ENTITY_A, "m")
        assert loaded is not None and loaded.seq == 9 and loaded.value == b"v9"
        # Incoming 9 (equal to latest): accepted, despite older cells beneath.
        assert await store.save(a_record("m", seq=9, value=b"v9b"))
        loaded = await store.load(ENTITY_A, "m")
        assert loaded is not None and loaded.value == b"v9b"
    finally:
        await store.close()
