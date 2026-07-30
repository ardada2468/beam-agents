"""The Firestore `MemoryStore` against the emulator (memory-stores capability).

Runs the shared conformance suite against the compose Firestore emulator,
covering "The Firestore store guards upserts with a transaction": the
transactional read-compare-write is the backend's atomic guard, and the
seq-guard and prefix-search requirements must hold through it. Requires
`make compose-up` (the `firestore-emulator` service on localhost:8087);
override with `FIRESTORE_EMULATOR_HOST`.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from beam_agents.memory.stores import MemoryStore
from beam_agents.memory.stores.firestore import FirestoreMemoryStore

from ._conformance import ENTITY_A, MemoryStoreConformance, a_record

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Installed in the integration lane only (mirrored from the memory-stores extra).
pytest.importorskip("google.cloud.firestore")

pytestmark = [pytest.mark.integration, pytest.mark.slow]

EMULATOR_HOST = os.environ.get("FIRESTORE_EMULATOR_HOST", "localhost:8087")
PROJECT = "beam-agents-test"


@pytest.fixture(autouse=True, scope="module")
def _emulator_env() -> None:
    os.environ.setdefault("FIRESTORE_EMULATOR_HOST", EMULATOR_HOST)
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", PROJECT)


def _store() -> FirestoreMemoryStore:
    # A per-test collection keeps reruns from colliding on the fixed entity
    # keys the conformance suite uses.
    return FirestoreMemoryStore(PROJECT, f"ltm-{uuid.uuid4().hex[:12]}")


class TestFirestoreMemoryStoreConformance(MemoryStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryStore]:
        store = _store()
        yield store
        await store.close()


async def test_a_stale_seq_save_never_overwrites_mid_transaction() -> None:
    # Scenario: Transactional guard under the conformance suite — the
    # stale-write half stated explicitly: a stale-seq save observed by the
    # transaction never overwrites the newer document.
    store = _store()
    try:
        newer = a_record("m", seq=9, value=b"v9")
        assert await store.save(newer)

        assert not await store.save(a_record("m", seq=2, value=b"stale"))

        assert await store.load(ENTITY_A, "m") == newer
    finally:
        await store.close()
