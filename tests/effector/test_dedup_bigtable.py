"""The Bigtable dedup store for the effector-dedup capability.

Runs the shared `DedupStore` conformance suite against the Bigtable emulator,
plus the Bigtable-specific requirement scenarios. Requires `make compose-up`
(the `bigtable-emulator` service on localhost:8086); override with
`BIGTABLE_EMULATOR_HOST`.

The emulator implements `CheckAndMutateRow` and the filter set this store
depends on, so the conditional-claim semantics — the part that actually has to
be atomic — are exercised for real rather than mocked.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import uuid
from collections.abc import AsyncIterator

import pytest

from beam_agents.effector.dedup import (
    BigtableDedupStore,
    Claimed,
    DedupStore,
    Done,
    InFlight,
    _encode_lease_expiry,
)

from ._dedup_conformance import LEASE_MS, TTL_MS, DedupStoreConformance, a_result

# Installed in the integration lane; see the note in test_service_integration.py.
Client = pytest.importorskip("google.cloud.bigtable").Client
MaxAgeGCRule = pytest.importorskip("google.cloud.bigtable.column_family").MaxAgeGCRule

pytestmark = [pytest.mark.integration, pytest.mark.slow]

EMULATOR_HOST = os.environ.get("BIGTABLE_EMULATOR_HOST", "localhost:8086")
PROJECT = "beam-agents-test"
INSTANCE = "effector"


def _create_table(table_id: str) -> None:
    """Provision a table with the effector's column family on the emulator."""
    client = Client(project=PROJECT, admin=True)
    instance = client.instance(INSTANCE)
    table = instance.table(table_id)
    # The GC rule is the production expiry mechanism for terminal records; the
    # value here only has to outlive the test.
    table.create(
        column_families={
            BigtableDedupStore.COLUMN_FAMILY: MaxAgeGCRule(datetime.timedelta(hours=1))
        }
    )
    client.close()


@pytest.fixture(autouse=True, scope="module")
def _emulator_env() -> None:
    os.environ.setdefault("BIGTABLE_EMULATOR_HOST", EMULATOR_HOST)
    # The emulator accepts any credentials; the client still wants a project.
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", PROJECT)


def _store(clock: object = None) -> BigtableDedupStore:
    table_id = f"dedup-{uuid.uuid4().hex[:12]}"
    _create_table(table_id)
    if clock is None:
        return BigtableDedupStore(PROJECT, INSTANCE, table_id)
    return BigtableDedupStore(PROJECT, INSTANCE, table_id, clock=clock)  # type: ignore[arg-type]


class TestBigtableDedupStoreConformance(DedupStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[DedupStore]:
        store = _store()
        yield store
        await store.close()

    async def advance(self, store: DedupStore, ms: int) -> None:
        # Lease expiry is decided against this store's clock, so a real sleep is
        # what moves it — the same thing that happens in production.
        await asyncio.sleep(ms / 1000)


async def test_claiming_is_conditional_in_a_single_conditional_mutation() -> None:
    # Scenario: Claiming is conditional in a single conditional mutation.
    store = _store()
    try:
        first, second = await asyncio.gather(
            store.claim("i-race", LEASE_MS), store.claim("i-race", LEASE_MS)
        )

        outcomes = [first, second]
        assert sum(isinstance(o, Claimed) for o in outcomes) == 1
        assert sum(isinstance(o, InFlight) for o in outcomes) == 1
    finally:
        await store.close()


async def test_lease_expiry_is_expressed_as_a_value_range_predicate() -> None:
    # Scenario: Lease expiry is expressed as a value-range predicate. Driving
    # the store's clock forward (rather than sleeping) isolates the predicate:
    # nothing but the encoded expiry value has changed.
    now = [1_700_000_000_000]
    store = _store(clock=lambda: now[0])
    try:
        first = await store.claim("i-1", LEASE_MS)
        assert isinstance(first, Claimed)
        assert isinstance(await store.claim("i-1", LEASE_MS), InFlight)

        now[0] += LEASE_MS + 1

        second = await store.claim("i-1", LEASE_MS)
        assert isinstance(second, Claimed)
        assert second.token != first.token
    finally:
        await store.close()


async def test_a_completed_row_reports_done() -> None:
    # Scenario: A completed row reports Done.
    store = _store()
    try:
        claimed = await store.claim("i-1", LEASE_MS)
        assert isinstance(claimed, Claimed)
        result = a_result("i-1")
        assert await store.complete("i-1", claimed.token, result, TTL_MS)

        outcome = await store.claim("i-1", LEASE_MS)

        assert isinstance(outcome, Done)
        assert outcome.result is not None
        assert outcome.result.SerializeToString(deterministic=True) == result.SerializeToString(
            deterministic=True
        )
    finally:
        await store.close()


async def test_terminal_expiry_is_a_read_time_predicate_not_a_gc_rule() -> None:
    # Scenario: Terminal-record expiry is a read-time predicate, not a GC rule.
    # The table's GC maxage is an hour and the TTL here is 600ms, so garbage
    # collection cannot be what expires this record. Driving the store's clock
    # instead of sleeping makes that airtight: no wall time passes at all, so
    # only the encoded `rexp` value can decide the outcome.
    now = [1_700_000_000_000]
    store = _store(clock=lambda: now[0])
    try:
        claimed = await store.claim("i-ttl", LEASE_MS)
        assert isinstance(claimed, Claimed)
        assert await store.complete("i-ttl", claimed.token, a_result("i-ttl"), TTL_MS)
        assert isinstance(await store.claim("i-ttl", LEASE_MS), Done)

        now[0] += TTL_MS + 1

        assert isinstance(await store.claim("i-ttl", LEASE_MS), Claimed)
    finally:
        await store.close()


async def test_a_superseded_owner_cell_cannot_satisfy_the_ownership_predicate() -> None:
    # Scenario: A superseded owner cell cannot satisfy the ownership predicate.
    # Bigtable keeps every cell version, so the re-claim below leaves the stale
    # token in the owner column underneath the fresh one. A predicate that can
    # match any version lets the expired worker complete over its successor.
    now = [1_700_000_000_000]
    store = _store(clock=lambda: now[0])
    try:
        stale = await store.claim("i-stale", LEASE_MS)
        assert isinstance(stale, Claimed)
        now[0] += LEASE_MS + 1
        fresh = await store.claim("i-stale", LEASE_MS)
        assert isinstance(fresh, Claimed)
        assert fresh.token != stale.token

        assert not await store.complete("i-stale", stale.token, a_result("i-stale"), TTL_MS)

        # The fresh owner's claim is untouched, and still completable.
        assert isinstance(await store.claim("i-stale", LEASE_MS), InFlight)
        assert await store.complete("i-stale", fresh.token, a_result("i-stale"), TTL_MS)
    finally:
        await store.close()


async def test_a_completed_row_never_collects_a_stray_claim() -> None:
    # The conditional mutation writes a claim only on the false branch, so a
    # terminal row is never re-claimed and then reported Done — which would
    # leave the next delivery waiting out a lease for no reason.
    store = _store()
    try:
        claimed = await store.claim("i-1", LEASE_MS)
        assert isinstance(claimed, Claimed)
        await store.complete("i-1", claimed.token, a_result("i-1"), TTL_MS)

        for _ in range(3):
            assert isinstance(await store.claim("i-1", LEASE_MS), Done)
    finally:
        await store.close()


async def test_ownership_survives_a_lease_expiry_byte_containing_a_newline() -> None:
    # Regression guard: the ownership predicate must never regex over binary.
    # A lease expiry whose big-endian encoding contains 0x0A would defeat an
    # RE2 `.*` prefix, so the token lives in its own column.
    newline_expiry = int.from_bytes(b"\x00\x00\x01\x8b\x0a\xe5\x68\x00", "big")
    assert b"\x0a" in _encode_lease_expiry(newline_expiry)
    now = [newline_expiry - LEASE_MS]
    store = _store(clock=lambda: now[0])
    try:
        claimed = await store.claim("i-1", LEASE_MS)
        assert isinstance(claimed, Claimed)

        assert await store.complete("i-1", claimed.token, a_result("i-1"), TTL_MS)
    finally:
        await store.close()
