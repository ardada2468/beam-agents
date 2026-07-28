"""The Redis dedup store for the effector-dedup capability.

Runs the shared `DedupStore` conformance suite against a live Redis, plus the
Redis-specific requirement scenarios. Requires `make compose-up` (Redis on
localhost:16379); override with `BEAM_AGENTS_REDIS_URL`.

Lease and TTL scenarios here exercise *server-side* expiry, which is the point
of the Redis implementation: the in-memory store can fake a clock, but a real
deployment has several workers with skewed ones, so expiry has to be the
server's opinion.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest

from beam_agents.effector.dedup import Claimed, DedupStore, Done, InFlight, RedisDedupStore

from ._dedup_conformance import LEASE_MS, TTL_MS, DedupStoreConformance, a_result

pytestmark = [pytest.mark.integration, pytest.mark.slow]

REDIS_URL = os.environ.get("BEAM_AGENTS_REDIS_URL", "redis://localhost:16379")


def _store() -> RedisDedupStore:
    # A per-test key prefix keeps parallel runs and reruns from colliding on
    # the fixed intent ids the conformance suite uses.
    return RedisDedupStore(REDIS_URL, key_prefix=f"beam-agents-test:{uuid.uuid4().hex}:")


class TestRedisDedupStoreConformance(DedupStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[DedupStore]:
        store = _store()
        yield store
        await store.close()


async def test_claim_complete_and_re_claim_round_trip_against_redis() -> None:
    # Scenario: Claim, complete, and re-claim round-trip against Redis.
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


async def test_a_stale_owner_cannot_clobber_the_new_owners_record_in_redis() -> None:
    # Scenario: A stale owner cannot clobber the new owner's record. The
    # compare-and-set script is what makes this true: without it, a worker
    # whose lease expired mid-execution would overwrite its successor's result.
    store = _store()
    try:
        stale = await store.claim("i-1", 200)
        assert isinstance(stale, Claimed)
        await asyncio.sleep(0.4)
        fresh = await store.claim("i-1", LEASE_MS)
        assert isinstance(fresh, Claimed)

        assert not await store.complete(
            "i-1", stale.token, a_result("i-1", payload=b"stale"), TTL_MS
        )

        assert isinstance(await store.claim("i-1", LEASE_MS), InFlight)
    finally:
        await store.close()


async def test_lease_expiry_is_the_servers_opinion_not_the_clients() -> None:
    # The store never reads a local clock for leases: expiry comes from Redis
    # key TTL, so workers with skewed clocks still agree on who owns what.
    store = _store()
    try:
        assert isinstance(await store.claim("i-1", 200), Claimed)
        assert isinstance(await store.claim("i-1", 200), InFlight)

        await asyncio.sleep(0.4)

        assert isinstance(await store.claim("i-1", 200), Claimed)
    finally:
        await store.close()


async def test_a_routed_approval_is_terminal_without_a_result_in_redis() -> None:
    store = _store()
    try:
        claimed = await store.claim("i-approval", LEASE_MS)
        assert isinstance(claimed, Claimed)
        assert await store.complete("i-approval", claimed.token, None, TTL_MS)

        outcome = await store.claim("i-approval", LEASE_MS)

        assert isinstance(outcome, Done)
        assert outcome.result is None
    finally:
        await store.close()
