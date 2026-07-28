"""The shared `DedupStore` behavioral suite for the effector-dedup capability.

Every requirement in "DedupStore is a three-state claim protocol" and "Lease and
result TTLs bound in-flight and terminal records" is expressed once, here, and
run against every backend: the in-memory store (offline), Redis, and Bigtable.
A backend that passes this suite is substitutable for the others — which is the
whole point of the protocol.

Subclasses supply a ``store`` fixture and an ``advance`` implementation: the
in-memory store moves an injectable clock, the real backends sleep, so the same
lease/TTL scenarios run against server-side expiry without special-casing.
"""

from __future__ import annotations

import asyncio

import pytest

from beam_agents._protos import ToolResult
from beam_agents.effector.dedup import Claimed, DedupStore, Done, InFlight

# Short enough that the real backends can sleep through them, long enough that a
# slow CI machine does not expire a lease mid-assertion.
LEASE_MS = 600
TTL_MS = 600


def a_result(intent_id: str = "i-1", *, payload: bytes = b'{"ok":true}') -> ToolResult:
    return ToolResult(
        intent_id=intent_id,
        entity_key=b"k-1",
        seq=7,
        status=ToolResult.OK,
        payload=payload,
        completed_at_ms=1_700_000_000_000,
    )


class DedupStoreConformance:
    """Behavioral contract every `DedupStore` implementation must satisfy."""

    @pytest.fixture
    def store(self) -> DedupStore:
        raise NotImplementedError("subclasses provide a store fixture")

    async def advance(self, store: DedupStore, ms: int) -> None:
        """Move time forward by ``ms`` for this backend."""
        await asyncio.sleep(ms / 1000)

    async def test_a_first_claim_on_an_unseen_intent_is_granted(self, store: DedupStore) -> None:
        # Scenario: A first claim on an unseen intent is granted.
        outcome = await store.claim("i-new", LEASE_MS)

        assert isinstance(outcome, Claimed)
        assert outcome.token

    async def test_concurrent_claims_yield_a_single_owner(self, store: DedupStore) -> None:
        # Scenario: Concurrent claims yield a single owner.
        first, second = await asyncio.gather(
            store.claim("i-race", LEASE_MS),
            store.claim("i-race", LEASE_MS),
        )

        outcomes = [first, second]
        assert sum(isinstance(o, Claimed) for o in outcomes) == 1
        assert sum(isinstance(o, InFlight) for o in outcomes) == 1

    async def test_a_completed_intent_reports_done_with_its_stored_result(
        self, store: DedupStore
    ) -> None:
        # Scenario: A completed intent reports Done with its stored result.
        claimed = await store.claim("i-done", LEASE_MS)
        assert isinstance(claimed, Claimed)
        result = a_result("i-done")
        assert await store.complete("i-done", claimed.token, result, TTL_MS)

        outcome = await store.claim("i-done", LEASE_MS)

        assert isinstance(outcome, Done)
        assert outcome.result is not None
        assert outcome.result.intent_id == result.intent_id
        assert outcome.result.status == ToolResult.OK
        assert outcome.result.payload == result.payload
        assert outcome.result.seq == result.seq
        assert outcome.result.entity_key == result.entity_key

    async def test_the_republished_result_is_byte_identical_to_the_stored_one(
        self, store: DedupStore
    ) -> None:
        # Scenario: The republished result is byte-identical to the stored one.
        claimed = await store.claim("i-bytes", LEASE_MS)
        assert isinstance(claimed, Claimed)
        result = a_result("i-bytes")
        stored_bytes = result.SerializeToString(deterministic=True)
        await store.complete("i-bytes", claimed.token, result, TTL_MS)

        outcome = await store.claim("i-bytes", LEASE_MS)

        assert isinstance(outcome, Done)
        assert outcome.result is not None
        assert outcome.result.SerializeToString(deterministic=True) == stored_bytes

    async def test_completion_by_a_non_owner_is_refused(self, store: DedupStore) -> None:
        # Scenario: Completion by a non-owner is refused.
        claimed = await store.claim("i-owner", LEASE_MS)
        assert isinstance(claimed, Claimed)

        assert not await store.complete("i-owner", "not-the-token", a_result("i-owner"), TTL_MS)

        # The record still reflects the original claim: still in flight, not done.
        assert isinstance(await store.claim("i-owner", LEASE_MS), InFlight)

    async def test_release_frees_the_intent_for_a_new_owner(self, store: DedupStore) -> None:
        # Scenario: Release frees the intent for a new owner.
        claimed = await store.claim("i-release", LEASE_MS)
        assert isinstance(claimed, Claimed)

        assert await store.release("i-release", claimed.token)
        second = await store.claim("i-release", LEASE_MS)

        assert isinstance(second, Claimed)
        assert second.token != claimed.token

    async def test_release_by_a_non_owner_is_refused(self, store: DedupStore) -> None:
        # The mirror of the completion guard: a worker whose lease expired must
        # not be able to free the new owner's claim.
        claimed = await store.claim("i-release-guard", LEASE_MS)
        assert isinstance(claimed, Claimed)

        assert not await store.release("i-release-guard", "not-the-token")
        assert isinstance(await store.claim("i-release-guard", LEASE_MS), InFlight)

    async def test_an_unexpired_lease_is_not_re_claimable(self, store: DedupStore) -> None:
        # Scenario: An unexpired lease is not re-claimable.
        assert isinstance(await store.claim("i-lease", LEASE_MS), Claimed)

        assert isinstance(await store.claim("i-lease", LEASE_MS), InFlight)

    async def test_an_expired_lease_is_re_claimable(self, store: DedupStore) -> None:
        # Scenario: An expired lease is re-claimable.
        first = await store.claim("i-lease-expiry", LEASE_MS)
        assert isinstance(first, Claimed)

        await self.advance(store, LEASE_MS + 100)
        second = await store.claim("i-lease-expiry", LEASE_MS)

        assert isinstance(second, Claimed)
        assert second.token != first.token

    async def test_an_expired_terminal_record_reads_as_unseen(self, store: DedupStore) -> None:
        # Scenario: An expired terminal record reads as unseen.
        claimed = await store.claim("i-ttl", LEASE_MS)
        assert isinstance(claimed, Claimed)
        await store.complete("i-ttl", claimed.token, a_result("i-ttl"), TTL_MS)

        await self.advance(store, TTL_MS + 100)
        outcome = await store.claim("i-ttl", LEASE_MS)

        assert isinstance(outcome, Claimed)

    async def test_a_terminal_record_without_a_result_reports_done_with_none(
        self, store: DedupStore
    ) -> None:
        # Approval-kind intents are marked terminal so redelivery cannot
        # double-notify, but they publish no ToolResult (effector-execution:
        # "Approval intents are routed ... and never executed").
        claimed = await store.claim("i-approval", LEASE_MS)
        assert isinstance(claimed, Claimed)
        assert await store.complete("i-approval", claimed.token, None, TTL_MS)

        outcome = await store.claim("i-approval", LEASE_MS)

        assert isinstance(outcome, Done)
        assert outcome.result is None

    async def test_a_stale_owner_cannot_clobber_the_new_owners_record(
        self, store: DedupStore
    ) -> None:
        # Scenario: A stale owner cannot clobber the new owner's record (stated
        # for Redis, required of every backend: `complete` is conditional on
        # still owning the claim, and an expired lease is no longer owned).
        stale = await store.claim("i-stale", LEASE_MS)
        assert isinstance(stale, Claimed)
        await self.advance(store, LEASE_MS + 100)
        fresh = await store.claim("i-stale", LEASE_MS)
        assert isinstance(fresh, Claimed)

        assert not await store.complete(
            "i-stale", stale.token, a_result("i-stale", payload=b"stale"), TTL_MS
        )

        # The fresh owner still holds an unresolved claim.
        assert isinstance(await store.claim("i-stale", LEASE_MS), InFlight)
        assert await store.complete(
            "i-stale", fresh.token, a_result("i-stale", payload=b"fresh"), TTL_MS
        )
        outcome = await store.claim("i-stale", LEASE_MS)
        assert isinstance(outcome, Done)
        assert outcome.result is not None
        assert outcome.result.payload == b"fresh"

    async def test_distinct_intent_ids_do_not_interfere(self, store: DedupStore) -> None:
        # Dedup is per intent_id; correctness invariant 2 makes those ids
        # deterministic, so cross-talk between them would collapse real effects.
        first = await store.claim("i-a", LEASE_MS)
        second = await store.claim("i-b", LEASE_MS)

        assert isinstance(first, Claimed)
        assert isinstance(second, Claimed)
