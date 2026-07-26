"""The in-memory dedup store for the effector-dedup capability.

Runs the shared `DedupStore` conformance suite (`_dedup_conformance.py`) against
`InMemoryDedupStore` offline, and pins the encoding the Bigtable range filter
depends on.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from beam_agents.effector.dedup import (
    Claimed,
    DedupStore,
    Done,
    InFlight,
    InMemoryDedupStore,
    decode_lease_expiry,
    encode_lease_expiry,
)

from ._dedup_conformance import LEASE_MS, DedupStoreConformance, a_result


class _FakeClock:
    """Injectable millisecond clock, so lease/TTL scenarios never sleep."""

    def __init__(self, now_ms: int = 1_700_000_000_000) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


class TestInMemoryDedupStoreConformance(DedupStoreConformance):
    @pytest.fixture
    def store(self) -> DedupStore:
        return InMemoryDedupStore(clock=_FakeClock())

    async def advance(self, store: DedupStore, ms: int) -> None:
        assert isinstance(store, InMemoryDedupStore)
        clock = store.clock
        assert isinstance(clock, _FakeClock)
        clock.now_ms += ms


async def test_the_store_is_a_dedupstore() -> None:
    # The protocol is runtime-checkable so a misconfigured injection fails at
    # construction rather than on the first intent.
    assert isinstance(InMemoryDedupStore(), DedupStore)


async def test_an_expired_claim_is_forgotten_rather_than_reported_done() -> None:
    # An expired lease must read as re-claimable, never as a terminal record:
    # confusing the two would publish a result that was never produced.
    clock = _FakeClock()
    store = InMemoryDedupStore(clock=clock)
    assert isinstance(await store.claim("i-1", LEASE_MS), Claimed)

    clock.now_ms += LEASE_MS

    outcome = await store.claim("i-1", LEASE_MS)
    assert isinstance(outcome, Claimed)
    assert not isinstance(outcome, Done)


async def test_completing_an_unclaimed_intent_is_refused() -> None:
    store = InMemoryDedupStore(clock=_FakeClock())

    assert not await store.complete("never-claimed", "token", a_result(), 1_000)
    assert isinstance(await store.claim("never-claimed", LEASE_MS), Claimed)


async def test_completing_an_already_completed_intent_is_refused() -> None:
    # The claim is consumed by completion, so a second complete cannot rewrite
    # a terminal result — redelivery republishes, it does not overwrite.
    clock = _FakeClock()
    store = InMemoryDedupStore(clock=clock)
    claimed = await store.claim("i-1", LEASE_MS)
    assert isinstance(claimed, Claimed)
    await store.complete("i-1", claimed.token, a_result(payload=b"first"), 10_000)

    assert not await store.complete("i-1", claimed.token, a_result(payload=b"second"), 10_000)

    outcome = await store.claim("i-1", LEASE_MS)
    assert isinstance(outcome, Done)
    assert outcome.result is not None
    assert outcome.result.payload == b"first"


async def test_a_lease_expiring_exactly_at_now_reads_as_expired() -> None:
    # Fail-closed boundary, matching hitl.intent_expired: the inclusive edge
    # belongs to "expired", so a lease can never be indefinitely live.
    clock = _FakeClock()
    store = InMemoryDedupStore(clock=clock)
    claimed = await store.claim("i-1", LEASE_MS)
    assert isinstance(claimed, Claimed)

    clock.now_ms += LEASE_MS

    assert isinstance(await store.claim("i-1", LEASE_MS), Claimed)


async def test_in_flight_is_reported_for_a_live_claim() -> None:
    store = InMemoryDedupStore(clock=_FakeClock())
    await store.claim("i-1", LEASE_MS)

    assert isinstance(await store.claim("i-1", LEASE_MS), InFlight)


@given(st.integers(min_value=0, max_value=2**63 - 1), st.integers(min_value=0, max_value=2**63 - 1))
def test_lease_encoding_preserves_numeric_order_under_byte_comparison(a: int, b: int) -> None:
    # Bigtable's ValueRangeFilter compares values lexicographically, so the
    # "lease not yet expired" predicate is only correct if byte order agrees
    # with numeric order over the whole range.
    assert (encode_lease_expiry(a) < encode_lease_expiry(b)) == (a < b)
    assert decode_lease_expiry(encode_lease_expiry(a)) == a


def test_lease_encoding_is_fixed_width() -> None:
    # Fixed width is what makes lexicographic comparison total: variable-width
    # encodings sort "10" before "9".
    assert len(encode_lease_expiry(0)) == len(encode_lease_expiry(2**63))


def test_a_negative_lease_expiry_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        encode_lease_expiry(-1)
