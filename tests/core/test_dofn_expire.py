"""`on_expire`: demoting expiring working memory to the long-term tier.

Covers "on_expire flushes expiring working memory to the long-term tier before
the TTL wipe". Driven with fake state handles and a fake `MemoryStore` rather
than through a pipeline, for the same reason `test_dofn_ttl` is: the behavior
under test is *what the callback does with the state it is handed*, including
the two orderings that matter — flush before wipe, and no wipe at all when the
flush fails.

TTL fire has no activation context (design D4), so this is the one place the
runtime performs the side effect correctness invariant 5 carves out: a single
idempotent upsert keyed by `(entity_key, seq)`, bounded on the async bridge.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import Continuation, LlmCacheBlob, MemoryBlob, ToolIntent
from beam_agents.core.agent import Complete
from beam_agents.core.bridge import ActivationTimeout
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import REASON_TTL_WIPED_SUSPENSION, _AgentDoFn
from beam_agents.memory import ExpiringMemory, FlushToLongterm, Memory
from beam_agents.memory.stores import InMemoryMemoryStore, MemoryRecord, MemoryStore
from beam_agents.memory.stores.base import _encode_envelope
from beam_agents.model.fake import FakeLLM
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeValue

_KEY = b"k"
_SEQ = 4
_FIRED_AT_MS = 12_000


class _RecordingStore(InMemoryMemoryStore):
    """Reference store recording every save's envelope bytes; can fail closed."""

    def __init__(self, *, fail_saves: bool = False) -> None:
        super().__init__()
        self.fail_saves = fail_saves
        self.saved: list[MemoryRecord] = []
        self.envelopes: list[bytes] = []

    async def _save(self, record: MemoryRecord) -> bool:
        if self.fail_saves:
            raise ConnectionError("long-term store unreachable")
        self.saved.append(record)
        self.envelopes.append(_encode_envelope(record))
        return await super()._save(record)


async def _unused_agent(ctx: ActivationContext) -> Complete:  # pragma: no cover - never run
    raise AssertionError("the TTL callback must not run an activation")


def _memory_blob() -> MemoryBlob:
    memory = Memory(now_ms=1_000)
    memory.append("log", b"one", max_items=8)
    memory.set("profile", b"vip")
    return memory.to_blob()


class _Expiry:
    """One DoFn plus the fake handles a single `on_ttl` firing is given."""

    def __init__(
        self,
        store: _RecordingStore | None,
        *,
        on_expire: Any = None,
        memory_blob: MemoryBlob | None = None,
        continuation: Continuation | None = None,
        monkeypatch: pytest.MonkeyPatch,
        activation_timeout_s: float = 30.0,
    ) -> None:
        if store is not None:
            monkeypatch.setattr(
                "beam_agents.core.dofn.build_memory_store",
                lambda scheme, parts: store,
            )
        self.dofn = _AgentDoFn(
            _unused_agent,
            provider_factory=FakeLLM,
            longterm_memory="memory://" if store is not None else None,
            on_expire=on_expire,
            activation_timeout_s=activation_timeout_s,
            cancel_grace_s=0.5,
        )
        self.memory = FakeValue(memory_blob)
        self.continuation = FakeValue(continuation)
        self.llm_cache = FakeValue(LlmCacheBlob())
        self.pending = FakeBag([ToolIntent(intent_id="intent-1")])
        self.seq = FakeSum(_SEQ)

    def fire(self) -> list[Any]:
        self.dofn.setup()
        try:
            return list(
                self.dofn.on_ttl(
                    key=_KEY,
                    timestamp=Timestamp(micros=_FIRED_AT_MS * 1000),
                    memory=self.memory,
                    continuation=self.continuation,
                    llm_cache=self.llm_cache,
                    pending=self.pending,
                    seq=self.seq,
                )
            )
        finally:
            self.dofn.teardown()

    @property
    def states(self) -> list[Any]:
        return [self.memory, self.continuation, self.llm_cache, self.pending, self.seq]


def test_expiring_memory_lands_in_the_long_term_tier_and_state_is_wiped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Expiring memory lands in the long-term tier and state is wiped.
    store = _RecordingStore()
    blob = _memory_blob()
    expiry = _Expiry(store, on_expire=FlushToLongterm(), memory_blob=blob, monkeypatch=monkeypatch)

    emitted = expiry.fire()

    assert emitted == []
    assert len(store.saved) == 1
    record = store.saved[0]
    assert record.entity_key == _KEY
    assert record.seq == _SEQ
    # The timer's firing timestamp is the expiry time — a replay-stable clock,
    # never a wall-clock reading.
    assert record.updated_at_ms == _FIRED_AT_MS
    assert record.value == blob.SerializeToString(deterministic=True)
    # ...and only then the wipe.
    assert all(state.cleared for state in expiry.states)


def test_a_retried_timer_bundle_deduplicates_to_one_logical_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: A retried timer bundle deduplicates to one logical write. The
    # upsert is a pure function of committed keyed state and the firing
    # timestamp, so a re-fired bundle re-derives byte-identical bytes and the
    # store's `(entity_key, seq)` guard collapses them onto one row.
    store = _RecordingStore()
    blob = _memory_blob()
    first = _Expiry(store, on_expire=FlushToLongterm(), memory_blob=blob, monkeypatch=monkeypatch)
    first.fire()
    # The retry reads the same (un-wiped, rolled-back) state the first attempt did.
    second = _Expiry(store, on_expire=FlushToLongterm(), memory_blob=blob, monkeypatch=monkeypatch)
    second.fire()

    assert len(store.envelopes) == 2
    assert store.envelopes[0] == store.envelopes[1]
    assert len({record.key for record in store.saved}) == 1


def test_flush_failure_preserves_state_for_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: Flush failure preserves state for retry. Fail-closed: the wipe
    # runs only after the flush succeeds, so the retry finds exactly the state
    # it needs. Wiping anyway would silently defeat the hook's whole purpose.
    store = _RecordingStore(fail_saves=True)
    expiry = _Expiry(
        store, on_expire=FlushToLongterm(), memory_blob=_memory_blob(), monkeypatch=monkeypatch
    )

    with pytest.raises(ConnectionError):
        expiry.fire()

    assert not any(state.cleared for state in expiry.states)
    assert expiry.memory.value is not None


def test_an_unset_hook_preserves_todays_expiry_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Unset hook preserves today's expiry behavior — no store
    # interaction, the same wipe, and the same `ttl_wiped_suspension` record,
    # which is what keeps the `ttl_expiry` conformance scenario green.
    store = _RecordingStore()
    cont = Continuation(state_schema_version=1, seq=_SEQ, deadline_ms=90_000)
    expiry = _Expiry(
        store,
        on_expire=None,
        memory_blob=_memory_blob(),
        continuation=cont,
        monkeypatch=monkeypatch,
    )

    emitted = expiry.fire()

    assert store.saved == []
    assert [record.value.reason for record in emitted if record.tag == "errors"] == [
        REASON_TTL_WIPED_SUSPENSION
    ]
    assert all(state.cleared for state in expiry.states)


def test_empty_working_memory_is_wiped_without_a_store_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An idle key that never wrote anything has nothing to demote; a store round
    # trip per empty expiry would be pure cost.
    store = _RecordingStore()
    expiry = _Expiry(
        store, on_expire=FlushToLongterm(), memory_blob=MemoryBlob(), monkeypatch=monkeypatch
    )

    expiry.fire()

    assert store.saved == []
    assert all(state.cleared for state in expiry.states)


def test_a_hook_without_a_store_refuses_before_touching_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `AgentConfig` refuses `on_expire` without `longterm_memory`, so a DoFn in
    # this state was wired past the config -- and the hook has nowhere to flush
    # to. Refusing names the field to set; the alternatives are a wipe that
    # silently discards exactly the memory the hook exists to preserve, or an
    # `AttributeError` on `None` that names nothing. The guard checks *both*
    # handles because either one missing makes the flush impossible, so a
    # store-less bridge-having DoFn -- which is every DoFn that reached
    # `setup()` -- must still be refused.
    expiry = _Expiry(
        None, on_expire=FlushToLongterm(), memory_blob=_memory_blob(), monkeypatch=monkeypatch
    )

    with pytest.raises(RuntimeError) as exc_info:
        expiry.fire()

    assert str(exc_info.value) == (
        "AgentConfig.on_expire is set but no long-term store is available; "
        "set AgentConfig.longterm_memory to a store URI"
    )
    # Fail-closed, like the flush-failure route: nothing wiped.
    assert not any(state.cleared for state in expiry.states)


def test_the_hook_is_bounded_by_the_activation_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The hook is the one place the TTL callback performs a real side effect,
    # and it runs against an external store on the shared bridge. Unbounded, a
    # wedged store stalls the worker with no timer bundle ever failing; bounded,
    # it surfaces as a failed bundle the runner retries. The hook here outlasts
    # the 50ms budget but finishes in ~300ms, so a submission that dropped its
    # timeout would *complete* rather than hang -- an assertion failure, not a
    # test that never returns.
    store = _RecordingStore()

    async def slow_flush(store: MemoryStore, expiring: ExpiringMemory) -> None:
        await asyncio.sleep(0.3)

    expiry = _Expiry(
        store,
        on_expire=slow_flush,
        memory_blob=_memory_blob(),
        monkeypatch=monkeypatch,
        activation_timeout_s=0.05,
    )

    with pytest.raises(ActivationTimeout):
        expiry.fire()

    assert store.saved == []
    assert not any(state.cleared for state in expiry.states)


def test_the_flush_runs_before_the_wipe(monkeypatch: pytest.MonkeyPatch) -> None:
    # The ordering is the requirement, not an implementation detail: the store
    # must see the blob while the state that produced it is still there.
    observed: list[bool] = []

    class _OrderingStore(_RecordingStore):
        async def _save(self, record: MemoryRecord) -> bool:
            observed.append(expiry.memory.value is not None)
            return await super()._save(record)

    store = _OrderingStore()
    expiry = _Expiry(
        store, on_expire=FlushToLongterm(), memory_blob=_memory_blob(), monkeypatch=monkeypatch
    )

    expiry.fire()

    assert observed == [True]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
