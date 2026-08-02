"""Commit-tail flush of staged long-term upserts (memory-facade capability).

Drives ``run_activation`` with scripted store fakes to cover "Long-term saves
stage in the activation and flush only on success": a failed activation flushes
nothing, a flush failure fails the activation closed through the existing
``ActivationFailed`` path, staged saves are visible to same-activation reads
before any flush, and a successful activation's flush lands seq-guarded rows
stamped with the activation's frozen ``seq``/``now_ms``.
"""

from __future__ import annotations

import pytest

from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.core.loop import (
    ActivationFailed,
    LongtermFlushFailed,
    _flush_longterm,
    run_activation,
)
from beam_agents.memory.stores import InMemoryMemoryStore, MemoryRecord
from tests.core._dofn_helpers import make_pong_provider

_TTL_MS = 60_000


class _ScriptedStore(InMemoryMemoryStore):
    """In-memory store recording saves, able to raise at flush time."""

    def __init__(self, *, fail_saves: bool = False) -> None:
        super().__init__()
        self.saved: list[MemoryRecord] = []
        self.fail_saves = fail_saves

    async def _save(self, record: MemoryRecord) -> bool:
        if self.fail_saves:
            raise ConnectionError("store unreachable at flush")
        self.saved.append(record)
        return await super()._save(record)


async def saving_agent(ctx: ActivationContext) -> Complete:
    """Stage two long-term saves computed from the event (blind upserts)."""
    ctx.memory.longterm.save("profile", b"p:" + ctx.single_event)
    ctx.memory.longterm.save("case/1", b"c:" + ctx.single_event)
    return Complete(output=b"done")


async def save_then_raise_agent(ctx: ActivationContext) -> Complete:
    ctx.memory.longterm.save("profile", b"should-not-flush")
    raise RuntimeError("agent failure after staging")


async def read_your_writes_agent(ctx: ActivationContext) -> Complete:
    """Assert the overlay inside the activation, then complete."""
    ctx.memory.longterm.save("profile", b"staged")
    loaded = await ctx.memory.longterm.load("profile")
    assert loaded is not None and loaded.value == b"staged"
    found = await ctx.memory.longterm.search("prof", limit=5)
    assert [r.value for r in found] == [b"staged"]
    return Complete(output=b"read-my-write")


async def saving_suspend_agent(ctx: ActivationContext) -> Suspend:
    ctx.memory.longterm.save("profile", b"pre-suspend")
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=_TTL_MS)


async def plain_agent(ctx: ActivationContext) -> Complete:
    return Complete(output=b"plain")


# -- Scenario: staged saves flush in the commit tail on success ---------------


async def test_a_completed_activation_flushes_its_staged_upserts() -> None:
    store = _ScriptedStore()

    result = await run_activation(
        saving_agent,
        entity_key=b"k",
        seq=3,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        event=b"e1",
        longterm_store=store,
    )

    assert result.status == "completed"
    expected = [
        MemoryRecord(entity_key=b"k", key="profile", value=b"p:e1", seq=3, updated_at_ms=1000),
        MemoryRecord(entity_key=b"k", key="case/1", value=b"c:e1", seq=3, updated_at_ms=1000),
    ]
    assert store.saved == expected
    assert list(result.upserts) == expected
    assert await store.load(b"k", "profile") == expected[0]


async def test_a_suspending_activation_flushes_its_staged_upserts() -> None:
    # A suspension is a successful return and a commit; its staged saves flush.
    store = _ScriptedStore()

    result = await run_activation(
        saving_suspend_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        longterm_store=store,
    )

    assert result.status == "suspended"
    assert [r.key for r in store.saved] == ["profile"]
    assert [r.key for r in result.upserts] == ["profile"]


async def test_an_activation_without_saves_touches_no_store() -> None:
    store = _ScriptedStore()

    result = await run_activation(
        plain_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        longterm_store=store,
    )

    assert result.upserts == []
    assert store.saved == []


# -- Scenario: A failed activation flushes nothing ----------------------------


async def test_a_failed_activation_flushes_nothing() -> None:
    store = _ScriptedStore()

    with pytest.raises(ActivationFailed):
        await run_activation(
            save_then_raise_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
            longterm_store=store,
        )

    assert store.saved == []
    assert await store.load(b"k", "profile") is None


# -- Scenario: A flush failure fails the activation closed --------------------


async def test_a_flush_failure_fails_the_activation_closed() -> None:
    store = _ScriptedStore(fail_saves=True)

    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            saving_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
            event=b"e",
            longterm_store=store,
        )

    # The wrap names the flush as the failing step, with the store's own error
    # as its cause — the typed record the DoFn's dead letter is built from.
    cause = excinfo.value.__cause__
    assert isinstance(cause, LongtermFlushFailed)
    assert isinstance(cause.__cause__, ConnectionError)
    # Which record failed, by name. `saving_agent` stages "profile" then
    # "case/1" and the store fails every save, so the first one is the one the
    # error must name: the flush walks the upserts in staging order and stops
    # at the first failure, and an operator triaging a partially-applied flush
    # needs to know where it stopped. Both the attribute and the message carry
    # it, because the DoFn reads the attribute while the dead letter's detail
    # carries the `repr`.
    assert cause.key == "profile"
    assert str(cause) == "long-term flush failed for key 'profile'"


# -- Scenario: Staged saves are visible to reads before any flush -------------


async def test_staged_saves_are_visible_to_reads_before_any_flush() -> None:
    store = _ScriptedStore()

    result = await run_activation(
        read_your_writes_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        longterm_store=store,
    )

    assert result.outputs == [b"read-my-write"]
    # The read happened against the overlay; the flush came only afterwards.
    assert [r.key for r in store.saved] == ["profile"]


# -- Unconfigured pipelines ---------------------------------------------------


async def test_without_a_store_the_accessor_raises_and_nothing_changes() -> None:
    # Scenario: Unconfigured pipelines behave exactly as today.
    with pytest.raises(ActivationFailed) as excinfo:
        await run_activation(
            saving_agent,
            entity_key=b"k",
            seq=0,
            now_ms=1000,
            provider=make_pong_provider(),
            memory_blob=None,
            cache_blob=None,
            event=b"e",
        )

    cause = excinfo.value.__cause__
    assert isinstance(cause, RuntimeError)
    assert "AgentConfig.longterm_memory" in str(cause)


async def test_flushing_upserts_without_a_store_names_the_wiring_bug() -> None:
    # `_flush_longterm`'s guard, driven directly. Staging an upsert requires
    # `ctx.memory.longterm`, which requires a store, so no agent can reach this
    # state -- but the guard is the thing standing between a future rewiring
    # and a silently swallowed flush, and an assertion is only as good as the
    # name it fails with. The store is cleared after staging for exactly the
    # reason `test_a_half_built_runtime_is_refused_too` clears the DoFn's
    # provider: it is the only way to present the half-built runtime the guard
    # exists to refuse.
    store = _ScriptedStore()
    ctx = ActivationContext(
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
        longterm_store=store,
    )
    ctx.memory.longterm.save("profile", b"staged")
    ctx._longterm_store = None

    with pytest.raises(AssertionError) as excinfo:
        await _flush_longterm(ctx)

    assert str(excinfo.value) == "staged upserts without a configured store"
    assert store.saved == []


async def test_without_saves_the_result_carries_no_upserts() -> None:
    result = await run_activation(
        plain_agent,
        entity_key=b"k",
        seq=0,
        now_ms=1000,
        provider=make_pong_provider(),
        memory_blob=None,
        cache_blob=None,
    )
    assert result.upserts == []
