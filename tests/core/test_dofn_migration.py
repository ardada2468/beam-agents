"""The read-path migration hook in `_AgentDoFn`, driven with fake handles.

Every keyed-state read (`MEMORY`, `CONTINUATION`, `LLM_CACHE`) passes through
`migrate_to_current` before any field is interpreted, and migration at read
time writes nothing — the migrated view reaches durable state only through the
next successful commit. Driven with the fake handles in `_dofn_fakes` (inside
the mutation gate's selection) under a monkeypatched current version and
test-double steps: no real v2 schema exists, and none is needed.

A version from the future is the one failure this DoFn does NOT dead-letter:
the typed error propagates out of `process` and the timer callbacks so the
bundle fails with zero mutation, and rolling the binary forward recovers the
key losslessly.
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam
import pytest
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
)
from beam_agents.core import migration
from beam_agents.core.agent import Complete, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import (
    REASON_ERROR,
    REASON_ORPHANED,
    REASON_TTL_WIPED_SUSPENSION,
    _AgentDoFn,
)
from beam_agents.core.migration import StateSchemaFromFutureError
from beam_agents.hitl import HitlPolicy
from beam_agents.memory import Memory
from beam_agents.model.replay_cache import ReplayCache, compute_cache_key
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeTimer, FakeValue, scripted_clock
from tests.core._dofn_helpers import (
    escalate_once,
    make_pong_provider,
    model_agent,
    raising_agent,
    request,
    suspend_then_complete_agent,
)

_KEY = b"k"
_NOW_MS = 1_000
_TTL_MS = 100_000


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate registrations: each test decorates into a throwaway copy."""
    monkeypatch.setattr(migration, "_REGISTRY", dict(migration._REGISTRY))


@pytest.fixture
def bumped(monkeypatch: pytest.MonkeyPatch) -> None:
    """A version-2 binary over version-1 state: current bumped to 2, one
    observable test-double step registered per versioned blob type.

    The doubles are pure and deliberately visible in behavior — uppercased
    memory values and cache responses, a doubled continuation deadline — so a
    hook that skipped migration fails these tests on output, not just on the
    version stamp.
    """
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)

    def memory_step(blob: MemoryBlob) -> MemoryBlob:
        migrated = MemoryBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = 2
        for entry in migrated.entries:
            # First byte is the facade's kind tag; the payload follows it.
            entry.value = entry.value[:1] + entry.value[1:].upper()
        return migrated

    def continuation_step(cont: Continuation) -> Continuation:
        migrated = Continuation()
        migrated.CopyFrom(cont)
        migrated.state_schema_version = 2
        migrated.deadline_ms = cont.deadline_ms * 2
        return migrated

    def cache_step(blob: LlmCacheBlob) -> LlmCacheBlob:
        migrated = LlmCacheBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = 2
        for entry in migrated.entries:
            entry.response = entry.response.upper()
        return migrated

    migration.migration(MemoryBlob, from_version=1)(memory_step)
    migration.migration(Continuation, from_version=1)(continuation_step)
    migration.migration(LlmCacheBlob, from_version=1)(cache_step)


def _register_stamp_only_steps(from_version: int) -> None:
    """Identity steps that only advance the stamp, for roll-forward tests."""

    to_version = from_version + 1

    def memory_stamp(blob: MemoryBlob) -> MemoryBlob:
        migrated = MemoryBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = to_version
        return migrated

    def continuation_stamp(cont: Continuation) -> Continuation:
        migrated = Continuation()
        migrated.CopyFrom(cont)
        migrated.state_schema_version = to_version
        return migrated

    def cache_stamp(blob: LlmCacheBlob) -> LlmCacheBlob:
        migrated = LlmCacheBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = to_version
        return migrated

    migration.migration(MemoryBlob, from_version=from_version)(memory_stamp)
    migration.migration(Continuation, from_version=from_version)(continuation_stamp)
    migration.migration(LlmCacheBlob, from_version=from_version)(cache_stamp)


async def _echo_greeting_agent(ctx: ActivationContext) -> Complete:
    """Complete with the `greeting` memory scalar: the migrated view made
    visible on the main output."""
    value = ctx.memory.get("greeting")
    assert value is not None
    return Complete(output=value)


def _memory_blob(version: int) -> MemoryBlob:
    memory = Memory(now_ms=_NOW_MS)
    memory.set("greeting", b"hello")
    blob = memory.to_blob()
    blob.state_schema_version = version
    return blob


def _cache_blob(version: int, response: bytes, *, seq: int) -> LlmCacheBlob:
    """A replay cache holding `response` under the key `model_agent` hashes to."""
    req = request()
    cache = ReplayCache(None, now_ms=_NOW_MS)
    cache.put(
        compute_cache_key(
            req.model_id, req.messages, req.tools_schema, req.sampling_params, _KEY, seq
        ),
        response,
    )
    blob = cache.to_blob()
    blob.state_schema_version = version
    return blob


def _continuation(
    version: int, *, deadline_ms: int, seq: int = 3, step_index: int = 2
) -> Continuation:
    return Continuation(
        state_schema_version=version,
        seq=seq,
        step_index=step_index,
        pending_intent_ids=[intent_id_for(_KEY, seq, step_index - 1)],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=500,
        deadline_ms=deadline_ms,
    )


def _event(payload: bytes = b"go") -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS, external_event=payload)


def _tool_result(intent_id: str, payload: bytes = b"done") -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS)
    envelope.tool_result.intent_id = intent_id
    envelope.tool_result.entity_key = _KEY
    envelope.tool_result.payload = payload
    envelope.tool_result.status = ToolResult.OK
    return envelope


class _Driver:
    """One DoFn plus the fake handles a single `process` call is given."""

    def __init__(
        self,
        agent: Any,
        *,
        hitl_policy: HitlPolicy | None = None,
        memory_blob: MemoryBlob | None = None,
        cache_blob: LlmCacheBlob | None = None,
        continuation: Continuation | None = None,
        pending: list[ToolIntent] | None = None,
        seq: int = 0,
    ) -> None:
        self.dofn = _AgentDoFn(
            agent,
            provider_factory=make_pong_provider,
            ttl_ms=_TTL_MS,
            hitl_policy=hitl_policy,
            monotonic_ns=scripted_clock(),
        )
        self.memory = FakeValue(memory_blob if memory_blob is not None else MemoryBlob())
        self.continuation = FakeValue(continuation)
        self.llm_cache = FakeValue(cache_blob if cache_blob is not None else LlmCacheBlob())
        self.pending = FakeBag(pending)
        self.seq = FakeSum(seq)
        self.ttl_timer = FakeTimer()
        self.hitl_timer = FakeTimer()

    def process(self, envelope: AgentEnvelope) -> list[Any]:
        self.dofn.setup()
        try:
            return list(
                self.dofn.process(
                    (_KEY, envelope),
                    memory=self.memory,
                    continuation=self.continuation,
                    llm_cache=self.llm_cache,
                    pending=self.pending,
                    seq=self.seq,
                    ttl_timer=self.ttl_timer,
                    hitl_timer=self.hitl_timer,
                )
            )
        finally:
            self.dofn.teardown()

    def fire_ttl(self, *, fired_at_ms: int) -> list[Any]:
        return list(
            self.dofn.on_ttl(
                key=_KEY,
                timestamp=Timestamp(micros=fired_at_ms * 1000),
                memory=self.memory,
                continuation=self.continuation,
                llm_cache=self.llm_cache,
                pending=self.pending,
                seq=self.seq,
            )
        )

    def fire_hitl(self, *, fired_at_ms: int) -> list[Any]:
        return list(
            self.dofn.on_hitl(
                key=_KEY,
                timestamp=Timestamp(micros=fired_at_ms * 1000),
                continuation=self.continuation,
                pending=self.pending,
                hitl_timer=self.hitl_timer,
                ttl_timer=self.ttl_timer,
            )
        )


def _main(emitted: list[Any]) -> list[Any]:
    return [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)]


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


# --- Requirement: Keyed state is migrated lazily on first read inside the DoFn -


def test_an_old_blob_is_migrated_on_read_and_committed_at_the_current_version(
    bumped: None,
) -> None:
    # Scenario: An old blob is migrated on read and committed at the current
    # version. The activation observes the migrated (uppercased) memory view,
    # and the blobs written back at commit read version 2 — through the
    # existing commit writes, with no write at read time.
    driver = _Driver(_echo_greeting_agent, memory_blob=_memory_blob(1))

    emitted = driver.process(_event())

    assert _main(emitted) == [b"HELLO"]
    assert driver.memory.value.state_schema_version == 2
    assert driver.memory.value.entries[0].value[1:] == b"HELLO"
    assert driver.llm_cache.value.state_schema_version == 2


def test_a_cached_response_is_read_through_the_migrated_cache_view(bumped: None) -> None:
    # Same scenario, LLM_CACHE leg: the replay-cache hit serves the *migrated*
    # response (uppercased by the 1 -> 2 double), never the stored v1 bytes,
    # and the cache blob commits at the current version.
    driver = _Driver(model_agent, cache_blob=_cache_blob(1, b"cached", seq=0))

    emitted = driver.process(_event())

    assert _main(emitted) == [b"CACHED"]
    assert driver.llm_cache.value.state_schema_version == 2
    assert driver.llm_cache.value.entries[0].response == b"CACHED"


def test_a_resume_is_admitted_against_the_migrated_continuation(bumped: None) -> None:
    # Same scenario, CONTINUATION leg: admission interprets the *migrated*
    # deadline. The stored v1 deadline (800) has already passed at now=1000;
    # the 1 -> 2 double doubles it to 1600, so the resume is admitted — a hook
    # that skipped migration would orphan this result instead.
    cont = _continuation(1, deadline_ms=800)
    intent_id = cont.pending_intent_ids[0]
    driver = _Driver(
        suspend_then_complete_agent,
        continuation=cont,
        pending=[ToolIntent(intent_id=intent_id, expires_at_ms=_NOW_MS + 60_000)],
    )

    emitted = driver.process(_tool_result(intent_id))

    assert _main(emitted) == [b"resumed:done"]
    assert _tagged(emitted, "errors") == []
    # The resumed activation completed: its rebuilt state commits at current.
    assert driver.memory.value.state_schema_version == 2
    assert driver.llm_cache.value.state_schema_version == 2
    assert driver.continuation.value is None


def test_a_failed_activation_leaves_old_version_bytes_untouched(bumped: None) -> None:
    # Scenario: A failed activation leaves old-version bytes untouched. The
    # element is dead-lettered per the existing failure route and the stored
    # blobs still carry their original version-1 bytes, unmodified.
    seeded_memory = _memory_blob(1)
    seeded_cache = _cache_blob(1, b"cached", seq=0)
    driver = _Driver(raising_agent, memory_blob=seeded_memory, cache_blob=seeded_cache)

    emitted = driver.process(_event())

    errors = _tagged(emitted, "errors")
    assert [e.reason for e in errors] == [REASON_ERROR]
    assert driver.memory.value is seeded_memory
    assert driver.memory.value.state_schema_version == 1
    assert driver.memory.value.entries[0].value[1:] == b"hello"
    assert driver.llm_cache.value is seeded_cache
    assert driver.llm_cache.value.state_schema_version == 1
    assert driver.seq.value == 0


def test_a_refused_resume_mutates_nothing(bumped: None) -> None:
    # A resume refused by admission (unknown intent) is orphaned and the
    # stored old-version continuation stays byte-for-byte as it was.
    cont = _continuation(1, deadline_ms=800)
    driver = _Driver(suspend_then_complete_agent, continuation=cont)

    emitted = driver.process(_tool_result("unknown-intent"))

    errors = _tagged(emitted, "errors")
    assert [e.reason for e in errors] == [REASON_ORPHANED]
    assert errors[0].detail.startswith("unknown_intent:")
    assert driver.continuation.value is cont
    assert driver.continuation.value.state_schema_version == 1


# --- Requirement: Keyed state is migrated lazily on first read inside the DoFn
# (Scenario: Timer callbacks interpret only migrated continuations) ------------


def test_a_stale_hitl_fire_against_the_migrated_deadline_mutates_nothing(bumped: None) -> None:
    # The stored v1 deadline (30_000) would read as passed at 40_000; the
    # migrated deadline (60_000) has not arrived, so the fire is stale: no
    # output, no mutation — a fail-closed mechanism that misreads an old
    # layout's deadline is not fail-closed.
    cont = _continuation(1, deadline_ms=30_000)
    driver = _Driver(suspend_then_complete_agent, continuation=cont)

    emitted = driver.fire_hitl(fired_at_ms=40_000)

    assert emitted == []
    assert driver.continuation.value is cont
    assert driver.continuation.value.state_schema_version == 1
    assert not driver.pending.cleared


def test_an_escalation_writes_the_migrated_continuation_back_at_the_current_version(
    bumped: None,
) -> None:
    # Scenario: Timer callbacks interpret only migrated continuations. The
    # escalation route reads `deadline_ms`/`seq`/`escalations`/
    # `pending_intent_ids` off the migrated view and writes the escalated
    # continuation back stamped at the current version.
    cont = _continuation(1, deadline_ms=30_000)
    driver = _Driver(
        suspend_then_complete_agent,
        hitl_policy=HitlPolicy(max_escalations=2, on_timeout=escalate_once),
        continuation=cont,
    )

    emitted = driver.fire_hitl(fired_at_ms=60_000)

    intents = _tagged(emitted, "intents")
    assert [i.tool_name for i in intents] == ["pager"]
    # Minted from the migrated continuation's own cursor and seq.
    assert intents[0].intent_id == intent_id_for(_KEY, cont.seq, cont.step_index)
    escalated = driver.continuation.value
    assert escalated.state_schema_version == 2
    assert escalated.escalations == 1
    # escalate_once extends by 5_000 from the fire time.
    assert escalated.deadline_ms == 65_000
    assert escalated.pending_intent_ids == [*cont.pending_intent_ids, intents[0].intent_id]


def test_on_ttl_reports_the_migrated_continuations_deadline(bumped: None) -> None:
    # `on_ttl` over a live suspension dead-letters with the continuation's
    # `seq` and `deadline_ms` — read off the migrated view, not the v1 bytes.
    driver = _Driver(suspend_then_complete_agent, continuation=_continuation(1, deadline_ms=30_000))

    emitted = driver.fire_ttl(fired_at_ms=200_000)

    errors = _tagged(emitted, "errors")
    assert [e.reason for e in errors] == [REASON_TTL_WIPED_SUSPENSION]
    assert errors[0].detail == "seq=3,deadline_ms=60000"
    assert driver.continuation.cleared


# --- Requirement: A state version from the future fails fast ------------------


@pytest.mark.parametrize(
    "seeded",
    [
        {"memory_blob": MemoryBlob(state_schema_version=2)},
        {"cache_blob": LlmCacheBlob(state_schema_version=2)},
    ],
    ids=["memory", "llm_cache"],
)
def test_a_future_version_blob_fails_the_bundle(seeded: dict[str, Any]) -> None:
    # Scenario: A future-version blob fails the bundle. The typed error
    # propagates out of `process` — no `.errors` record, no output, and no
    # keyed-state mutation, so the runner's retry (under a rolled-forward
    # binary) starts from exactly the state this attempt found.
    driver = _Driver(_echo_greeting_agent, **seeded)

    with pytest.raises(StateSchemaFromFutureError):
        driver.process(_event())

    assert driver.memory.value == seeded.get("memory_blob", MemoryBlob())
    assert driver.llm_cache.value == seeded.get("cache_blob", LlmCacheBlob())
    assert driver.seq.value == 0
    assert driver.ttl_timer.set_to is None
    assert not driver.memory.cleared and not driver.llm_cache.cleared


def test_a_future_version_continuation_fails_the_resume_before_admission() -> None:
    cont = _continuation(2, deadline_ms=60_000)
    driver = _Driver(suspend_then_complete_agent, continuation=cont)

    with pytest.raises(StateSchemaFromFutureError):
        driver.process(_tool_result(cont.pending_intent_ids[0]))

    assert driver.continuation.value is cont
    assert driver.seq.value == 0


def test_a_future_version_continuation_fails_both_timer_callbacks() -> None:
    # Fail-fast happens before any field is interpreted, including in the
    # timer callbacks: `on_ttl` must not wipe and `on_hitl` must not route on
    # a `deadline_ms` it cannot read correctly.
    cont = _continuation(2, deadline_ms=2_000)
    driver = _Driver(suspend_then_complete_agent, continuation=cont)

    with pytest.raises(StateSchemaFromFutureError):
        driver.fire_ttl(fired_at_ms=200_000)
    with pytest.raises(StateSchemaFromFutureError):
        driver.fire_hitl(fired_at_ms=200_000)

    assert driver.continuation.value is cont
    assert not driver.memory.cleared
    assert not driver.pending.cleared


def test_rolling_forward_recovers_the_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: Rolling forward recovers the key. The same element that failed
    # under the version-1 binary processes cleanly once the binary's current
    # version reaches the blob's — with no residue from the failed attempts.
    seeded = _memory_blob(2)
    driver = _Driver(_echo_greeting_agent, memory_blob=seeded)
    with pytest.raises(StateSchemaFromFutureError):
        driver.process(_event())

    # Roll the binary forward: current becomes 2, with the 1 -> 2 chain the
    # bumped binary ships (stamp-only doubles here).
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)
    _register_stamp_only_steps(1)

    emitted = driver.process(_event())

    assert _main(emitted) == [b"hello"]
    assert _tagged(emitted, "errors") == []
    assert driver.memory.value.state_schema_version == 2
    assert driver.seq.value == 1
