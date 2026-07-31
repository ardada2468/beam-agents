"""The `export_request` route: one read-only snapshot off the keyed stream.

Driven with the fake state/timer handles from `_dofn_fakes` rather than a
pipeline, so the new branches in `process` sit inside the mutation gate's test
selection (the TestPipeline suites are deselected under mutmut).

`setup()` is deliberately never called on the DoFn here: the export route must
not touch the bridge or the provider, so a handler that accidentally ran an
activation would trip `_activate`'s "setup() not called" assertion and surface
as an `.errors` dead letter — which every test below asserts the absence of.

Covers the `state-snapshot-export` scenarios: "Snapshot captures the blobs a
subsequent activation would load", "State and seq are untouched by an export",
"An export produces no activation outputs", and "A retried bundle re-emits a
byte-identical snapshot".
"""

from __future__ import annotations

from typing import Any

import apache_beam as beam

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    StateSnapshot,
    ToolIntent,
)
from beam_agents.core.dofn import _AgentDoFn
from beam_agents.core.migration import CURRENT_STATE_SCHEMA_VERSION
from beam_agents.core.snapshot import serialize_snapshot
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeTimer, FakeValue
from tests.core._dofn_helpers import make_pong_provider, seq_agent

_KEY = b"entity-1"
_NOW_MS = 1_700_000_000_000


def _export(request_id: str = "req-1", *, now_ms: int = _NOW_MS) -> AgentEnvelope:
    return AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=now_ms,
        export_request=AgentEnvelope.StateExportRequest(request_id=request_id),
    )


def _memory_blob() -> MemoryBlob:
    blob = MemoryBlob(state_schema_version=1, total_value_bytes=4)
    blob.entries.add(key="alpha", value=b"aa", last_access_ms=10)
    blob.entries.add(key="beta", value=b"bb", last_access_ms=20)
    return blob


def _cache_blob() -> LlmCacheBlob:
    blob = LlmCacheBlob(state_schema_version=1, total_response_bytes=5)
    blob.entries.add(
        cache_key="0" * 64,
        response=b"hello",
        response_digest=b"\x01\x02",
        created_at_ms=10,
        last_access_ms=20,
    )
    return blob


def _continuation() -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=7,
        step_index=2,
        pending_intent_ids=["i-1"],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=10,
        deadline_ms=99_000,
    )


def _pending() -> list[ToolIntent]:
    return [
        ToolIntent(intent_id="i-1", entity_key=_KEY, seq=7, step_index=1, tool_name="http.post"),
        ToolIntent(intent_id="i-2", entity_key=_KEY, seq=7, step_index=2, tool_name="http.get"),
    ]


class _Driver:
    """One DoFn plus the fake handles a single export `process` call is given."""

    def __init__(
        self,
        *,
        memory_blob: MemoryBlob | None = None,
        cache_blob: LlmCacheBlob | None = None,
        continuation: Continuation | None = None,
        pending: list[ToolIntent] | None = None,
        seq: int = 0,
    ) -> None:
        self.dofn = _AgentDoFn(seq_agent, provider_factory=make_pong_provider)
        self.memory = FakeValue(memory_blob)
        self.continuation = FakeValue(continuation)
        self.llm_cache = FakeValue(cache_blob)
        self.pending = FakeBag(pending)
        self.seq = FakeSum(seq)
        self.ttl_timer = FakeTimer()
        self.hitl_timer = FakeTimer()

    def process(self, envelope: AgentEnvelope) -> list[Any]:
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


def _snapshots(emitted: list[Any]) -> list[StateSnapshot]:
    return [
        e.value for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == "snapshots"
    ]


def _populated_driver() -> _Driver:
    return _Driver(
        memory_blob=_memory_blob(),
        cache_blob=_cache_blob(),
        continuation=_continuation(),
        pending=_pending(),
        seq=7,
    )


# --- Requirement: An export request yields one snapshot from the keyed stream --


def test_snapshot_captures_the_blobs_a_subsequent_activation_would_load() -> None:
    # Scenario: Snapshot captures the blobs a subsequent activation would load.
    driver = _populated_driver()

    emitted = driver.process(_export())
    snapshots = _snapshots(emitted)

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.entity_key == _KEY
    assert snapshot.seq == 7
    assert snapshot.request_id == "req-1"
    assert snapshot.state_schema_version == CURRENT_STATE_SCHEMA_VERSION
    # Field-for-field against exactly what the next activation's reads return.
    assert snapshot.memory == driver.memory.read()
    assert snapshot.llm_cache == driver.llm_cache.read()
    assert snapshot.continuation == driver.continuation.read()
    assert list(snapshot.pending) == driver.pending.read()


def test_snapshot_at_ms_is_the_requests_event_time_not_a_wall_clock() -> None:
    # The snapshot's own timestamp is replay-deterministic: the request
    # envelope's event time, so a retried bundle re-emits identical bytes.
    driver = _populated_driver()

    snapshot = _snapshots(driver.process(_export(now_ms=42)))[0]

    assert snapshot.snapshot_at_ms == 42


def test_an_unsuspended_key_exports_no_continuation() -> None:
    # Presence, not a zero-valued placeholder: the loader has to distinguish
    # "not suspended" from "suspended at step 0".
    driver = _Driver(memory_blob=_memory_blob(), cache_blob=_cache_blob(), seq=3)

    snapshot = _snapshots(driver.process(_export()))[0]

    assert not snapshot.HasField("continuation")
    assert list(snapshot.pending) == []
    assert snapshot.seq == 3


def test_a_retried_bundle_re_emits_a_byte_identical_snapshot() -> None:
    # Scenario: A retried bundle re-emits a byte-identical snapshot.
    first = _snapshots(_populated_driver().process(_export()))[0]
    second = _snapshots(_populated_driver().process(_export()))[0]

    assert first.SerializeToString(deterministic=True) == second.SerializeToString(
        deterministic=True
    )


# --- Requirement: Export is read-only ----------------------------------------


def test_state_and_seq_are_untouched_by_an_export() -> None:
    # Scenario: State and seq are untouched by an export.
    driver = _populated_driver()
    before = (
        driver.memory.read(),
        driver.llm_cache.read(),
        driver.continuation.read(),
        driver.pending.read(),
        driver.seq.read(),
    )

    driver.process(_export())

    assert (
        driver.memory.read(),
        driver.llm_cache.read(),
        driver.continuation.read(),
        driver.pending.read(),
        driver.seq.read(),
    ) == before
    assert driver.seq.read() == 7  # no increment: this is not an activation
    assert not driver.memory.cleared
    assert not driver.continuation.cleared
    assert not driver.llm_cache.cleared
    assert not driver.pending.cleared
    # No timer is set or cleared either: an export is not a commit.
    assert driver.ttl_timer.set_to is None
    assert not driver.ttl_timer.cleared
    assert driver.hitl_timer.set_to is None
    assert not driver.hitl_timer.cleared


def test_an_export_produces_no_activation_outputs() -> None:
    # Scenario: An export produces no activation outputs. The DoFn was never
    # `setup()`, so an activation would have dead-lettered on `.errors`.
    emitted = _populated_driver().process(_export())

    assert len(emitted) == 1
    tagged = [e for e in emitted if isinstance(e, beam.pvalue.TaggedOutput)]
    assert [e.tag for e in tagged] == ["snapshots"]
    for tag in ("intents", "traces", "errors"):
        assert [e for e in tagged if e.tag == tag] == []


# --- Requirement: `.snapshots` is a tagged output with an optional sink -------


def test_the_message_bus_encoding_keys_by_entity_and_serializes_deterministically() -> None:
    # Scenario: A configured snapshots sink receives serialized snapshots keyed
    # by entity -- "message-bus schemes SHALL receive `(entity_key,
    # deterministic proto bytes)` pairs keyed by `entity_key`". The key is what
    # keeps one entity's snapshots in order through a single partition, and the
    # bytes are what the replay CLI loads; both are asserted here, runner-free,
    # because the only other place this encoder runs is inside `_WriteSnapshots`
    # in a pipeline.
    snapshot = _snapshots(_populated_driver().process(_export()))[0]

    key, encoded = serialize_snapshot(snapshot)

    assert key == _KEY
    assert StateSnapshot.FromString(encoded) == snapshot


def test_the_encoding_is_a_pure_function_of_the_snapshot() -> None:
    # The byte-identity half of "A retried bundle re-emits a byte-identical
    # snapshot", carried through the sink encoder rather than stopping at the
    # message: what a retried bundle actually re-publishes is these bytes.
    first = _snapshots(_populated_driver().process(_export()))[0]
    second = _snapshots(_populated_driver().process(_export()))[0]

    assert serialize_snapshot(first) == serialize_snapshot(second)


def test_an_export_of_a_never_seen_key_emits_an_empty_snapshot() -> None:
    # Absent state reads as `None` from every handle; the export still produces
    # exactly one snapshot rather than raising or emitting nothing, so an
    # operator's request always gets an answer.
    driver = _Driver()

    snapshot = _snapshots(driver.process(_export("req-2")))[0]

    assert snapshot.seq == 0
    assert list(snapshot.memory.entries) == []
    assert list(snapshot.llm_cache.entries) == []
    assert not snapshot.HasField("continuation")
    assert snapshot.request_id == "req-2"
