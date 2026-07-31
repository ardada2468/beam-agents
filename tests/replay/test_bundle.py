"""Replay-bundle loading: framing, scope selection, migration, and refusals.

Covers the `replay-cli` scenarios "A resume replay is reconstructed from the
continuation", "A mismatched envelope is refused", "An older-schema snapshot
replays after migration", and "A newer-schema snapshot fails closed".
"""

from __future__ import annotations

from typing import Any

import pytest

from beam_agents._protos import AgentEnvelope, Continuation, LlmCacheBlob, MemoryBlob, TraceEvent
from beam_agents.core import migration
from beam_agents.core.migration import CURRENT_STATE_SCHEMA_VERSION
from beam_agents.observability.traces import ACTIVATION_KIND
from beam_agents.replay.bundle import (
    ReplayIrreproducibleError,
    ReplayUsageError,
    build_bundle,
    frame_trace_events,
    load_envelope,
    load_snapshot,
    parse_trace_stream,
    run_replay,
)
from tests.replay._fixtures import (
    KEY,
    NOW_MS,
    SEQ,
    exact_replay_agent,
    run_original,
    run_original_failure,
    run_original_resume,
    suspending_agent,
)


def _bundle(original: Any, **kwargs: Any) -> Any:
    return build_bundle(
        snapshot=original.snapshot,
        traces=original.traces,
        envelope=original.envelope,
        **kwargs,
    )


# --- Requirement: the CLI reconstructs an activation and re-runs it locally ----


def test_a_trace_stream_round_trips_through_the_varint_framing() -> None:
    # The canonical interchange: the bytes `serialize_trace_event` produces,
    # each preceded by its varint length.
    original = run_original()

    framed = frame_trace_events(original.traces)
    parsed = parse_trace_stream(framed)

    assert [e.SerializeToString(deterministic=True) for e in parsed] == [
        e.SerializeToString(deterministic=True) for e in original.traces
    ]


def test_a_truncated_trace_stream_is_refused() -> None:
    framed = frame_trace_events(run_original().traces)

    with pytest.raises(ReplayUsageError, match="truncated"):
        parse_trace_stream(framed[:-2])


def test_the_target_scope_defaults_to_the_highest_traced_seq() -> None:
    original = run_original()
    older = [TraceEvent(entity_key=KEY, seq=SEQ - 1, event_type=TraceEvent.ACTIVATION_START)]

    bundle = build_bundle(
        snapshot=original.snapshot,
        traces=[*older, *original.traces],
        envelope=original.envelope,
    )

    assert bundle.seq == SEQ
    assert bundle.entity_key == KEY
    assert [e.seq for e in bundle.traced] == [SEQ] * len(bundle.traced)


def test_an_explicit_seq_overrides_the_default() -> None:
    original = run_original()
    older = [
        TraceEvent(
            entity_key=KEY,
            seq=SEQ - 1,
            event_type=TraceEvent.ACTIVATION_START,
            start_ms=NOW_MS - 5_000,
            attributes={ACTIVATION_KIND: "start"},
        )
    ]

    bundle = build_bundle(
        snapshot=original.snapshot,
        traces=[*older, *original.traces],
        envelope=original.envelope,
        seq=SEQ - 1,
    )

    assert bundle.seq == SEQ - 1
    assert bundle.now_ms == NOW_MS - 5_000


def test_a_seq_with_no_traced_events_is_refused() -> None:
    original = run_original()

    with pytest.raises(ReplayUsageError, match="no traced events"):
        _bundle(original, seq=99)


def test_now_ms_is_recovered_from_the_traced_activation_start() -> None:
    # Scenario support for "A replayed activation reproduces the traced
    # outcome": the clock is the traced one, never a wall-clock read.
    original = run_original(now_ms=NOW_MS + 777)

    bundle = _bundle(original)

    assert bundle.now_ms == NOW_MS + 777


def test_now_ms_falls_back_to_the_attempts_first_event_when_it_only_errored() -> None:
    # A failed attempt commits nothing, so its `.traces` carry only the
    # synthesized ERROR event — whose `start_ms` is the same injected clock.
    original = run_original_failure()

    bundle = _bundle(original)

    assert [e.event_type for e in bundle.traced] == [TraceEvent.ERROR]
    assert bundle.now_ms == NOW_MS


def test_a_mismatched_envelope_is_refused() -> None:
    # Scenario: A mismatched envelope is refused.
    original = run_original()
    foreign = AgentEnvelope(entity_key=b"other-key", event_time_ms=NOW_MS, external_event=b"go")

    with pytest.raises(ReplayUsageError) as excinfo:
        build_bundle(snapshot=original.snapshot, traces=original.traces, envelope=foreign)

    message = str(excinfo.value)
    assert KEY.hex() in message
    assert b"other-key".hex() in message


def test_a_resume_replay_is_reconstructed_from_the_continuation() -> None:
    # Scenario: A resume replay is reconstructed from the continuation.
    original = run_original_resume()
    assert original.snapshot.HasField("continuation")

    bundle = _bundle(original)

    assert bundle.is_resume is True
    assert bundle.step_index == original.snapshot.continuation.step_index
    assert bundle.adapter_snapshot == original.snapshot.continuation.snapshot

    outcome = run_replay(bundle, suspending_agent)

    # It resumed rather than starting fresh: a fresh start would have staged a
    # second intent and suspended again.
    assert outcome.status == "completed"
    assert outcome.outputs == (b"resumed:done",)
    assert outcome.intents == ()


def test_a_resume_envelope_without_a_continuation_is_refused() -> None:
    original = run_original_resume()
    original.snapshot.ClearField("continuation")

    with pytest.raises(ReplayUsageError, match="continuation"):
        _bundle(original)


def test_a_resume_envelope_naming_an_unpended_intent_is_refused() -> None:
    original = run_original_resume()
    original.envelope.tool_result.intent_id = "ghost"

    with pytest.raises(ReplayUsageError, match="ghost"):
        _bundle(original)


# --- Requirement: snapshots migrate on load and newer schemas are refused ------


def test_a_snapshot_newer_than_the_package_fails_closed() -> None:
    # Scenario: A newer-schema snapshot fails closed.
    original = run_original()
    original.snapshot.state_schema_version = CURRENT_STATE_SCHEMA_VERSION + 1

    with pytest.raises(ReplayUsageError) as excinfo:
        load_snapshot(original.snapshot.SerializeToString(deterministic=True))

    message = str(excinfo.value)
    assert str(CURRENT_STATE_SCHEMA_VERSION + 1) in message
    assert str(CURRENT_STATE_SCHEMA_VERSION) in message
    assert "upgrade beam-agents" in message


def test_a_blob_newer_than_the_package_fails_closed_too() -> None:
    # The envelope's stamp is not the only version that matters: each embedded
    # blob carries its own, and a future one is refused the same way.
    original = run_original()
    original.snapshot.memory.state_schema_version = CURRENT_STATE_SCHEMA_VERSION + 1

    with pytest.raises(ReplayUsageError) as excinfo:
        load_snapshot(original.snapshot.SerializeToString(deterministic=True))

    assert "MemoryBlob" in str(excinfo.value)


def test_a_pre_versioned_blob_normalizes_to_the_baseline_on_load() -> None:
    # Version 0 is the pre-versioned baseline (proto3 zero-default), which the
    # migration hook normalizes rather than rejecting.
    original = run_original()
    original.snapshot.memory.state_schema_version = 0
    original.snapshot.llm_cache.state_schema_version = 0

    loaded = load_snapshot(original.snapshot.SerializeToString(deterministic=True))

    assert loaded.llm_cache.entries[0].response == b"pong"


def test_an_older_schema_snapshot_replays_after_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: An older-schema snapshot replays after migration. Simulate the
    # bump the way the compat gate does — raise the current version and ship
    # its steps — and assert the loader runs every blob through the registry's
    # migration rather than reimplementing one.
    #
    # The original runs FIRST, under the old version: its blobs are stamped by
    # the writer's reading of `CURRENT_STATE_SCHEMA_VERSION`, so this is a
    # genuine older-schema snapshot rather than one relabelled after the fact.
    original = run_original()
    assert original.snapshot.memory.state_schema_version == 1

    monkeypatch.setattr(migration, "_REGISTRY", dict(migration._REGISTRY))
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)

    @migration.migration(MemoryBlob, from_version=1)
    def _memory_v1_to_v2(blob: MemoryBlob) -> MemoryBlob:
        migrated = MemoryBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = 2
        migrated.entries.add(key="migrated", value=b"yes", last_access_ms=0)
        return migrated

    @migration.migration(LlmCacheBlob, from_version=1)
    def _cache_v1_to_v2(blob: LlmCacheBlob) -> LlmCacheBlob:
        migrated = LlmCacheBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = 2
        return migrated

    @migration.migration(Continuation, from_version=1)
    def _continuation_v1_to_v2(cont: Continuation) -> Continuation:
        migrated = Continuation()
        migrated.CopyFrom(cont)
        migrated.state_schema_version = 2
        return migrated

    loaded = load_snapshot(original.snapshot.SerializeToString(deterministic=True))

    assert loaded.memory.state_schema_version == 2
    assert [e.key for e in loaded.memory.entries][-1] == "migrated"
    assert loaded.llm_cache.state_schema_version == 2
    # Migration is applied to the in-memory copy only; nothing is written back.
    assert original.snapshot.memory.state_schema_version == 1
    # ...and the migrated blobs are what the replay runs against.
    bundle = build_bundle(snapshot=loaded, traces=original.traces, envelope=original.envelope)
    outcome = run_replay(bundle, exact_replay_agent)
    assert outcome.provider_calls == 0
    assert any(entry.key == "migrated" for entry in outcome.memory_blob.entries)


def test_a_missing_migration_step_is_irreproducible_not_a_silent_guess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A chain with a gap must never be papered over: the loader surfaces the
    # registry's own error rather than interpreting old bytes under new rules.
    original = run_original()
    monkeypatch.setattr(migration, "_REGISTRY", dict(migration._REGISTRY))
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)

    with pytest.raises(ReplayIrreproducibleError, match="no migration registered"):
        load_snapshot(original.snapshot.SerializeToString(deterministic=True))


# --- loaders ------------------------------------------------------------------


def test_unparseable_inputs_are_refused_as_usage_errors() -> None:
    with pytest.raises(ReplayUsageError, match="snapshot"):
        load_snapshot(b"\xff\xff\xff\xff\xff\xff")
    with pytest.raises(ReplayUsageError, match="envelope"):
        load_envelope(b"\xff\xff\xff\xff\xff\xff")


def test_a_loaded_snapshot_round_trips_its_scope() -> None:
    original = run_original()

    loaded = load_snapshot(original.snapshot.SerializeToString(deterministic=True))

    assert loaded.entity_key == KEY
    assert loaded.seq == SEQ + 1  # the counter after the activation committed
    assert loaded.request_id == "req-1"
