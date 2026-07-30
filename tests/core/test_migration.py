"""Registry semantics for the `state-migration` capability (`core/migration.py`).

Every test registers its steps into an isolated copy of the registry (the
`_registry` autouse fixture), so the module-import-time registry — empty at
`CURRENT_STATE_SCHEMA_VERSION = 1` — is never mutated across tests. Chain
walking is exercised with test-double steps and a monkeypatched current
version: no real v2 schema exists, and none is needed to pin the semantics.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from beam_agents._protos import Continuation, LlmCacheBlob, MemoryBlob
from beam_agents.core import migration
from beam_agents.core.context import ActivationContext
from beam_agents.core.migration import (
    CURRENT_STATE_SCHEMA_VERSION,
    VERSIONED_MESSAGE_TYPES,
    MigrationStepError,
    MissingMigrationError,
    StateSchemaFromFutureError,
    migrate_to_current,
)
from beam_agents.memory import Memory
from beam_agents.model.fake import FakeLLM
from beam_agents.model.replay_cache import ReplayCache


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate registrations: each test decorates into a throwaway copy."""
    monkeypatch.setattr(migration, "_REGISTRY", dict(migration._REGISTRY))


def _memory_blob(version: int, marker: bytes = b"v1") -> MemoryBlob:
    blob = MemoryBlob(state_schema_version=version, total_value_bytes=len(marker))
    blob.entries.add(key="marker", value=marker, last_access_ms=1_000)
    return blob


def _step_appending(suffix: bytes, *, to_version: int) -> Callable[[MemoryBlob], MemoryBlob]:
    """A pure single-step `MemoryBlob` migration double: stamps its known
    target version (as a real registered step does — the incoming stamp may
    read the raw `0` of a pre-versioned blob) and appends `suffix` to the
    marker entry, so composition order is observable.
    """

    def step(blob: MemoryBlob) -> MemoryBlob:
        migrated = MemoryBlob()
        migrated.CopyFrom(blob)
        migrated.state_schema_version = to_version
        migrated.entries[0].value += suffix
        return migrated

    return step


# --- Requirement: Migrations form a per-message chain of single-step pure functions


def test_a_single_step_migration_upgrades_one_version(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: A single-step migration upgrades one version.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)
    migration.migration(MemoryBlob, from_version=1)(_step_appending(b"+2", to_version=2))

    migrated = migrate_to_current(_memory_blob(1))

    assert migrated.state_schema_version == 2
    assert migrated.entries[0].value == b"v1+2"


def test_chains_compose_across_multiple_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: Chains compose across multiple versions.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 3)
    migration.migration(MemoryBlob, from_version=1)(_step_appending(b"+2", to_version=2))
    migration.migration(MemoryBlob, from_version=2)(_step_appending(b"+3", to_version=3))

    migrated = migrate_to_current(_memory_blob(1))

    # Both steps ran, in order: the 1->2 suffix lands before the 2->3 one.
    assert migrated.state_schema_version == 3
    assert migrated.entries[0].value == b"v1+2+3"


def test_a_gap_in_the_chain_is_a_hard_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: A gap in the chain is a hard error.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 3)
    migration.migration(MemoryBlob, from_version=1)(_step_appending(b"+2", to_version=2))
    # No 2 -> 3 step: the chain has a gap and no partially-migrated value may
    # escape.

    with pytest.raises(MissingMigrationError) as excinfo:
        migrate_to_current(_memory_blob(1))

    assert excinfo.value.message_type is MemoryBlob
    assert excinfo.value.from_version == 2
    assert "MemoryBlob" in str(excinfo.value)
    assert "2" in str(excinfo.value)


def test_a_current_version_blob_passes_through_untouched() -> None:
    # Scenario: A current-version blob passes through untouched.
    invoked: list[MemoryBlob] = []

    def spy(blob: MemoryBlob) -> MemoryBlob:  # pragma: no cover - must not run
        invoked.append(blob)
        return blob

    migration.migration(MemoryBlob, from_version=CURRENT_STATE_SCHEMA_VERSION)(spy)
    blob = _memory_blob(CURRENT_STATE_SCHEMA_VERSION)

    assert migrate_to_current(blob) is blob  # identical object, no copy
    assert invoked == []


def test_absent_state_passes_through_as_none() -> None:
    # The DoFn's read sites hand `state.read()` straight to the hook; a fresh
    # key reads `None` and must stay `None` — there is nothing to migrate.
    assert migrate_to_current(None) is None


def test_a_step_that_fails_to_advance_the_stamp_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `migrate_to_current` verifies each applied step advanced the version
    # stamp; a step that forgets is a broken migration, not a skipped one.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)

    def forgetful(blob: MemoryBlob) -> MemoryBlob:
        migrated = MemoryBlob()
        migrated.CopyFrom(blob)
        return migrated  # still reads version 1

    migration.migration(MemoryBlob, from_version=1)(forgetful)

    with pytest.raises(MigrationStepError) as excinfo:
        migrate_to_current(_memory_blob(1))

    assert excinfo.value.message_type is MemoryBlob
    assert excinfo.value.from_version == 1
    assert excinfo.value.produced_version == 1


def test_chains_are_per_message_type(monkeypatch: pytest.MonkeyPatch) -> None:
    # A `MemoryBlob` step must never be consulted for a `Continuation`: the
    # registry is keyed by (message type, from_version), not version alone.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)
    migration.migration(MemoryBlob, from_version=1)(_step_appending(b"+2", to_version=2))

    with pytest.raises(MissingMigrationError) as excinfo:
        migrate_to_current(Continuation(state_schema_version=1, seq=3))

    assert excinfo.value.message_type is Continuation


def test_duplicate_registration_is_refused() -> None:
    # One authoritative step per (type, from_version): silently replacing a
    # registered migration would let two modules disagree about a version's
    # meaning.
    migration.migration(MemoryBlob, from_version=1)(_step_appending(b"+2", to_version=2))

    with pytest.raises(ValueError, match="MemoryBlob"):
        migration.migration(MemoryBlob, from_version=1)(_step_appending(b"again", to_version=2))


@pytest.mark.parametrize("from_version", [0, -1])
def test_registering_below_the_baseline_version_is_refused(from_version: int) -> None:
    # Version 0 is the pre-versioned *reading* of a stored blob, normalized to
    # the baseline 1 before the chain is walked — so a `0 -> 1` step could
    # never run, and a negative version is nonsense. Refused at registration
    # (import time), where the mistake is cheap to see.
    with pytest.raises(ValueError, match="MemoryBlob"):
        migration.migration(MemoryBlob, from_version=from_version)(
            _step_appending(b"+", to_version=1)
        )


# --- Requirement: The runtime owns a single current state schema version -------


def test_version_zero_reads_as_the_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: Version zero reads as the baseline. proto3 zero-defaults mean a
    # default-constructed blob reads 0; the runtime has only ever written 1, so
    # baseline semantics are the only semantics it can have.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)
    migration.migration(MemoryBlob, from_version=1)(_step_appending(b"+2", to_version=2))

    migrated = migrate_to_current(_memory_blob(0))

    assert migrated.state_schema_version == 2
    assert migrated.entries[0].value == b"v1+2"


def test_version_zero_at_the_baseline_current_passes_through() -> None:
    # At CURRENT = 1 a version-0 blob is already current once normalized; the
    # identity fast path applies with no step consulted.
    blob = _memory_blob(0)
    assert migrate_to_current(blob) is blob


def test_a_future_version_blob_fails_the_bundle() -> None:
    # Scenario: A future-version blob fails the bundle (registry half: the
    # typed error names the message type, the found version, and the binary's
    # current version; the DoFn half lives in test_dofn_migration).
    blob = _memory_blob(CURRENT_STATE_SCHEMA_VERSION + 1)

    with pytest.raises(StateSchemaFromFutureError) as excinfo:
        migrate_to_current(blob)

    assert excinfo.value.message_type is MemoryBlob
    assert excinfo.value.found_version == CURRENT_STATE_SCHEMA_VERSION + 1
    assert excinfo.value.current_version == CURRENT_STATE_SCHEMA_VERSION
    message = str(excinfo.value)
    assert "MemoryBlob" in message
    assert str(CURRENT_STATE_SCHEMA_VERSION + 1) in message


def test_the_versioned_set_is_exactly_the_three_state_blobs() -> None:
    # Design D1: migration applies to the three messages that carry
    # `state_schema_version` and live in ReadModifyWriteState — no more, no
    # fewer. The completeness meta-tests in test_schema_compat iterate this
    # tuple, so it drifting from the proto would silently shrink the gate.
    assert (MemoryBlob, Continuation, LlmCacheBlob) == VERSIONED_MESSAGE_TYPES


def test_the_current_version_is_one_at_introduction() -> None:
    # The machinery lands against an all-identity landscape: version 1, no
    # registered steps, every read the identity fast path.
    assert CURRENT_STATE_SCHEMA_VERSION == 1


# --- Requirement: The runtime owns a single current state schema version
# (Scenario: Writers stamp the current version) --------------------------------


def test_writers_stamp_the_current_version() -> None:
    # Scenario: Writers stamp the current version. All three runtime writers
    # derive their stamp from the one constant, not a literal.
    assert Memory(now_ms=1_000).to_blob().state_schema_version == CURRENT_STATE_SCHEMA_VERSION
    assert (
        ReplayCache(None, now_ms=1_000).to_blob().state_schema_version
        == CURRENT_STATE_SCHEMA_VERSION
    )

    ctx = ActivationContext(
        entity_key=b"k",
        seq=0,
        now_ms=1_000,
        provider=FakeLLM(),
        memory_blob=None,
        cache_blob=None,
    )
    continuation = ctx.build_continuation(snapshot=b"s", adapter="test", deadline_ms=9_000)
    assert continuation.state_schema_version == CURRENT_STATE_SCHEMA_VERSION


def test_writers_follow_the_constant_not_a_literal(monkeypatch: pytest.MonkeyPatch) -> None:
    # The stamp is read from `core/migration.py` at call time: bumping the
    # constant moves every writer at once, with no second edit to forget.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 5)

    assert Memory(now_ms=1_000).to_blob().state_schema_version == 5
    assert ReplayCache(None, now_ms=1_000).to_blob().state_schema_version == 5

    ctx = ActivationContext(
        entity_key=b"k",
        seq=0,
        now_ms=1_000,
        provider=FakeLLM(),
        memory_blob=None,
        cache_blob=None,
    )
    assert (
        ctx.build_continuation(snapshot=b"s", adapter="test", deadline_ms=9).state_schema_version
        == 5
    )
