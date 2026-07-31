"""Golden-corpus compat tests: every committed version must replay to current.

Guards the two-tier evolution rule (design D5/D6 of add-state-schema-migration,
extending add-wire-schemas-and-coders D6). Fixtures live per schema version
under ``tests/core/golden/v<N>/``; each committed blob is decoded with the
current bindings, migrated to ``CURRENT_STATE_SCHEMA_VERSION`` (versioned state
blobs only — the wire messages stay additive-only and are asserted as decoded),
and asserted field-equal against the same builders that produced the bytes.
These tests deliberately do NOT assert byte-identical re-encoding: a protobuf
library upgrade may change serialization details while remaining
wire-compatible.

The corpus replay and the completeness meta-tests carry the offline
``semantics`` marker — project.md names state compat (golden blobs) a semantics
gate — so they ride the required ``ci`` offline semantics selection. The
completeness meta-tests are the executable merge gate on breaking changes: a
``CURRENT_STATE_SCHEMA_VERSION`` bump that ships without its migration steps or
its corpus directories goes red here, naming exactly what is missing.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from google.protobuf.message import Message

from beam_agents._protos import Continuation, LlmCacheBlob, MemoryBlob, ToolIntent, TraceEvent
from beam_agents.core import migration
from beam_agents.core.migration import (
    CURRENT_STATE_SCHEMA_VERSION,
    VERSIONED_MESSAGE_TYPES,
    migrate_to_current,
)
from tests.core.golden.generate import CORPUS, write_current

GOLDEN_DIR = Path(__file__).parent / "golden"

_ALL_FIXTURES = sorted((version, name) for version, fixtures in CORPUS.items() for name in fixtures)


def _replayed(message: Message) -> Message:
    """`migrate_to_current` for the versioned state blobs; identity otherwise.

    The unversioned wire messages (design D1) never migrate — their corpus
    entries pin additive decode compatibility only.
    """
    if isinstance(message, MemoryBlob):
        return migrate_to_current(message)
    if isinstance(message, Continuation):
        return migrate_to_current(message)
    if isinstance(message, LlmCacheBlob):
        return migrate_to_current(message)
    return message


# --- Requirement: A cross-version golden corpus replays every historical
# version to current -----------------------------------------------------------


@pytest.mark.semantics
@pytest.mark.parametrize(("version", "name"), _ALL_FIXTURES)
def test_every_historical_version_replays_to_current(version: int, name: str) -> None:
    # Scenario: Every historical version replays to current.
    # Scenario: Golden blobs decode with current bindings.
    expected = CORPUS[version][name]
    blob_path = GOLDEN_DIR / f"v{version}" / f"{name}.bin"
    assert blob_path.exists(), f"missing committed golden fixture: {blob_path}"

    decoded = type(expected)()
    decoded.ParseFromString(blob_path.read_bytes())  # must not raise

    # Field-level equality after migration, never byte-identical re-encode.
    assert _replayed(decoded) == _replayed(expected)


@pytest.mark.semantics
def test_the_corpus_cannot_silently_shrink() -> None:
    # Scenario: The corpus cannot silently shrink.
    # Scenario: Golden corpus is laid out per version.
    assert _missing_version_directories(CURRENT_STATE_SCHEMA_VERSION) == []
    assert _missing_versioned_fixtures(CURRENT_STATE_SCHEMA_VERSION) == []
    for version, fixtures in CORPUS.items():
        committed = {path.stem for path in (GOLDEN_DIR / f"v{version}").glob("*.bin")}
        assert committed == set(fixtures), f"v{version} fixtures out of sync with builders"
    # No fixture sits outside a version directory.
    assert list(GOLDEN_DIR.glob("*.bin")) == []
    # The v1 baseline covers all eight message types (the versioned three plus
    # the additive-only wire messages, `StateSnapshot` among them).
    assert len({type(message) for message in CORPUS[1].values()}) == 8


@pytest.mark.semantics
def test_every_pre_current_version_has_a_registered_migration_step() -> None:
    # The registry side of the gate, at the shipped current version: every
    # (type, n) for n in 1..CURRENT-1 has a step (vacuously true at CURRENT=1).
    assert _missing_migration_steps(CURRENT_STATE_SCHEMA_VERSION) == []


# --- Requirement: Breaking proto changes are gated on migration and corpus
# artifacts --------------------------------------------------------------------


def _missing_version_directories(current: int) -> list[str]:
    return [f"v{v}" for v in range(1, current + 1) if not (GOLDEN_DIR / f"v{v}").is_dir()]


def _missing_versioned_fixtures(current: int) -> list[str]:
    """Versioned blob types lacking a fixture in a version directory.

    All three versioned types were introduced at v1, so every version directory
    owes each of them at least one fixture.
    """
    missing: list[str] = []
    for version in range(1, current + 1):
        fixture_types = {type(message) for message in CORPUS.get(version, {}).values()}
        missing.extend(
            f"v{version}/{message_type.__name__}"
            for message_type in VERSIONED_MESSAGE_TYPES
            if message_type not in fixture_types
        )
    return missing


def _missing_migration_steps(current: int) -> list[str]:
    return [
        f"{message_type.__name__} from_version={version}"
        for message_type in VERSIONED_MESSAGE_TYPES
        for version in range(1, current)
        if (message_type, version) not in migration._REGISTRY
    ]


@pytest.mark.semantics
def test_a_version_bump_without_migration_functions_fails_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: A version bump without migration functions fails CI. Simulate
    # the bump: raise the constant without shipping the steps, and the
    # completeness check names every message type and missing version.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)

    missing = _missing_migration_steps(migration.CURRENT_STATE_SCHEMA_VERSION)

    assert missing == [
        "MemoryBlob from_version=1",
        "Continuation from_version=1",
        "LlmCacheBlob from_version=1",
    ]


@pytest.mark.semantics
def test_a_version_bump_without_a_corpus_entry_fails_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: A version bump without a corpus entry fails CI. The same
    # simulated bump without a frozen v2 directory (or v2 fixtures) goes red
    # naming the missing directory and the fixtures every versioned type owes.
    monkeypatch.setattr(migration, "CURRENT_STATE_SCHEMA_VERSION", 2)
    current = migration.CURRENT_STATE_SCHEMA_VERSION

    assert _missing_version_directories(current) == ["v2"]
    assert _missing_versioned_fixtures(current) == [
        "v2/MemoryBlob",
        "v2/Continuation",
        "v2/LlmCacheBlob",
    ]


def test_the_generator_writes_only_the_current_versions_directory(tmp_path: Path) -> None:
    # Scenario: Historical corpus directories are frozen. `write_current` into
    # a fresh tree creates exactly `v<CURRENT>/` — there is no code path that
    # can touch a historical directory, which is what makes the committed
    # bytes the artifact.
    written = write_current(tmp_path)

    assert {path.parent for path in written} == {tmp_path / f"v{CURRENT_STATE_SCHEMA_VERSION}"}
    assert {child.name for child in tmp_path.iterdir()} == {f"v{CURRENT_STATE_SCHEMA_VERSION}"}
    assert {path.stem for path in written} == set(CORPUS[CURRENT_STATE_SCHEMA_VERSION])


# --- Requirement: Schema evolution is additive and golden-blob guarded --------


def test_pre_v1_baseline_blobs_decode_with_fields_added_later() -> None:
    # Scenario: An intent written before kind existed reads as a tool call.
    # Scenario: Escalation count defaults to zero.
    # The committed `v1/tool_intent`/`v1/continuation` blobs were serialized
    # before `ToolIntent.kind` and `Continuation.escalations` existed, so they
    # are the real pre-field bytes, not a reconstruction.
    intent = ToolIntent()
    intent.ParseFromString((GOLDEN_DIR / "v1" / "tool_intent.bin").read_bytes())
    assert intent.kind == ToolIntent.TOOL_KIND_UNSPECIFIED

    cont = Continuation()
    cont.ParseFromString((GOLDEN_DIR / "v1" / "continuation.bin").read_bytes())
    assert cont.escalations == 0

    # Scenario: An intent written without trace_id still decodes.
    # Same bytes, one schema generation later: `trace_id` is additive under
    # state_schema_version = 1, so a pre-field intent reads as "no trace
    # correlation available" rather than failing.
    assert intent.trace_id == b""


def test_trace_event_correlation_ids_round_trip_at_wire_widths() -> None:
    # Scenario: Correlation identifiers round-trip at their wire widths.
    event = TraceEvent(
        trace_id=bytes(range(16)),
        span_id=bytes(range(8)),
        parent_span_id=bytes(range(8, 16)),
    )
    decoded = TraceEvent()
    decoded.ParseFromString(event.SerializeToString(deterministic=True))

    assert decoded.trace_id == bytes(range(16))
    assert decoded.span_id == bytes(range(8))
    assert decoded.parent_span_id == bytes(range(8, 16))


def test_suspended_event_type_round_trips() -> None:
    # Scenario: The suspension event type round-trips.
    event = TraceEvent(event_type=TraceEvent.SUSPENDED)
    decoded = TraceEvent()
    decoded.ParseFromString(event.SerializeToString(deterministic=True))
    assert decoded.event_type == TraceEvent.SUSPENDED

    # A reader that predates the value sees an unrecognized enum number, not a
    # parse failure. Proto3 open enums keep the number, so decoding the same
    # bytes into a message whose enum stops at ERROR yields 7 — which is what
    # an older binding does, and why this is additive.
    assert TraceEvent.SUSPENDED == 7
    assert decoded.event_type not in (
        TraceEvent.EVENT_TYPE_UNSPECIFIED,
        TraceEvent.ERROR,
    )
