"""The `BatchPolicy` configuration surface and the pure flush-decision helpers.

Beam-free: everything here is `AgentConfig` construction-time validation and
three predicates with no state, no clock, and no runner. The behaviors they
decide are exercised against the DoFn in `test_dofn_batching.py` and end to end
in `test_dofn_pipeline.py`.
"""

from __future__ import annotations

import pytest

from beam_agents.core.batching import (
    BUFFER_HEADROOM,
    DEFAULT_MAX_BATCH_SIZE,
    DEFAULT_MAX_WAIT_MS,
    BatchPolicy,
    BatchSettings,
    buffer_is_full,
    should_flush_on_size,
    should_flush_on_timer,
)
from beam_agents.core.transform import AgentConfig
from tests.core._dofn_helpers import make_pong_provider

# --- Requirement: BatchPolicy is opt-in configuration and NONE preserves
# --- per-event semantics ------------------------------------------------------


def test_none_policy_preserves_existing_semantics() -> None:
    # Scenario: NONE policy preserves existing semantics -- the construction
    # half of it. The default config names no batching at all, so the DoFn it
    # builds has no batch settings to act on and the element path is the
    # pre-change one. (The runtime half is asserted end to end in
    # test_dofn_pipeline.py.)
    config = AgentConfig(provider_factory=make_pong_provider)

    assert config.batch_policy is BatchPolicy.NONE
    assert config.max_batch_size is None
    assert config.max_wait_ms is None
    assert config.max_buffered_events is None
    assert config.batch_settings() is None


def test_adaptive_defaults_are_positive_and_derive_the_buffer_cap() -> None:
    # The opt-in needs no knobs: ADAPTIVE alone resolves the documented
    # defaults, and the hard buffer cap is derived from the size threshold so a
    # user who tunes one does not silently leave the other at a value that
    # cannot hold a batch.
    config = AgentConfig(provider_factory=make_pong_provider, batch_policy=BatchPolicy.ADAPTIVE)

    settings = config.batch_settings()
    assert settings == BatchSettings(
        max_batch_size=DEFAULT_MAX_BATCH_SIZE,
        max_wait_ms=DEFAULT_MAX_WAIT_MS,
        max_buffered_events=BUFFER_HEADROOM * DEFAULT_MAX_BATCH_SIZE,
    )
    assert settings is not None
    assert settings.max_buffered_events >= settings.max_batch_size


def test_adaptive_knobs_are_carried_through_verbatim() -> None:
    config = AgentConfig(
        provider_factory=make_pong_provider,
        batch_policy=BatchPolicy.ADAPTIVE,
        max_batch_size=3,
        max_wait_ms=500,
        max_buffered_events=7,
    )

    assert config.batch_settings() == BatchSettings(
        max_batch_size=3, max_wait_ms=500, max_buffered_events=7
    )


@pytest.mark.parametrize(
    ("knobs", "field_name"),
    [
        ({"max_batch_size": 0}, "max_batch_size"),
        ({"max_batch_size": -1}, "max_batch_size"),
        ({"max_wait_ms": 0}, "max_wait_ms"),
        ({"max_wait_ms": -5}, "max_wait_ms"),
        ({"max_buffered_events": 0}, "max_buffered_events"),
        ({"max_batch_size": 8, "max_buffered_events": 4}, "max_buffered_events"),
    ],
)
def test_misconfigured_adaptive_knobs_fail_at_the_construction_site(
    knobs: dict[str, int], field_name: str
) -> None:
    # Scenario: Misconfigured batch knobs fail at the construction site. The
    # message names the offending field, and it is raised before any pipeline
    # exists.
    with pytest.raises(ValueError, match=f"AgentConfig.{field_name}"):
        AgentConfig(
            provider_factory=make_pong_provider,
            batch_policy=BatchPolicy.ADAPTIVE,
            **knobs,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("field_name", ["max_batch_size", "max_wait_ms", "max_buffered_events"])
def test_a_batch_knob_under_the_none_policy_is_refused(field_name: str) -> None:
    # Scenario: Misconfigured batch knobs fail at the construction site (the
    # NONE half). A knob that silently does nothing is a misconfiguration trap,
    # so setting one without opting in raises rather than being ignored.
    with pytest.raises(ValueError, match=f"AgentConfig.{field_name}"):
        AgentConfig(provider_factory=make_pong_provider, **{field_name: 4})  # type: ignore[arg-type]


# --- Requirement: the pure flush-decision helpers -----------------------------


def test_the_size_trigger_fires_at_the_threshold_and_not_before() -> None:
    assert should_flush_on_size(2, 3, continuation_live=False) is False
    assert should_flush_on_size(3, 3, continuation_live=False) is True
    # Deferral (D6) can grow the buffer past the threshold; the trigger stays
    # true, so the flush runs as soon as the suspension resolves.
    assert should_flush_on_size(9, 3, continuation_live=False) is True


def test_the_size_trigger_defers_under_a_live_continuation() -> None:
    # Scenario: The size trigger defers during a suspension. A flush that
    # suspended would overwrite the live continuation and orphan its intents.
    assert should_flush_on_size(3, 3, continuation_live=True) is False
    assert should_flush_on_size(99, 3, continuation_live=True) is False


def test_the_timer_trigger_needs_a_non_empty_buffer_and_no_live_continuation() -> None:
    assert should_flush_on_timer(1, continuation_live=False) is True
    # Scenario: A stale flush firing over an empty buffer is a no-op.
    assert should_flush_on_timer(0, continuation_live=False) is False
    # Scenario: A timer firing during a suspension does not overwrite the
    # continuation.
    assert should_flush_on_timer(4, continuation_live=True) is False
    assert should_flush_on_timer(0, continuation_live=True) is False


def test_the_overflow_predicate_caps_the_buffer_at_max_buffered_events() -> None:
    # Scenario: Overflow during deferral is explicit.
    assert buffer_is_full(3, 4) is False
    assert buffer_is_full(4, 4) is True
    assert buffer_is_full(5, 4) is True


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
