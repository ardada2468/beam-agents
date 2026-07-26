"""Unit tests for the chaos commit-failure helper.

Drives real `_AgentDoFn` pipelines (not a mocked object) so the assertions are
about actual commit/retry behavior, not about the helper's internal bookkeeping
in isolation.
"""

from __future__ import annotations

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_agents._protos import AgentEnvelope
from beam_agents.core.dofn import _AgentDoFn
from beam_agents.core.loop import ActivationResult
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.testing.chaos import Matcher, fail_first_matching_commit
from tests.core._dofn_helpers import keyed, make_pong_provider, seq_agent


def _event(key: bytes, payload: bytes, t_ms: int = 1000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


def _spy(*, return_value: bool) -> tuple[list[int], Matcher]:
    calls: list[int] = []

    def matcher(result: ActivationResult) -> bool:
        calls.append(1)
        return return_value

    return calls, matcher


def test_first_matching_commit_fails_then_succeeds_on_retry() -> None:
    # Scenario: only the first matching commit fails.
    calls, matcher = _spy(return_value=True)
    with fail_first_matching_commit(matcher), BeamTestPipeline() as p:
        envs = p | beam.Create([_event(b"k", b"go")])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        # Beam's own retry recovers from the forced failure: the
        # activation still commits and produces its normal output.
        assert_that(out.output, equal_to([b"0"]))
        assert_that(out.errors, equal_to([]), label="no-errors")

    # The matcher is only ever consulted for the first (failing) attempt;
    # once tripped, every later commit (including the retry) short-circuits
    # straight to the original commit logic.
    assert len(calls) == 1


def test_non_matching_commit_is_never_failed() -> None:
    # Scenario: a non-matching commit is never failed.
    calls, matcher = _spy(return_value=False)
    with fail_first_matching_commit(matcher), BeamTestPipeline() as p:
        envs = p | beam.Create([_event(b"k", b"a", 1000), _event(b"k2", b"b", 1000)])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"0", b"0"]))
        assert_that(out.errors, equal_to([]), label="no-errors")

    # Every commit attempt consults a matcher that never matches, so nothing
    # is ever failed and the matcher is evaluated once per (successful,
    # first-attempt) commit.
    assert len(calls) == 2


def test_original_commit_is_restored_after_the_context_exits() -> None:
    # Not part of a spec scenario directly, but load-bearing: the fault must
    # not leak into later pipelines/tests.
    original = _AgentDoFn._commit
    with fail_first_matching_commit():
        assert _AgentDoFn._commit is not original
    assert _AgentDoFn._commit is original


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
