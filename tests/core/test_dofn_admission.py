"""Unit tests for the resume-admission predicate (fail-closed layer 1).

`_admission_failure` is a pure function of a `Continuation`, the `PENDING`
intents, an `intent_id`, and the element's clock, so it is tested directly
here rather than through a pipeline -- which also keeps it inside the mutation
gate's test selection (the DoFn pipeline suites are deselected there).
"""

from __future__ import annotations

import pytest

from beam_agents._protos import Continuation, ToolIntent
from beam_agents.core.dofn import (
    DETAIL_DEADLINE_PASSED,
    DETAIL_INTENT_EXPIRED,
    DETAIL_NO_CONTINUATION,
    DETAIL_UNKNOWN_INTENT,
    _admission_failure,
)


def _live_continuation(deadline_ms: int = 2_000) -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=0,
        step_index=1,
        pending_intent_ids=["intent-1"],
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=deadline_ms,
    )


def _pending(intent_id: str = "intent-1", expires_at_ms: int = 60_000) -> list[ToolIntent]:
    return [ToolIntent(intent_id=intent_id, expires_at_ms=expires_at_ms)]


def test_admission_accepts_a_live_unexpired_resume() -> None:
    assert _admission_failure(_live_continuation(), _pending(), "intent-1", 1_500) is None


def test_admission_refuses_when_there_is_no_continuation() -> None:
    assert _admission_failure(None, [], "intent-1", 1_500) == DETAIL_NO_CONTINUATION


def test_admission_refuses_an_intent_id_the_continuation_never_pended() -> None:
    assert _admission_failure(_live_continuation(), _pending(), "other", 1_500) == (
        DETAIL_UNKNOWN_INTENT
    )


@pytest.mark.parametrize("now_ms", [2_000, 2_001])
def test_admission_refuses_at_or_after_the_deadline(now_ms: int) -> None:
    # Scenario: An approval arriving after the deadline is refused even before
    # the timer fires. The boundary is inclusive: live means strictly before.
    assert _admission_failure(_live_continuation(), _pending(), "intent-1", now_ms) == (
        DETAIL_DEADLINE_PASSED
    )


@pytest.mark.parametrize("deadline_ms", [0, -1])
@pytest.mark.parametrize("now_ms", [0, -1_000])
def test_admission_treats_a_non_positive_deadline_as_expired(deadline_ms: int, now_ms: int) -> None:
    # Fail closed: "no deadline recorded" reads as "do not resume", never as
    # "resume forever". The runtime never writes one, so this is corruption --
    # and it stays refused even for a clock that predates the epoch, where a
    # plain `now_ms >= deadline_ms` comparison would let it through.
    cont = _live_continuation(deadline_ms=deadline_ms)
    assert _admission_failure(cont, _pending(), "intent-1", now_ms) == DETAIL_DEADLINE_PASSED


def test_admission_admits_the_smallest_positive_deadline() -> None:
    # The non-positive check must not swallow a real (if absurdly small)
    # deadline: at deadline 1, the clock alone decides.
    cont = _live_continuation(deadline_ms=1)
    assert _admission_failure(cont, _pending(), "intent-1", 0) is None
    assert _admission_failure(cont, _pending(), "intent-1", 1) == DETAIL_DEADLINE_PASSED


def test_admission_refuses_a_result_whose_pending_intent_expired() -> None:
    # Scenario: A result whose intent has expired is refused. Reachable when a
    # rewritten continuation (an escalation) outlives the intent it replaced.
    cont = _live_continuation(deadline_ms=100_000)
    assert _admission_failure(cont, _pending(expires_at_ms=1_400), "intent-1", 1_500) == (
        DETAIL_INTENT_EXPIRED
    )


def test_admission_ignores_expiries_of_other_pending_intents() -> None:
    cont = _live_continuation(deadline_ms=100_000)
    pending = _pending(expires_at_ms=60_000) + _pending("intent-2", expires_at_ms=1)
    assert _admission_failure(cont, pending, "intent-1", 1_500) is None


def test_admission_survives_a_pending_bag_that_lost_the_intent() -> None:
    # The bag is cleared and rewritten on every commit; the continuation's
    # deadline remains the authority when no matching entry is left.
    assert _admission_failure(_live_continuation(), [], "intent-1", 1_500) is None
