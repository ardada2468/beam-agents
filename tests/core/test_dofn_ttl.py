"""`on_ttl`: the working-memory GC callback and what it does with a live suspension.

Driven directly with fake state doubles rather than through a pipeline, for the
same reason `test_dofn_hitl_timer` is: the scenario under test is *what the
callback does with the state it is handed*, and the pipeline can only produce
the one firing order the runner chooses. The end-to-end ordering between
`TTL_TIMER` and `HITL_TIMER` lives in `test_dofn_streaming`.
"""

from __future__ import annotations

from typing import Any

from beam_agents._protos import Continuation, LlmCacheBlob, MemoryBlob, ToolIntent
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import (
    REASON_TTL_WIPED_SUSPENSION,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.model.fake import FakeLLM

_KEY = b"k"


class _FakeState:
    """Stand-in for any of the DoFn's value/bag/combining state specs."""

    def __init__(self, value: Any = None) -> None:
        self.value = value
        self.cleared = False

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.value = value

    def add(self, value: Any) -> None:
        self.value = value

    def clear(self) -> None:
        self.value = None
        self.cleared = True


async def _unused_agent(ctx: ActivationContext) -> Complete:  # pragma: no cover - never invoked
    raise AssertionError("the TTL callback must not run an activation")


def _continuation() -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=3,
        step_index=2,
        pending_intent_ids=["intent-1"],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=9_000,
    )


def _fire(cont: Continuation | None) -> tuple[list[Any], list[_FakeState]]:
    dofn = _AgentDoFn(_unused_agent, provider_factory=FakeLLM)
    memory = _FakeState(MemoryBlob())
    continuation = _FakeState(cont)
    llm_cache = _FakeState(LlmCacheBlob())
    pending = _FakeState([ToolIntent(intent_id="intent-1")])
    seq = _FakeState(3)
    emitted = list(
        dofn.on_ttl(
            key=_KEY,
            memory=memory,
            continuation=continuation,
            llm_cache=llm_cache,
            pending=pending,
            seq=seq,
        )
    )
    return emitted, [memory, continuation, llm_cache, pending, seq]


# --- Requirement: a TTL fire over a live suspension is reported, not silent ----


def test_a_ttl_fire_over_a_live_suspension_emits_a_dead_letter_record() -> None:
    # Scenario: A TTL fire over a live suspension emits a dead-letter record.
    # Watermark and wall clock are different clocks, so a backlog replay can
    # reach the (re-armed) event-time mark before real time reaches the
    # deadline. The suspension is genuinely unrecoverable at that point -- but
    # it must not vanish without a trace.
    emitted, states = _fire(_continuation())

    assert len(emitted) == 1
    tagged = emitted[0]
    assert tagged.tag == "errors"
    assert tagged.value == ActivationError(
        _KEY, REASON_TTL_WIPED_SUSPENSION, "seq=3,deadline_ms=9000"
    )
    # The wipe is unconditional: reporting the loss does not rescue the key.
    assert all(state.cleared for state in states)


def test_a_ttl_fire_with_no_live_suspension_stays_silent() -> None:
    # Scenario: A TTL fire with no live suspension stays silent. The overwhelming
    # majority of TTL fires are ordinary idle-key GC and must not dead-letter.
    emitted, states = _fire(None)

    assert emitted == []
    assert all(state.cleared for state in states)
