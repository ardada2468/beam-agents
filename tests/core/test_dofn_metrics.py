"""What the DoFn records, driven with fake state/timer handles.

Directly driving `process` and the two timer callbacks keeps these tests inside
the mutation gate's selection (the pipeline suites are deselected there) and
lets each activation outcome be asserted in isolation. What a fake sink cannot
show is whether the updates reach a real Beam metrics container -- that is
`test_dofn_pipeline.py`'s job, and the reason both kinds of test exist.
"""

from __future__ import annotations

from collections.abc import Iterator
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
from beam_agents.core.agent import Complete, FallbackContext, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.core.dofn import DETAIL_NO_CONTINUATION, _AgentDoFn
from beam_agents.core.loop import ActivationResult
from beam_agents.hitl import Deny, Drop, Escalate, HitlPolicy, Route
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.observability.metrics import (
    COUNTER_ACTIVATIONS,
    COUNTER_AGENT_ERRORS,
    COUNTER_INTENTS_EMITTED,
    COUNTER_LLM_CALLS,
    COUNTER_ORPHANED_RESULTS,
    COUNTER_SUSPENSIONS,
    COUNTER_TOOL_CALLS,
    DISTRIBUTION_ACTIVATION_MS,
    DISTRIBUTION_COMPLETION_TOKENS,
    DISTRIBUTION_ITERATIONS,
    DISTRIBUTION_LLM_MS,
    DISTRIBUTION_MEMORY_BYTES,
    DISTRIBUTION_OVERHEAD_MS,
    DISTRIBUTION_PROMPT_TOKENS,
    DISTRIBUTION_TOKENS,
    ActivationTally,
    MetricsSink,
    RuntimeMetrics,
)
from tests.core._dofn_helpers import (
    append_agent,
    inline_tool_agent,
    make_pong_provider,
    make_tool_registry,
    model_agent,
    raising_agent,
    seq_agent,
    suspend_then_complete_agent,
)

_KEY = b"k"


def _make_briefly_slow_provider() -> FakeLLM:
    """Provider slow enough to blow a 50ms activation budget, fast enough that a
    runtime which failed to apply that budget returns rather than hangs.
    """
    return FakeLLM([(match_any(), respond_with(b"pong", latency_ms=1_000))])


class _RecordingMetrics:
    """`MetricsSink` double: keeps counter totals and every distribution sample."""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.samples: dict[str, list[int]] = {}

    def incr(self, name: str, n: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + n

    def observe(self, name: str, value: int) -> None:
        self.samples.setdefault(name, []).append(value)


class _FakeValue:
    def __init__(self, value: Any = None) -> None:
        self.value = value

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.value = value

    def clear(self) -> None:
        self.value = None


class _FakeBag:
    def __init__(self, items: list[ToolIntent] | None = None) -> None:
        self.items = list(items or [])

    def read(self) -> list[ToolIntent]:
        return list(self.items)

    def add(self, item: ToolIntent) -> None:
        self.items.append(item)

    def clear(self) -> None:
        self.items = []


class _FakeSum:
    def __init__(self, value: int = 0) -> None:
        self.value = value

    def read(self) -> int:
        return self.value

    def add(self, n: int) -> None:
        self.value += n

    def clear(self) -> None:
        self.value = 0


class _FakeTimer:
    def __init__(self) -> None:
        self.set_to: Timestamp | None = None
        self.cleared = False

    def set(self, ts: Timestamp) -> None:
        self.set_to = ts

    def clear(self) -> None:
        self.cleared = True


def _main(emitted: list[Any]) -> list[Any]:
    """Main-output elements only. A committed activation also yields its two
    `.traces` records, which are not what these assertions are about.
    """
    return [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)]


def _tagged(emitted: list[Any], tag: str) -> list[Any]:
    return [e for e in emitted if isinstance(e, beam.pvalue.TaggedOutput) and e.tag == tag]


def _event(payload: bytes = b"go") -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=1_000, external_event=payload)


def _orphaned_result() -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=1_000)
    envelope.tool_result.intent_id = "ghost"
    return envelope


def _clock(*readings_ns: int) -> Any:
    """Monotonic double; falls back to a fixed reading once the script runs out
    so a test can pin one duration without scripting every internal reading.
    """
    remaining = list(readings_ns)

    def read() -> int:
        return remaining.pop(0) if remaining else 0

    return read


def _run(
    agent: Any,
    envelope: AgentEnvelope,
    *,
    provider_factory: Any = make_pong_provider,
    activation_timeout_s: float = 30.0,
    monotonic_ns: Any = None,
    continuation: Continuation | None = None,
    pending: list[ToolIntent] | None = None,
    seq_value: int = 0,
    handles: dict[str, Any] | None = None,
    tool_registry: Any = None,
) -> tuple[_RecordingMetrics, list[Any], _FakeSum]:
    metrics = _RecordingMetrics()
    dofn = _AgentDoFn(
        agent,
        provider_factory=provider_factory,
        activation_timeout_s=activation_timeout_s,
        tool_registry=tool_registry,
        metrics=metrics,
        monotonic_ns=monotonic_ns if monotonic_ns is not None else _clock(),
    )
    seq = _FakeSum(seq_value)
    state = {
        "memory": _FakeValue(MemoryBlob()),
        "continuation": _FakeValue(continuation),
        "llm_cache": _FakeValue(LlmCacheBlob()),
        "pending": _FakeBag(pending),
        "seq": seq,
        "ttl_timer": _FakeTimer(),
        "hitl_timer": _FakeTimer(),
    }
    if handles is not None:
        # Out-param for the few tests that assert on what the state/timer
        # handles received, rather than only on what was emitted or recorded.
        handles.update(state)
    dofn.setup()
    try:
        emitted = list(dofn.process((_KEY, envelope), **state))
    finally:
        dofn.teardown()
    return metrics, emitted, seq


# --- Requirement: commit-path metrics are recorded inside the commit ----------


def test_a_committed_activation_records_the_commit_path_metrics() -> None:
    metrics, emitted, seq = _run(append_agent, _event(b"hello"))

    assert _main(emitted) == [b"hello#0"]
    assert metrics.counters[COUNTER_ACTIVATIONS] == 1
    # `activations` and the SEQ increment are the same event by definition.
    assert seq.value == 1
    assert COUNTER_SUSPENSIONS not in metrics.counters
    assert COUNTER_INTENTS_EMITTED not in metrics.counters
    assert len(metrics.samples[DISTRIBUTION_ITERATIONS]) == 1
    assert len(metrics.samples[DISTRIBUTION_MEMORY_BYTES]) == 1
    assert len(metrics.samples[DISTRIBUTION_ACTIVATION_MS]) == 1
    # The agent wrote one ring entry, so the committed blob is not empty.
    assert metrics.samples[DISTRIBUTION_MEMORY_BYTES][0] > 0


def test_memory_bytes_samples_the_committed_working_memory_size() -> None:
    metrics, _, _ = _run(seq_agent, _event())

    # `seq_agent` writes nothing, so the committed blob is empty -- and the
    # sample is still taken, because "this key holds nothing" is a fact worth
    # having on the distribution.
    assert metrics.samples[DISTRIBUTION_MEMORY_BYTES] == [0]


def test_a_suspending_activation_records_suspensions_and_its_intent() -> None:
    metrics, emitted, _ = _run(suspend_then_complete_agent, _event())

    assert _main(emitted) == []
    assert len(_tagged(emitted, "intents")) == 1
    assert metrics.counters[COUNTER_ACTIVATIONS] == 1
    assert metrics.counters[COUNTER_SUSPENSIONS] == 1
    assert metrics.counters[COUNTER_INTENTS_EMITTED] == 1
    assert metrics.samples[DISTRIBUTION_ITERATIONS] == [1]


def test_a_model_call_is_counted_and_timed_through_the_commit() -> None:
    metrics, emitted, _ = _run(model_agent, _event())

    assert _main(emitted) == [b"pong"]
    assert metrics.counters[COUNTER_LLM_CALLS] == 1
    assert len(metrics.samples[DISTRIBUTION_LLM_MS]) == 1
    assert COUNTER_TOOL_CALLS not in metrics.counters
    # Scenario: An activation with no decoded usage contributes no sample. The
    # runtime's own `call_model` never decodes the opaque response, so nothing
    # reports usage on this path and a zero sample would be a fiction. The cost
    # pair obeys the same rule, so all three stay absent together.
    assert DISTRIBUTION_TOKENS not in metrics.samples
    assert DISTRIBUTION_PROMPT_TOKENS not in metrics.samples
    assert DISTRIBUTION_COMPLETION_TOKENS not in metrics.samples


def test_activation_ms_is_measured_from_the_injected_clock() -> None:
    # Scenario: Timings are injectable in tests. The bracket is around the
    # bounded bridge submission, so the first two readings are the activation's.
    metrics, _, _ = _run(seq_agent, _event(), monotonic_ns=_clock(1_000_000, 9_500_000))

    # 8.5ms floored: a Beam distribution carries integers, and half a
    # millisecond of resolution is not worth a float that cannot be recorded.
    assert metrics.samples[DISTRIBUTION_ACTIVATION_MS] == [8]
    assert isinstance(metrics.samples[DISTRIBUTION_ACTIVATION_MS][0], int)


def test_the_same_clock_times_the_activation_and_its_model_call() -> None:
    # One injected clock is threaded from the DoFn through `run_activation` into
    # the context, so both durations come from the same scripted timeline:
    # activation start, provider-call start, provider-call end, activation end.
    metrics, _, _ = _run(
        model_agent, _event(), monotonic_ns=_clock(0, 1_000_000, 4_000_000, 10_000_000)
    )

    assert metrics.samples[DISTRIBUTION_LLM_MS] == [3]
    assert metrics.samples[DISTRIBUTION_ACTIVATION_MS] == [10]
    # Scenario: Overhead subtracts model and tool time from the activation.
    # 10ms of wall time minus the 3ms provider call: the release-gate figure.
    assert metrics.samples[DISTRIBUTION_OVERHEAD_MS] == [7]


def test_an_inline_tool_is_counted_timed_and_excluded_from_overhead() -> None:
    # Scenario: A read-only tool runs inline on the runtime surface and is
    # counted -- through the commit path, with the tool's wall time excluded
    # from `overhead_ms` alongside the model call's. Clock readings: activation
    # start, tool start, tool end, activation end.
    metrics, emitted, _ = _run(
        inline_tool_agent,
        _event(b"ab"),
        tool_registry=make_tool_registry(),
        monotonic_ns=_clock(0, 1_000_000, 3_000_000, 10_000_000),
    )

    assert _main(emitted) == [b"AB"]
    assert metrics.counters[COUNTER_TOOL_CALLS] == 1
    assert COUNTER_LLM_CALLS not in metrics.counters
    assert metrics.samples[DISTRIBUTION_ACTIVATION_MS] == [10]
    assert metrics.samples[DISTRIBUTION_OVERHEAD_MS] == [8]


def test_a_plain_activation_has_overhead_equal_to_its_wall_time() -> None:
    # No model calls, no tools: the whole activation is runtime overhead, and
    # the sample count tracks `activations` one for one.
    metrics, _, _ = _run(seq_agent, _event(), monotonic_ns=_clock(2_000_000, 8_000_000))

    assert metrics.samples[DISTRIBUTION_ACTIVATION_MS] == [6]
    assert metrics.samples[DISTRIBUTION_OVERHEAD_MS] == [6]


# --- Requirement: a failed activation records no commit-path metric ----------


def test_a_raising_activation_records_only_the_error_and_its_duration() -> None:
    # Scenario: A failed activation records no commit-path metric.
    metrics, emitted, seq = _run(raising_agent, _event())

    # One dead letter, plus the ERROR trace event the failure routes now
    # emit beside it (trace-events).
    assert [e.tag for e in emitted] == ["errors", "traces"]
    assert emitted[0].tag == "errors"
    assert metrics.counters == {COUNTER_AGENT_ERRORS: 1}
    assert seq.value == 0
    # Scenario: A timed-out activation is still timed -- and so is a raising
    # one: the wall time was spent either way.
    assert len(metrics.samples[DISTRIBUTION_ACTIVATION_MS]) == 1
    assert set(metrics.samples) == {DISTRIBUTION_ACTIVATION_MS}


def test_a_timed_out_activation_records_the_error_and_its_duration() -> None:
    # Scenario: A timed-out activation is still timed. The timeout tail is the
    # most interesting part of the distribution, so it must not be dropped.
    #
    # The provider's latency is one second rather than `make_slow_provider`'s
    # thirty: long enough that the 50ms budget always expires first, short
    # enough that an implementation which forgot to *apply* the budget finishes
    # and fails this assertion instead of hanging.
    metrics, emitted, seq = _run(
        model_agent,
        _event(),
        provider_factory=_make_briefly_slow_provider,
        activation_timeout_s=0.05,
    )

    # One dead letter, plus the ERROR trace event the failure routes now
    # emit beside it (trace-events).
    assert [e.tag for e in emitted] == ["errors", "traces"]
    assert emitted[0].tag == "errors"
    assert metrics.counters == {COUNTER_AGENT_ERRORS: 1}
    assert seq.value == 0
    assert len(metrics.samples[DISTRIBUTION_ACTIVATION_MS]) == 1


def test_a_refused_resume_records_only_the_orphan_counter() -> None:
    # Scenario: A refused resume records no commit-path metric. No agent ran,
    # so there is no duration to report either.
    metrics, emitted, seq = _run(seq_agent, _orphaned_result())

    # One dead letter, plus the ERROR trace event the failure routes now
    # emit beside it (trace-events).
    assert [e.tag for e in emitted] == ["errors", "traces"]
    assert emitted[0].tag == "errors"
    # The refused intent id is carried on the record, so triage can tell *which*
    # result was orphaned rather than only that one was.
    assert emitted[0].value.detail == f"{DETAIL_NO_CONTINUATION}:ghost"
    assert emitted[0].value.entity_key == _KEY
    assert metrics.counters == {COUNTER_ORPHANED_RESULTS: 1}
    assert metrics.samples == {}
    assert seq.value == 0


def test_a_refused_approval_is_routed_and_counted_like_a_refused_result() -> None:
    # The approval variant is a separate routing branch in `process`: an
    # envelope that took the fresh-activation path instead would run the agent
    # and commit, so this pins the branch as well as the counter.
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=1_000)
    envelope.approval.intent_id = "ghost-approval"
    envelope.approval.approved = True

    metrics, emitted, seq = _run(seq_agent, envelope)

    # One dead letter, plus the ERROR trace event the failure routes now
    # emit beside it (trace-events).
    assert [e.tag for e in emitted] == ["errors", "traces"]
    assert emitted[0].tag == "errors"
    assert emitted[0].value.detail == f"{DETAIL_NO_CONTINUATION}:ghost-approval"
    assert metrics.counters == {COUNTER_ORPHANED_RESULTS: 1}
    assert seq.value == 0


def test_an_admitted_resume_commits_and_records_against_the_continuation() -> None:
    # The resume path with a *live* continuation: the activation runs against
    # the suspended activation's seq and step cursor, commits, and records the
    # commit-path metrics like any other activation.
    intent_id = intent_id_for(_KEY, 7, 0)
    cont = Continuation(
        state_schema_version=1,
        seq=7,
        step_index=1,
        pending_intent_ids=[intent_id],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=500,
        deadline_ms=60_000,
    )
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=1_000)
    envelope.tool_result.intent_id = intent_id
    envelope.tool_result.entity_key = _KEY
    envelope.tool_result.payload = b"done"
    envelope.tool_result.status = ToolResult.OK
    handles: dict[str, Any] = {}

    metrics, emitted, seq = _run(
        suspend_then_complete_agent,
        envelope,
        continuation=cont,
        pending=[ToolIntent(intent_id=intent_id, expires_at_ms=60_000)],
        seq_value=1,
        handles=handles,
    )

    assert _main(emitted) == [b"resumed:done"]
    assert metrics.counters == {COUNTER_ACTIVATIONS: 1}
    # A resume commits like any activation: SEQ advances, the continuation is
    # cleared, its pending intents are dropped, and both timers are re-armed or
    # cleared -- all of which need the state handles `process` forwarded.
    assert seq.value == 2
    assert handles["continuation"].value is None
    assert handles["pending"].items == []
    assert handles["memory"].value is not None
    assert handles["llm_cache"].value is not None
    assert handles["ttl_timer"].set_to is not None
    assert handles["hitl_timer"].cleared is True
    # Scenario: A resumed activation samples only its own steps.
    assert metrics.samples[DISTRIBUTION_ITERATIONS] == [0]
    assert len(metrics.samples[DISTRIBUTION_ACTIVATION_MS]) == 1


# --- Requirement: dead letters are counted wherever they are emitted ---------


async def _unused_agent(ctx: ActivationContext) -> Complete:  # pragma: no cover - never invoked
    raise AssertionError("a timer callback must not run an activation")


def _timer_dofn(metrics: _RecordingMetrics, policy: HitlPolicy | None = None) -> _AgentDoFn:
    return _AgentDoFn(
        _unused_agent,
        provider_factory=make_pong_provider,
        hitl_policy=policy,
        metrics=metrics,
    )


def _continuation(deadline_ms: int = 2_000) -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=3,
        step_index=2,
        pending_intent_ids=["intent-1"],
        adapter="test",
        snapshot=b"waiting",
        suspended_at_ms=1_000,
        deadline_ms=deadline_ms,
    )


def _fire_ttl(cont: Continuation | None) -> tuple[_RecordingMetrics, list[Any]]:
    metrics = _RecordingMetrics()
    emitted = list(
        _timer_dofn(metrics).on_ttl(
            key=_KEY,
            timestamp=Timestamp(micros=12_000 * 1000),
            memory=_FakeValue(MemoryBlob()),
            continuation=_FakeValue(cont),
            llm_cache=_FakeValue(LlmCacheBlob()),
            pending=_FakeBag(),
            seq=_FakeSum(3),
        )
    )
    return metrics, emitted


def _fire_hitl(policy: HitlPolicy | None) -> tuple[_RecordingMetrics, list[Any]]:
    metrics = _RecordingMetrics()
    emitted = list(
        _timer_dofn(metrics, policy).on_hitl(
            key=_KEY,
            timestamp=Timestamp(micros=2_000 * 1000),
            continuation=_FakeValue(_continuation()),
            pending=_FakeBag(),
            hitl_timer=_FakeTimer(),
            ttl_timer=_FakeTimer(),
        )
    )
    return metrics, emitted


def test_a_ttl_fire_over_a_live_suspension_counts_an_agent_error() -> None:
    # Scenario: A timer-emitted dead letter is counted. A record on `.errors`
    # that no counter accounts for would break the partition invariant.
    metrics, emitted = _fire_ttl(_continuation())

    assert [e.tag for e in emitted] == ["errors", "traces"]
    assert metrics.counters == {COUNTER_AGENT_ERRORS: 1}


def test_an_ordinary_ttl_fire_counts_nothing() -> None:
    metrics, emitted = _fire_ttl(None)

    assert emitted == []
    assert metrics.counters == {}


def test_a_hitl_drop_counts_an_agent_error_but_not_an_activation() -> None:
    # Scenario: A HITL timeout drop is counted. A timer fire is not a committed
    # activation, so `activations` must not move.
    def route(_fallback: FallbackContext) -> Route:
        return Drop("gave_up")

    metrics, emitted = _fire_hitl(HitlPolicy(on_timeout=route))

    # One dead letter, plus the ERROR trace event the failure routes now
    # emit beside it (trace-events).
    assert [e.tag for e in emitted] == ["errors", "traces"]
    assert emitted[0].tag == "errors"
    assert metrics.counters == {COUNTER_AGENT_ERRORS: 1}


def test_a_hitl_deny_counts_nothing() -> None:
    # Deny ends the wait with an ordinary output, not a dead letter.
    def route(_fallback: FallbackContext) -> Route:
        return Deny(b"degraded")

    metrics, emitted = _fire_hitl(HitlPolicy(on_timeout=route))

    # The degraded answer, plus the ERROR trace event: the wait still ended
    # without a real answer, and the trace records that on either route.
    assert emitted[0] == b"degraded"
    assert [e.tag for e in emitted[1:]] == ["traces"]
    assert metrics.counters == {}


def test_a_hitl_escalation_counts_its_intent() -> None:
    # Scenario: Intent count matches the intents output -- including the intent
    # a timer callback mints, which never passes through `_commit`.
    def route(_fallback: FallbackContext) -> Route:
        return Escalate(tool_name="pager", args_json="{}", timeout_ms=5_000)

    metrics, emitted = _fire_hitl(HitlPolicy(on_timeout=route, max_escalations=2))

    # The intent, plus its INTENT_EMITTED trace event (trace-events).
    assert [e.tag for e in emitted] == ["intents", "traces"]
    assert emitted[0].tag == "intents"
    assert metrics.counters == {COUNTER_INTENTS_EMITTED: 1}


def test_a_tally_carrying_tool_calls_and_usage_records_both() -> None:
    # `tool_calls` and `tokens` are reachable only from a tally that carries
    # them, and the DoFn currently drives `ActivationContext` -- which has no
    # inline-tool surface and never decodes provider usage, so neither can move
    # through a pipeline yet. They are wired for the `AgentContext`/adapter path,
    # which produces both, so the mapping from tally to metric is asserted here
    # against the recorder directly rather than left untested until then.
    metrics = _RecordingMetrics()
    dofn = _AgentDoFn(_unused_agent, provider_factory=make_pong_provider, metrics=metrics)
    result = ActivationResult(
        status="completed",
        seq=0,
        memory_blob=MemoryBlob(total_value_bytes=12),
        cache_blob=LlmCacheBlob(),
        intents=[ToolIntent(intent_id="i-1"), ToolIntent(intent_id="i-2")],
        traces=[],
        outputs=[],
        continuation=None,
        hitl_deadline_ms=None,
        tally=ActivationTally(
            llm_calls=2,
            tool_calls=3,
            iterations=5,
            total_tokens=41,
            prompt_tokens=30,
            completion_tokens=11,
            usage_observed=True,
            llm_ms=[7, 9],
            tool_ms=[2, 1, 1],
        ),
    )

    dofn._record_commit(result, activation_ms=30)

    assert metrics.counters == {
        COUNTER_ACTIVATIONS: 1,
        COUNTER_LLM_CALLS: 2,
        COUNTER_TOOL_CALLS: 3,
        # By the intent count, not one per commit.
        COUNTER_INTENTS_EMITTED: 2,
    }
    assert metrics.samples == {
        DISTRIBUTION_MEMORY_BYTES: [12],
        DISTRIBUTION_ITERATIONS: [5],
        # Scenario: Decoded usage is sampled. The total plus the input/output
        # split a provider price sheet is quoted in, one sample each.
        DISTRIBUTION_TOKENS: [41],
        DISTRIBUTION_PROMPT_TOKENS: [30],
        DISTRIBUTION_COMPLETION_TOKENS: [11],
        DISTRIBUTION_LLM_MS: [7, 9],
        # 30 - (7+9) llm - (2+1+1) tool.
        DISTRIBUTION_OVERHEAD_MS: [10],
    }


def test_an_activation_with_no_decoded_usage_records_no_cost_sample() -> None:
    # Scenario: An activation with no decoded usage contributes no sample. And
    # Scenario: A replayed walk bills nothing -- an all-cache-hit activation
    # accumulates no usage (the facade bills only provider-reached calls), so it
    # reaches the recorder with `usage_observed` false however many tokens its
    # budget meter charged.
    metrics = _RecordingMetrics()
    dofn = _AgentDoFn(_unused_agent, provider_factory=make_pong_provider, metrics=metrics)
    result = ActivationResult(
        status="completed",
        seq=0,
        memory_blob=MemoryBlob(),
        cache_blob=LlmCacheBlob(),
        intents=[],
        traces=[],
        outputs=[],
        continuation=None,
        hitl_deadline_ms=None,
        tally=ActivationTally(iterations=2, usage_observed=False),
    )

    dofn._record_commit(result, activation_ms=5)

    assert DISTRIBUTION_TOKENS not in metrics.samples
    assert DISTRIBUTION_PROMPT_TOKENS not in metrics.samples
    assert DISTRIBUTION_COMPLETION_TOKENS not in metrics.samples


def test_concurrent_call_durations_clamp_overhead_at_zero() -> None:
    # Scenario: Concurrent calls clamp overhead at zero. An agent that gathers
    # two calls concurrently can make summed call time exceed wall time; a
    # negative sample would be nonsense on a distribution of durations.
    metrics = _RecordingMetrics()
    dofn = _AgentDoFn(_unused_agent, provider_factory=make_pong_provider, metrics=metrics)
    result = ActivationResult(
        status="completed",
        seq=0,
        memory_blob=MemoryBlob(),
        cache_blob=LlmCacheBlob(),
        intents=[],
        traces=[],
        outputs=[],
        continuation=None,
        hitl_deadline_ms=None,
        tally=ActivationTally(llm_calls=2, llm_ms=[8, 9]),
    )

    dofn._record_commit(result, activation_ms=10)

    assert metrics.samples[DISTRIBUTION_OVERHEAD_MS] == [0]


# --- Lifecycle: the bridge and provider this recording path runs on -----------


def test_setup_builds_the_bridge_with_the_configured_cancel_grace() -> None:
    # One bridge thread and one provider per DoFn instance, built in `setup()`
    # (not `__init__`: the DoFn is constructed at pipeline-construction time and
    # pickled to the worker, where neither may exist yet). The cancel grace is
    # what bounds draining a cancelled activation, so it has to reach the bridge.
    dofn = _AgentDoFn(_unused_agent, provider_factory=make_pong_provider, cancel_grace_s=1.25)

    dofn.setup()
    try:
        bridge = dofn._bridge
        assert bridge is not None
        assert bridge._cancel_grace_s == 1.25
        assert dofn._provider is not None
    finally:
        dofn.teardown()


def test_teardown_stops_the_bridge_and_clears_both_handles() -> None:
    # A DoFn instance is reused across bundles and torn down once; leaving a
    # started bridge or a live provider behind leaks a thread per instance.
    dofn = _AgentDoFn(_unused_agent, provider_factory=make_pong_provider)
    dofn.setup()
    bridge = dofn._bridge
    assert bridge is not None

    dofn.teardown()

    assert dofn._bridge is None
    assert dofn._provider is None
    assert bridge._thread is None or not bridge._thread.is_alive()


# --- Requirement: the recorder is an injectable seam -------------------------


def test_the_dofn_defaults_to_the_beam_backed_recorder() -> None:
    dofn = _AgentDoFn(_unused_agent, provider_factory=make_pong_provider)

    assert isinstance(dofn._metrics, RuntimeMetrics)


def test_the_dofn_holds_the_injected_recorder_and_clock() -> None:
    metrics = _RecordingMetrics()
    clock = _clock()
    dofn = _AgentDoFn(
        _unused_agent, provider_factory=make_pong_provider, metrics=metrics, monotonic_ns=clock
    )

    assert dofn._metrics is metrics
    assert dofn._monotonic_ns is clock


def test_the_process_generator_is_lazy_but_the_commit_still_records() -> None:
    # `_commit` is a generator: recording placed after the yields would be
    # contingent on how the consumer drains it. Beam always drains fully, but
    # the ordering must not depend on that.
    metrics = _RecordingMetrics()
    dofn = _AgentDoFn(
        seq_agent, provider_factory=make_pong_provider, metrics=metrics, monotonic_ns=_clock()
    )
    dofn.setup()
    try:
        emitted: Iterator[object] = dofn.process(
            (_KEY, _event()),
            memory=_FakeValue(MemoryBlob()),
            continuation=_FakeValue(None),
            llm_cache=_FakeValue(LlmCacheBlob()),
            pending=_FakeBag(),
            seq=_FakeSum(),
            ttl_timer=_FakeTimer(),
            hitl_timer=_FakeTimer(),
        )
        first = next(iter(emitted))
    finally:
        dofn.teardown()

    assert first == b"0"
    assert metrics.counters[COUNTER_ACTIVATIONS] == 1


def test_the_recording_sink_is_never_read_back() -> None:
    # Metrics are output only: nothing in the runtime may branch on a recorded
    # value, so the sink protocol has no reader at all.
    assert set(dir(MetricsSink)) >= {"incr", "observe"}
    assert not any(name.startswith(("get", "read", "value")) for name in dir(MetricsSink))


def test_beam_output_tags_are_unchanged_by_counting() -> None:
    # The counting wrapper returns the same TaggedOutput the pure builder makes.
    _, emitted, _ = _run(raising_agent, _event())

    assert isinstance(emitted[0], beam.pvalue.TaggedOutput)
    assert emitted[0].tag == "errors"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
