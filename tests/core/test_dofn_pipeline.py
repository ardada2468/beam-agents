"""Pipeline tests for single-activation routing and state topology.

Covers the bounded (order-insensitive) scenarios: the state/timer topology is
protobuf-coded and pickle-free, a fresh key seeds seq 0, an event starts an
activation, and an unmatched resume routes to ``.errors``. Ordered multi-element
scenarios (resume, seq progression, timeouts, timers, interleaving) live in
test_dofn_streaming.

Adaptive batching's runner-level behavior lives here too, in its own section:
the ``FLUSH_TIMER`` scenarios need scripted ``TestStream`` processing-time
advances (never ``sleep()``), and whether a REAL_TIME timer set from the
runtime's injected wall clock actually fires is exactly what no fake handle can
show. The buffering/flush *logic* is asserted runner-free in
test_dofn_batching.py.
"""

from __future__ import annotations

import time
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.coders.coders import VarIntCoder
from apache_beam.coders.typecoders import registry as coder_registry
from apache_beam.metrics.metric import MetricResults, MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.agent import intent_id_for
from beam_agents.core.batching import BatchPolicy, BatchSettings
from beam_agents.core.coders import DeterministicProtoCoder, register_coders
from beam_agents.core.dofn import (
    DETAIL_NO_CONTINUATION,
    REASON_ORPHANED,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.hitl import HITL_TIMEOUT_OUTPUT
from beam_agents.observability.metrics import NAMESPACE
from tests.core._dofn_helpers import (
    FixedClock,
    SteppingClock,
    append_agent,
    batch_join_agent,
    batch_suspend_agent,
    inline_tool_agent,
    keyed,
    make_pong_provider,
    make_tool_registry,
    outcome_routing_agent,
    seq_agent,
    suspend_then_complete_agent,
    usage_reporting_agent,
)

# Large enough that working-memory GC never fires mid-stream in the batching
# scenarios below (their watermark advances are seconds; this is ~11 days).
_BIG_TTL_MS = 1_000_000_000


def _event(key: bytes, payload: bytes, t_ms: int = 1000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


# --- Requirement: keyed state and timer topology -------------------------------


def test_proto_state_specs_use_deterministic_coders() -> None:
    # Scenario: state specs are protobuf-backed and pickle-free.
    for spec in (_AgentDoFn.MEMORY, _AgentDoFn.CONTINUATION, _AgentDoFn.LLM_CACHE):
        assert isinstance(spec.coder, DeterministicProtoCoder)
    assert isinstance(_AgentDoFn.PENDING.coder, DeterministicProtoCoder)
    # SEQ is an integer counter, coded as a varint (not pickle).
    assert isinstance(_AgentDoFn.SEQ.coder, VarIntCoder)


def test_run_agent_registers_deterministic_envelope_coder() -> None:
    register_coders()
    resolved = coder_registry.get_coder(AgentEnvelope)
    assert isinstance(resolved, DeterministicProtoCoder)
    assert resolved.is_deterministic() is True


# --- Requirement: element routing by envelope kind -----------------------------


def test_fresh_key_reads_seq_zero() -> None:
    # Scenario: a fresh key reads versioned-empty facades and zero seq.
    with BeamTestPipeline() as p:
        envs = p | beam.Create([_event(b"k", b"go")])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"0"]))


def test_event_starts_activation() -> None:
    # Scenario: event starts an activation (and its memory write commits).
    with BeamTestPipeline() as p:
        envs = p | beam.Create([_event(b"k", b"hello")])
        out = keyed(envs) | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"hello#0"]))


def test_orphaned_result_routes_to_errors() -> None:
    # Scenario: orphaned resume mutates nothing (no continuation to match).
    envelope = AgentEnvelope(entity_key=b"k", event_time_ms=1000)
    envelope.tool_result.intent_id = "ghost"
    envelope.tool_result.status = ToolResult.OK

    with BeamTestPipeline() as p:
        envs = p | beam.Create([envelope])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([]), label="no-output")
        assert_that(
            out.errors,
            equal_to(
                [ActivationError(b"k", REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:ghost", 1000)]
            ),
            label="orphaned-error",
        )


def test_orphaned_approval_routes_to_errors() -> None:
    # Scenario: an approval-kind element with no matching continuation is also
    # routed as orphaned (exercises the approval routing branch).
    envelope = AgentEnvelope(entity_key=b"k", event_time_ms=1000)
    envelope.approval.intent_id = "ghost"
    envelope.approval.approved = True

    with BeamTestPipeline() as p:
        envs = p | beam.Create([envelope])
        out = keyed(envs) | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([]), label="no-output")
        assert_that(
            out.errors,
            equal_to(
                [ActivationError(b"k", REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:ghost", 1000)]
            ),
            label="orphaned-approval",
        )


# --- Requirement: counters close over the transform's outputs -----------------


def _runtime_metrics(
    result: beam.runners.runner.PipelineResult,
) -> tuple[dict[str, int], dict[str, Any]]:
    """Counter totals and distribution results for the runtime namespace.

    Reads `MetricResult.result`, which falls back to the attempted value: most
    runners (this one included) do not populate committed metrics, and the
    spec's own requirement is that these are attempted values.
    """
    query = result.metrics().query(MetricsFilter().with_namespace(NAMESPACE))
    counters = {m.key.metric.name: m.result for m in query[MetricResults.COUNTERS]}
    distributions = {m.key.metric.name: m.result for m in query[MetricResults.DISTRIBUTIONS]}
    return counters, distributions


def _expect_two_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 2, f"expected exactly two dead letters, got {items!r}"


def _expect_one_intent(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one intent, got {items!r}"


def test_runtime_counters_close_over_the_transform_outputs() -> None:
    # Scenarios: Every declared metric is queryable after a pipeline run; Error
    # counts partition the errors output; Intent count matches the intents
    # output. Asserting the counters against the *same run's* element counts is
    # the only thing that catches an emission site that forgot to count.
    orphan = AgentEnvelope(entity_key=b"orphan", event_time_ms=1000)
    orphan.tool_result.intent_id = "ghost"
    elements = [
        _event(b"ok", b"hello"),
        _event(b"act", b"ACT"),
        _event(b"fail", b"FAIL"),
        orphan,
    ]

    with BeamTestPipeline() as p:
        out = keyed(p | beam.Create(elements)) | RunAgent(
            outcome_routing_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"hello", b"acted"]), label="outputs")
        assert_that(out.errors, _expect_two_errors, label="errors")
        assert_that(out.intents, _expect_one_intent, label="intents")

    counters, distributions = _runtime_metrics(p.result)

    assert counters["activations"] == 2
    assert counters["intents_emitted"] == 1
    assert counters["agent_errors"] == 1
    assert counters["orphaned_results"] == 1
    # The partition: every dead letter is accounted for by exactly one counter.
    assert counters["agent_errors"] + counters["orphaned_results"] == 2
    # Scenario: Suspensions are a subset of activations.
    assert counters.get("suspensions", 0) == 0
    assert counters.get("suspensions", 0) <= counters["activations"]

    # One sample per committed activation, and one per agent run: the failing
    # element contributes a duration but no commit-path sample.
    assert distributions["memory_bytes"].count == 2
    assert distributions["iterations"].count == 2
    assert distributions["activation_ms"].count == 3
    assert distributions["activation_ms"].min >= 0
    # The `ACT` branch consumed one step; the plain completion consumed none.
    assert distributions["iterations"].sum == 1


def test_the_usage_distributions_are_queryable_after_a_pipeline_run() -> None:
    # Scenario: Every declared metric is queryable after a pipeline run -- the
    # cost pair included. `tokens` alone samples totals, but input and output
    # tokens are priced differently by every provider, so the split is what a
    # price sheet multiplies and it has to reach the runner's dashboard.
    with BeamTestPipeline() as p:
        out = keyed(p | beam.Create([_event(b"u", b"go")])) | RunAgent(
            usage_reporting_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"reported"]))

    _, distributions = _runtime_metrics(p.result)

    assert distributions["tokens"].sum == 12
    assert distributions["prompt_tokens"].sum == 7
    assert distributions["completion_tokens"].sum == 5
    # One sample each per committed activation with known usage, so the three
    # counts move together and a mean is comparable across them.
    assert distributions["prompt_tokens"].count == 1
    assert distributions["completion_tokens"].count == 1


def test_a_suspension_is_counted_and_never_exceeds_activations() -> None:
    # Scenario: Suspensions are a subset of activations.
    #
    # The event time is an hour ahead of the wall clock so the suspension's
    # real-time HITL deadline cannot be reached while the pipeline runs, and the
    # only thing that can end the suspension is the end-of-input watermark
    # advance firing `TTL_TIMER`. That fire is deliberately not asserted on:
    # whether it lands before the pipeline drains is a race, and its dead
    # letter's `agent_errors` increment would not be visible anyway --
    # `_AgentDoFn` declares a REAL_TIME timer, which forces every RunAgent
    # pipeline onto the *classic* DirectRunner (`_FnApiRunnerSupportVisitor` in
    # `direct_runner.py`), whose metrics implementation reports one bundle's
    # updates and drops the rest. That is the runner, not this code: a plain
    # `beam.ParDo` counter under the same runner reports 1 for 3 elements split
    # across three TestStream groups. The timer path's counting is asserted with
    # fake handles in test_dofn_metrics, where no runner is in the way.
    #
    # What *is* deterministic here is the element's own bundle, which is where
    # every counter below comes from.
    future_ms = int(time.time() * 1000) + 3_600_000
    envelope = AgentEnvelope(entity_key=b"s", event_time_ms=future_ms, external_event=b"go")

    with BeamTestPipeline() as p:
        out = keyed(p | beam.Create([envelope])) | RunAgent(
            suspend_then_complete_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([]), label="no-output")
        assert_that(out.intents, _expect_one_intent, label="intents")

    counters, _ = _runtime_metrics(p.result)

    assert counters["activations"] == 1
    assert counters["suspensions"] == 1
    assert counters["suspensions"] <= counters["activations"]
    assert counters["intents_emitted"] == 1


def test_an_inline_tool_reports_through_a_real_pipeline() -> None:
    # Scenario: A read-only tool runs inline on the runtime surface and is
    # counted -- end to end. Also proves an `AgentConfig.tool_registry` holding
    # a decorated Tool (with its dynamically-created pydantic argument model)
    # survives Beam's pickling of the DoFn to the DirectRunner worker.
    with BeamTestPipeline() as p:
        out = keyed(p | beam.Create([_event(b"t", b"ab")])) | RunAgent(
            inline_tool_agent,
            config=AgentConfig(
                provider_factory=make_pong_provider, tool_registry=make_tool_registry()
            ),
        )
        assert_that(out.output, equal_to([b"AB"]))

    counters, distributions = _runtime_metrics(p.result)

    assert counters["tool_calls"] == 1
    assert counters["activations"] == 1
    # Scenario: Overhead subtracts model and tool time from the activation --
    # here with real clocks, so only the identities are asserted: one sample
    # per committed activation, never exceeding the activation's wall time.
    assert distributions["overhead_ms"].count == counters["activations"]
    assert distributions["overhead_ms"].sum <= distributions["activation_ms"].sum


def test_model_calls_made_on_the_bridge_thread_are_counted() -> None:
    # The regression test for the thread-locality trap: Beam resolves a metric
    # cell through a thread-local state sampler, so a counter incremented from
    # the async bridge thread is discarded with no error. An implementation that
    # counts at the call site instead of staging the tally passes every
    # fake-sink test in test_dofn_metrics and reports zero here.
    with BeamTestPipeline() as p:
        out = keyed(p | beam.Create([_event(b"m", b"MODEL")])) | RunAgent(
            outcome_routing_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(out.output, equal_to([b"pong"]))

    counters, distributions = _runtime_metrics(p.result)

    assert counters["llm_calls"] == 1
    # Scenario: A provider-reached call is timed once -- the sample count equals
    # the call count, so a dashboard can divide one by the other.
    assert distributions["llm_ms"].count == counters["llm_calls"]
    # Scenario: An activation with no decoded usage contributes no sample. The
    # runtime's `call_model` never decodes the opaque response bytes.
    assert "tokens" not in distributions


# --- Requirement: adaptive batching, end to end -------------------------------


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _timed(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    return TimestampedValue(_event(key, payload, t_ms), t_ms / 1000)


def _timed_result(key: bytes, intent_id: str, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.tool_result.intent_id = intent_id
    env.tool_result.entity_key = key
    env.tool_result.payload = b"done"
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


def _adaptive(
    agent: Any,
    *,
    settings: BatchSettings,
    time_fn: Any,
) -> beam.ParDo:
    """An `ADAPTIVE` `_AgentDoFn` as a raw `ParDo`, so the wall clock is injectable.

    `RunAgent`/`AgentConfig` deliberately expose no clock knob — a wall clock is
    a test seam, like `metrics` and `monotonic_ns`, not user configuration — so
    the scenarios that must arm `FLUSH_TIMER` on the `TestStream` clock's
    timeline construct the DoFn directly. `RunAgent`'s own forwarding of the
    batch knobs is covered by the bounded size-flush test below.
    """
    register_coders()
    dofn = _AgentDoFn(
        agent,
        provider_factory=make_pong_provider,
        ttl_ms=_BIG_TTL_MS,
        batch=settings,
        time_fn=time_fn,
    )
    pardo: beam.ParDo = beam.ParDo(dofn).with_outputs("intents", "traces", "errors", main="output")
    return pardo


def test_none_policy_preserves_existing_semantics() -> None:
    # Scenario: NONE policy preserves existing semantics; Scenario: The batch
    # topology is inert under NONE. The sixth spec and third timer are declared
    # unconditionally -- protobuf-coded and REAL_TIME like the rest of the
    # topology -- and the default-configured pipeline still activates once per
    # event, with `ctx.event` as bytes. (The whole pre-existing suite is the
    # rest of this scenario's assertion.)
    assert isinstance(_AgentDoFn.BATCH.coder, DeterministicProtoCoder)
    assert _AgentDoFn.FLUSH_TIMER.time_domain == TimeDomain.REAL_TIME

    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_timed(b"k", b"a", 1000)])
        .add_elements([_timed(b"k", b"b", 2000)])
        .add_elements([_timed(b"k", b"c", 3000)])
        .advance_processing_time(10)  # nothing armed: no flush timer exists here
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | RunAgent(
            append_agent,
            config=AgentConfig(provider_factory=make_pong_provider, ttl_ms=_BIG_TTL_MS),
        )
        # One activation per event, each seeing the previous one's committed
        # memory: byte-for-byte what this pipeline produced before the change.
        assert_that(out.output, equal_to([b"a#0", b"a,b#1", b"a,b,c#2"]), label="per-event")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_an_undersized_buffer_flushes_when_max_wait_elapses() -> None:
    # Scenario: An undersized buffer flushes when max_wait elapses. The
    # roadmap-mandated verification: `max_wait` is honored by a processing-time
    # timer, driven by scripted `advance_processing_time` and never a `sleep()`.
    # The buffer never reaches `max_batch_size`, so the timer is the only thing
    # that can produce this output at all.
    settings = BatchSettings(max_batch_size=10, max_wait_ms=500, max_buffered_events=40)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_timed(b"k", b"a", 1000)])  # arms FLUSH_TIMER at 0.5s
        .add_elements([_timed(b"k", b"b", 2000)])  # buffered; no re-arm
        .advance_processing_time(1)  # 1.0s > 0.5s -> the timer fires
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | _adaptive(
            batch_join_agent, settings=settings, time_fn=FixedClock(0.0)
        )
        assert_that(out.output, equal_to([b"a|b#0"]), label="timer-flush")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_the_wait_is_measured_from_the_first_buffered_event() -> None:
    # Scenario: The wait is measured from the first buffered event. Under a
    # clock that steps 400ms per reading, the mark armed by "a" (0.5s) is
    # reached by the 0.6s advance while a per-element re-arm (0.9s) would not
    # be -- so a flush containing exactly the first two events is only
    # reachable if the second element left the mark alone. The
    # instance-independent form of the same claim (one `set`, not two) is
    # asserted with fake handles in test_dofn_batching.py.
    settings = BatchSettings(max_batch_size=10, max_wait_ms=500, max_buffered_events=40)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_timed(b"k", b"a", 1000)])
        .add_elements([_timed(b"k", b"b", 2000)])
        .advance_processing_time(0.6)  # fires the mark armed from "a"
        .add_elements([_timed(b"k", b"c", 3000)])  # starts a fresh buffer
        .advance_processing_time(1)
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | _adaptive(
            batch_join_agent, settings=settings, time_fn=SteppingClock(0.0, 0.4)
        )
        assert_that(out.output, equal_to([b"a|b#0", b"c#1"]), label="two-flushes")
        assert_that(out.errors, equal_to([]), label="no-errors")


def _one_flush_of_five(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one flush output, got {items!r}"
    payloads, _, seq = items[0].rpartition(b"#")
    # Order within a bounded `Create` bundle is the runner's business; that the
    # five events reached ONE activation at seq 0 is this scenario's claim.
    assert sorted(payloads.split(b"|")) == [b"a", b"b", b"c", b"d", b"e"]
    assert seq == b"0"


def test_a_batch_of_n_events_consumes_one_seq() -> None:
    # Scenario: A batch of N events consumes one seq; Scenario: A batch flush is
    # one activation. Driven through `RunAgent`, so this is also the proof that
    # `AgentConfig`'s batch knobs are forwarded into the DoFn.
    elements = [_event(b"k", payload) for payload in (b"a", b"b", b"c", b"d", b"e")]

    with BeamTestPipeline() as p:
        out = keyed(p | beam.Create(elements)) | RunAgent(
            batch_join_agent,
            config=AgentConfig(
                provider_factory=make_pong_provider,
                batch_policy=BatchPolicy.ADAPTIVE,
                max_batch_size=5,
                max_wait_ms=500,
            ),
        )
        assert_that(out.output, _one_flush_of_five, label="one-flush")
        assert_that(out.errors, equal_to([]), label="no-errors")

    counters, distributions = _runtime_metrics(p.result)

    # `activations` increases by one -- not five -- matching the single SEQ
    # increment, and the flush contributes one `batch_size` sample of 5.
    assert counters["activations"] == 1
    assert counters["events_buffered"] == 5
    assert counters["batch_flushes_size"] == 1
    assert distributions["batch_size"].count == 1
    assert distributions["batch_size"].sum == 5


# --- Requirement: a suspending batch activation suspends and resumes as a whole


def test_a_batch_suspension_persists_one_continuation_and_resumes_together() -> None:
    # Scenarios: A batch suspension persists one continuation for the whole
    # batch; The batch resumes together. Two events flush as one activation at
    # seq 0, which stages exactly one intent and suspends; the single matching
    # result resumes it once, at that same seq, from its snapshot.
    settings = BatchSettings(max_batch_size=2, max_wait_ms=500, max_buffered_events=8)
    intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_timed(b"k", b"a", 0)])
        .add_elements([_timed(b"k", b"b", 0)])  # size flush -> suspends
        .add_elements([_timed_result(b"k", intent_id, 500)])  # inside the deadline
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | _adaptive(
            batch_suspend_agent, settings=settings, time_fn=FixedClock(0.0)
        )
        # One resume, at the batch's seq, carrying the whole batch's snapshot.
        assert_that(out.output, equal_to([b"resumed:a|b#0"]), label="whole-batch-resume")
        ids = out.intents | "ids" >> beam.Map(lambda i: (i.intent_id, i.seq))
        assert_that(ids, equal_to([(intent_id, 0)]), label="one-intent")
        assert_that(out.errors, equal_to([]), label="no-errors")


def test_hitl_timeout_fails_the_whole_batch_closed() -> None:
    # Scenario: HITL timeout fails the whole batch closed. One fallback output
    # for the batch -- not one per batched event -- and the late result for the
    # batch's intent is orphaned.
    settings = BatchSettings(max_batch_size=2, max_wait_ms=500, max_buffered_events=8)
    intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_timed(b"k", b"a", 0)])
        .add_elements([_timed(b"k", b"b", 0)])  # flush suspends; HITL at 1000ms
        .advance_processing_time(5)  # -> fires the real-time HITL timer
        .add_elements([_timed_result(b"k", intent_id, 100)])  # continuation gone
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | _adaptive(
            batch_suspend_agent, settings=settings, time_fn=FixedClock(0.0)
        )
        assert_that(out.output, equal_to([HITL_TIMEOUT_OUTPUT]), label="one-fallback")
        reasons = out.errors | "reasons" >> beam.Map(lambda e: e.reason)
        assert_that(reasons, equal_to([REASON_ORPHANED]), label="orphaned")


def test_resolution_flushes_the_deferred_buffer_promptly() -> None:
    # Scenario: Resolution flushes the deferred buffer promptly. While the
    # batch at seq 0 is suspended, two more events buffer past
    # `max_batch_size` without flushing; the resume commit re-arms FLUSH_TIMER
    # at the resolution's wall clock, and the deferred batch flushes in its own
    # callback with its own SEQ increment.
    settings = BatchSettings(max_batch_size=2, max_wait_ms=500, max_buffered_events=8)
    intent_id = intent_id_for(b"k", 0, 0)
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_timed(b"k", b"a", 0)])
        .add_elements([_timed(b"k", b"b", 0)])  # size flush -> suspends at seq 0
        .add_elements([_timed(b"k", b"c", 100)])  # deferred
        .add_elements([_timed(b"k", b"d", 100)])  # at the threshold, still deferred
        .add_elements([_timed_result(b"k", intent_id, 500)])  # resume commits
        .advance_processing_time(1)  # -> the re-armed FLUSH_TIMER fires
        .advance_watermark_to_infinity()
    )
    with _streaming_pipeline() as p:
        out = keyed(p | stream) | _adaptive(
            batch_suspend_agent, settings=settings, time_fn=FixedClock(0.0)
        )
        # The deferred flush runs at seq 2: the suspending flush and the resume
        # are each one committed activation, so each consumed one SEQ, and the
        # deferred batch gets its own -- a third activation, not a fragment of
        # either.
        assert_that(
            out.output,
            equal_to([b"resumed:a|b#0", b"c|d#2"]),
            label="resume-then-deferred-flush",
        )
        assert_that(out.errors, equal_to([]), label="no-errors")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
