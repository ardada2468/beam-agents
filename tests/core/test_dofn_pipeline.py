"""Pipeline tests for single-activation routing and state topology.

Covers the bounded (order-insensitive) scenarios: the state/timer topology is
protobuf-coded and pickle-free, a fresh key seeds seq 0, an event starts an
activation, and an unmatched resume routes to ``.errors``. Ordered multi-element
scenarios (resume, seq progression, timeouts, timers, interleaving) live in
test_dofn_streaming.
"""

from __future__ import annotations

import time
from typing import Any

import apache_beam as beam
import pytest
from apache_beam.coders.coders import VarIntCoder
from apache_beam.coders.typecoders import registry as coder_registry
from apache_beam.metrics.metric import MetricResults, MetricsFilter

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.coders import DeterministicProtoCoder, register_coders
from beam_agents.core.dofn import (
    DETAIL_NO_CONTINUATION,
    REASON_ORPHANED,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.observability.metrics import NAMESPACE
from tests.core._dofn_helpers import (
    append_agent,
    keyed,
    make_pong_provider,
    outcome_routing_agent,
    seq_agent,
    suspend_then_complete_agent,
)


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
            equal_to([ActivationError(b"k", REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:ghost")]),
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
            equal_to([ActivationError(b"k", REASON_ORPHANED, f"{DETAIL_NO_CONTINUATION}:ghost")]),
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
