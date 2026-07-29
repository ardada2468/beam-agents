"""Tests for the `runtime-metrics` capability's metric surface.

Covers the surface itself — the declared names, the handle-building discipline,
and the two sink implementations — without a pipeline. Whether the recorded
values actually reach a Beam metrics container is a pipeline question, answered
in `tests/core/test_dofn_pipeline.py`; a fake sink can never prove it.
"""

from __future__ import annotations

import pickle
from typing import Any
from unittest import mock

import pytest

from beam_agents.memory import facade as memory_facade
from beam_agents.observability.metrics import (
    COUNTER_ACTIVATIONS,
    COUNTER_AGENT_ERRORS,
    COUNTER_INTENTS_EMITTED,
    COUNTER_LLM_CALLS,
    COUNTER_ORPHANED_RESULTS,
    COUNTER_SUSPENSIONS,
    COUNTER_TOOL_CALLS,
    COUNTERS,
    DISTRIBUTION_ACTIVATION_MS,
    DISTRIBUTION_ITERATIONS,
    DISTRIBUTION_LLM_MS,
    DISTRIBUTION_MEMORY_BYTES,
    DISTRIBUTION_TOKENS,
    DISTRIBUTIONS,
    NAMESPACE,
    ActivationTally,
    MetricsSink,
    NullMetrics,
    RuntimeMetrics,
)

# --- Requirement: the runtime publishes a fixed metric surface ----------------


def test_the_declared_names_are_the_seven_counters_and_five_distributions() -> None:
    # Scenario: Every declared metric is queryable after a pipeline run -- the
    # half of it that does not need a pipeline. Names are the observable
    # contract: a rename silently breaks every dashboard built on them, so the
    # literal strings are pinned here rather than derived from the constants.
    assert NAMESPACE == "beam_agents.runtime"
    assert COUNTERS == (
        "activations",
        "llm_calls",
        "tool_calls",
        "intents_emitted",
        "agent_errors",
        "suspensions",
        "orphaned_results",
    )
    assert DISTRIBUTIONS == (
        "activation_ms",
        "llm_ms",
        "tokens",
        "memory_bytes",
        "iterations",
    )
    assert (
        COUNTER_ACTIVATIONS,
        COUNTER_LLM_CALLS,
        COUNTER_TOOL_CALLS,
        COUNTER_INTENTS_EMITTED,
        COUNTER_AGENT_ERRORS,
        COUNTER_SUSPENSIONS,
        COUNTER_ORPHANED_RESULTS,
    ) == COUNTERS
    assert (
        DISTRIBUTION_ACTIVATION_MS,
        DISTRIBUTION_LLM_MS,
        DISTRIBUTION_TOKENS,
        DISTRIBUTION_MEMORY_BYTES,
        DISTRIBUTION_ITERATIONS,
    ) == DISTRIBUTIONS


def test_the_namespace_is_distinct_from_the_memory_one() -> None:
    # Scenario: The memory namespace is untouched. `soft_cap_warnings` predates
    # this surface and keeps its own namespace.
    assert memory_facade._METRIC_NAMESPACE == "beam_agents.memory"
    assert NAMESPACE != memory_facade._METRIC_NAMESPACE


def test_runtime_metrics_builds_one_handle_per_name_and_reuses_them() -> None:
    # Handles are built once per recorder, not per call: `Metrics.counter()`
    # allocates a MetricName and a delegating object every time, and this sits
    # on the per-element path under a 15ms overhead budget.
    with mock.patch("beam_agents.observability.metrics.Metrics") as metrics:
        sink = RuntimeMetrics()

        built_counters = metrics.counter.call_count
        built_distributions = metrics.distribution.call_count
        sink.incr(COUNTER_ACTIVATIONS)
        sink.incr(COUNTER_ACTIVATIONS, 4)
        sink.observe(DISTRIBUTION_ITERATIONS, 7)

        assert built_counters == len(COUNTERS)
        assert built_distributions == len(DISTRIBUTIONS)
        assert metrics.counter.call_count == built_counters
        assert metrics.distribution.call_count == built_distributions
        assert metrics.counter.call_args_list[0] == mock.call(NAMESPACE, COUNTER_ACTIVATIONS)


def test_runtime_metrics_routes_each_update_to_its_own_handle() -> None:
    handles: dict[str, Any] = {}

    def make_handle(_namespace: str, name: str) -> Any:
        handles[name] = mock.Mock()
        return handles[name]

    with mock.patch("beam_agents.observability.metrics.Metrics") as metrics:
        metrics.counter.side_effect = make_handle
        metrics.distribution.side_effect = make_handle
        sink = RuntimeMetrics()

        sink.incr(COUNTER_AGENT_ERRORS)
        sink.incr(COUNTER_INTENTS_EMITTED, 3)
        sink.observe(DISTRIBUTION_LLM_MS, 12)

    handles[COUNTER_AGENT_ERRORS].inc.assert_called_once_with(1)
    handles[COUNTER_INTENTS_EMITTED].inc.assert_called_once_with(3)
    handles[DISTRIBUTION_LLM_MS].update.assert_called_once_with(12)
    handles[COUNTER_ORPHANED_RESULTS].inc.assert_not_called()
    handles[DISTRIBUTION_TOKENS].update.assert_not_called()


def test_an_undeclared_name_is_refused_rather_than_silently_recorded() -> None:
    # A typo must fail loudly in a test, not create a phantom metric nobody
    # ever queries.
    sink = RuntimeMetrics()

    with pytest.raises(KeyError):
        sink.incr("activation")  # missing the plural
    with pytest.raises(KeyError):
        sink.observe(COUNTER_ACTIVATIONS, 1)  # a counter, not a distribution


def test_recording_outside_a_beam_context_is_harmless() -> None:
    # Scenario: Recording outside a Beam context is harmless. Off a Beam worker
    # thread there is no state sampler, so Beam discards the update; the caller
    # must not have to care.
    sink = RuntimeMetrics()

    sink.incr(COUNTER_ACTIVATIONS)
    sink.incr(COUNTER_SUSPENSIONS, 2)
    sink.observe(DISTRIBUTION_ACTIVATION_MS, 5)
    sink.observe(DISTRIBUTION_MEMORY_BYTES, 1024)


def test_both_sinks_satisfy_the_sink_protocol() -> None:
    assert isinstance(RuntimeMetrics(), MetricsSink)
    assert isinstance(NullMetrics(), MetricsSink)


def test_null_metrics_records_nothing_and_accepts_any_name() -> None:
    # The no-op exists for components constructed outside a pipeline; it must
    # not validate names, or it becomes a second place to keep in sync.
    sink = NullMetrics()

    sink.incr(COUNTER_ACTIVATIONS)
    sink.observe(DISTRIBUTION_TOKENS, 3)
    sink.incr("anything")


def test_the_recorder_survives_pickling() -> None:
    # `_AgentDoFn` holds a recorder and is pickled to the worker at pipeline
    # construction; a handle that cannot round-trip fails at submit time.
    restored = pickle.loads(pickle.dumps(RuntimeMetrics()))

    restored.incr(COUNTER_ACTIVATIONS)
    restored.observe(DISTRIBUTION_ITERATIONS, 1)


# --- Requirement: the tally is a worker-local accumulator ---------------------


def test_a_fresh_tally_is_all_zero_with_no_usage_observed() -> None:
    tally = ActivationTally()

    assert tally.llm_calls == 0
    assert tally.tool_calls == 0
    assert tally.iterations == 0
    assert tally.total_tokens == 0
    assert tally.usage_observed is False
    assert tally.llm_ms == []


def test_two_tallies_do_not_share_their_duration_list() -> None:
    # A mutable default would make every activation in a worker share one list.
    first = ActivationTally()
    second = ActivationTally()

    first.llm_ms.append(5)

    assert second.llm_ms == []


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
