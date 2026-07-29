"""Activation-latency Beam metrics recorded by the DoFn.

The counterpart of the zero-width-span decision (design D7): every trace event
takes both timestamps from the injected activation clock, so *duration* lives
here — Beam metrics surfaced to runner dashboards — instead of being faked into
trace bytes with a wall-clock read that would make replayed bundles emit
different records.

Driven with fake state/timer doubles like the other `test_dofn_*` suites, so
the instrumentation stays inside the mutation gate's test selection. The
metrics container is installed the way Beam's own metric tests do it: a
`for_test` state sampler with a scoped state carrying the container.
"""

from __future__ import annotations

from typing import Any

from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.worker import statesampler
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.dofn import _AgentDoFn
from beam_agents.model.fake import FakeLLM
from beam_agents.observability.metrics import (
    ACTIVATION_MS,
    ACTIVATIONS_COMPLETED,
    ACTIVATIONS_FAILED,
    ACTIVATIONS_SUSPENDED,
    METRICS_NAMESPACE,
)
from tests.core._dofn_helpers import raising_agent, seq_agent, suspend_then_complete_agent

_KEY = b"k"
_NOW_MS = 5_000


class _FakeState:
    def __init__(self, value: Any = None) -> None:
        self.value = value

    def read(self) -> Any:
        return self.value

    def write(self, value: Any) -> None:
        self.value = value

    def add(self, value: Any) -> None:
        return None

    def clear(self) -> None:
        self.value = None


class _FakeTimer:
    def set(self, ts: Timestamp) -> None:
        return None

    def clear(self) -> None:
        return None


class _SteppingClock:
    """A monotonic double advancing exactly 4 s per read.

    One activation reads it twice (start, end), so every activation measures
    exactly 4000 ms — exact enough to make the distribution assertions
    equality-based, and coarse enough that the ms conversion's off-by-one
    mutants (`* 1000` -> `* 1001` is 4004, not 4000) cannot hide inside the
    `int()` truncation the way a sub-second step would let them.
    """

    def __init__(self) -> None:
        self._now = 100.0

    def __call__(self) -> float:
        value = self._now
        self._now += 4.0
        return value


class _Container:
    """Context manager installing a metrics container for direct DoFn calls."""

    def __init__(self) -> None:
        self.container = MetricsContainer("test-step")
        self._sampler = statesampler.for_test()
        self._state = self._sampler.scoped_state(
            "test-step", "process", metrics_container=self.container
        )

    def __enter__(self) -> _Container:
        statesampler.set_current_tracker(self._sampler)
        self._state.__enter__()
        return self

    def __exit__(self, *exc: object) -> None:
        self._state.__exit__(*exc)
        statesampler.set_current_tracker(None)

    def counter(self, name: MetricName) -> int:
        for key, value in self.container.get_cumulative().counters.items():
            if key.metric == name:
                return int(value)
        return 0

    def distribution(self, name: MetricName) -> tuple[int, int]:
        """Return ``(sum, count)`` for the distribution, or ``(0, 0)``."""
        for key, value in self.container.get_cumulative().distributions.items():
            if key.metric == name:
                return int(value.sum), int(value.count)
        return 0, 0


def _process(dofn: _AgentDoFn, envelope: AgentEnvelope) -> list[Any]:
    dofn.setup()
    try:
        return list(
            dofn.process(
                (_KEY, envelope),
                memory=_FakeState(),
                continuation=_FakeState(),
                llm_cache=_FakeState(),
                pending=_FakeState([]),
                seq=_FakeState(0),
                ttl_timer=_FakeTimer(),
                hitl_timer=_FakeTimer(),
            )
        )
    finally:
        dofn.teardown()


def _event() -> AgentEnvelope:
    return AgentEnvelope(entity_key=_KEY, event_time_ms=_NOW_MS, external_event=b"go")


# --- Requirement: Activation latency is measured as Beam metrics -------------


def test_a_completed_activation_records_its_latency_and_outcome() -> None:
    # Scenario: A completed activation records its latency and outcome.
    dofn = _AgentDoFn(seq_agent, provider_factory=FakeLLM, monotonic=_SteppingClock())

    with _Container() as metrics:
        _process(dofn, _event())

    assert metrics.distribution(ACTIVATION_MS) == (4000, 1)
    assert metrics.counter(ACTIVATIONS_COMPLETED) == 1
    assert metrics.counter(ACTIVATIONS_SUSPENDED) == 0
    assert metrics.counter(ACTIVATIONS_FAILED) == 0


def test_a_suspending_activation_counts_as_suspended() -> None:
    # Scenario: A suspending activation counts as suspended.
    dofn = _AgentDoFn(
        suspend_then_complete_agent, provider_factory=FakeLLM, monotonic=_SteppingClock()
    )

    with _Container() as metrics:
        _process(dofn, _event())

    assert metrics.distribution(ACTIVATION_MS) == (4000, 1)
    assert metrics.counter(ACTIVATIONS_SUSPENDED) == 1
    assert metrics.counter(ACTIVATIONS_COMPLETED) == 0


def test_a_failed_activation_counts_as_failed_and_pollutes_no_latency() -> None:
    # Scenario: A failed activation counts as failed without a latency sample.
    # A timeout's "latency" is just the configured budget and a raise's is
    # wherever the agent happened to blow up; folding either into the
    # distribution would corrupt the p99 the distribution exists to watch.
    dofn = _AgentDoFn(raising_agent, provider_factory=FakeLLM, monotonic=_SteppingClock())

    with _Container() as metrics:
        _process(dofn, _event())

    assert metrics.counter(ACTIVATIONS_FAILED) == 1
    assert metrics.distribution(ACTIVATION_MS) == (0, 0)
    assert metrics.counter(ACTIVATIONS_COMPLETED) == 0


def test_an_orphaned_resume_records_no_activation_metric() -> None:
    # Scenario: A refused resume is not an activation. No activation ran, so
    # neither the distribution nor any outcome counter may move; the
    # `orphaned_result` dead-letter record owns that signal.
    dofn = _AgentDoFn(seq_agent, provider_factory=FakeLLM, monotonic=_SteppingClock())
    orphan = AgentEnvelope(
        entity_key=_KEY,
        event_time_ms=_NOW_MS,
        tool_result=ToolResult(intent_id="ghost", entity_key=_KEY, status=ToolResult.OK),
    )

    with _Container() as metrics:
        _process(dofn, orphan)

    assert metrics.distribution(ACTIVATION_MS) == (0, 0)
    assert metrics.counter(ACTIVATIONS_COMPLETED) == 0
    assert metrics.counter(ACTIVATIONS_SUSPENDED) == 0
    assert metrics.counter(ACTIVATIONS_FAILED) == 0


def test_metric_names_are_the_documented_dashboard_contract() -> None:
    # Renaming a metric silently breaks every dashboard built on it; the names
    # are wire surface the same way proto fields are.
    assert METRICS_NAMESPACE == "beam_agents"
    assert MetricName("beam_agents", "activation_ms") == ACTIVATION_MS
    assert MetricName("beam_agents", "activations_completed") == ACTIVATIONS_COMPLETED
    assert MetricName("beam_agents", "activations_suspended") == ACTIVATIONS_SUSPENDED
    assert MetricName("beam_agents", "activations_failed") == ACTIVATIONS_FAILED


def test_recording_outside_a_metrics_container_is_a_silent_no_op() -> None:
    # Direct DoFn invocation (these very suites) runs with no container
    # installed; the instrumentation must never make that an error.
    dofn = _AgentDoFn(seq_agent, provider_factory=FakeLLM, monotonic=_SteppingClock())
    emitted = _process(dofn, _event())
    assert emitted[0] == b"0"
