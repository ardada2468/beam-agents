"""Tests for the `trace-exporters` capability's OTLP path: the deterministic
TraceEvent -> OTLP span mapping, the activation-span election, the batched
non-blocking export DoFn's drop-and-count failure policy, and the offline
import boundary of the optional `otlp` extra.
"""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass, field

import apache_beam as beam
import httpx
import pytest

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import Status

from beam_agents._protos import TraceEvent
from beam_agents.core.transform import AgentConfig
from beam_agents.observability.otlp import (
    COUNTER_BATCHES_SENT,
    COUNTER_EXPORT_FAILURES,
    COUNTER_SPANS_DROPPED,
    COUNTER_SPANS_EXPORTED,
    DEFAULT_SERVICE_NAME,
    WriteTracesToOtlp,
    _encode_batch,
    _event_to_span,
    _OtlpExportDoFn,
)

_TRACE_ID = bytes(range(16))
_SPAN_ID = bytes(range(8))
_PARENT_ID = bytes(range(8, 16))


def _event(
    event_type: TraceEvent.EventType = TraceEvent.LLM_CALL,
    *,
    attributes: dict[str, str] | None = None,
    span_id: bytes = _SPAN_ID,
) -> TraceEvent:
    return TraceEvent(
        trace_id=_TRACE_ID,
        span_id=span_id,
        parent_span_id=_PARENT_ID,
        entity_key=b"key-1",
        seq=7,
        step_index=2,
        event_type=event_type,
        attributes=attributes
        if attributes is not None
        else {"gen_ai.request.model": "m-1", "beam_agents.cache_hit": "false"},
        start_ms=1_700_000_000_000,
        end_ms=1_700_000_000_000,
    )


# --- Requirement: Deterministic TraceEvent-to-OTLP mapping --------------------


def test_ids_survive_the_mapping_unchanged() -> None:
    # Scenario: IDs survive the mapping unchanged.
    span = _event_to_span(_event())

    assert span is not None
    assert span.trace_id == _TRACE_ID
    assert span.span_id == _SPAN_ID
    assert span.parent_span_id == _PARENT_ID


def test_timestamps_become_unix_nanos() -> None:
    span = _event_to_span(_event())

    assert span is not None
    assert span.start_time_unix_nano == 1_700_000_000_000 * 1_000_000
    assert span.end_time_unix_nano == 1_700_000_000_000 * 1_000_000


def test_attributes_map_to_string_key_values_sorted() -> None:
    span = _event_to_span(_event())

    assert span is not None
    got = [(kv.key, kv.value.string_value) for kv in span.attributes]
    # Sorted by key so two encodings of one event are byte-identical.
    assert got == [("beam_agents.cache_hit", "false"), ("gen_ai.request.model", "m-1")]


def test_span_name_is_the_lowercase_event_type_name() -> None:
    llm = _event_to_span(_event(TraceEvent.LLM_CALL))
    tool = _event_to_span(_event(TraceEvent.TOOL_CALL))

    assert llm is not None and llm.name == "llm_call"
    assert tool is not None and tool.name == "tool_call"


def test_mapping_is_deterministic() -> None:
    # Scenario: Mapping is deterministic. Two independently constructed events
    # must serialize to identical bytes, or at-least-once export could not
    # dedup on content.
    one = _event_to_span(_event())
    two = _event_to_span(_event())

    assert one is not None and two is not None
    assert one.SerializeToString(deterministic=True) == two.SerializeToString(deterministic=True)


def test_error_event_maps_to_error_status_span() -> None:
    # Scenario: An ERROR event maps to an error-status span.
    span = _event_to_span(
        _event(TraceEvent.ERROR, attributes={"beam_agents.reason": "activation_timeout"})
    )

    assert span is not None
    assert span.status.code == Status.STATUS_CODE_ERROR
    assert ("beam_agents.reason", "activation_timeout") in [
        (kv.key, kv.value.string_value) for kv in span.attributes
    ]


def test_non_error_events_carry_no_error_status() -> None:
    span = _event_to_span(_event(TraceEvent.LLM_CALL))

    assert span is not None
    assert span.status.code != Status.STATUS_CODE_ERROR


def test_batch_encoding_carries_service_name_resource() -> None:
    span = _event_to_span(_event())
    assert span is not None

    request = ExportTraceServiceRequest()
    request.ParseFromString(_encode_batch([span], service_name=DEFAULT_SERVICE_NAME))

    assert len(request.resource_spans) == 1
    resource_attrs = {
        kv.key: kv.value.string_value for kv in request.resource_spans[0].resource.attributes
    }
    assert resource_attrs["service.name"] == "beam-agents"
    assert request.resource_spans[0].scope_spans[0].spans[0] == span


def test_batch_encoding_honors_service_name_override() -> None:
    span = _event_to_span(_event())
    assert span is not None

    request = ExportTraceServiceRequest()
    request.ParseFromString(_encode_batch([span], service_name="my-pipeline"))

    resource_attrs = {
        kv.key: kv.value.string_value for kv in request.resource_spans[0].resource.attributes
    }
    assert resource_attrs["service.name"] == "my-pipeline"


# --- Requirement: The activation span is exported once ------------------------


def test_activation_start_produces_no_span() -> None:
    # Scenario: START is skipped, END is exported. START and END share one span
    # id, and OTLP names a span by (trace_id, span_id).
    assert _event_to_span(_event(TraceEvent.ACTIVATION_START)) is None


def test_activation_end_is_the_activation_span() -> None:
    span = _event_to_span(
        _event(
            TraceEvent.ACTIVATION_END,
            attributes={
                "beam_agents.activation.status": "completed",
                "beam_agents.activation.kind": "start",
            },
        )
    )

    assert span is not None
    assert span.span_id == _SPAN_ID
    got = {kv.key: kv.value.string_value for kv in span.attributes}
    assert got["beam_agents.activation.status"] == "completed"
    assert got["beam_agents.activation.kind"] == "start"


def test_every_non_start_event_type_exports_one_span() -> None:
    # Scenario: Non-activation events all export.
    for event_type in (
        TraceEvent.LLM_CALL,
        TraceEvent.TOOL_CALL,
        TraceEvent.INTENT_EMITTED,
        TraceEvent.SUSPENDED,
        TraceEvent.ERROR,
    ):
        assert _event_to_span(_event(event_type)) is not None


# --- Requirement: Non-blocking batched export ---------------------------------


@dataclass
class _RecordingMetrics:
    """MetricsSink fake recording (name, n, thread ident) per increment."""

    incremented: list[tuple[str, int, int]] = field(default_factory=list)

    def incr(self, name: str, n: int = 1) -> None:
        self.incremented.append((name, n, threading.get_ident()))

    def observe(self, name: str, value: int) -> None:  # pragma: no cover
        raise AssertionError("the OTLP exporter declares no distributions")

    def total(self, name: str) -> int:
        return sum(n for got, n, _ in self.incremented if got == name)


class _RecordingTransport(httpx.BaseTransport):
    """Collects requests and the thread each was handled on; scriptable result."""

    def __init__(
        self,
        *,
        status: int = 200,
        connect_error: bool = False,
        started: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.requests: list[httpx.Request] = []
        self.threads: list[int] = []
        self._status = status
        self._connect_error = connect_error
        self._started = started
        self._release = release

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.threads.append(threading.get_ident())
        if self._started is not None:
            self._started.set()
        if self._release is not None:
            assert self._release.wait(timeout=5.0), "test never released the transport"
        if self._connect_error:
            raise httpx.ConnectError("collector unreachable", request=request)
        return httpx.Response(self._status)


def _dofn(
    transport: httpx.BaseTransport,
    metrics: _RecordingMetrics,
    *,
    batch_size: int = 2,
    flush_deadline_s: float = 2.0,
    queue_batches: int = 8,
) -> _OtlpExportDoFn:
    dofn = _OtlpExportDoFn(
        "http://collector:4318/v1/traces",
        batch_size=batch_size,
        flush_deadline_s=flush_deadline_s,
        queue_batches=queue_batches,
        service_name=DEFAULT_SERVICE_NAME,
        transport=transport,
        metrics=metrics,
    )
    dofn.setup()
    return dofn


def _run_events(dofn: _OtlpExportDoFn, count: int) -> None:
    for _ in range(count):
        dofn.process(_event())


def _await_drained(dofn: _OtlpExportDoFn, timeout_s: float = 5.0) -> None:
    """Wait (bounded) until the sender holds no in-flight spans."""
    state = dofn._state  # white-box seam, mirroring the DoFn tests elsewhere
    with state.lock:
        deadline = time.monotonic() + timeout_s
        while state.in_flight_spans > 0:
            remaining = deadline - time.monotonic()
            assert remaining > 0, "sender never drained"
            state.drained.wait(remaining)


def test_process_performs_no_network_io_on_the_calling_thread() -> None:
    # Scenario: The element path performs no network I/O.
    transport = _RecordingTransport()
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics)
    try:
        _run_events(dofn, 4)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert transport.requests, "batches must have been exported"
    assert threading.get_ident() not in transport.threads


def test_full_batches_are_exported_and_counted() -> None:
    transport = _RecordingTransport()
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2)
    try:
        _run_events(dofn, 5)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    # 5 events at batch_size 2: two full batches plus a flushed partial of 1.
    sizes = sorted(
        len(_decode_request(r).resource_spans[0].scope_spans[0].spans) for r in transport.requests
    )
    assert sizes == [1, 2, 2]
    assert metrics.total(COUNTER_SPANS_EXPORTED) == 5
    assert metrics.total(COUNTER_BATCHES_SENT) == 3
    assert metrics.total(COUNTER_SPANS_DROPPED) == 0


def test_activation_start_is_not_batched() -> None:
    transport = _RecordingTransport()
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=1)
    try:
        dofn.process(_event(TraceEvent.ACTIVATION_START))
        dofn.process(_event(TraceEvent.ACTIVATION_END))
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert metrics.total(COUNTER_SPANS_EXPORTED) == 1


def test_a_full_queue_drops_rather_than_blocks() -> None:
    # Scenario: A full queue drops rather than blocks.
    started = threading.Event()
    release = threading.Event()
    transport = _RecordingTransport(started=started, release=release)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2, queue_batches=1)
    try:
        # First batch: picked up by the sender, which blocks inside the
        # transport until released — the queue is empty again once `started`.
        _run_events(dofn, 2)
        assert started.wait(timeout=5.0)
        # Second batch fills the size-1 queue; third finds it full and drops.
        _run_events(dofn, 2)
        _run_events(dofn, 2)
        release.set()
        dofn.finish_bundle()
    finally:
        release.set()
        dofn.teardown()

    assert metrics.total(COUNTER_SPANS_DROPPED) == 2
    assert metrics.total(COUNTER_SPANS_EXPORTED) == 4


def test_a_dead_collector_never_raises_and_counts_drops() -> None:
    # Scenario: A dead collector does not fail bundles (DoFn level): every call
    # returns normally and the loss is counted.
    transport = _RecordingTransport(connect_error=True)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2, flush_deadline_s=0.3)
    try:
        _run_events(dofn, 4)
        dofn.finish_bundle()
        # A batch the sender still held at the first flush's deadline resolves
        # by its own bounded retry; its drop lands in the next bundle's counts.
        # Wait (bounded) for the sender to settle, then close a second bundle.
        _await_drained(dofn)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert metrics.total(COUNTER_SPANS_DROPPED) == 4
    assert metrics.total(COUNTER_SPANS_EXPORTED) == 0
    assert metrics.total(COUNTER_EXPORT_FAILURES) > 0


def test_a_connection_failure_retries_within_the_deadline_then_drops() -> None:
    transport = _RecordingTransport(connect_error=True)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2, flush_deadline_s=0.3)
    try:
        _run_events(dofn, 2)
        dofn.finish_bundle()
        # The batch may still be inside the sender's bounded retry when the
        # flush deadline expires; its drop lands in the next bundle's counts.
        _await_drained(dofn)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    # Retried (more than one attempt) but bounded by the deadline, then dropped.
    assert len(transport.requests) > 1
    assert metrics.total(COUNTER_SPANS_DROPPED) == 2


def test_a_client_error_is_not_retried() -> None:
    # Scenario: A client error is not retried.
    transport = _RecordingTransport(status=400)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2)
    try:
        _run_events(dofn, 2)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert len(transport.requests) == 1
    assert metrics.total(COUNTER_SPANS_DROPPED) == 2
    assert metrics.total(COUNTER_EXPORT_FAILURES) == 1


def test_bundle_completion_is_bounded_by_the_flush_deadline() -> None:
    # Scenario: Bundle completion is bounded by the flush deadline. The sender
    # is wedged inside the transport, so the queued batch cannot drain; the
    # flush must give up within the deadline and count the loss.
    started = threading.Event()
    release = threading.Event()
    transport = _RecordingTransport(started=started, release=release)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2, queue_batches=2, flush_deadline_s=0.3)
    try:
        _run_events(dofn, 2)
        assert started.wait(timeout=5.0)
        _run_events(dofn, 2)  # sits in the queue behind the wedged send
        dofn.finish_bundle()

        # The queued-but-undeliverable batch was counted as dropped.
        assert metrics.total(COUNTER_SPANS_DROPPED) == 2
    finally:
        release.set()
        dofn.teardown()


def test_counters_are_recorded_only_from_finish_bundle_on_the_beam_thread() -> None:
    # Scenario: Export counters are recorded on the Beam thread. A metric
    # update from the sender thread would be silently discarded by Beam.
    transport = _RecordingTransport()
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=1)
    try:
        _run_events(dofn, 3)
        assert metrics.incremented == []  # nothing recorded mid-bundle
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert metrics.incremented != []
    assert {ident for _, _, ident in metrics.incremented} == {threading.get_ident()}


def test_counts_are_deltas_not_cumulative_re_reports() -> None:
    # Two bundles through one DoFn: the second bundle's recording must not
    # re-report the first bundle's spans.
    transport = _RecordingTransport()
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2)
    try:
        _run_events(dofn, 2)
        dofn.finish_bundle()
        _run_events(dofn, 2)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert metrics.total(COUNTER_SPANS_EXPORTED) == 4


def _decode_request(request: httpx.Request) -> ExportTraceServiceRequest:
    assert request.headers["content-type"] == "application/x-protobuf"
    decoded = ExportTraceServiceRequest()
    decoded.ParseFromString(request.read())
    return decoded


# --- Requirement: Export failures never fail the pipeline (pipeline level) ----


def test_a_dead_collector_does_not_fail_the_pipeline() -> None:
    # Scenario: A dead collector does not fail bundles — end to end through a
    # real (Test)Pipeline run, which would surface any exception as a failure.
    events = [_event() for _ in range(4)]
    with BeamTestPipeline() as p:
        _ = (
            p
            | beam.Create(events)
            | WriteTracesToOtlp(
                "http://collector:4318/v1/traces",
                flush_deadline_s=0.3,
                transport_factory=_failing_transport,
            )
        )
    # Reaching here means every bundle committed despite the dead collector.


def _failing_transport() -> httpx.BaseTransport:
    return httpx.MockTransport(_raise_connect_error)


def _raise_connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("collector unreachable", request=request)


# --- Requirement: An `otlp://` traces sink scheme (import boundary) -----------


def _hide_opentelemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [key for key in sys.modules if key.startswith("opentelemetry")]:
        monkeypatch.delitem(sys.modules, name)
    # A None entry makes any fresh `import opentelemetry[...]` raise ImportError.
    monkeypatch.setitem(sys.modules, "opentelemetry", None)


def test_transform_construction_without_the_extra_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Validation does not import the OTLP dependency (the resolution
    # half): constructing the exporter without opentelemetry-proto installed
    # raises an actionable error naming the extra.
    _hide_opentelemetry(monkeypatch)

    with pytest.raises(ImportError, match=r"beam-agents\[otlp\]"):
        WriteTracesToOtlp("http://collector:4318/v1/traces")


def test_config_validation_succeeds_without_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Scenario: Validation does not import the OTLP dependency. AgentConfig
    # construction validates the URI import-free.
    _hide_opentelemetry(monkeypatch)

    config = AgentConfig(provider_factory=lambda: None, traces_to="otlp://collector:4318")  # type: ignore[arg-type,return-value]
    assert config.traces_to == "otlp://collector:4318"
