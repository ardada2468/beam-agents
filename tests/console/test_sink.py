"""Tests for the `console-ingest` capability's push path: the `console://`
sink's record encoding, its batched drop-and-count export contract, and the
wrapping resolver that adds the scheme without touching `core/transform.py`.

The shape mirrors `tests/observability/test_otlp.py` deliberately: the sink is
the OTLP exporter's contract under a different encoding (design D3), so the
same failure postures are asserted the same way — a wedged sender, a full
queue, a dead endpoint, a client error — and the differences that matter
(`ACTIVATION_START` is transmitted, three record kinds instead of one) are
asserted as differences.
"""

from __future__ import annotations

import importlib
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import apache_beam as beam
import httpx
import pytest

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline

from beam_agents._protos import ActivationErrorRecord, StateSnapshot, TraceEvent
from beam_agents.console._sink import (
    COUNTER_BATCHES_SENT,
    COUNTER_EXPORT_FAILURES,
    COUNTER_RECORDS_DROPPED,
    COUNTER_RECORDS_EXPORTED,
    COUNTERS,
    INGEST_PATHS,
    NAMESPACE,
    ConsoleSinkResolver,
    WriteToConsole,
    _BeamConsoleMetrics,
    _ConsoleExportDoFn,
    _encode_element,
    _frame,
    _parse_console_uri,
)
from beam_agents.core.dofn import ActivationError
from beam_agents.core.transform import (
    AgentConfig,
    DefaultSinkResolver,
    SinkResolver,
    UnknownSinkSchemeError,
)
from beam_agents.replay.bundle import frame_trace_events, parse_trace_stream

if TYPE_CHECKING:
    from apache_beam.runners.runner import PipelineResult

_TRACE_ID = bytes(range(16))
_SPAN_ID = bytes(range(8))
_PARENT_ID = bytes(range(8, 16))
_ENDPOINT = "http://console:8787"


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


# --- Requirement: The console sink never fails or slows an activation ---------
# (encoding half: what one record becomes on the wire)


def test_a_trace_event_is_framed_as_its_own_deterministic_bytes() -> None:
    payload = _encode_element("traces", _event())

    assert payload == _event().SerializeToString(deterministic=True)


def test_framing_matches_the_runtime_s_own_trace_stream_framing() -> None:
    # The console reads the same varint framing the replay CLI writes, so the
    # sink must produce a stream `replay.bundle.parse_trace_stream` accepts.
    events = [_event(TraceEvent.ACTIVATION_START), _event(TraceEvent.LLM_CALL)]
    framed = _frame([_encode_element("traces", event) for event in events])

    assert framed == frame_trace_events(events)
    assert parse_trace_stream(framed) == events


def test_an_activation_error_dataclass_encodes_as_the_wire_record() -> None:
    # `.errors` carries `ActivationError` — a dataclass, not a proto — so the
    # sink is the thing that puts it on the wire as `ActivationErrorRecord`.
    error = ActivationError(
        entity_key=b"key-1", reason="activation_timeout", detail="d", event_time_ms=42
    )

    decoded = ActivationErrorRecord()
    decoded.ParseFromString(_encode_element("errors", error))

    assert decoded == ActivationErrorRecord(
        entity_key=b"key-1", reason="activation_timeout", detail="d", event_time_ms=42
    )


def test_an_error_record_proto_encodes_unchanged() -> None:
    record = ActivationErrorRecord(entity_key=b"k", reason="activation_error", event_time_ms=1)

    assert _encode_element("errors", record) == record.SerializeToString(deterministic=True)


def test_a_snapshot_encodes_as_its_own_deterministic_bytes() -> None:
    snapshot = StateSnapshot(entity_key=b"k", seq=3, snapshot_at_ms=9, state_schema_version=1)

    assert _encode_element("snapshots", snapshot) == snapshot.SerializeToString(deterministic=True)


def test_a_record_longer_than_a_single_varint_byte_still_frames() -> None:
    # A frame length of 128+ needs a continuation byte; getting that wrong
    # would only show up on realistically-sized records.
    big = _event(attributes={"beam_agents.detail": "x" * 500})
    framed = _frame([_encode_element("traces", big)])

    assert framed == frame_trace_events([big])
    assert parse_trace_stream(framed) == [big]


@pytest.mark.parametrize(
    ("record_kind", "element"),
    [
        ("traces", StateSnapshot(entity_key=b"k")),
        ("errors", _event()),
        ("snapshots", ActivationErrorRecord(entity_key=b"k")),
    ],
)
def test_an_element_of_the_wrong_type_is_a_wiring_error(record_kind: str, element: Any) -> None:
    # Not a delivery failure: drop-and-count covers an absent console, not a
    # sink wired to the wrong PCollection, which must be loud.
    with pytest.raises(TypeError, match=record_kind):
        _encode_element(record_kind, element)


# --- Requirement: The console sink never fails or slows an activation ---------


@dataclass
class _RecordingMetrics:
    """MetricsSink fake recording (name, n, thread ident) per increment."""

    incremented: list[tuple[str, int, int]] = field(default_factory=list)

    def incr(self, name: str, n: int = 1) -> None:
        self.incremented.append((name, n, threading.get_ident()))

    def observe(self, name: str, value: int) -> None:  # pragma: no cover
        raise AssertionError("the console sink declares no distributions")

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
        request.read()  # buffer the body before the caller's client is closed
        self.requests.append(request)
        self.threads.append(threading.get_ident())
        if self._started is not None:
            self._started.set()
        if self._release is not None:
            assert self._release.wait(timeout=5.0), "test never released the transport"
        if self._connect_error:
            raise httpx.ConnectError("console unreachable", request=request)
        return httpx.Response(self._status)

    def events(self) -> list[TraceEvent]:
        """Every trace event delivered, in delivery order."""
        delivered: list[TraceEvent] = []
        for request in self.requests:
            assert request.headers["content-type"] == "application/x-protobuf"
            delivered.extend(parse_trace_stream(request.content))
        return delivered


def _dofn(
    transport: httpx.BaseTransport,
    metrics: _RecordingMetrics,
    *,
    record_kind: str = "traces",
    batch_size: int = 2,
    flush_deadline_s: float = 2.0,
    queue_batches: int = 8,
) -> _ConsoleExportDoFn:
    dofn = _ConsoleExportDoFn(
        _ENDPOINT,
        record_kind=record_kind,
        batch_size=batch_size,
        flush_deadline_s=flush_deadline_s,
        queue_batches=queue_batches,
        transport=transport,
        metrics=metrics,
    )
    dofn.setup()
    return dofn


def _run_events(dofn: _ConsoleExportDoFn, count: int) -> None:
    for _ in range(count):
        dofn.process(_event())


def _await_drained(dofn: _ConsoleExportDoFn, timeout_s: float = 5.0) -> None:
    """Wait (bounded) until the sender holds no in-flight records."""
    state = dofn._state  # white-box seam, mirroring the OTLP exporter's tests
    with state.lock:
        deadline = time.monotonic() + timeout_s
        while state.in_flight_records > 0:
            remaining = deadline - time.monotonic()
            assert remaining > 0, "sender never drained"
            state.drained.wait(remaining)


def test_start_events_are_delivered() -> None:
    # Scenario: Start events are delivered. The OTLP exporter drops
    # ACTIVATION_START because it shares a span id with ACTIVATION_END; the
    # native record carries `event_type`, so both survive.
    transport = _RecordingTransport()
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2)
    try:
        dofn.process(_event(TraceEvent.ACTIVATION_START))
        dofn.process(_event(TraceEvent.ACTIVATION_END))
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    delivered = [event.event_type for event in transport.events()]
    assert TraceEvent.ACTIVATION_START in delivered
    assert delivered == [TraceEvent.ACTIVATION_START, TraceEvent.ACTIVATION_END]
    assert metrics.total(COUNTER_RECORDS_EXPORTED) == 2


def test_each_record_kind_posts_to_its_own_endpoint() -> None:
    for kind, path in INGEST_PATHS.items():
        transport = _RecordingTransport()
        dofn = _dofn(transport, _RecordingMetrics(), record_kind=kind, batch_size=1)
        try:
            dofn.process(_element_for(kind))
            dofn.finish_bundle()
        finally:
            dofn.teardown()

        assert [str(r.url) for r in transport.requests] == [f"{_ENDPOINT}{path}"]


def _element_for(record_kind: str) -> Any:
    if record_kind == "traces":
        return _event()
    if record_kind == "errors":
        return ActivationError(entity_key=b"k", reason="activation_error")
    return StateSnapshot(entity_key=b"k", seq=1)


def test_process_performs_no_network_io_on_the_calling_thread() -> None:
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

    # 5 records at batch_size 2: two full batches plus a flushed partial of 1.
    sizes = sorted(len(parse_trace_stream(r.content)) for r in transport.requests)
    assert sizes == [1, 2, 2]
    assert metrics.total(COUNTER_RECORDS_EXPORTED) == 5
    assert metrics.total(COUNTER_BATCHES_SENT) == 3
    assert metrics.total(COUNTER_RECORDS_DROPPED) == 0


def test_a_full_queue_drops_rather_than_blocks() -> None:
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

    assert metrics.total(COUNTER_RECORDS_DROPPED) == 2
    assert metrics.total(COUNTER_RECORDS_EXPORTED) == 4


def test_an_absent_console_never_raises_and_counts_drops() -> None:
    transport = _RecordingTransport(connect_error=True)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2, flush_deadline_s=0.3)
    try:
        _run_events(dofn, 4)
        dofn.finish_bundle()
        # A batch the sender still held at the first flush's deadline resolves
        # by its own bounded retry; its drop lands in the next bundle's counts.
        _await_drained(dofn)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert metrics.total(COUNTER_RECORDS_DROPPED) == 4
    assert metrics.total(COUNTER_RECORDS_EXPORTED) == 0
    assert metrics.total(COUNTER_EXPORT_FAILURES) > 0


def test_a_connection_failure_retries_within_the_deadline_then_drops() -> None:
    transport = _RecordingTransport(connect_error=True)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2, flush_deadline_s=0.3)
    try:
        _run_events(dofn, 2)
        dofn.finish_bundle()
        _await_drained(dofn)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert len(transport.requests) > 1
    assert metrics.total(COUNTER_RECORDS_DROPPED) == 2


def test_a_client_error_is_not_retried() -> None:
    transport = _RecordingTransport(status=400)
    metrics = _RecordingMetrics()
    dofn = _dofn(transport, metrics, batch_size=2)
    try:
        _run_events(dofn, 2)
        dofn.finish_bundle()
    finally:
        dofn.teardown()

    assert len(transport.requests) == 1
    assert metrics.total(COUNTER_RECORDS_DROPPED) == 2
    assert metrics.total(COUNTER_EXPORT_FAILURES) == 1


def test_bundle_completion_is_bounded_by_the_flush_deadline() -> None:
    # The sender is wedged inside the transport, so the queued batch cannot
    # drain; the flush must give up within the deadline and count the loss.
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

        assert metrics.total(COUNTER_RECORDS_DROPPED) == 2
    finally:
        release.set()
        dofn.teardown()


def test_teardown_under_a_wedged_sender_never_raises_on_the_sender_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # "Never raises" has to hold on the sender thread too: closing the client
    # out from under an in-flight send would surface as an unhandled exception
    # in a daemon thread, which no caller can catch.
    started = threading.Event()
    release = threading.Event()
    transport = _RecordingTransport(started=started, release=release)
    thread_failures: list[object] = []
    monkeypatch.setattr(threading, "excepthook", thread_failures.append)
    dofn = _dofn(transport, _RecordingMetrics(), batch_size=1, flush_deadline_s=0.05)
    try:
        dofn.process(_event())
        assert started.wait(timeout=5.0)
        dofn.teardown()  # the join times out: the sender is wedged mid-send

        assert not dofn._client.is_closed
        release.set()
        dofn._sender.join(timeout=5.0)
    finally:
        release.set()
        dofn._client.close()

    assert thread_failures == []


def test_counters_are_recorded_only_from_finish_bundle_on_the_beam_thread() -> None:
    # A metric update from the sender thread would be silently discarded by
    # Beam's statesampler, so every increment must happen on the Beam thread.
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


def test_the_beam_metrics_sink_declares_only_its_four_counters() -> None:
    # An undeclared name must fail in a test rather than create a phantom
    # metric, the same guarantee `RuntimeMetrics` gives.
    sink = _BeamConsoleMetrics()

    assert set(sink._counters) == set(COUNTERS)
    with pytest.raises(KeyError, match="no distributions"):
        sink.observe("llm_ms", 1)


def test_counts_are_deltas_not_cumulative_re_reports() -> None:
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

    assert metrics.total(COUNTER_RECORDS_EXPORTED) == 4


# --- Requirement: an unreachable console at pipeline level --------------------


def _failing_transport() -> httpx.BaseTransport:
    return httpx.MockTransport(_raise_connect_error)


def _raise_connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("console unreachable", request=request)


def _counter_totals(result: PipelineResult) -> dict[str, int]:
    """Every declared counter's value off a finished pipeline.

    ``committed`` where the runner reports it, ``attempted`` otherwise: the
    drop-and-count contract is about the numbers being *published*, and which
    of the two a given runner fills in is the runner's business.
    """
    totals = {}
    for name in COUNTERS:
        query = result.metrics().query(MetricsFilter().with_namespace(NAMESPACE).with_name(name))
        totals[name] = sum(
            counter.committed if counter.committed is not None else counter.attempted
            for counter in query["counters"]
        )
    return totals


def test_an_unreachable_console_does_not_fail_the_pipeline() -> None:
    # Scenario: An unreachable console does not fail the pipeline. Run through a
    # real pipeline, which would surface any exception as a failed state.
    events = [_event() for _ in range(4)]
    pipeline = BeamTestPipeline()
    _ = (
        pipeline
        | beam.Create(events)
        | WriteToConsole(
            _ENDPOINT,
            batch_size=2,
            flush_deadline_s=0.3,
            transport_factory=_failing_transport,
        )
    )
    result = pipeline.run()
    result.wait_until_finish()

    assert result.state == PipelineState.DONE
    totals = _counter_totals(result)
    assert totals[COUNTER_RECORDS_DROPPED] > 0
    assert totals[COUNTER_EXPORT_FAILURES] > 0
    assert totals[COUNTER_RECORDS_EXPORTED] == 0


# --- Requirement: A `console://` sink delivers traces, errors, and snapshots ---


def test_a_console_uri_is_accepted_on_all_three_outputs() -> None:
    # Scenario: A console URI is accepted on all three outputs.
    resolver = ConsoleSinkResolver()
    config = AgentConfig(
        provider_factory=lambda: None,  # type: ignore[arg-type,return-value]
        traces_to="console://localhost:8787",
        errors_to="console://localhost:8787",
        snapshots_to="console://localhost:8787",
        sink_resolver=resolver,
    )

    kinds = {}
    for field_name in ("traces_to", "errors_to", "snapshots_to"):
        transform = resolver.resolve(field_name, getattr(config, field_name))
        assert isinstance(transform, WriteToConsole)
        kinds[field_name] = (transform._record_kind, transform._endpoint)

    assert kinds == {
        "traces_to": ("traces", "http://localhost:8787"),
        "errors_to": ("errors", "http://localhost:8787"),
        "snapshots_to": ("snapshots", "http://localhost:8787"),
    }


def test_the_console_resolver_satisfies_the_sink_resolver_protocol() -> None:
    assert isinstance(ConsoleSinkResolver(), SinkResolver)


def test_a_console_uri_defaults_to_the_console_s_own_port() -> None:
    transform = ConsoleSinkResolver().resolve("traces_to", "console://localhost")

    assert isinstance(transform, WriteToConsole)
    assert transform._endpoint == "http://localhost:8787"


def test_a_console_uri_honours_tls_and_exporter_options() -> None:
    uri = "console://box:9000?tls=true&batch_size=4&flush_deadline_s=0.5&queue_batches=2"
    transform = ConsoleSinkResolver().resolve("traces_to", uri)

    assert isinstance(transform, WriteToConsole)
    assert transform._endpoint == "https://box:9000"
    assert transform._batch_size == 4
    assert transform._flush_deadline_s == 0.5
    assert transform._queue_batches == 2


def test_resolver_options_are_defaults_a_uri_option_overrides() -> None:
    resolver = ConsoleSinkResolver(batch_size=16, queue_batches=3)

    plain = resolver.resolve("traces_to", "console://box")
    overridden = resolver.resolve("traces_to", "console://box?batch_size=4")

    assert isinstance(plain, WriteToConsole)
    assert isinstance(overridden, WriteToConsole)
    assert (plain._batch_size, plain._queue_batches) == (16, 3)
    assert (overridden._batch_size, overridden._queue_batches) == (4, 3)


def test_console_is_refused_for_the_intents_sink() -> None:
    # Intents cause side effects and need a lossless sink; this one drops by
    # contract, exactly as `otlp://` does.
    with pytest.raises(UnknownSinkSchemeError, match="intents_to"):
        ConsoleSinkResolver().validate("intents_to", "console://localhost:8787")


@pytest.mark.parametrize(
    "uri",
    [
        "console://",
        "console:///ingest",
        "console://box:notaport",
        "console://box/some/path",
        "console://box?batch_size=0",
        "console://box?flush_deadline_s=0",
        "console://box?flush_deadline_s=soon",
        "console://box?nope=1",
        "console://box?tls=maybe",
    ],
)
def test_a_malformed_console_uri_is_rejected_before_the_pipeline_runs(uri: str) -> None:
    # Scenario: A malformed console URI is rejected before the pipeline runs.
    with pytest.raises(UnknownSinkSchemeError, match="traces_to"):
        ConsoleSinkResolver().validate("traces_to", uri)


def test_a_hostless_console_uri_fails_agent_config_construction() -> None:
    with pytest.raises(UnknownSinkSchemeError, match=r"console://"):
        AgentConfig(
            provider_factory=lambda: None,  # type: ignore[arg-type,return-value]
            traces_to="console://",
            sink_resolver=ConsoleSinkResolver(),
        )


def _hide_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any fresh `import httpx` raise, leaving already-bound names alone."""
    for name in [key for key in sys.modules if key == "httpx" or key.startswith("httpx.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "httpx", None)


def test_validation_never_imports_an_http_client(monkeypatch: pytest.MonkeyPatch) -> None:
    # Scenario: A malformed console URI is rejected ... without importing any
    # HTTP client. `validate` runs at `AgentConfig` construction, so it must
    # hold with httpx unimportable — which is only checkable because the sink
    # module does not import it at module scope.
    _hide_httpx(monkeypatch)

    with pytest.raises(UnknownSinkSchemeError, match=r"console://"):
        AgentConfig(
            provider_factory=lambda: None,  # type: ignore[arg-type,return-value]
            traces_to="console://",
            sink_resolver=ConsoleSinkResolver(),
        )
    config = AgentConfig(
        provider_factory=lambda: None,  # type: ignore[arg-type,return-value]
        traces_to="console://localhost:8787",
        sink_resolver=ConsoleSinkResolver(),
    )
    assert config.traces_to == "console://localhost:8787"
    assert sys.modules["httpx"] is None  # nothing along the way pulled it back in


def test_the_sink_module_imports_without_an_http_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _hide_httpx(monkeypatch)
    monkeypatch.delitem(sys.modules, "beam_agents.console._sink")

    # Imported by name so the fresh copy stays local: rebinding the module in
    # this test's own namespace would leave later tests comparing classes from
    # two different module objects.
    reimported = importlib.import_module("beam_agents.console._sink")

    assert reimported.SCHEME == "console"


# --- Requirement: Other schemes are unaffected --------------------------------

_DELEGATED = [
    ("traces_to", "kafka://broker:9092/traces"),
    ("errors_to", "kafka://broker:9092/errors"),
    ("intents_to", "kafka://broker:9092/intents"),
    ("traces_to", "pubsub://my-project/traces"),
    ("snapshots_to", "pubsub://my-project/snaps"),
    ("traces_to", "bigquery://my-project/my_dataset/traces"),
    ("errors_to", "bigquery://my-project/my_dataset/errors"),
    ("traces_to", "otlp://collector:4318"),
]


def _describe(transform: beam.PTransform) -> tuple[Any, ...]:
    """A comparable description of a resolved sink transform.

    Structural rather than by equality: Beam's write transforms define no
    ``__eq__`` (and the Kafka one carries a fresh uuid per construction), so
    two identically-configured writers are never equal objects.
    """
    inner: Any = getattr(transform, "_sink", transform)
    identity: tuple[Any, ...] = (
        type(transform).__name__,
        getattr(transform, "_to_row", None),
        type(inner).__name__,
    )
    reference = getattr(inner, "table_reference", None)
    if reference is not None:
        identity += (reference.projectId, reference.datasetId, reference.tableId)
        identity += (str(inner.schema), inner.additional_bq_parameters)
    for attribute in ("full_topic", "_endpoint", "_uri", "_urn"):
        if hasattr(inner, attribute):
            identity += (attribute, getattr(inner, attribute))
    return identity


@pytest.mark.parametrize(("field_name", "uri"), _DELEGATED)
def test_other_schemes_are_unaffected(field_name: str, uri: str) -> None:
    # Scenario: Other schemes are unaffected.
    console = ConsoleSinkResolver()
    default = DefaultSinkResolver()

    console.validate(field_name, uri)
    default.validate(field_name, uri)

    assert _describe(console.resolve(field_name, uri)) == _describe(
        default.resolve(field_name, uri)
    )


class _SpyResolver:
    """A ``SinkResolver`` that records calls and forwards them verbatim."""

    def __init__(self) -> None:
        self.delegate = DefaultSinkResolver()
        self.calls: list[tuple[str, str, str]] = []
        self.resolved: list[beam.PTransform] = []

    def validate(self, field_name: str, uri: str) -> None:
        self.calls.append(("validate", field_name, uri))
        self.delegate.validate(field_name, uri)

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        self.calls.append(("resolve", field_name, uri))
        transform = self.delegate.resolve(field_name, uri)
        self.resolved.append(transform)
        return transform


def test_a_delegated_uri_reaches_the_delegate_verbatim() -> None:
    spy = _SpyResolver()
    resolver = ConsoleSinkResolver(spy)

    resolver.validate("traces_to", "kafka://broker:9092/traces")
    transform = resolver.resolve("traces_to", "kafka://broker:9092/traces")

    assert spy.calls == [
        ("validate", "traces_to", "kafka://broker:9092/traces"),
        ("resolve", "traces_to", "kafka://broker:9092/traces"),
    ]
    # The delegate's own object, not a copy: nothing is re-wrapped on the way out.
    assert transform is spy.resolved[-1]


def test_a_console_uri_never_reaches_the_delegate() -> None:
    spy = _SpyResolver()
    resolver = ConsoleSinkResolver(spy)

    resolver.validate("traces_to", "console://localhost:8787")
    resolver.resolve("traces_to", "console://localhost:8787")

    assert spy.calls == []


def test_an_unknown_scheme_still_fails_with_the_delegate_s_error() -> None:
    with pytest.raises(UnknownSinkSchemeError, match="unknown sink URI scheme"):
        ConsoleSinkResolver().validate("traces_to", "carrier-pigeon://box")


# --- Requirement: the transform's own construction ----------------------------


def test_an_unknown_record_kind_is_a_construction_error() -> None:
    with pytest.raises(ValueError, match="record_kind"):
        WriteToConsole(_ENDPOINT, record_kind="intents")


def test_a_trailing_slash_on_the_endpoint_is_normalized() -> None:
    transform = WriteToConsole("http://console:8787/")

    assert transform._endpoint == "http://console:8787"


def test_the_uri_parser_is_the_one_validate_and_resolve_share() -> None:
    endpoint, options = _parse_console_uri("traces_to", "console://box:1234?batch_size=8")

    assert endpoint == "http://box:1234"
    assert options == {"batch_size": 8}
