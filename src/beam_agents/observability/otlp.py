"""The OTLP/HTTP trace exporter: TraceEvent -> OTLP mapping and the batched,
non-blocking export DoFn behind the ``otlp://`` traces sink scheme.

See the change design (``openspec/changes/add-trace-exporters/design.md``) for
the load-bearing decisions: ``httpx`` + ``opentelemetry-proto`` instead of the
OTel SDK, whose process-global tracer and daemon threads violate this repo's
no-global-mutable-state rule (D1); batch in ``process()``, send from one
background thread, flush bounded by a deadline in ``finish_bundle()`` (D2);
drop-and-count on every delivery failure, never raise — a dead collector costs
telemetry, not pipeline availability (D3); and one event maps to one span, with
``ACTIVATION_END`` elected as the activation span (D4).

The mapping table:

======================  ====================================================
``trace_id`` etc.       pass through byte-for-byte (16/8/8 — already OTel
                        wire widths by construction)
``start_ms``/``end_ms`` x 10^6 -> ``start/end_time_unix_nano``
``attributes``          string ``KeyValue``s, sorted by key
span ``name``           the lowercase event-type name (``llm_call``, ...)
``ERROR`` events        ``status.code = STATUS_CODE_ERROR``
``ACTIVATION_START``    **not exported**: it shares its span id with
                        ``ACTIVATION_END`` (they bracket one activation
                        attempt), OTLP names a span by ``(trace_id,
                        span_id)``, and END is strictly more informative —
                        it carries ``activation.status`` alongside the same
                        ``activation.kind``. The event itself stays on
                        ``.traces`` for every other consumer.
======================  ====================================================

Export is best-effort at-least-once: a retried bundle re-sends byte-identical
spans (the mapping is deterministic), which a collector dedups exactly on
``(trace_id, span_id)``. The lossless record is the ``.traces`` PCollection;
anyone needing guaranteed retention points ``traces_to`` at Kafka/Pub/Sub/
BigQuery instead.

``opentelemetry-proto`` (the ``otlp`` extra) is imported lazily so this module
— and sink-URI *validation*, which never constructs the transform — works
without it installed.

Importing this module has no side effects.
"""

from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING, Any

import apache_beam as beam
import httpx

from beam_agents._protos import TraceEvent
from beam_agents.observability.metrics import MetricsSink
from beam_agents.observability.traces import REASON

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from opentelemetry.proto.trace.v1.trace_pb2 import Span

__all__ = [
    "COUNTERS",
    "COUNTER_BATCHES_SENT",
    "COUNTER_EXPORT_FAILURES",
    "COUNTER_SPANS_DROPPED",
    "COUNTER_SPANS_EXPORTED",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_FLUSH_DEADLINE_S",
    "DEFAULT_QUEUE_BATCHES",
    "DEFAULT_SERVICE_NAME",
    "NAMESPACE",
    "WriteTracesToOtlp",
]

DEFAULT_SERVICE_NAME = "beam-agents"
DEFAULT_BATCH_SIZE = 512
DEFAULT_FLUSH_DEADLINE_S = 5.0
DEFAULT_QUEUE_BATCHES = 8

# Distinct from `beam_agents.runtime`: these count telemetry delivery, not
# agent work, and the exporter's drop-and-count contract is only auditable if
# its counters cannot be confused with the runtime's.
NAMESPACE = "beam_agents.otlp"
COUNTER_SPANS_EXPORTED = "spans_exported"
COUNTER_SPANS_DROPPED = "spans_dropped"
COUNTER_EXPORT_FAILURES = "export_failures"
COUNTER_BATCHES_SENT = "batches_sent"
COUNTERS = (
    COUNTER_SPANS_EXPORTED,
    COUNTER_SPANS_DROPPED,
    COUNTER_EXPORT_FAILURES,
    COUNTER_BATCHES_SENT,
)

# First retry backoff; doubled per attempt, always clamped to the remaining
# share of the flush deadline (design D3: bounded retry inside the deadline).
_INITIAL_BACKOFF_S = 0.05

_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_CLIENT_ERROR_LOW = 400
_HTTP_SERVER_ERROR_LOW = 500


def _require_otlp() -> Any:
    """Import and return the OTLP trace proto modules, or raise naming the extra."""
    try:
        from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
        from opentelemetry.proto.common.v1 import common_pb2
        from opentelemetry.proto.resource.v1 import resource_pb2
        from opentelemetry.proto.trace.v1 import trace_pb2
    except ImportError as exc:
        raise ImportError(
            "the otlp:// traces sink requires opentelemetry-proto; "
            "install the extra: pip install 'beam-agents[otlp]'"
        ) from exc
    return trace_service_pb2, common_pb2, resource_pb2, trace_pb2


def _event_to_span(event: TraceEvent) -> Span | None:
    """Map one ``TraceEvent`` to one OTLP span; ``None`` for ``ACTIVATION_START``.

    A pure function of the event: mapping the same event twice yields
    byte-identical serialized spans (attributes are emitted sorted by key), so
    at-least-once export dedups exactly on ``(trace_id, span_id)``.
    """
    if event.event_type == TraceEvent.ACTIVATION_START:
        return None
    _, common_pb2, _, trace_pb2 = _require_otlp()
    span = trace_pb2.Span(
        trace_id=event.trace_id,
        span_id=event.span_id,
        parent_span_id=event.parent_span_id,
        name=str(TraceEvent.EventType.Name(event.event_type)).lower(),
        kind=trace_pb2.Span.SPAN_KIND_INTERNAL,
        start_time_unix_nano=event.start_ms * 1_000_000,
        end_time_unix_nano=event.end_ms * 1_000_000,
        attributes=[
            common_pb2.KeyValue(
                key=key, value=common_pb2.AnyValue(string_value=event.attributes[key])
            )
            for key in sorted(event.attributes)
        ],
    )
    if event.event_type == TraceEvent.ERROR:
        span.status.code = trace_pb2.Status.STATUS_CODE_ERROR
        span.status.message = event.attributes.get(REASON, "")
    return span


def _encode_batch(spans: Sequence[Span], *, service_name: str) -> bytes:
    """Encode one batch as a deterministic ``ExportTraceServiceRequest``."""
    trace_service_pb2, common_pb2, resource_pb2, trace_pb2 = _require_otlp()
    request = trace_service_pb2.ExportTraceServiceRequest(
        resource_spans=[
            trace_pb2.ResourceSpans(
                resource=resource_pb2.Resource(
                    attributes=[
                        common_pb2.KeyValue(
                            key="service.name",
                            value=common_pb2.AnyValue(string_value=service_name),
                        )
                    ]
                ),
                scope_spans=[
                    trace_pb2.ScopeSpans(
                        scope=common_pb2.InstrumentationScope(name="beam_agents"),
                        spans=list(spans),
                    )
                ],
            )
        ]
    )
    return bytes(request.SerializeToString(deterministic=True))


class _BeamOtlpMetrics:
    """Beam-backed :class:`MetricsSink` over the exporter's declared counters.

    Same shape and rationale as ``RuntimeMetrics``: handles built once, an
    undeclared name fails in a test rather than creating a phantom metric.
    The exporter declares no distributions.
    """

    def __init__(self) -> None:
        from apache_beam.metrics.metric import Metrics

        self._counters = {name: Metrics.counter(NAMESPACE, name) for name in COUNTERS}

    def incr(self, name: str, n: int = 1) -> None:
        self._counters[name].inc(n)

    def observe(self, name: str, value: int) -> None:
        raise KeyError(f"the OTLP exporter declares no distributions, got {name!r}")


class _SenderState:
    """Counts shared between the Beam thread and the sender thread.

    Plain ints under one lock, read-and-reset by ``finish_bundle`` on the Beam
    thread — a Beam metric update from the sender thread would be silently
    discarded (the documented ``statesampler`` fact ``observability/metrics.py``
    is built around). ``in_flight_spans`` tracks enqueued-but-unsent spans so
    the flush can wait for drain and give up on the deadline.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.drained = threading.Condition(self.lock)
        self.exported = 0
        self.dropped = 0
        self.failures = 0
        self.batches_sent = 0
        self.in_flight_spans = 0

    def take_counts(self) -> dict[str, int]:
        """Read and reset the tallies (Beam thread, at finish_bundle)."""
        with self.lock:
            counts = {
                COUNTER_SPANS_EXPORTED: self.exported,
                COUNTER_SPANS_DROPPED: self.dropped,
                COUNTER_EXPORT_FAILURES: self.failures,
                COUNTER_BATCHES_SENT: self.batches_sent,
            }
            self.exported = self.dropped = self.failures = self.batches_sent = 0
        return counts


# The sender-loop shutdown sentinel. Anything with identity works; a dedicated
# object cannot collide with a real batch.
_STOP = object()


class _OtlpExportDoFn(beam.DoFn):
    """Batches trace events and exports them OTLP/HTTP off the element path.

    ``process()`` maps and buffers — no I/O, no blocking beyond a
    ``put_nowait``. One daemon sender thread drains a bounded queue of
    batches; ``finish_bundle()`` flushes the partial batch, waits for drain up
    to the flush deadline, drops (and counts) what will not drain, and records
    the tallied counters from the Beam thread. Delivery failure is telemetry
    loss by contract (design D3): nothing here ever raises for it.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_deadline_s: float = DEFAULT_FLUSH_DEADLINE_S,
        queue_batches: int = DEFAULT_QUEUE_BATCHES,
        service_name: str = DEFAULT_SERVICE_NAME,
        transport: httpx.BaseTransport | None = None,
        transport_factory: Callable[[], httpx.BaseTransport] | None = None,
        metrics: MetricsSink | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        # `transport`/`transport_factory`/`metrics`/`monotonic`/`sleep` are test
        # seams, mirroring `_AgentDoFn`'s: no `AgentConfig` knob exists for
        # them. A factory (picklable) is what pipeline tests inject; a direct
        # transport serves in-process DoFn tests.
        self._endpoint = endpoint
        self._batch_size = batch_size
        self._flush_deadline_s = flush_deadline_s
        self._queue_batches = queue_batches
        self._service_name = service_name
        self._transport = transport
        self._transport_factory = transport_factory
        self._metrics = metrics
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        self._sleep = sleep if sleep is not None else time.sleep

    def setup(self) -> None:
        _require_otlp()  # fail at worker startup, not first element
        transport = self._transport
        if transport is None and self._transport_factory is not None:
            transport = self._transport_factory()
        self._client = httpx.Client(transport=transport)
        self._sink: MetricsSink = (
            self._metrics if self._metrics is not None else (_BeamOtlpMetrics())
        )
        self._state = _SenderState()
        self._batch: list[Span] = []
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=self._queue_batches)
        self._sender = threading.Thread(
            target=self._send_loop, name="beam-agents-otlp-sender", daemon=True
        )
        self._sender.start()

    def process(self, element: TraceEvent) -> None:
        span = _event_to_span(element)
        if span is None:
            return
        self._batch.append(span)
        if len(self._batch) >= self._batch_size:
            self._enqueue()

    def finish_bundle(self) -> None:
        if self._batch:
            self._enqueue()
        deadline = self._monotonic() + self._flush_deadline_s
        state = self._state
        with state.lock:
            while state.in_flight_spans > 0:
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    break
                state.drained.wait(remaining)
        self._drop_undrained()
        for name, count in state.take_counts().items():
            if count:
                self._sink.incr(name, count)

    def teardown(self) -> None:
        self._queue.put(_STOP)
        self._sender.join(timeout=self._flush_deadline_s)
        self._client.close()

    # -- Beam thread helpers ---------------------------------------------------

    def _enqueue(self) -> None:
        batch, self._batch = self._batch, []
        state = self._state
        with state.lock:
            state.in_flight_spans += len(batch)
        try:
            self._queue.put_nowait(batch)
        except queue.Full:
            # The collector is slower than the pipeline. Blocking here would
            # convert telemetry lag into pipeline backpressure; drop instead.
            with state.lock:
                state.in_flight_spans -= len(batch)
                state.dropped += len(batch)

    def _drop_undrained(self) -> None:
        """Empty the queue after a drain timeout, counting the loss.

        Only actually-dequeued batches are counted — truthfully dropped, they
        can no longer send. The single batch the sender may hold right now is
        not: its own retry loop is bounded by the same deadline, and its
        outcome lands in a later bundle's (cumulative) counts.
        """
        state = self._state
        while True:
            try:
                batch = self._queue.get_nowait()
            except queue.Empty:
                return
            if batch is _STOP:  # pragma: no cover - teardown-only race
                self._queue.put(_STOP)
                return
            with state.lock:
                state.in_flight_spans -= len(batch)
                state.dropped += len(batch)

    # -- sender thread ---------------------------------------------------------

    def _send_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is _STOP:
                return
            self._send(item)
            state = self._state
            with state.lock:
                state.in_flight_spans -= len(item)
                state.drained.notify_all()

    def _send(self, batch: list[Span]) -> None:
        """POST one batch, retrying with backoff inside the flush deadline."""
        payload = _encode_batch(batch, service_name=self._service_name)
        state = self._state
        deadline = self._monotonic() + self._flush_deadline_s
        backoff = _INITIAL_BACKOFF_S
        while True:
            retryable = True
            try:
                response = self._client.post(
                    self._endpoint,
                    content=payload,
                    headers={"content-type": "application/x-protobuf"},
                )
            except httpx.HTTPError:
                pass  # transport failure: retryable
            else:
                if response.is_success:
                    with state.lock:
                        state.exported += len(batch)
                        state.batches_sent += 1
                    return
                # 4xx (except 429) would fail identically on every retry.
                retryable = not (
                    _HTTP_CLIENT_ERROR_LOW <= response.status_code < _HTTP_SERVER_ERROR_LOW
                    and response.status_code != _HTTP_TOO_MANY_REQUESTS
                )
            with state.lock:
                state.failures += 1
            remaining = deadline - self._monotonic()
            if not retryable or remaining <= 0:
                with state.lock:
                    state.dropped += len(batch)
                return
            self._sleep(min(backoff, remaining))
            backoff *= 2


class WriteTracesToOtlp(beam.PTransform):
    """Writes ``TraceEvent``s to an OTLP/HTTP collector, best-effort.

    The terminal transform an ``otlp://`` ``traces_to`` URI resolves to.
    Constructing it requires the ``otlp`` extra (``opentelemetry-proto``);
    the actionable ImportError here is what a missing extra surfaces as at
    pipeline-expansion time, while URI *validation* stays import-free.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_deadline_s: float = DEFAULT_FLUSH_DEADLINE_S,
        queue_batches: int = DEFAULT_QUEUE_BATCHES,
        service_name: str = DEFAULT_SERVICE_NAME,
        transport_factory: Callable[[], httpx.BaseTransport] | None = None,
    ) -> None:
        super().__init__()
        _require_otlp()
        self._endpoint = endpoint
        self._batch_size = batch_size
        self._flush_deadline_s = flush_deadline_s
        self._queue_batches = queue_batches
        self._service_name = service_name
        self._transport_factory = transport_factory

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        """Attach the batched, non-blocking OTLP export DoFn to ``pcoll``.

        Best-effort by construction: the exporter drops spans rather than
        blocking the pipeline on a slow collector, which is why ``otlp://``
        is rejected for the lossless sinks.
        """
        return pcoll | "ExportOtlp" >> beam.ParDo(
            _OtlpExportDoFn(
                self._endpoint,
                batch_size=self._batch_size,
                flush_deadline_s=self._flush_deadline_s,
                queue_batches=self._queue_batches,
                service_name=self._service_name,
                transport_factory=self._transport_factory,
            )
        )
