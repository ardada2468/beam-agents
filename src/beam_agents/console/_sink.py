"""The ``console://`` sink: pipeline records pushed straight to a console.

This deliberately copies ``observability/otlp.py``'s shape rather than inventing
a second telemetry-delivery posture (design D3). Batch in ``process()``, hand
batches to one daemon sender through a bounded queue, and **drop-and-count**:
never raise, never retry indefinitely, never apply backpressure.

Any other posture is unsound. A console is exactly the kind of endpoint that
goes away mid-pipeline — a developer closes the laptop — and telemetry delivery
failing must never fail an activation or slow the agent's real work. Reusing the
contract also means a reader who understands the OTLP exporter already
understands this one, and the drop behaviour is auditable the same way.

One deliberate difference from the OTLP exporter: this transmits
``ACTIVATION_START``. OTLP drops it because it shares a span ID with
``ACTIVATION_END`` and the format cannot represent two events on one span; the
native record carries ``event_type`` as a first-class field, so the start event
is both representable and load-bearing — it is what distinguishes a fresh
attempt from a resume.

The wire form is the protos themselves, varint-length-delimited, which is the
framing ``replay/bundle.py`` already publishes and ``_ingest.py`` reverses. A
batch is therefore a self-describing stream rather than a bespoke envelope, and
an ingest endpoint reading it cannot disagree with the replay CLI about where
one record ends.

:class:`ConsoleSinkResolver` *wraps* ``DefaultSinkResolver`` rather than
extending it, so no module on the hot path is modified and every other scheme
keeps behaving exactly as it does today (design D2).

``httpx`` is imported inside the DoFn that needs a client, not at module scope,
even though it is a core dependency. ``SinkResolver.validate`` is required to be
import-free, it runs at ``AgentConfig`` construction, and this module is what a
user imports in order to *get* a resolver — so keeping the client out of module
scope is the difference between that property being checkable and being merely
asserted. The client is only ever needed on a worker.

Importing this module has no side effects.
"""

from __future__ import annotations

import queue
import threading
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

from beam_agents._protos import ActivationErrorRecord, StateSnapshot, TraceEvent
from beam_agents.console._app import DEFAULT_PORT
from beam_agents.core.dofn import ActivationError
from beam_agents.core.transform import DefaultSinkResolver, UnknownSinkSchemeError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import httpx

    from beam_agents.core.transform import SinkResolver
    from beam_agents.observability.metrics import MetricsSink

__all__ = [
    "COUNTERS",
    "COUNTER_BATCHES_SENT",
    "COUNTER_EXPORT_FAILURES",
    "COUNTER_RECORDS_DROPPED",
    "COUNTER_RECORDS_EXPORTED",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_FLUSH_DEADLINE_S",
    "DEFAULT_QUEUE_BATCHES",
    "INGEST_PATHS",
    "NAMESPACE",
    "RECORD_KINDS",
    "SCHEME",
    "ConsoleSinkResolver",
    "WriteToConsole",
]

SCHEME = "console"

DEFAULT_BATCH_SIZE = 256
DEFAULT_FLUSH_DEADLINE_S = 2.0
DEFAULT_QUEUE_BATCHES = 8

# Distinct from `beam_agents.runtime` and from `beam_agents.otlp`: these count
# telemetry delivery to one particular sink, not agent work, and the
# drop-and-count contract is only auditable if the counters cannot be confused
# with either of the others.
NAMESPACE = "beam_agents.console"
COUNTER_RECORDS_EXPORTED = "records_exported"
COUNTER_RECORDS_DROPPED = "records_dropped"
COUNTER_EXPORT_FAILURES = "export_failures"
COUNTER_BATCHES_SENT = "batches_sent"
COUNTERS = (
    COUNTER_RECORDS_EXPORTED,
    COUNTER_RECORDS_DROPPED,
    COUNTER_EXPORT_FAILURES,
    COUNTER_BATCHES_SENT,
)

# The three native ingest endpoints (`_app.py`), one per record kind. The kind
# picks both the path and the encoder, so a sink can never post a snapshot to
# the traces endpoint.
INGEST_PATHS = {
    "traces": "/ingest/traces",
    "errors": "/ingest/errors",
    "snapshots": "/ingest/snapshots",
}
RECORD_KINDS = tuple(INGEST_PATHS)

# Which record kind each sink field carries. `intents_to` is absent on purpose:
# intents cause side effects and need a lossless sink, and this one drops by
# contract — the same reason `DefaultSinkResolver` refuses `otlp://` for it.
_FIELD_RECORD_KINDS = {
    "traces_to": "traces",
    "errors_to": "errors",
    "snapshots_to": "snapshots",
}

_CONSOLE_GRAMMAR = (
    "expected console://<host>[:<port>][?tls=true&batch_size=N&flush_deadline_s=S&queue_batches=N]"
)

# First retry backoff; doubled per attempt, always clamped to the remaining
# share of the flush deadline (the OTLP exporter's bounded-retry shape).
_INITIAL_BACKOFF_S = 0.05

_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_CLIENT_ERROR_LOW = 400
_HTTP_SERVER_ERROR_LOW = 500

_VARINT_CONTINUATION_BIT = 0x80
_VARINT_PAYLOAD_BITS = 7


# --- URI grammar --------------------------------------------------------------


def _parse_bool(value: str) -> bool:
    if value not in ("true", "false"):
        raise ValueError(value)
    return value == "true"


def _parse_positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(value)
    return parsed


def _parse_positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(value)
    return parsed


# Option-name -> value parser; an unknown option is a KeyError, reported with
# the same grammar message as an unparseable value.
_OPTION_PARSERS: dict[str, Callable[[str], Any]] = {
    "batch_size": _parse_positive_int,
    "queue_batches": _parse_positive_int,
    "flush_deadline_s": _parse_positive_float,
}


def _parse_console_uri(field_name: str, uri: str) -> tuple[str, dict[str, Any]]:
    """Parse ``console://<host>[:<port>][?opts]`` into ``(base URL, options)``.

    Import-free, like every ``validate`` path, and the single grammar both
    :meth:`ConsoleSinkResolver.validate` and :meth:`ConsoleSinkResolver.resolve`
    go through, so a URI that validates is a URI that resolves. The ingest path
    is implied rather than spelled: the record kind chooses it, not the user.
    """
    parsed = urlparse(uri)
    try:
        # `.port` raises ValueError (rather than returning None) for a
        # non-numeric or out-of-range port.
        port = parsed.port
        hostname = parsed.hostname
    except ValueError as exc:
        raise UnknownSinkSchemeError(
            f"{field_name}: malformed console URI {uri!r}; {_CONSOLE_GRAMMAR}"
        ) from exc
    if not hostname:
        raise UnknownSinkSchemeError(
            f"{field_name}: malformed console URI {uri!r}; {_CONSOLE_GRAMMAR}"
        )
    if parsed.path and parsed.path != "/":
        raise UnknownSinkSchemeError(
            f"{field_name}: console URI {uri!r} must not carry a path (the ingest "
            f"endpoint is implied by the record kind); {_CONSOLE_GRAMMAR}"
        )
    options: dict[str, Any] = {}
    tls = False
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        value = values[-1]
        try:
            if key == "tls":
                tls = _parse_bool(value)
            else:
                options[key] = _OPTION_PARSERS[key](value)
        except (KeyError, ValueError) as exc:
            raise UnknownSinkSchemeError(
                f"{field_name}: bad console URI option {key}={value!r} in {uri!r}; "
                f"{_CONSOLE_GRAMMAR}"
            ) from exc
    scheme = "https" if tls else "http"
    if port is None:
        port = DEFAULT_PORT
    return f"{scheme}://{hostname}:{port}", options


# --- record encoding ----------------------------------------------------------


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & (_VARINT_CONTINUATION_BIT - 1)
        value >>= _VARINT_PAYLOAD_BITS
        if value:
            out.append(byte | _VARINT_CONTINUATION_BIT)
        else:
            out.append(byte)
            return bytes(out)


def _frame(payloads: Sequence[bytes]) -> bytes:
    """Concatenate payloads as one varint-length-delimited stream.

    Byte-identical to ``replay.bundle.frame_trace_events`` for trace payloads —
    asserted in the tests — but generic over the record kind, because errors and
    snapshots reach the console in the same framing and a second framing would
    be a second thing to get wrong.
    """
    out = bytearray()
    for payload in payloads:
        out += _varint(len(payload)) + payload
    return bytes(out)


def _encode_element(record_kind: str, element: Any) -> bytes:
    """Serialize one pipeline element to its native wire record.

    ``.errors`` carries :class:`ActivationError` — a dataclass, not a proto — so
    this is where it becomes the published :class:`ActivationErrorRecord`, the
    same record ``core/error_records.py`` puts on a bus (unwrapped: the
    ``AgentEnvelope`` wrapper exists to make an errors *topic* a valid RunAgent
    input, and the console is not one).

    A wrong element type raises: drop-and-count covers an absent console, not a
    sink wired to the wrong PCollection, which is a pipeline bug that must be
    loud at the first element rather than silently counted as telemetry loss.
    """
    if record_kind == "traces":
        if isinstance(element, TraceEvent):
            return bytes(element.SerializeToString(deterministic=True))
    elif record_kind == "errors":
        if isinstance(element, ActivationError):
            element = ActivationErrorRecord(
                entity_key=element.entity_key,
                reason=element.reason,
                detail=element.detail,
                event_time_ms=element.event_time_ms,
            )
        if isinstance(element, ActivationErrorRecord):
            return bytes(element.SerializeToString(deterministic=True))
    elif record_kind == "snapshots" and isinstance(element, StateSnapshot):
        return bytes(element.SerializeToString(deterministic=True))
    raise TypeError(
        f"the console {record_kind} sink cannot encode a {type(element).__name__}; "
        f"it is wired to the wrong output"
    )


# --- export -------------------------------------------------------------------


class _BeamConsoleMetrics:
    """Beam-backed :class:`MetricsSink` over the sink's declared counters.

    Same shape and rationale as ``RuntimeMetrics`` and the OTLP exporter's:
    handles built once, an undeclared name fails in a test rather than creating
    a phantom metric. The sink declares no distributions.
    """

    def __init__(self) -> None:
        self._counters = {name: Metrics.counter(NAMESPACE, name) for name in COUNTERS}

    def incr(self, name: str, n: int = 1) -> None:
        self._counters[name].inc(n)

    def observe(self, name: str, value: int) -> None:
        raise KeyError(f"the console sink declares no distributions, got {name!r}")


class _SenderState:
    """Counts shared between the Beam thread and the sender thread.

    Plain ints under one lock, read-and-reset by ``finish_bundle`` on the Beam
    thread — a Beam metric update from the sender thread would be silently
    discarded (the documented ``statesampler`` fact ``observability/metrics.py``
    is built around). ``in_flight_records`` tracks enqueued-but-unsent records
    so the flush can wait for drain and give up on the deadline.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.drained = threading.Condition(self.lock)
        self.exported = 0
        self.dropped = 0
        self.failures = 0
        self.batches_sent = 0
        self.in_flight_records = 0

    def take_counts(self) -> dict[str, int]:
        """Read and reset the tallies (Beam thread, at finish_bundle)."""
        with self.lock:
            counts = {
                COUNTER_RECORDS_EXPORTED: self.exported,
                COUNTER_RECORDS_DROPPED: self.dropped,
                COUNTER_EXPORT_FAILURES: self.failures,
                COUNTER_BATCHES_SENT: self.batches_sent,
            }
            self.exported = self.dropped = self.failures = self.batches_sent = 0
        return counts


# The sender-loop shutdown sentinel. Anything with identity works; a dedicated
# object cannot collide with a real batch.
_STOP = object()


class _ConsoleExportDoFn(beam.DoFn):
    """Batches records and POSTs them to the console off the element path.

    ``process()`` serializes and buffers — no I/O, no blocking beyond a
    ``put_nowait``. One daemon sender thread drains a bounded queue of batches;
    ``finish_bundle()`` flushes the partial batch, waits for drain up to the
    flush deadline, drops (and counts) what will not drain, and records the
    tallied counters from the Beam thread. Delivery failure is telemetry loss by
    contract (design D3): nothing here ever raises for it.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        record_kind: str = "traces",
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_deadline_s: float = DEFAULT_FLUSH_DEADLINE_S,
        queue_batches: int = DEFAULT_QUEUE_BATCHES,
        transport: httpx.BaseTransport | None = None,
        transport_factory: Callable[[], httpx.BaseTransport] | None = None,
        metrics: MetricsSink | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        # `transport`/`transport_factory`/`metrics`/`monotonic`/`sleep` are test
        # seams, mirroring the OTLP exporter's: no `AgentConfig` knob exists for
        # them. A factory (picklable) is what pipeline tests inject; a direct
        # transport serves in-process DoFn tests.
        self._url = endpoint + INGEST_PATHS[record_kind]
        self._record_kind = record_kind
        self._batch_size = batch_size
        self._flush_deadline_s = flush_deadline_s
        self._queue_batches = queue_batches
        self._transport = transport
        self._transport_factory = transport_factory
        self._metrics = metrics
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        self._sleep = sleep if sleep is not None else time.sleep

    def setup(self) -> None:
        # PLC0415-is-the-point: keeping the HTTP client out of module scope is
        # what makes `ConsoleSinkResolver.validate`'s import-free requirement a
        # checkable property rather than an assertion (see the module docstring).
        # A worker is the only place a client is ever needed.
        import httpx  # noqa: PLC0415

        # Bound once, so the sender's except clause needs no import of its own.
        self._http_error: type[Exception] = httpx.HTTPError
        transport = self._transport
        if transport is None and self._transport_factory is not None:
            transport = self._transport_factory()
        self._client = httpx.Client(transport=transport)
        self._sink: MetricsSink = (
            self._metrics if self._metrics is not None else _BeamConsoleMetrics()
        )
        self._state = _SenderState()
        self._batch: list[bytes] = []
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=self._queue_batches)
        self._sender = threading.Thread(
            target=self._send_loop, name="beam-agents-console-sender", daemon=True
        )
        self._sender.start()

    def process(self, element: Any) -> None:
        self._batch.append(_encode_element(self._record_kind, element))
        if len(self._batch) >= self._batch_size:
            self._enqueue()

    def finish_bundle(self) -> None:
        if self._batch:
            self._enqueue()
        deadline = self._monotonic() + self._flush_deadline_s
        state = self._state
        with state.lock:
            while state.in_flight_records > 0:
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
        if not self._sender.is_alive():
            self._client.close()
        # Else the sender is still inside a send that outran the deadline, and
        # closing the client under it would raise on *its* thread — an
        # unhandled exception in a daemon thread, from a sink whose whole
        # contract is that it never raises. The DoFn is being discarded, so the
        # client goes with it; the thread is a daemon and cannot hold the
        # worker open.

    # -- Beam thread helpers ---------------------------------------------------

    def _enqueue(self) -> None:
        batch, self._batch = self._batch, []
        state = self._state
        with state.lock:
            state.in_flight_records += len(batch)
        try:
            self._queue.put_nowait(batch)
        except queue.Full:
            # The console is slower than the pipeline. Blocking here would
            # convert telemetry lag into pipeline backpressure; drop instead.
            with state.lock:
                state.in_flight_records -= len(batch)
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
                state.in_flight_records -= len(batch)
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
                state.in_flight_records -= len(item)
                state.drained.notify_all()

    def _send(self, batch: list[bytes]) -> None:
        """POST one batch, retrying with backoff inside the flush deadline."""
        payload = _frame(batch)
        state = self._state
        deadline = self._monotonic() + self._flush_deadline_s
        backoff = _INITIAL_BACKOFF_S
        while True:
            retryable = True
            try:
                response = self._client.post(
                    self._url,
                    content=payload,
                    headers={"content-type": "application/x-protobuf"},
                )
            except self._http_error:
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


class WriteToConsole(beam.PTransform):
    """Best-effort delivery of pipeline records to a console endpoint.

    Accepts ``TraceEvent``, ``ActivationError`` (or an already-built
    ``ActivationErrorRecord``), and ``StateSnapshot`` elements — the element
    types of ``.traces``, ``.errors``, and ``.snapshots``. Returns an empty
    ``PCollection``: this is a terminal write, and nothing downstream should be
    able to depend on delivery having happened.

    ``endpoint`` is the console's base URL (``http://host:port``); the ingest
    path comes from ``record_kind``, so one sink can never post a snapshot to
    the traces endpoint.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        record_kind: str = "traces",
        batch_size: int = DEFAULT_BATCH_SIZE,
        flush_deadline_s: float = DEFAULT_FLUSH_DEADLINE_S,
        queue_batches: int = DEFAULT_QUEUE_BATCHES,
        transport_factory: Callable[[], httpx.BaseTransport] | None = None,
    ) -> None:
        """Configure delivery to ``endpoint`` for one record kind."""
        super().__init__()
        if record_kind not in INGEST_PATHS:
            raise ValueError(
                f"record_kind must be one of {list(RECORD_KINDS)}, got {record_kind!r}"
            )
        self._endpoint = endpoint.rstrip("/")
        self._record_kind = record_kind
        self._batch_size = batch_size
        self._flush_deadline_s = flush_deadline_s
        self._queue_batches = queue_batches
        self._transport_factory = transport_factory

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        """Batch elements and hand them to the background sender.

        Best-effort by construction: the sender drops records rather than
        blocking the pipeline on a console that is slow or gone, which is why
        ``console://`` is refused for ``intents_to``.
        """
        return pcoll | "ExportConsole" >> beam.ParDo(
            _ConsoleExportDoFn(
                self._endpoint,
                record_kind=self._record_kind,
                batch_size=self._batch_size,
                flush_deadline_s=self._flush_deadline_s,
                queue_batches=self._queue_batches,
                transport_factory=self._transport_factory,
            )
        )


class ConsoleSinkResolver:
    """A ``SinkResolver`` that adds ``console://`` and delegates everything else.

    Install it where the sinks are already chosen::

        AgentConfig(
            ...,
            traces_to="console://localhost:8787",
            errors_to="console://localhost:8787",
            sink_resolver=ConsoleSinkResolver(),
        )

    Unlike ``otlp://`` — which the default resolver refuses for anything but
    traces, because the OTLP encoding cannot represent an error record or a
    state snapshot — ``console://`` is accepted for ``traces_to``, ``errors_to``,
    and ``snapshots_to``. The native encoding is the protos themselves, so there
    is nothing to lose.

    ``**options`` are per-transform defaults (``batch_size``,
    ``flush_deadline_s``, ``queue_batches``) applied to every ``console://``
    sink this resolver builds; the same option spelled in a URI wins, so one
    sink can be tuned without moving the others.
    """

    def __init__(self, delegate: SinkResolver | None = None, **options: Any) -> None:
        """Wrap ``delegate``, defaulting to the runtime's own resolver."""
        # Composition, not inheritance: `core/transform.py` is untouched and
        # every scheme it knows keeps resolving through its own code (D2).
        self._delegate: SinkResolver = delegate if delegate is not None else DefaultSinkResolver()
        self._options = options

    def validate(self, field_name: str, uri: str) -> None:
        """Reject a URI that cannot serve ``field_name``.

        Import-free, as the protocol requires: this runs at ``AgentConfig``
        construction and must not pull an HTTP client or touch the network.
        """
        if urlparse(uri).scheme != SCHEME:
            self._delegate.validate(field_name, uri)
            return
        if field_name not in _FIELD_RECORD_KINDS:
            raise UnknownSinkSchemeError(
                f"{field_name}: console:// is a best-effort telemetry sink (it drops on "
                "delivery failure) and is valid only for traces_to, errors_to, and "
                "snapshots_to; intents need a lossless sink (kafka://, pubsub://, or "
                "bigquery://)"
            )
        _parse_console_uri(field_name, uri)

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        """Build the writer transform ``uri`` names for ``field_name``."""
        if urlparse(uri).scheme != SCHEME:
            return self._delegate.resolve(field_name, uri)
        self.validate(field_name, uri)
        endpoint, options = _parse_console_uri(field_name, uri)
        return WriteToConsole(
            endpoint,
            record_kind=_FIELD_RECORD_KINDS[field_name],
            **{**self._options, **options},
        )
