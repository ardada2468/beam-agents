"""Read trace events off a topic a pipeline is already writing.

The cheapest adoption path there is: a deployment exporting with
``traces_to="kafka://…"`` gets a console by starting one, with no pipeline
change, no redeploy, and no restart.

Two defaults are deliberate. It reads **from the end** by default, because a
viewer opened now is almost always asking about now, and replaying a retained
topic from the beginning on every start would make the first minute useless.
And it **commits no offsets**, so restarting never blocks on a consumer group's
rebalance and two consoles watching one topic never starve each other — this is
a read-only viewer, and durable group membership would be a liability rather
than a feature. Each is one argument to ``AIOKafkaConsumer`` and neither has any
other implementation: ``auto_offset_reset`` decides where a fresh consumer
starts, and with ``group_id=None`` there is no group to commit *to*, which makes
"commits no offsets" structural rather than a discipline to keep.

The URI grammar is the runtime's own — ``kafka://<brokers>/<topic>``, parsed by
``effector.config.parse_transport_uri``. Importing that parser rather than
writing a fourth copy of it is what keeps a value copied verbatim out of a
pipeline's ``traces_to`` from being read differently here than it was written;
the effector's package imports no client library and no Beam, so the dependency
costs nothing at import time. (``core.transform``'s copy is the one that cannot
be reused: it lives on a resolver that imports Beam.)

Decoding is deliberately stricter than ``ParseFromString``. Protobuf accepts an
empty payload as a valid message of every type, so "it parsed" does not mean "it
is a trace event"; an event with no ``trace_id``/``span_id`` cannot be addressed
by the store's dedup key ``(trace_id, span_id, event_type)`` and is counted as a
decode failure rather than stored as a row nothing can ever reach.

``aiokafka`` is imported inside the constructor, so importing this module works
with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from google.protobuf.message import DecodeError

from beam_agents._protos import TraceEvent
from beam_agents.console import _ingest
from beam_agents.console._records import PROVENANCE_KAFKA
from beam_agents.effector.config import parse_transport_uri, redact_uri

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents.console._records import RecordBatch
    from beam_agents.console._store import ConsoleStore

__all__ = ["EXTRA_NAME", "KafkaTraceSource"]

# Named in the error a missing client raises, so the fix is in the message.
EXTRA_NAME = "console-ingest"

# The name a rejected URI is reported under. It is the CLI flag that carries it,
# so the message points at what the user typed.
_URI_FIELD = "kafka_traces_from"


def _parse_kafka_uri(uri: str) -> tuple[str, str]:
    """Split ``kafka://<brokers>/<topic>`` into its two parts.

    Rejects every other transport scheme: ``parse_transport_uri`` also accepts
    ``pubsub://``, which this source cannot read, and accepting it here would
    fail later with a client error instead of now with a configuration one.
    """
    scheme, parts = parse_transport_uri(_URI_FIELD, uri)
    if scheme != "kafka":
        raise ValueError(
            f"{_URI_FIELD}: {redact_uri(uri)!r} is a {scheme} URI; the console's trace "
            "consumer reads Kafka only, as kafka://<bootstrap-servers>/<topic>"
        )
    brokers, topic = parts
    return brokers, topic


def _missing_client_error(cause: ImportError) -> ImportError:
    """The actionable constructor-time error for an absent optional client."""
    return ImportError(
        "KafkaTraceSource requires the 'aiokafka' client library, which is not installed; "
        f"install the {EXTRA_NAME!r} extra (pip install 'beam-agents[{EXTRA_NAME}]') "
        f"or add the client to your environment [{cause}]"
    )


class KafkaTraceSource:
    """A background consumer that stores trace events from a Kafka topic.

    ``uri`` uses the same ``kafka://<brokers>/<topic>`` grammar the runtime's
    sink resolver already parses, so the value can be copied verbatim from the
    pipeline's ``traces_to``.

    ``on_batch`` is called with each batch after it is stored — how the live
    stream learns that ingest happened without this class knowing what a
    subscriber is. Extra keyword arguments are forwarded to ``AIOKafkaConsumer``
    (broker security settings, fetch tuning), except ``consumer``, which
    *replaces* the client outright: the injection seam the offline tests drive,
    mirroring ``BigQueryTraceSource``'s ``client``.
    """

    def __init__(
        self,
        uri: str,
        store: ConsoleStore,
        *,
        from_beginning: bool = False,
        on_batch: Callable[[RecordBatch], None] | None = None,
        **options: Any,
    ) -> None:
        """Configure the consumer; raise naming the extra if the client is absent."""
        # The URI is parsed before the client is imported, so a typo is reported
        # as a typo rather than as a missing dependency.
        brokers, topic = _parse_kafka_uri(uri)
        self._store = store
        self._on_batch = on_batch
        self._records_stored = 0
        self._decode_failures = 0
        self._stopped = False

        consumer = options.pop("consumer", None)
        if consumer is None:
            try:
                from aiokafka import AIOKafkaConsumer  # noqa: PLC0415
            except ImportError as exc:
                raise _missing_client_error(exc) from exc

            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=brokers,
                # No group: nothing to rebalance on restart, nothing to commit,
                # and no way for a second console to take partitions away from
                # this one.
                group_id=None,
                enable_auto_commit=False,
                auto_offset_reset="earliest" if from_beginning else "latest",
                **options,
            )
        self._consumer = consumer

    @property
    def decode_failures(self) -> int:
        """Messages that were not valid trace events, counted and skipped."""
        return self._decode_failures

    @property
    def records_stored(self) -> int:
        """Trace events successfully handed to the store."""
        return self._records_stored

    async def run(self) -> None:
        """Consume until cancelled, storing what decodes and counting what does not."""
        try:
            # Inside the `try` so that a broker that refuses the connection
            # still releases whatever the half-started client opened.
            await self._consumer.start()
            async for message in self._consumer:
                self._consume(message)
        except Exception:
            # `stop()` from another task tears the consumer down under the
            # iterator, which surfaces as a client error rather than as the end
            # of the stream. Once stop() has been called, that is a shutdown in
            # progress, not a failure to report.
            if not self._stopped:
                raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the consumer and release its connections. Idempotent."""
        if self._stopped:
            return
        self._stopped = True
        await self._consumer.stop()

    def _consume(self, message: Any) -> None:
        """Store one message's event, or count it as undecodable and move on."""
        event = _decode(message.value)
        if event is None:
            # One malformed message on a topic is not a reason for a viewer to
            # stop viewing: count it, skip it, keep consuming.
            self._decode_failures += 1
            return
        # Protos to the one normalizer, rows to the store, and nothing built by
        # hand in between (design D7).
        batch = _ingest.normalize(events=(event,), provenance=PROVENANCE_KAFKA)
        self._store.write(batch)
        self._records_stored += len(batch.events)
        if self._on_batch is not None:
            self._on_batch(batch)


def _decode(payload: bytes | None) -> TraceEvent | None:
    """Decode one message into a trace event, or ``None`` if it is not one.

    ``None`` covers three cases a live topic really produces: a tombstone (no
    value at all), bytes that are not a protobuf message, and bytes that parse
    but carry no span identity — see the module docstring on why the last one is
    not a valid event either.
    """
    if not payload:
        return None
    event = TraceEvent()
    try:
        event.ParseFromString(payload)
    except (DecodeError, UnicodeDecodeError):
        # `DecodeError` is what a malformed or truncated frame raises;
        # `UnicodeDecodeError` is what a `string` field carrying non-UTF-8 bytes
        # raises in the pure-Python implementation. Both mean "not this message".
        return None
    if not event.trace_id or not event.span_id:
        return None
    return event
