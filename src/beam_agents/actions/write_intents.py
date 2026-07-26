"""``WriteIntents`` — the outbox writer for ``ToolIntent`` side effects.

Usage::

    result = keyed_intents | WriteIntents("kafka://broker:9092/agent-intents")
    result.dead_letter  # (element, reason) pairs that failed to serialize

Input is a pre-keyed ``PCollection[KV[bytes, ToolIntent]]`` — the caller keys
by ``entity_key`` upstream (mirroring ``RunAgent``'s pre-keyed-input
contract). ``WriteIntents`` does not re-key elements; it validates the input
is KV-shaped at pipeline-construction (``expand``) time and raises
``ValueError`` otherwise.

Two outbox URI schemes are supported: ``kafka://<brokers>/<topic>`` and
``pubsub://<project>/<topic>``. The URI is parsed and validated at
construction time, before any pipeline exists; IO client modules are
imported lazily so construction never requires a Kafka/Pub/Sub dependency to
be importable.

Every intent is written keyed by its raw ``entity_key`` bytes (the Kafka
message key, or the Pub/Sub ``orderingKey`` derived from it), so intents for
a given entity are routed to a single partition/ordering key rather than
scattered across the topic. Beam itself makes no intra-``PCollection``
ordering guarantee (a key's intents can split across bundles or be
reprocessed out of order on retry on a distributed runner), so this routing
is what lets a consumer group a key's intents together -- it is not, by
itself, a guarantee that wire order equals emission order. A consumer that
needs a total order for a key should order by ``ToolIntent.seq``. A
serialization failure is routed to the ``.dead_letter`` tagged output instead
of failing the bundle or silently dropping the intent.

Importing this module has no side effects.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import apache_beam as beam
from apache_beam.typehints.typehints import AnyTypeConstraint, TupleHint

if TYPE_CHECKING:
    from collections.abc import Callable

    from beam_agents._protos import ToolIntent

DEAD_LETTER_TAG = "dead_letter"

_SCHEMES = frozenset({"kafka", "pubsub"})
_KV_ARITY = 2  # a KV pair is a 2-tuple: (key, value)


class UnknownIntentsSchemeError(ValueError):
    """An outbox URI's scheme is unrecognized, or the URI is malformed for its scheme."""


def _parse_intents_uri(uri: str) -> tuple[str, tuple[str, str]]:
    """Parse and validate an outbox URI, import-free.

    Returns ``(scheme, (authority, topic))`` where ``authority`` is the
    broker list for ``kafka://`` or the project for ``pubsub://``.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme
    if scheme not in _SCHEMES:
        raise UnknownIntentsSchemeError(
            f"WriteIntents: unknown outbox URI scheme {(scheme or uri)!r} in {uri!r}; "
            f"expected one of {sorted(_SCHEMES)}"
        )
    segments = [s for s in parsed.path.split("/") if s]
    if not parsed.netloc or len(segments) != 1:
        expected = (
            "kafka://<brokers>/<topic>" if scheme == "kafka" else "pubsub://<project>/<topic>"
        )
        raise UnknownIntentsSchemeError(
            f"WriteIntents: malformed {scheme} URI {uri!r}; expected {expected}"
        )
    return scheme, (parsed.netloc, segments[0])


def _build_kafka_writer(brokers: str, topic: str) -> beam.PTransform:
    from apache_beam.io.kafka import WriteToKafka

    return WriteToKafka(
        producer_config={"bootstrap.servers": brokers, "enable.idempotence": "true"},
        topic=topic,
    )


# Pub/Sub rejects an ordering key longer than this many bytes; entity_key.hex()
# doubles the key's byte length, so entity_key itself must stay at or under
# half this to avoid a publish-time rejection.
_PUBSUB_ORDERING_KEY_LIMIT = 1024


class _OrderedPubsubWriteDoFn(beam.DoFn):
    """Publishes keyed payloads to Pub/Sub with message ordering enabled.

    Deliberately bypasses ``apache_beam.io.gcp.pubsub.WriteToPubSub``: verified
    against a live Pub/Sub emulator that its ``_PubSubWriteDoFn._flush()``
    never forwards ``PubsubMessage.ordering_key`` to the underlying
    ``publish()`` call, so every message publishes unordered regardless of the
    key set on it. Publishing directly with ``enable_message_ordering=True``
    is the only way to actually get Pub/Sub's ordering-key delivery on this
    outbox scheme.
    """

    def __init__(self, project: str, topic: str) -> None:
        self._project = project
        self._topic_name = topic

    def setup(self) -> None:
        # google.cloud is a namespace package; mypy can't see pubsub_v1 as an
        # attribute of it even with ignore_missing_imports on google.*.
        from google.cloud import pubsub_v1  # type: ignore[attr-defined]

        self._client = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(
                enable_message_ordering=True,
                flow_control=pubsub_v1.types.PublishFlowControl(
                    limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK
                ),
            )
        )
        self._topic_path = self._client.topic_path(self._project, self._topic_name)

    def start_bundle(self) -> None:
        self._futures: list[tuple[str, Any]] = []

    def process(self, element: tuple[bytes, bytes]) -> None:
        key, payload = element
        ordering_key = key.hex()
        if len(ordering_key) > _PUBSUB_ORDERING_KEY_LIMIT:
            raise ValueError(
                f"WriteIntents: entity_key is {len(key)} bytes; its hex encoding "
                f"({len(ordering_key)} bytes) exceeds Pub/Sub's "
                f"{_PUBSUB_ORDERING_KEY_LIMIT}-byte ordering-key limit"
            )
        future = self._client.publish(self._topic_path, payload, ordering_key=ordering_key)
        self._futures.append((ordering_key, future))

    def finish_bundle(self) -> None:
        futures, self._futures = self._futures, []
        for ordering_key, future in futures:
            try:
                future.result()
            except Exception:
                # An unrecoverable publish error permanently pauses this
                # ordering key in the client's OrderedSequencer -- every later
                # publish on it fails until resumed. The client is built in
                # setup() and survives bundle retries, so the key stays wedged
                # forever unless it's resumed here before the error surfaces.
                self._client.resume_publish(self._topic_path, ordering_key)
                raise

    def teardown(self) -> None:
        self._client.stop()


class _PubsubOutboxWriter(beam.PTransform):
    """Publishes keyed payloads to Pub/Sub, routed by ordering key."""

    def __init__(self, project: str, topic: str) -> None:
        super().__init__()
        self._project = project
        self._topic = topic

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        return pcoll | "PublishOrdered" >> beam.ParDo(
            _OrderedPubsubWriteDoFn(self._project, self._topic)
        )


def _build_pubsub_writer(project: str, topic: str) -> beam.PTransform:
    return _PubsubOutboxWriter(project, topic)


_WRITERS: dict[str, Callable[[str, str], beam.PTransform]] = {
    "kafka": _build_kafka_writer,
    "pubsub": _build_pubsub_writer,
}


@dataclass(frozen=True, slots=True)
class WriteIntentsResult:
    """``WriteIntents``'s output: the dead-letter branch for serialization failures."""

    dead_letter: beam.pvalue.PCollection


def is_kv_shaped(element_type: object) -> bool:
    """True if ``element_type`` is absent/erased, or is exactly KV (2-tuple) shaped.

    Shared by ``WriteIntents`` and ``RunAgent``, whose KV-input validation is
    otherwise identical logic over two different element types.
    """
    return (
        element_type is None
        or isinstance(element_type, AnyTypeConstraint)
        or (
            isinstance(element_type, TupleHint.TupleConstraint)
            and len(element_type.tuple_types) == _KV_ARITY
        )
    )


def _validate_kv_input(pcoll: beam.pvalue.PCollection) -> None:
    """Raise ``ValueError`` if ``pcoll`` is positively not KV-shaped.

    An absent/erased element type is allowed to pass; only a definite
    non-pair type is rejected.
    """
    if not is_kv_shaped(pcoll.element_type):
        raise ValueError(
            "WriteIntents requires a PCollection[KV[bytes, ToolIntent]] input "
            f"(keyed by entity_key); got element type {pcoll.element_type!r}. Key upstream "
            "with beam.WithKeys(lambda intent: intent.entity_key)"
            ".with_output_types(tuple[bytes, ToolIntent]) before WriteIntents."
        )


class _SerializeIntent(beam.DoFn):
    """Serializes ``ToolIntent`` to canonical bytes; dead-letters on failure."""

    def process(self, element: tuple[bytes, ToolIntent]):
        key, intent = element
        try:
            payload = intent.SerializeToString(deterministic=True)
        except Exception as exc:  # any failure is dead-lettered, never raised
            yield beam.pvalue.TaggedOutput(DEAD_LETTER_TAG, (element, str(exc)))
            return
        yield key, payload


def encode_intent_dead_letter(element: tuple[tuple[bytes, ToolIntent], str]) -> tuple[bytes, bytes]:
    """Encode a ``.dead_letter`` element as ``KV[bytes, bytes]``.

    ``.dead_letter`` elements are ``((entity_key, ToolIntent), reason)`` --
    the shape `RunAgent` needs to route them onward to an `errors_to` sink,
    which (for the Kafka scheme, the pairing this is exercised against)
    requires ``KV[bytes, bytes]``. The failed intent's own serialization is
    what failed, so this carries the identifying fields it does have rather
    than re-attempting ``SerializeToString``.
    """
    (key, intent), reason = element
    detail = json.dumps(
        {
            "reason": reason,
            "intent_id": intent.intent_id,
            "seq": intent.seq,
            "tool_name": intent.tool_name,
        }
    )
    return key, detail.encode("utf-8")


class WriteIntents(beam.PTransform):
    """Writes keyed ``ToolIntent``s to an outbox topic, routed by ``entity_key``.

    Consumes ``PCollection[KV[bytes, ToolIntent]]`` keyed by ``entity_key``
    and writes to ``kafka://<brokers>/<topic>`` or ``pubsub://<project>/<topic>``.
    """

    def __init__(
        self,
        uri: str,
        *,
        writer_factory: Callable[[str], beam.PTransform] | None = None,
    ) -> None:
        """Construct a ``WriteIntents`` writing to ``uri``.

        ``uri`` is parsed and validated immediately, raising
        ``UnknownIntentsSchemeError`` (a ``ValueError``) for an unrecognized
        scheme or a malformed URI. ``writer_factory``, if given, replaces the
        real Kafka/Pub/Sub writer with a caller-supplied one (for tests);
        it is called with ``uri`` and must return the terminal write
        ``PTransform`` for ``PCollection[KV[bytes, bytes]]``.
        """
        super().__init__()
        self._uri = uri
        self._scheme, self._parts = _parse_intents_uri(uri)
        self._writer_factory = writer_factory

    def _build_writer(self) -> beam.PTransform:
        if self._writer_factory is not None:
            return self._writer_factory(self._uri)
        return _WRITERS[self._scheme](*self._parts)

    def expand(self, pcoll: beam.pvalue.PCollection) -> WriteIntentsResult:
        _validate_kv_input(pcoll)
        # Explicit output type on the ParDo itself: the Kafka cross-language
        # expansion service needs a concrete KvCoder<ByteArrayCoder,
        # ByteArrayCoder> reported for the main output, not Beam's generic/
        # pickle-based fallback coder.
        serializer = beam.ParDo(_SerializeIntent()).with_output_types(tuple[bytes, bytes])
        tagged = pcoll | "SerializeIntent" >> serializer.with_outputs(
            DEAD_LETTER_TAG, main="serialized"
        )
        tagged.serialized | "WriteToOutbox" >> self._build_writer()
        return WriteIntentsResult(dead_letter=tagged.dead_letter)
