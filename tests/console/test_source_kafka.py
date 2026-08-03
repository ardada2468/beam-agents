"""The Kafka trace source: reading a topic a pipeline is already writing.

Offline, the consumer is a fake async iterable and the store records the batches
it is handed, so the whole path — decode, normalize, store, count — runs with
`aiokafka` absent. That absence is the point: the `console-ingest` extra is not
installed in the unit lane, and the missing-client test proves the constructor
says so rather than surfacing a transitive `ImportError`.

`_ingest.normalize` is patched here because it is still a placeholder (unit 2
owns it). The patch builds real `EventRow`s from `_records.py`, which *is*
complete, so what reaches the fake store is the real row vocabulary. When
`normalize` lands, the patch can be deleted and these tests should pass against
the real one unchanged.

The `integration`-marked test drives the same source against the `redpanda`
service in `docker/compose.yaml` (host port 19092), which is the only way to
assert that a real broker, a real consumer, and a real deserialization path
agree with the fakes.
"""

from __future__ import annotations

import asyncio
import os
import sys
import types
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.console import _ingest
from beam_agents.console._records import PROVENANCE_KAFKA, EventRow, RecordBatch
from beam_agents.console._sources._kafka import EXTRA_NAME, KafkaTraceSource
from beam_agents.observability.exporters import serialize_trace_event
from beam_agents.observability.traces import span_id_for, trace_id_for

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from beam_agents._protos import ActivationErrorRecord, StateSnapshot
    from beam_agents.console._store import ConsoleStore

BROKERS = os.environ.get("BEAM_AGENTS_KAFKA_BROKERS", "localhost:19092")

NOW_MS = 1_700_000_000_000

# Bytes that cannot be a `TraceEvent`: the leading byte decodes as field 13 with
# wire type 6, which protobuf has no such thing as.
GARBAGE = b"not a serialized trace event"


# --------------------------------------------------------------------------
# Fixtures for what the runtime actually puts on the topic


def a_trace_event(
    *,
    entity_key: bytes = b"key-1",
    seq: int = 1,
    step_index: int = 0,
    event_type: TraceEvent.EventType = TraceEvent.LLM_CALL,
    model: str = "fake/echo",
) -> TraceEvent:
    """One event with the identity the runtime derives, not a hand-made shape."""
    return TraceEvent(
        trace_id=trace_id_for(entity_key, seq),
        span_id=span_id_for(entity_key, seq, TraceEvent.EventType.Name(event_type), step_index),
        parent_span_id=span_id_for(entity_key, seq, "ACTIVATION", 0),
        entity_key=entity_key,
        seq=seq,
        step_index=step_index,
        event_type=event_type,
        attributes={"gen_ai.request.model": model},
        # Zero-width by design (add-trace-events D7): both ends are one clock read.
        start_ms=NOW_MS,
        end_ms=NOW_MS,
    )


def encoded(event: TraceEvent) -> bytes:
    """The bytes a `kafka://` traces sink puts on the topic, via the real encoder."""
    _key, payload = serialize_trace_event(event)
    return payload


# --------------------------------------------------------------------------
# The offline seams: a consumer that yields messages, a store that records


@dataclass(frozen=True)
class FakeMessage:
    """The one field of `aiokafka`'s ConsumerRecord this source reads."""

    value: bytes | None


class FakeConsumer:
    """An async-iterable stand-in for `AIOKafkaConsumer`.

    `hold_open` keeps the iterator pending after the scripted messages run out,
    which is what a live topic does — it is how "consumes until cancelled" is
    asserted without a broker.
    """

    def __init__(self, payloads: Iterable[bytes | None], *, hold_open: bool = False) -> None:
        self.messages = [FakeMessage(value=payload) for payload in payloads]
        self.started = 0
        self.stopped = 0
        self.commits = 0
        self.drained = asyncio.Event()
        self._hold_open = hold_open

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def commit(self, offsets: object = None) -> None:
        # Never called: the source commits no offsets. Present so that "it did
        # not commit" is an assertion rather than the absence of a method.
        self.commits += 1

    async def __aiter__(self) -> AsyncIterator[FakeMessage]:
        for message in self.messages:
            yield message
        self.drained.set()
        if self._hold_open:
            await asyncio.Event().wait()


class StoppingConsumer:
    """A consumer whose pending fetch fails once it is stopped.

    What `aiokafka` does: stopping a consumer from another task raises
    `ConsumerStoppedError` out of the `__anext__` the iterator is waiting on,
    rather than ending the stream. `__aiter__`/`__anext__` are spelled out
    (instead of an async generator) so the raising path has no unreachable
    `yield` after it.
    """

    def __init__(self) -> None:
        self.released = asyncio.Event()
        self.started = 0
        self.stopped = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1
        self.released.set()

    def __aiter__(self) -> StoppingConsumer:
        return self

    async def __anext__(self) -> FakeMessage:
        await self.released.wait()
        raise RuntimeError("consumer stopped")


class FakeStore:
    """A `ConsoleStore` stand-in that keeps every batch it was written."""

    def __init__(self) -> None:
        self.batches: list[RecordBatch] = []

    def write(self, batch: RecordBatch) -> int:
        self.batches.append(batch)
        return len(batch)

    @property
    def events(self) -> list[EventRow]:
        return [row for batch in self.batches for row in batch.events]


def as_store(store: FakeStore) -> ConsoleStore:
    """Present the fake under the annotated type (unit 1 owns the real one)."""
    return cast("ConsoleStore", store)


def fake_normalize(
    *,
    events: Sequence[TraceEvent] = (),
    errors: Sequence[ActivationErrorRecord] = (),
    snapshots: Sequence[StateSnapshot] = (),
    provenance: str,
) -> RecordBatch:
    """Stand in for the real normalizer, producing the real row vocabulary."""
    assert not errors and not snapshots  # this source decodes traces only
    return RecordBatch(
        events=tuple(
            EventRow(
                trace_id=event.trace_id.hex(),
                span_id=event.span_id.hex(),
                parent_span_id=event.parent_span_id.hex(),
                entity_key=event.entity_key.hex(),
                seq=event.seq,
                step_index=event.step_index,
                event_type=TraceEvent.EventType.Name(event.event_type),
                start_ms=event.start_ms,
                end_ms=event.end_ms,
                attributes=dict(event.attributes),
                provenance=provenance,
            )
            for event in events
        )
    )


@pytest.fixture(autouse=True)
def _normalizer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_ingest, "normalize", fake_normalize)


class RecordingConsumer:
    """Records the kwargs `KafkaTraceSource` constructs a real consumer with."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


def install_fake_aiokafka(monkeypatch: pytest.MonkeyPatch) -> list[RecordingConsumer]:
    """Put a fake `aiokafka` on `sys.modules` and return the consumers built."""
    created: list[RecordingConsumer] = []

    def factory(*args: Any, **kwargs: Any) -> RecordingConsumer:
        consumer = RecordingConsumer(*args, **kwargs)
        created.append(consumer)
        return consumer

    module = types.ModuleType("aiokafka")
    module.AIOKafkaConsumer = factory  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aiokafka", module)
    return created


async def drain(source: KafkaTraceSource, consumer: FakeConsumer) -> None:
    """Run until the scripted messages are consumed, then stop."""
    task = asyncio.create_task(source.run())
    try:
        await asyncio.wait_for(consumer.drained.wait(), timeout=5.0)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await source.stop()


# --------------------------------------------------------------------------
# Scenario: Traces on an existing topic appear in the console


async def test_traces_on_an_existing_topic_appear_in_the_console() -> None:
    events = [
        a_trace_event(event_type=TraceEvent.ACTIVATION_START),
        a_trace_event(event_type=TraceEvent.LLM_CALL, step_index=1),
        a_trace_event(event_type=TraceEvent.ACTIVATION_END, step_index=2),
    ]
    consumer = FakeConsumer([encoded(event) for event in events])
    store = FakeStore()

    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)
    await drain(source, consumer)

    assert source.records_stored == 3
    assert source.decode_failures == 0
    stored = store.events
    assert [row.event_type for row in stored] == [
        "ACTIVATION_START",
        "LLM_CALL",
        "ACTIVATION_END",
    ]
    assert [row.trace_id for row in stored] == [event.trace_id.hex() for event in events]
    assert stored[1].attributes["gen_ai.request.model"] == "fake/echo"


async def test_stored_records_carry_kafka_provenance() -> None:
    consumer = FakeConsumer([encoded(a_trace_event())])
    store = FakeStore()

    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)
    await drain(source, consumer)

    assert [row.provenance for row in store.events] == [PROVENANCE_KAFKA]


async def test_each_stored_batch_reaches_the_on_batch_callback() -> None:
    seen: list[RecordBatch] = []
    consumer = FakeConsumer([encoded(a_trace_event()), GARBAGE, encoded(a_trace_event(seq=2))])
    store = FakeStore()

    source = KafkaTraceSource(
        f"kafka://{BROKERS}/traces", as_store(store), on_batch=seen.append, consumer=consumer
    )
    await drain(source, consumer)

    # One callback per stored batch, and none for the message that did not decode.
    assert len(seen) == 2
    assert seen == store.batches


# --------------------------------------------------------------------------
# Scenario: An undecodable message does not stop the consumer


async def test_an_undecodable_message_does_not_stop_the_consumer() -> None:
    before = a_trace_event(seq=1)
    after = a_trace_event(seq=2)
    consumer = FakeConsumer([encoded(before), GARBAGE, encoded(after)])
    store = FakeStore()

    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)
    await drain(source, consumer)

    assert source.decode_failures == 1
    assert source.records_stored == 2
    # The message *after* the bad one is what proves the consumer kept going.
    assert [row.trace_id for row in store.events] == [
        before.trace_id.hex(),
        after.trace_id.hex(),
    ]


async def test_a_message_that_decodes_to_no_identity_is_counted_and_skipped() -> None:
    # This one parses cleanly — protobuf is happy with any field subset — so
    # "it parsed" is not enough. Without a trace/span id the row could never be
    # reached through the store's dedup key, and the runtime never emits one.
    identityless = TraceEvent(seq=3, start_ms=NOW_MS).SerializeToString()
    consumer = FakeConsumer([identityless, encoded(a_trace_event())])
    store = FakeStore()

    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)
    await drain(source, consumer)

    assert source.decode_failures == 1
    assert source.records_stored == 1


@pytest.mark.parametrize("payload", [None, b""])
async def test_a_message_with_no_value_is_counted_and_skipped(payload: bytes | None) -> None:
    # A tombstone (`None`) and an empty frame: neither is a trace event, and
    # `TraceEvent().ParseFromString(b"")` succeeds, so neither can be left to
    # the parser to reject.
    consumer = FakeConsumer([payload, encoded(a_trace_event())])
    store = FakeStore()

    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)
    await drain(source, consumer)

    assert source.decode_failures == 1
    assert source.records_stored == 1


# --------------------------------------------------------------------------
# The two deliberate defaults: read from the end, commit nothing


def test_reading_from_the_end_is_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake_aiokafka(monkeypatch)

    KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()))

    assert created[0].kwargs["auto_offset_reset"] == "latest"


def test_from_beginning_reads_the_retained_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake_aiokafka(monkeypatch)

    KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()), from_beginning=True)

    assert created[0].kwargs["auto_offset_reset"] == "earliest"


def test_no_consumer_group_and_no_committed_offsets(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake_aiokafka(monkeypatch)

    KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()))

    # No group to rebalance on restart, and nothing committed to starve a
    # second console watching the same topic.
    assert created[0].kwargs["group_id"] is None
    assert created[0].kwargs["enable_auto_commit"] is False


def test_the_topic_and_brokers_come_from_the_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake_aiokafka(monkeypatch)

    KafkaTraceSource("kafka://broker-a:9092,broker-b:9092/agent-traces", as_store(FakeStore()))

    assert created[0].args == ("agent-traces",)
    assert created[0].kwargs["bootstrap_servers"] == "broker-a:9092,broker-b:9092"


def test_client_options_reach_the_consumer(monkeypatch: pytest.MonkeyPatch) -> None:
    created = install_fake_aiokafka(monkeypatch)

    KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()), security_protocol="SSL")

    assert created[0].kwargs["security_protocol"] == "SSL"


async def test_the_source_never_commits_an_offset() -> None:
    consumer = FakeConsumer([encoded(a_trace_event())])
    store = FakeStore()

    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)
    await drain(source, consumer)

    assert consumer.commits == 0


# --------------------------------------------------------------------------
# Lifecycle


async def test_run_consumes_until_cancelled() -> None:
    consumer = FakeConsumer([encoded(a_trace_event())], hold_open=True)
    store = FakeStore()
    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(store), consumer=consumer)

    task = asyncio.create_task(source.run())
    await asyncio.wait_for(consumer.drained.wait(), timeout=5.0)
    # Still running with nothing left to read, exactly as it would sit on a
    # quiet topic waiting for the pipeline's next activation.
    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert source.records_stored == 1
    assert consumer.started == 1
    assert consumer.stopped == 1


async def test_stopping_from_another_task_shuts_run_down_cleanly() -> None:
    consumer = StoppingConsumer()
    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()), consumer=consumer)

    task = asyncio.create_task(source.run())
    await asyncio.sleep(0)  # let `run()` reach the iterator
    await source.stop()

    # Returns rather than raising: a client torn down under a pending fetch is
    # what shutdown looks like, not a failure a viewer should report.
    await asyncio.wait_for(task, timeout=5.0)
    assert consumer.stopped == 1


async def test_a_client_error_while_running_is_not_swallowed() -> None:
    consumer = StoppingConsumer()
    consumer.released.set()  # fails on the first fetch, with no stop() in sight
    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()), consumer=consumer)

    with pytest.raises(RuntimeError, match="consumer stopped"):
        await source.run()

    # Still released its connections on the way out.
    assert consumer.stopped == 1


async def test_stop_is_idempotent() -> None:
    consumer = FakeConsumer([])
    source = KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()), consumer=consumer)

    await source.stop()
    await source.stop()

    assert consumer.stopped == 1


async def test_counters_start_at_zero() -> None:
    source = KafkaTraceSource(
        f"kafka://{BROKERS}/traces", as_store(FakeStore()), consumer=FakeConsumer([])
    )

    assert source.records_stored == 0
    assert source.decode_failures == 0


# --------------------------------------------------------------------------
# Configuration errors, and the missing client


def test_a_missing_client_names_the_console_ingest_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    # Forced absence rather than assumed absence: this must hold in the
    # integration lane too, where aiokafka *is* installed.
    monkeypatch.setitem(sys.modules, "aiokafka", None)

    with pytest.raises(ImportError) as exc_info:
        KafkaTraceSource(f"kafka://{BROKERS}/traces", as_store(FakeStore()))

    message = str(exc_info.value)
    assert "aiokafka" in message
    assert EXTRA_NAME in message
    assert f"beam-agents[{EXTRA_NAME}]" in message


@pytest.mark.parametrize(
    "uri",
    [
        "kafka://localhost:19092",  # no topic
        "kafka:///traces",  # no brokers
        "kafka://localhost:19092/a/b",  # two path segments
        "pubsub://project/traces",  # another transport's scheme
        "not-a-uri",
    ],
)
def test_a_malformed_uri_is_rejected_naming_the_value(uri: str) -> None:
    with pytest.raises(ValueError, match="kafka"):
        KafkaTraceSource(uri, as_store(FakeStore()), consumer=FakeConsumer([]))


def test_a_malformed_uri_is_rejected_before_the_client_is_imported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The URI is the user's typo; the extra is the environment's problem. With
    # both wrong, the URI is what gets reported — `pytest.raises(ValueError)`
    # fails outright on the ImportError the unparsed path would raise.
    monkeypatch.setitem(sys.modules, "aiokafka", None)

    with pytest.raises(ValueError, match="malformed kafka URI"):
        KafkaTraceSource("kafka://localhost:19092", as_store(FakeStore()))


def test_credentials_in_a_malformed_uri_are_not_echoed() -> None:
    with pytest.raises(ValueError) as exc_info:
        KafkaTraceSource("kafka://user:hunter2@", as_store(FakeStore()))

    assert "hunter2" not in str(exc_info.value)


# --------------------------------------------------------------------------
# The same scenario against a real broker


@pytest.mark.integration
@pytest.mark.slow
async def test_traces_on_a_live_topic_appear_in_the_console() -> None:
    """Scenario: traces on an existing topic appear in the console (real broker).

    Requires `make compose-up-core` (Redpanda on localhost:19092). The topic is
    written first and the source started after, which is the adoption story:
    the pipeline is already running and unmodified when the console appears.
    """
    aiokafka = pytest.importorskip("aiokafka")
    topic = f"beam-agents-console-traces-{uuid.uuid4().hex[:8]}"
    entity_key = uuid.uuid4().bytes
    events = [
        a_trace_event(entity_key=entity_key, event_type=TraceEvent.ACTIVATION_START),
        a_trace_event(entity_key=entity_key, event_type=TraceEvent.LLM_CALL, step_index=1),
        a_trace_event(entity_key=entity_key, event_type=TraceEvent.ACTIVATION_END, step_index=2),
    ]

    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=BROKERS)
    await producer.start()
    try:
        for event in events[:1]:
            key, payload = serialize_trace_event(event)
            await producer.send_and_wait(topic, payload, key=key)
        # A message no decoder can make sense of, in the middle of the run.
        await producer.send_and_wait(topic, GARBAGE, key=entity_key)
        for event in events[1:]:
            key, payload = serialize_trace_event(event)
            await producer.send_and_wait(topic, payload, key=key)
    finally:
        await producer.stop()

    store = FakeStore()
    # `from_beginning` because the records predate the console, which is the
    # whole point of reading a topic somebody else is already writing.
    source = KafkaTraceSource(f"kafka://{BROKERS}/{topic}", as_store(store), from_beginning=True)
    task = asyncio.create_task(source.run())
    try:
        deadline = asyncio.get_running_loop().time() + 20.0
        while source.records_stored < len(events):
            assert asyncio.get_running_loop().time() < deadline, (
                f"stored {source.records_stored} of {len(events)} events"
            )
            await asyncio.sleep(0.2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await source.stop()

    assert source.decode_failures == 1
    assert [row.event_type for row in store.events] == [
        "ACTIVATION_START",
        "LLM_CALL",
        "ACTIVATION_END",
    ]
    assert {row.entity_key for row in store.events} == {entity_key.hex()}
