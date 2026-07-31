"""Transport and store adapter wiring, offline.

The Kafka/Pub/Sub/Redis/Bigtable adapters are exercised end-to-end in the
integration tier, but their *wiring* — the offset arithmetic, the ordering-key
derivation, the value framing, the filter shapes — is small, easy to get
subtly wrong, and cheap to pin without a broker. A wrong commit offset silently
skips or replays an intent; a missing ordering key silently loses per-key
order. Neither shows up as an exception.

Client libraries are replaced in ``sys.modules`` so these run in the offline
lane, where none of them is installed.
"""

from __future__ import annotations

import asyncio
import sys
import time
import types
from typing import Any

import pytest

from beam_agents._protos import ToolIntent, ToolResult
from beam_agents.effector.dedup import (
    BigtableDedupStore,
    Claimed,
    Done,
    InFlight,
    InMemoryDedupStore,
    RedisDedupStore,
    _encode_lease_expiry,
    build_dedup_store,
)
from beam_agents.effector.service import _wall_clock_ms
from beam_agents.effector.sinks import (
    InMemoryMessageSink,
    InMemoryResultSink,
    ProtoResultSink,
    build_message_sink,
    build_result_sink,
)
from beam_agents.effector.sources import (
    DeliveredIntent,
    KafkaIntentSource,
    PubSubIntentSource,
    build_intent_source,
)

NOW_MS = 1_700_000_000_000


def an_intent(intent_id: str = "intent-1") -> ToolIntent:
    return ToolIntent(
        intent_id=intent_id,
        entity_key=b"customer-7",
        seq=3,
        tool_name="charge",
        args_json="{}",
        expires_at_ms=NOW_MS + 1_000,
    )


def a_result() -> ToolResult:
    return ToolResult(
        intent_id="intent-1", entity_key=b"customer-7", seq=3, status=ToolResult.OK, payload=b"1"
    )


# -- Kafka ---------------------------------------------------------------------


class _FakeConsumer:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.subscribed: tuple[list[str], object] | None = None
        self.committed: list[dict[object, int]] = []
        self.messages: list[object] = []

    async def start(self) -> None:
        self.started = True

    def subscribe(self, topics: list[str], listener: object = None) -> None:
        self.subscribed = (topics, listener)

    async def commit(self, offsets: dict[object, int]) -> None:
        self.committed.append(offsets)

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self) -> object:
        async def _gen() -> object:
            for message in self.messages:
                yield message

        return _gen()


class _FakeProducer:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = 0
        self.stopped = 0
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def start(self) -> None:
        self.started += 1

    async def send_and_wait(self, topic: str, *, value: bytes, key: bytes) -> None:
        self.sent.append((topic, value, key))

    async def stop(self) -> None:
        self.stopped += 1


class _FakeTopicPartition:
    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition

    def __eq__(self, other: object) -> bool:
        return (self.topic, self.partition) == (other.topic, other.partition)  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))


class _FakeMessage:
    def __init__(
        self, value: bytes, topic: str, partition: int, offset: int, key: bytes | None = None
    ) -> None:
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset
        # Kafka messages carry a key; the source keeps it (with the raw value)
        # so an unverifiable delivery can be dead-lettered exactly as it arrived.
        self.key = key


@pytest.fixture
def fake_aiokafka(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    module = types.ModuleType("aiokafka")
    module.AIOKafkaConsumer = _FakeConsumer  # type: ignore[attr-defined]
    module.AIOKafkaProducer = _FakeProducer  # type: ignore[attr-defined]
    module.TopicPartition = _FakeTopicPartition  # type: ignore[attr-defined]
    module.ConsumerRebalanceListener = object  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aiokafka", module)
    return module


async def test_the_kafka_source_consumes_through_a_group_with_manual_commit(
    fake_aiokafka: types.ModuleType,
) -> None:
    source = KafkaIntentSource("broker:9092", "intents", "effector")
    consumer: Any = source._consumer

    assert consumer.kwargs["group_id"] == "effector"
    # Auto-commit would advance the offset before the result is published,
    # which is exactly the loss this design forbids.
    assert consumer.kwargs["enable_auto_commit"] is False
    assert consumer.kwargs["auto_offset_reset"] == "earliest"

    await source.start()
    assert consumer.started
    assert consumer.subscribed is not None
    assert consumer.subscribed[0] == ["intents"]


async def test_the_kafka_source_yields_partition_tagged_deliveries(
    fake_aiokafka: types.ModuleType,
) -> None:
    source = KafkaIntentSource("broker:9092", "intents", "effector")
    consumer: Any = source._consumer
    intent = an_intent()
    consumer.messages = [
        _FakeMessage(intent.SerializeToString(deterministic=True), "intents", 2, 41)
    ]

    delivered = [d async for d in source]

    assert len(delivered) == 1
    assert delivered[0].intent.intent_id == "intent-1"
    assert delivered[0].partition == "intents:2"
    assert delivered[0].handle == ("intents", 2, 41)


async def test_the_kafka_source_commits_the_next_offset_to_read(
    fake_aiokafka: types.ModuleType,
) -> None:
    # Kafka's committed offset is the *next* record to read, so committing the
    # processed offset verbatim would replay it forever.
    source = KafkaIntentSource("broker:9092", "intents", "effector")
    consumer: Any = source._consumer

    await source.commit(
        DeliveredIntent(intent=an_intent(), partition="intents:2", handle=("intents", 2, 41))
    )

    assert consumer.committed == [{_FakeTopicPartition("intents", 2): 42}]


async def test_the_kafka_sink_publishes_keyed_and_durable(
    fake_aiokafka: types.ModuleType,
) -> None:
    sink = build_message_sink("kafka", ("broker:9092", "results"))
    producer: Any = sink._producer  # type: ignore[attr-defined]

    await sink.publish(b"customer-7", b"payload")
    await sink.publish(b"customer-7", b"payload-2")

    # Idempotent producer, started once, and `send_and_wait` (not fire and
    # forget) so the publish is durable before the offset is committed.
    assert producer.kwargs["enable_idempotence"] is True
    assert producer.started == 1
    assert producer.sent == [
        ("results", b"payload", b"customer-7"),
        ("results", b"payload-2", b"customer-7"),
    ]

    await sink.close()
    assert producer.stopped == 1


async def test_the_proto_result_sink_keys_results_by_entity_key(
    fake_aiokafka: types.ModuleType,
) -> None:
    sink = build_result_sink("kafka", ("broker:9092", "results"))
    assert isinstance(sink, ProtoResultSink)
    result = a_result()

    await sink.publish(result)

    producer: Any = sink.inner._producer  # type: ignore[attr-defined]
    _topic, payload, key = producer.sent[0]
    assert key == b"customer-7"
    assert payload == result.SerializeToString(deterministic=True)


async def test_a_kafka_rebalance_hands_revoked_partitions_to_the_handler(
    fake_aiokafka: types.ModuleType,
) -> None:
    # This listener is what triggers claim release in production: without it a
    # reassigned partition's claim sits until its lease expires.
    source = KafkaIntentSource("broker:9092", "intents", "effector")
    revoked: list[str] = []
    source.set_revocation_handler(lambda partition: _record(revoked, partition))
    await source.start()
    consumer: Any = source._consumer
    listener = consumer.subscribed[1]

    await listener.on_partitions_revoked(
        [_FakeTopicPartition("intents", 2), _FakeTopicPartition("intents", 5)]
    )
    # Assignment is a no-op: a newly assigned partition has no claim to reclaim.
    assert await listener.on_partitions_assigned([_FakeTopicPartition("intents", 2)]) is None

    assert revoked == ["intents:2", "intents:5"]


async def _record(sink: list[str], partition: str) -> None:
    sink.append(partition)


async def test_a_rebalance_without_a_handler_is_harmless(
    fake_aiokafka: types.ModuleType,
) -> None:
    source = KafkaIntentSource("broker:9092", "intents", "effector")
    await source.start()
    consumer: Any = source._consumer

    assert await consumer.subscribed[1].on_partitions_revoked([_FakeTopicPartition("t", 0)]) is None


async def test_closing_the_kafka_source_stops_the_consumer(
    fake_aiokafka: types.ModuleType,
) -> None:
    source = KafkaIntentSource("broker:9092", "intents", "effector")

    await source.close()

    consumer: Any = source._consumer
    assert consumer.stopped


def test_the_source_builder_dispatches_on_scheme(fake_aiokafka: types.ModuleType) -> None:
    source = build_intent_source("kafka", ("broker:9092", "intents"), consumer_group="g")
    assert isinstance(source, KafkaIntentSource)


# -- Pub/Sub -------------------------------------------------------------------


class _FakePublisher:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.published: list[tuple[str, bytes, str]] = []
        self.stopped = False

    def topic_path(self, project: str, topic: str) -> str:
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic: str, payload: bytes, ordering_key: str = "") -> object:
        self.published.append((topic, payload, ordering_key))

        class _Future:
            def result(self) -> None:
                return None

        return _Future()

    def stop(self) -> None:
        self.stopped = True


class _FakeSubscriber:
    def __init__(self, **kwargs: object) -> None:
        self.subscribed: tuple[str, object] | None = None
        self.closed = False
        self.subscription = types.SimpleNamespace(enable_message_ordering=False)

    def subscription_path(self, project: str, subscription: str) -> str:
        return f"projects/{project}/subscriptions/{subscription}"

    def get_subscription(self, subscription: str) -> object:
        return self.subscription

    def subscribe(self, path: str, callback: object) -> object:
        self.subscribed = (path, callback)
        return types.SimpleNamespace(cancel=lambda: None)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_pubsub(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    module = types.ModuleType("pubsub_v1")
    module.PublisherClient = _FakePublisher  # type: ignore[attr-defined]
    module.SubscriberClient = _FakeSubscriber  # type: ignore[attr-defined]
    module.types = types.SimpleNamespace(  # type: ignore[attr-defined]
        PublisherOptions=lambda **kwargs: kwargs
    )
    cloud = types.ModuleType("google.cloud")
    cloud.pubsub_v1 = module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.pubsub_v1", module)
    monkeypatch.setitem(
        sys.modules,
        "google.cloud.pubsub_v1.types",
        types.SimpleNamespace(PublisherOptions=lambda **kwargs: kwargs),
    )
    return module


async def test_the_pubsub_sink_publishes_with_an_ordering_key(
    fake_pubsub: types.ModuleType,
) -> None:
    # The ordering key is the hex of entity_key, matching WriteIntents on the
    # intents topic — without it Pub/Sub delivers a key's messages unordered.
    sink = build_message_sink("pubsub", ("proj", "results"))

    await sink.publish(b"customer-7", b"payload")

    client: Any = sink._client  # type: ignore[attr-defined]
    assert client.kwargs["publisher_options"] == {"enable_message_ordering": True}
    assert client.published == [("projects/proj/topics/results", b"payload", b"customer-7".hex())]


async def test_the_pubsub_source_warns_when_ordering_is_disabled(
    fake_pubsub: types.ModuleType, caplog: pytest.LogCaptureFixture
) -> None:
    # Ordered delivery is a subscription-side precondition the effector cannot
    # set; the least it can do is say so loudly at startup.
    source = PubSubIntentSource("proj", "sub")

    await source.start()

    assert "message ordering" in caplog.text
    client: Any = source._client
    assert client.subscribed is not None
    assert client.subscribed[0] == "projects/proj/subscriptions/sub"


async def test_the_pubsub_source_acks_only_on_commit(fake_pubsub: types.ModuleType) -> None:
    source = PubSubIntentSource("proj", "sub")
    acked: list[str] = []
    message = types.SimpleNamespace(ack=lambda: acked.append("acked"))

    await source.commit(DeliveredIntent(intent=an_intent(), partition="customer-7", handle=message))

    assert acked == ["acked"]


async def test_the_pubsub_source_has_no_partitions_to_revoke(
    fake_pubsub: types.ModuleType,
) -> None:
    # Pub/Sub has no assignment to lose: a redelivery simply lands wherever the
    # ordering key goes next, so there is no claim to hand back on rebalance.
    source = PubSubIntentSource("proj", "sub")

    calls: list[str] = []
    source.set_revocation_handler(lambda partition: _record(calls, partition))

    # The handler is accepted (the protocol requires the method) and dropped:
    # there is no assignment to lose, so nothing can revoke.
    assert not hasattr(source, "_revocation_handler")
    assert calls == []


async def test_the_pubsub_source_yields_deliveries_partitioned_by_ordering_key(
    fake_pubsub: types.ModuleType,
) -> None:
    # The ordering key *is* the partition: it is the unit Pub/Sub sequences, so
    # it is the unit the service must process one at a time.
    source = PubSubIntentSource("proj", "sub")
    client: Any = source._client
    client.subscription.enable_message_ordering = True
    await source.start()
    intent = an_intent()
    callback = client.subscribed[1]

    # The client delivers on its own thread, and the callback blocks that
    # thread until the item is queued — that block is the backpressure. Calling
    # it from the loop thread would deadlock, which is exactly why production
    # never does.
    message = types.SimpleNamespace(
        data=intent.SerializeToString(deterministic=True), ordering_key="customer-7"
    )
    feeding = asyncio.create_task(asyncio.to_thread(callback, message))
    delivered = await anext(aiter(source))
    await feeding

    assert delivered.intent.intent_id == "intent-1"
    assert delivered.partition == "customer-7"


async def test_an_unreadable_subscription_does_not_stop_startup(
    fake_pubsub: types.ModuleType, caplog: pytest.LogCaptureFixture
) -> None:
    # A missing `get` permission is a common, survivable deployment gap: warn
    # about what cannot be checked rather than refusing to run.
    source = PubSubIntentSource("proj", "sub")
    client: Any = source._client

    def _denied(subscription: str) -> object:
        raise PermissionError("no access")

    client.get_subscription = _denied

    await source.start()

    assert client.subscribed is not None


async def test_closing_the_pubsub_source_cancels_the_stream(
    fake_pubsub: types.ModuleType,
) -> None:
    source = PubSubIntentSource("proj", "sub")
    await source.start()

    await source.close()

    client: Any = source._client
    assert client.closed


async def test_closing_the_pubsub_sink_stops_the_publisher(
    fake_pubsub: types.ModuleType,
) -> None:
    sink = build_message_sink("pubsub", ("proj", "results"))

    await sink.close()

    client: Any = sink._client  # type: ignore[attr-defined]
    assert client.stopped


# -- Redis ---------------------------------------------------------------------


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.set_calls: list[dict[str, object]] = []
        self.script_calls: list[tuple[str, list[str], list[object]]] = []
        self.closed = False

    async def set(self, key: str, value: bytes, nx: bool = False, px: int = 0) -> bool:
        self.set_calls.append({"key": key, "value": value, "nx": nx, "px": px})
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    def register_script(self, script: str) -> object:
        async def _run(keys: list[str], args: list[object]) -> int:
            self.script_calls.append((script, keys, args))
            return 1

        return _run

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    client = _FakeRedis()
    asyncio_module = types.SimpleNamespace(from_url=lambda uri: client)
    module = types.ModuleType("redis")
    module.asyncio = asyncio_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "redis", module)
    monkeypatch.setitem(sys.modules, "redis.asyncio", asyncio_module)
    return client


async def test_the_redis_store_claims_with_set_nx_px(fake_redis: _FakeRedis) -> None:
    store = RedisDedupStore("redis://localhost:6379")

    outcome = await store.claim("intent-1", 5_000)

    assert isinstance(outcome, Claimed)
    call = fake_redis.set_calls[0]
    assert call["key"] == "beam-agents:intent:intent-1"
    assert call["nx"] is True
    # Lease expiry is Redis's own, not a client-side clock.
    assert call["px"] == 5_000
    assert call["value"] == b"C" + outcome.token.encode()


async def test_the_redis_store_distinguishes_in_flight_from_done_by_tag(
    fake_redis: _FakeRedis,
) -> None:
    store = RedisDedupStore("redis://localhost:6379")
    await store.claim("intent-1", 5_000)

    assert isinstance(await store.claim("intent-1", 5_000), InFlight)

    result = a_result()
    fake_redis.values["beam-agents:intent:intent-1"] = b"D" + result.SerializeToString(
        deterministic=True
    )
    outcome = await store.claim("intent-1", 5_000)

    assert isinstance(outcome, Done)
    assert outcome.result is not None
    assert outcome.result.intent_id == "intent-1"


async def test_a_record_vanishing_between_set_and_get_reads_as_in_flight(
    fake_redis: _FakeRedis,
) -> None:
    # Racing with expiry must produce the *waiting* outcome, never a skip.
    class _VanishingRedis(_FakeRedis):
        async def set(self, key: str, value: bytes, nx: bool = False, px: int = 0) -> bool:
            return False

        async def get(self, key: str) -> bytes | None:
            return None

    store = RedisDedupStore("redis://localhost:6379")
    # Set through __dict__: the attribute's declared type comes from the redis
    # client, which is installed in the integration lane and absent in the
    # offline one, so a plain assignment type-checks differently in each.
    store.__dict__["_redis"] = _VanishingRedis()

    assert isinstance(await store.claim("intent-1", 5_000), InFlight)


async def test_redis_completion_and_release_go_through_owner_checked_scripts(
    fake_redis: _FakeRedis,
) -> None:
    store = RedisDedupStore("redis://localhost:6379")
    result = a_result()

    assert await store.complete("intent-1", "tok", result, 60_000)
    assert await store.release("intent-1", "tok")
    await store.close()

    complete_script, keys, args = fake_redis.script_calls[0]
    assert "GET" in complete_script and "SET" in complete_script
    assert keys == ["beam-agents:intent:intent-1"]
    # The compare value is the caller's claim, so a stale owner cannot write.
    assert args[0] == b"Ctok"
    assert args[1] == b"D" + result.SerializeToString(deterministic=True)
    assert args[2] == 60_000

    release_script, _, release_args = fake_redis.script_calls[1]
    assert "DEL" in release_script
    assert release_args == [b"Ctok"]
    assert fake_redis.closed


async def test_a_routed_approval_is_framed_as_done_with_no_result(
    fake_redis: _FakeRedis,
) -> None:
    store = RedisDedupStore("redis://localhost:6379")

    await store.complete("intent-1", "tok", None, 60_000)

    _, _, args = fake_redis.script_calls[0]
    assert args[1] == b"D"


def test_the_dedup_builder_dispatches_on_scheme(fake_redis: _FakeRedis) -> None:
    assert isinstance(build_dedup_store("redis", ("redis://localhost:6379",)), RedisDedupStore)


# -- Bigtable ------------------------------------------------------------------


class _FakeTable:
    def __init__(self) -> None:
        self.checks: list[tuple[bytes, object, object, object]] = []
        self.predicate_result = False
        self.rows: list[object] = []

    async def check_and_mutate_row(
        self,
        row_key: bytes,
        predicate: object,
        *,
        true_case_mutations: object = None,
        false_case_mutations: object = None,
    ) -> bool:
        self.checks.append((row_key, predicate, true_case_mutations, false_case_mutations))
        return self.predicate_result

    async def read_rows(self, query: object) -> list[object]:
        return self.rows


class _FakeBigtableClient:
    def __init__(self, project: str) -> None:
        self.project = project
        self.table = _FakeTable()
        self.closed = False

    def get_table(self, instance: str, table: str) -> _FakeTable:
        return self.table

    async def close(self) -> None:
        self.closed = True


class _Recorder:
    """Records constructor arguments so filter/mutation shapes are assertable."""

    def __init__(self, name: str, calls: list[tuple[str, tuple[object, ...], dict[str, object]]]):
        self._name = name
        self._calls = calls

    def __call__(self, *args: object, **kwargs: object) -> object:
        self._calls.append((self._name, args, kwargs))
        return types.SimpleNamespace(name=self._name, args=args, kwargs=kwargs)


@pytest.fixture
def fake_bigtable(monkeypatch: pytest.MonkeyPatch) -> tuple[_FakeBigtableClient, list[object]]:
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    holder: dict[str, _FakeBigtableClient] = {}

    def _client(project: str) -> _FakeBigtableClient:
        holder["client"] = _FakeBigtableClient(project)
        return holder["client"]

    row_filters = types.SimpleNamespace(
        **{
            name: _Recorder(name, calls)
            for name in (
                "RowFilterChain",
                "RowFilterUnion",
                "FamilyNameRegexFilter",
                "ColumnQualifierRegexFilter",
                "ValueRangeFilter",
                "ValueRegexFilter",
                "CellsColumnLimitFilter",
            )
        }
    )
    data = types.ModuleType("google.cloud.bigtable.data")
    data.BigtableDataClientAsync = _client  # type: ignore[attr-defined]
    data.SetCell = _Recorder("SetCell", calls)  # type: ignore[attr-defined]
    data.DeleteRangeFromColumn = _Recorder("DeleteRangeFromColumn", calls)  # type: ignore[attr-defined]
    data.ReadRowsQuery = _Recorder("ReadRowsQuery", calls)  # type: ignore[attr-defined]
    data.row_filters = row_filters  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google.cloud.bigtable.data", data)
    monkeypatch.setitem(sys.modules, "google.cloud.bigtable.data.row_filters", row_filters)
    return holder, calls  # type: ignore[return-value]


async def test_the_bigtable_store_claims_with_one_conditional_mutation(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    holder, calls = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    table = holder["client"].table

    outcome = await store.claim("intent-1", 5_000)

    assert isinstance(outcome, Claimed)
    row_key, _, true_case, false_case = table.checks[0]
    assert row_key == b"intent-1"
    # Nothing is written when the row is already claimed or terminal; the claim
    # is written only on the false branch, so a completed row never collects a
    # stray claim cell.
    assert true_case is None
    assert len(false_case) == 2
    set_calls = [c for c in calls if c[0] == "SetCell"]
    assert set_calls[0][1] == ("d", b"claim", _encode_lease_expiry(NOW_MS + 5_000))
    assert set_calls[1][1] == ("d", b"owner", outcome.token.encode())


async def test_the_bigtable_lease_predicate_is_an_exclusive_value_range(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    _, calls = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)

    await store.claim("intent-1", 5_000)

    # Two expiry predicates now: the claim's lease and the terminal record's
    # `rexp`. Both are the same exclusive value range, so a lease *or* a result
    # expiring exactly now reads as expired — matching the in-memory store and
    # hitl.intent_expired.
    range_calls = [c for c in calls if c[0] == "ValueRangeFilter"]
    assert len(range_calls) == 2
    for range_call in range_calls:
        assert range_call[2]["start_value"] == _encode_lease_expiry(NOW_MS)
        assert range_call[2]["inclusive_start"] is False

    # Every value predicate is pinned to the latest cell version; a superseded
    # cell must never satisfy one.
    assert [c[1] for c in calls if c[0] == "CellsColumnLimitFilter"] == [(1,), (1,)]


async def test_the_bigtable_ownership_predicate_matches_the_token_column_exactly(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    # The token lives in its own column so the predicate is an exact match on
    # ASCII hex, never a regex over the binary lease prefix (RE2's `.` does not
    # match a 0x0A byte).
    holder, calls = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    holder["client"].table.predicate_result = True

    assert await store.complete("intent-1", "deadbeef", a_result(), 60_000)

    (regex_call,) = [c for c in calls if c[0] == "ValueRegexFilter"]
    assert regex_call[1] == (b"deadbeef",)
    qualifiers = [c[1][0] for c in calls if c[0] == "ColumnQualifierRegexFilter"]
    assert b"owner" in qualifiers


async def test_bigtable_completion_clears_the_claim_and_writes_the_result(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    holder, calls = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    table = holder["client"].table
    table.predicate_result = True
    result = a_result()

    assert await store.complete("intent-1", "tok", result, 60_000)

    _, _, true_case, false_case = table.checks[0]
    assert false_case is None
    assert len(true_case) == 4
    sets = [c[1] for c in calls if c[0] == "SetCell"]
    assert sets[0] == ("d", b"result", result.SerializeToString(deterministic=True))
    # The record stamps its own expiry: `rexp` is what every read gates on, so
    # a lagging GC rule can never serve a result past its TTL.
    assert sets[1] == ("d", b"rexp", _encode_lease_expiry(NOW_MS + 60_000))
    deletes = [c[1] for c in calls if c[0] == "DeleteRangeFromColumn"]
    assert deletes == [("d", b"claim"), ("d", b"owner")]


async def test_a_taken_bigtable_row_is_read_back_to_distinguish_done(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    holder, _ = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    table = holder["client"].table
    table.predicate_result = True
    result = a_result()
    table.rows = [
        types.SimpleNamespace(
            cells=[
                types.SimpleNamespace(
                    qualifier=b"result", value=result.SerializeToString(deterministic=True)
                ),
                types.SimpleNamespace(
                    qualifier=b"rexp", value=_encode_lease_expiry(NOW_MS + 60_000)
                ),
            ]
        )
    ]

    outcome = await store.claim("intent-1", 5_000)

    assert isinstance(outcome, Done)
    assert outcome.result is not None
    assert outcome.result.intent_id == "intent-1"


async def test_a_bigtable_row_whose_result_expired_is_not_read_back_as_done(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    # The read-back gates on `rexp`, not on the presence of a result cell. A
    # terminal record past its TTL must not be reported Done just because the
    # GC rule has not gotten around to removing the cell yet.
    holder, _ = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    table = holder["client"].table
    table.predicate_result = True
    table.rows = [
        types.SimpleNamespace(
            cells=[
                types.SimpleNamespace(
                    qualifier=b"result", value=a_result().SerializeToString(deterministic=True)
                ),
                types.SimpleNamespace(qualifier=b"rexp", value=_encode_lease_expiry(NOW_MS)),
            ]
        )
    ]

    outcome = await store.claim("intent-1", 5_000)

    assert isinstance(outcome, InFlight)


async def test_a_taken_bigtable_row_with_no_result_reads_as_in_flight(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    holder, _ = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    table = holder["client"].table
    table.predicate_result = True
    table.rows = [
        types.SimpleNamespace(
            cells=[types.SimpleNamespace(qualifier=b"claim", value=_encode_lease_expiry(NOW_MS))]
        )
    ]

    assert isinstance(await store.claim("intent-1", 5_000), InFlight)


async def test_bigtable_release_clears_only_the_claim_columns(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    holder, calls = fake_bigtable
    store = BigtableDedupStore("proj", "inst", "table", clock=lambda: NOW_MS)
    holder["client"].table.predicate_result = True

    assert await store.release("intent-1", "tok")
    await store.close()

    deletes = [c[1] for c in calls if c[0] == "DeleteRangeFromColumn"]
    assert deletes == [("d", b"claim"), ("d", b"owner")]
    assert holder["client"].closed


def test_the_dedup_builder_dispatches_to_bigtable(
    fake_bigtable: tuple[Any, list[Any]],
) -> None:
    store = build_dedup_store("bigtable", ("proj", "inst", "table"))
    assert isinstance(store, BigtableDedupStore)


# -- in-memory adapters --------------------------------------------------------


async def test_the_in_memory_sinks_record_attempts_and_close() -> None:
    messages = InMemoryMessageSink()
    await messages.publish(b"k", b"v")
    await messages.close()

    assert messages.attempts == 1
    assert messages.closed

    results = InMemoryResultSink()
    await results.publish(a_result())
    await results.close()

    assert results.statuses == [ToolResult.OK]
    assert results.closed


async def test_an_injected_sink_failure_surfaces_to_the_caller() -> None:
    # The `fail` hook is how retry behavior is driven offline; it must actually
    # raise rather than be silently ignored.
    def _boom(attempt: int) -> None:
        raise ConnectionError("broker unavailable")

    sink = InMemoryMessageSink(fail=_boom)

    with pytest.raises(ConnectionError):
        await sink.publish(b"k", b"v")

    assert sink.published == []


async def test_closing_a_never_started_pubsub_source_is_safe(
    fake_pubsub: types.ModuleType,
) -> None:
    # Shutdown runs even when startup failed part-way; closing a source that
    # never subscribed must not raise.
    source = PubSubIntentSource("proj", "sub")

    await source.close()

    client: Any = source._client
    assert client.closed


async def test_closing_a_never_started_kafka_sink_is_safe(
    fake_aiokafka: types.ModuleType,
) -> None:
    sink = build_message_sink("kafka", ("broker:9092", "results"))

    await sink.close()

    producer: Any = sink._producer  # type: ignore[attr-defined]
    assert producer.stopped == 0


def test_the_default_clocks_read_wall_time() -> None:
    # The stores and the service default to wall time; tests inject a clock, so
    # the default path needs its own check.
    now = int(time.time() * 1000)
    assert abs(_wall_clock_ms() - now) < 5_000
    assert abs(InMemoryDedupStore().clock() - now) < 5_000
