"""In-pipeline Kafka publishers for the e2e gate.

Two jobs, one mechanism:

- ``publish_intents`` stands in for the production ``WriteIntents(kafka://…)``
  outbox writer, which is upstream-broken (Beam 2.60-2.72 leaks the Java-native
  ``kafka_write:v2`` urn into the cross-language expansion response — see
  ``tests/actions/test_write_intents_integration.py``) *and* could not run on
  this stack anyway (no Java SDK environment; see docker/README.md). It sets
  the message key exactly as ``WriteIntents`` does — the raw ``entity_key`` —
  and is **deliberately at-least-once**: a deterministic, seeded fraction of
  intents is published twice, manufacturing the duplicate sink writes the gate
  must tolerate (spec: duplicate deliveries never produce a second execution).
- ``publish_tagged`` carries the pipeline's other outputs (.output, .errors,
  traces as needed) to run-scoped topics so the post-run assertions can read
  them from outside the pipeline (design D6). Also at-least-once — Beam bundle
  retries alone can duplicate a publish — so assertions must collapse
  duplicates by identity, never count raw messages.

Duplication is decided by a hash of the record, not by an RNG stream: a
replayed bundle then re-decides identically, so the duplicate schedule is
reproducible from the seed regardless of how Beam retries or splits bundles.

Runs inside the beam-sdk-harness container: imports aiokafka (baked into the
image) and reaches the broker via the compose-internal listener.
"""

from __future__ import annotations

import asyncio
import threading
import zlib
from collections.abc import Coroutine
from typing import Any

import apache_beam as beam

from beam_agents._protos import ToolIntent

# Kept in sync with docker/compose.yaml's `internal` Redpanda listener.
INTERNAL_BROKER = "redpanda:9092"


def duplicate_decision(seed: int, payload: bytes, fraction: float) -> bool:
    """Deterministically decide whether this record is published twice."""
    if fraction <= 0.0:
        return False
    bucket = zlib.crc32(seed.to_bytes(8, "big", signed=True) + payload) % 10_000
    return bucket < int(fraction * 10_000)


class _KafkaPublishDoFn(beam.DoFn):
    """At-least-once publish of ``(key: bytes, value: bytes)`` to one topic."""

    def __init__(
        self, brokers: str, topic: str, *, duplicate_fraction: float = 0.0, seed: int = 0
    ) -> None:
        self._brokers = brokers
        self._topic = topic
        self._fraction = duplicate_fraction
        self._seed = seed
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._producer: Any = None

    # Beam may call process() from a different thread than setup(), and
    # aiokafka resolves the running loop internally — so the producer lives on
    # a dedicated loop thread (same pattern as the runtime's async bridge) and
    # every call crosses via run_coroutine_threadsafe.
    def setup(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name=f"outbox-{self._topic}", daemon=True
        )
        self._thread.start()

    def _call(self, coro: Coroutine[Any, Any, Any]) -> Any:
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=60)

    def _ensure_producer(self) -> None:
        if self._producer is None:
            from aiokafka import AIOKafkaProducer

            async def make() -> Any:
                producer = AIOKafkaProducer(bootstrap_servers=self._brokers)
                await producer.start()
                return producer

            self._producer = self._call(make())

    def process(self, element: tuple[bytes, bytes]) -> None:
        # No outputs: publishing is the effect. A non-generator process
        # returning None is a valid no-output DoFn.
        self._ensure_producer()
        key, value = element
        sends = 2 if duplicate_decision(self._seed, key + value, self._fraction) else 1
        for _ in range(sends):
            self._call(self._producer.send_and_wait(self._topic, key=key, value=value))

    def teardown(self) -> None:
        if self._producer is not None:
            self._call(self._producer.stop())
            self._producer = None
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=10)
            self._loop.close()
            self._loop = None
            self._thread = None


def publish_intents(
    intents: beam.pvalue.PCollection,
    topic: str,
    *,
    brokers: str = INTERNAL_BROKER,
    duplicate_fraction: float,
    seed: int,
) -> beam.pvalue.PCollection:
    """Publish ``KV[bytes, ToolIntent]`` exactly as WriteIntents would, plus dupes."""

    def encode(kv: tuple[bytes, ToolIntent]) -> tuple[bytes, bytes]:
        key, intent = kv
        return key, intent.SerializeToString()

    return (
        intents
        | f"EncodeIntents[{topic}]" >> beam.Map(encode)
        | f"PublishIntents[{topic}]"
        >> beam.ParDo(
            _KafkaPublishDoFn(brokers, topic, duplicate_fraction=duplicate_fraction, seed=seed)
        )
    )


def publish_tagged(
    pcoll: beam.pvalue.PCollection,
    topic: str,
    encode: Any,
    *,
    brokers: str = INTERNAL_BROKER,
) -> beam.pvalue.PCollection:
    """Publish any tagged output through ``encode: element -> (key, value)``."""
    return (
        pcoll
        | f"Encode[{topic}]" >> beam.Map(encode)
        | f"Publish[{topic}]" >> beam.ParDo(_KafkaPublishDoFn(brokers, topic))
    )
