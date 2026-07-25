"""Tests for the `write-intents-sink` capability: `WriteIntents`'s pre-keyed KV
requirement, URI-scheme construction validation, per-key order preservation,
deterministic serialization, dead-letter routing on serialization failure, and
the `RunAgent` sink-resolver integration.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from unittest import mock

import apache_beam as beam
import pytest
from apache_beam.coders.typecoders import registry as coder_registry
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.actions.write_intents import (
    DEAD_LETTER_TAG,
    UnknownIntentsSchemeError,
    WriteIntents,
    _SerializeIntent,
    _validate_kv_input,
)
from beam_agents.core.transform import (
    AgentConfig,
    DefaultSinkResolver,
    RunAgent,
    _KeyedWriteIntents,
)
from tests.core._dofn_helpers import make_pong_provider, suspend_then_complete_agent


@pytest.fixture(autouse=True)
def _restore_coder_registry() -> Iterator[None]:
    """Undo the process-global mutation from ``RunAgent.expand`` (which calls
    ``register_coders()``); see the identical fixture in test_transform.py.
    """
    saved = dict(coder_registry._coders)
    try:
        yield
    finally:
        coder_registry._coders = saved


def _intent(key: bytes, seq: int, tool_name: str = "http.post") -> ToolIntent:
    return ToolIntent(
        intent_id=f"id-{key.decode()}-{seq}", entity_key=key, seq=seq, tool_name=tool_name
    )


def _fake_writer_factory(sink: list[tuple[bytes, bytes]]) -> Callable[[str], beam.PTransform]:
    """Returns a writer_factory that records (key, payload) pairs in-memory.

    Only safe for direct DoFn-level or single-bundle assertions in this test
    module; not a general-purpose fake for cross-process runners.
    """

    def factory(uri: str) -> beam.PTransform:
        class _Recorder(beam.PTransform):
            def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
                return pcoll | beam.Map(sink.append)

        return _Recorder()

    return factory


# --- Requirement: WriteIntents consumes pre-keyed ToolIntent KV input ----------


def test_non_kv_input_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="KV"), BeamTestPipeline() as p:
        (p | beam.Create([_intent(b"k", 0)]) | WriteIntents("kafka://broker:9092/topic"))


@pytest.mark.parametrize("erased_type", [None, beam.typehints.Any])
def test_erased_element_type_is_allowed_through(erased_type: object) -> None:
    with BeamTestPipeline() as p:
        pcoll: beam.pvalue.PCollection = beam.pvalue.PCollection(p, element_type=erased_type)
        _validate_kv_input(pcoll)  # must not raise


def test_keyed_kv_input_flows_through() -> None:
    with BeamTestPipeline() as p:
        keyed = p | beam.Create([(b"k", _intent(b"k", 0))])
        result = keyed | WriteIntents(
            "kafka://broker:9092/topic", writer_factory=_fake_writer_factory([])
        )
        assert_that(result.dead_letter, equal_to([]))


# --- Requirement: WriteIntents dispatches on outbox URI scheme -----------------


def test_kafka_uri_resolves_and_validates() -> None:
    w = WriteIntents("kafka://broker:9092/agent-intents")
    assert w._scheme == "kafka"
    assert w._parts == ("broker:9092", "agent-intents")


def test_pubsub_uri_resolves_and_validates() -> None:
    w = WriteIntents("pubsub://my-project/agent-intents")
    assert w._scheme == "pubsub"
    assert w._parts == ("my-project", "agent-intents")


@pytest.mark.parametrize(
    "uri",
    [
        "sqs://queue/topic",  # unknown scheme
        "ftp://broker/topic",
        "not-a-uri",
    ],
)
def test_unknown_scheme_rejected_at_construction(uri: str) -> None:
    with pytest.raises(UnknownIntentsSchemeError):
        WriteIntents(uri)


@pytest.mark.parametrize(
    "uri",
    [
        "kafka://broker:9092",  # missing topic
        "kafka:///topic",  # missing brokers
        "pubsub://my-project",  # missing topic
        "pubsub:///topic",  # missing project
    ],
)
def test_incomplete_recognized_scheme_rejected_at_construction(uri: str) -> None:
    with pytest.raises(UnknownIntentsSchemeError):
        WriteIntents(uri)


def test_construction_does_not_import_io_clients() -> None:
    poisoned = {"apache_beam.io.kafka": None, "apache_beam.io.gcp.pubsub": None}
    with mock.patch.dict(sys.modules, poisoned):
        WriteIntents("kafka://broker:9092/topic")
        WriteIntents("pubsub://my-project/topic")


# --- Requirement: WriteIntents preserves per-key intent order ------------------


def test_serializer_dofn_preserves_order_within_a_key() -> None:
    # _SerializeIntent is a stateless 1:1 map with no grouping/windowing, so
    # calling it directly (bypassing runner scheduling) demonstrates it does
    # not itself introduce any reordering.
    dofn = _SerializeIntent()
    intents = [(b"k1", _intent(b"k1", 0)), (b"k1", _intent(b"k1", 1)), (b"k2", _intent(b"k2", 0))]
    results = [next(iter(dofn.process(e))) for e in intents]
    seqs_k1 = [ToolIntent.FromString(payload).seq for key, payload in results if key == b"k1"]
    assert seqs_k1 == [0, 1]


def test_distinct_keys_are_independently_partitioned() -> None:
    dofn = _SerializeIntent()
    intents = [(b"k1", _intent(b"k1", 0)), (b"k2", _intent(b"k2", 0))]
    results = [next(iter(dofn.process(e))) for e in intents]
    keys = {key for key, _ in results}
    assert keys == {b"k1", b"k2"}


# --- Requirement: WriteIntents serializes intents deterministically -----------


def test_identical_intents_serialize_identically() -> None:
    dofn = _SerializeIntent()
    a = next(iter(dofn.process((b"k", _intent(b"k", 5)))))
    b = next(iter(dofn.process((b"k", _intent(b"k", 5)))))
    assert a == b


# --- Requirement: WriteIntents routes serialization failures to dead-letter ---


class _ExplodingIntent:
    """A stand-in for ToolIntent whose serialization always fails."""

    def SerializeToString(self, deterministic: bool = False) -> bytes:  # matches proto API
        raise ValueError("boom")


def test_serialization_failure_is_dead_lettered_not_dropped() -> None:
    dofn = _SerializeIntent()
    element = (b"k", _ExplodingIntent())
    outputs = list(dofn.process(element))  # type: ignore[arg-type]  # deliberately wrong type
    assert len(outputs) == 1
    tagged = outputs[0]
    assert isinstance(tagged, beam.pvalue.TaggedOutput)
    assert tagged.tag == DEAD_LETTER_TAG
    dead_element, reason = tagged.value
    assert dead_element == element
    assert "boom" in reason


def test_dead_letter_pipeline_routes_failures_and_keeps_good_intents_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Force one real ToolIntent to fail serialization (by intent_id) while a
    # second serializes normally, proving a bundle survives a mid-batch
    # failure and only the failing element is dead-lettered.
    original = ToolIntent.SerializeToString

    def flaky(self: ToolIntent, deterministic: bool = False) -> bytes:
        if self.intent_id == "bad":
            raise ValueError("boom")
        return original(self, deterministic=deterministic)

    monkeypatch.setattr(ToolIntent, "SerializeToString", flaky)

    with BeamTestPipeline() as p:
        good = _intent(b"k1", 0, tool_name="ok")
        bad = ToolIntent(intent_id="bad", entity_key=b"k2")
        keyed = p | beam.Create([(b"k1", good), (b"k2", bad)])
        result = keyed | WriteIntents(
            "kafka://broker:9092/topic", writer_factory=_fake_writer_factory([])
        )
        reasons = result.dead_letter | beam.Map(lambda pair: pair[1])
        assert_that(reasons, equal_to(["boom"]))


# --- Requirement: WriteIntents is registered with the RunAgent sink resolver --


def test_resolver_returns_keyed_write_intents_for_kafka_intents() -> None:
    sink = DefaultSinkResolver().resolve("intents_to", "kafka://broker:9092/topic")
    assert isinstance(sink, _KeyedWriteIntents)


def test_resolver_returns_keyed_write_intents_for_pubsub_intents() -> None:
    sink = DefaultSinkResolver().resolve("intents_to", "pubsub://my-project/topic")
    assert isinstance(sink, _KeyedWriteIntents)


def test_resolver_leaves_traces_and_errors_as_bare_writers() -> None:
    sink = DefaultSinkResolver().resolve("traces_to", "kafka://broker:9092/topic")
    assert not isinstance(sink, _KeyedWriteIntents)
    assert isinstance(sink, beam.PTransform)


def test_run_agent_intents_to_kafka_resolves_to_write_intents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Swap the real Kafka writer for an in-memory one so this stays docker-free.
    recorded: list[tuple[bytes, bytes]] = []
    monkeypatch.setattr(
        "beam_agents.actions.write_intents._WRITERS",
        {
            "kafka": lambda *_: _fake_writer_factory(recorded)(""),
            "pubsub": lambda *_: beam.Map(lambda x: x),
        },
    )
    config = AgentConfig(
        provider_factory=make_pong_provider,
        intents_to="kafka://broker:9092/agent-intents",
    )
    options = beam.options.pipeline_options.PipelineOptions()
    options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True
    with BeamTestPipeline(options=options) as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements(
                [
                    TimestampedValue(
                        AgentEnvelope(entity_key=b"k", event_time_ms=0, external_event=b"go"), 0
                    )
                ]
            )
            .advance_watermark_to_infinity()
        )
        keyed = (
            p
            | stream
            | beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        )
        outputs = keyed | RunAgent(suspend_then_complete_agent, config=config)
        assert_that(
            outputs.intents | "names" >> beam.Map(lambda i: i.tool_name),
            equal_to(["http.post"]),
            label="intents-still-exposed",
        )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
