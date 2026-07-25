"""Tests for the `run-agent-transform` capability: `AgentConfig` construction-time
validation, the `SinkResolver` seam, `RunAgent`'s KV-input requirement, its four
named `RunAgentOutputs`, and sink-URI attachment.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from dataclasses import FrozenInstanceError, dataclass, field
from typing import Any
from unittest import mock

import apache_beam as beam
import pytest
from apache_beam.coders.typecoders import registry as coder_registry
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope
from beam_agents.core.transform import (
    AgentConfig,
    DefaultSinkResolver,
    RunAgent,
    RunAgentOutputs,
    UnknownSinkSchemeError,
    _validate_kv_input,
)
from tests.core._dofn_helpers import (
    append_agent,
    make_pong_provider,
    seq_agent,
    suspend_then_complete_agent,
)


def _event(key: bytes, payload: bytes, t_ms: int = 1000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


def _keyed(p: beam.Pipeline, *envelopes: AgentEnvelope) -> beam.pvalue.PCollection:
    return (
        p
        | beam.Create(list(envelopes))
        | beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )


def _streaming_pipeline() -> BeamTestPipeline:
    # Suspend/HITL-timer scenarios need a streaming pipeline: DirectRunner fires
    # REAL_TIME timers immediately on a batch pipeline's implicit completion,
    # which would race the "no HITL fire" assertions below. Mirrors the
    # identical helper in test_dofn_streaming.py.
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _keyed_stream(p: beam.Pipeline, *envelopes: AgentEnvelope) -> beam.pvalue.PCollection:
    stream = TestStream().advance_watermark_to(0)
    for envelope in envelopes:
        stream = stream.add_elements([TimestampedValue(envelope, envelope.event_time_ms / 1000)])
    stream = stream.advance_watermark_to_infinity()
    return (
        p
        | stream
        | beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )


@pytest.fixture(autouse=True)
def _restore_coder_registry() -> Iterator[None]:
    """Snapshot and restore the global coder registry (``RunAgent.expand`` calls
    ``register_coders()``); see the identical fixture in test_dofn_pipeline.py.
    """
    saved = dict(coder_registry._coders)
    try:
        yield
    finally:
        coder_registry._coders = saved


@dataclass
class _StubSinkResolver:
    """Records what it was asked to validate/resolve; never touches real IO."""

    validated: list[tuple[str, str]] = field(default_factory=list)
    resolved: list[str] = field(default_factory=list)

    def validate(self, field_name: str, uri: str) -> None:
        self.validated.append((field_name, uri))

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        self.resolved.append(uri)
        return beam.Map(lambda x: x)


class _RejectingSinkResolver:
    """Fails every validation; proves AgentConfig propagates the resolver's error."""

    def validate(self, field_name: str, uri: str) -> None:
        raise UnknownSinkSchemeError(f"{field_name}: rejected {uri!r}")

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:  # pragma: no cover
        raise AssertionError("resolve() should not be called when validate() rejects")


# --- Requirement: AgentConfig bundles runtime configuration and validates ------


def test_valid_config_constructs_and_is_immutable() -> None:
    config = AgentConfig(provider_factory=make_pong_provider)
    assert config.activation_timeout_s == 30.0
    assert config.ttl_ms == 3_600_000
    assert config.cancel_grace_s == 5.0
    with pytest.raises(FrozenInstanceError):
        config.ttl_ms = 1  # type: ignore[misc]


@pytest.mark.parametrize("knob", ["activation_timeout_s", "ttl_ms", "cancel_grace_s"])
@pytest.mark.parametrize("bad_value", [0, -1])
def test_non_positive_knob_is_rejected(knob: str, bad_value: float) -> None:
    kwargs: dict[str, Any] = {knob: bad_value}
    with pytest.raises(ValueError, match=knob):
        AgentConfig(provider_factory=make_pong_provider, **kwargs)


@pytest.mark.parametrize("field_name", ["intents_to", "traces_to", "errors_to"])
def test_unknown_sink_scheme_rejected_at_construction(field_name: str) -> None:
    kwargs: dict[str, Any] = {field_name: "ftp://nope/topic"}
    with pytest.raises(ValueError, match=field_name):
        AgentConfig(provider_factory=make_pong_provider, **kwargs)


def test_config_construction_propagates_resolver_validation_error() -> None:
    with pytest.raises(UnknownSinkSchemeError):
        AgentConfig(
            provider_factory=make_pong_provider,
            intents_to="whatever://x",
            sink_resolver=_RejectingSinkResolver(),
        )


def test_no_sink_uris_skips_resolver_validation() -> None:
    stub = _StubSinkResolver()
    AgentConfig(provider_factory=make_pong_provider, sink_resolver=stub)
    assert stub.validated == []


# --- Requirement: sink resolver seam -------------------------------------------


@pytest.mark.parametrize(
    "uri",
    [
        "kafka://broker:9092/my-topic",
        "pubsub://my-project/my-topic",
        "bigquery://my-project/my_dataset/my_table",
    ],
)
def test_default_resolver_validates_documented_schemes(uri: str) -> None:
    DefaultSinkResolver().validate("intents_to", uri)  # must not raise


@pytest.mark.parametrize(
    "uri",
    [
        "ftp://broker/topic",
        "kafka://broker:9092",  # missing topic segment
        "kafka:///topic",  # missing bootstrap servers
        "pubsub://my-project",  # missing topic
        "bigquery://my-project/my_dataset",  # missing table
        "not-a-uri",
    ],
)
def test_default_resolver_rejects_unknown_or_malformed_uri(uri: str) -> None:
    with pytest.raises(UnknownSinkSchemeError):
        DefaultSinkResolver().validate("intents_to", uri)


def test_default_resolver_validate_does_not_import_io_clients() -> None:
    # Poison the three IO modules in sys.modules: any `import` statement for
    # them raises ImportError regardless of prior caching. validate() must
    # never trigger that import.
    poisoned = {
        "apache_beam.io.kafka": None,
        "apache_beam.io.gcp.pubsub": None,
        "apache_beam.io.gcp.bigquery": None,
    }
    with mock.patch.dict(sys.modules, poisoned):
        resolver = DefaultSinkResolver()
        resolver.validate("intents_to", "kafka://broker:9092/topic")
        resolver.validate("traces_to", "pubsub://my-project/my-topic")
        resolver.validate("errors_to", "bigquery://my-project/my_dataset/my_table")


@pytest.mark.parametrize(
    "uri",
    [
        "kafka://broker:9092/my-topic",
        "pubsub://my-project/my-topic",
        "bigquery://my-project/my_dataset/my_table",
    ],
)
def test_default_resolver_resolve_returns_a_ptransform(uri: str) -> None:
    transform = DefaultSinkResolver().resolve("traces_to", uri)
    assert isinstance(transform, beam.PTransform)


# --- Requirement: RunAgent requires pre-keyed KV input -------------------------


def test_keyed_input_flows_through_run_agent() -> None:
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _event(b"k", b"go"))
        outputs = keyed | RunAgent(
            seq_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(outputs.output, equal_to([b"0"]))


def test_non_kv_input_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="KV"), BeamTestPipeline() as p:
        (
            p
            | beam.Create([_event(b"k", b"go")])
            | RunAgent(seq_agent, config=AgentConfig(provider_factory=make_pong_provider))
        )


@pytest.mark.parametrize("erased_type", [None, beam.typehints.Any])
def test_erased_element_type_is_allowed_through(erased_type: object) -> None:
    # Scenario (design D-note): an absent/erased element type hint must not be
    # positively rejected — the DoFn's downstream KV requirement is the
    # backstop. Exercises both real-world erasure shapes: a freshly
    # constructed PCollection (element_type=None) and an explicit Any hint.
    with BeamTestPipeline() as p:
        pcoll: beam.pvalue.PCollection = beam.pvalue.PCollection(p, element_type=erased_type)
        _validate_kv_input(pcoll)  # must not raise


# --- Requirement: RunAgent exposes four named outputs as RunAgentOutputs ------


def test_expand_returns_run_agent_outputs_with_four_named_pcollections() -> None:
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _event(b"k", b"hello"))
        outputs = keyed | RunAgent(
            append_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert isinstance(outputs, RunAgentOutputs)
        assert_that(outputs.output, equal_to([b"hello#0"]), label="output")
        assert_that(outputs.intents, equal_to([]), label="intents")
        assert_that(outputs.errors, equal_to([]), label="errors")
        assert_that(
            outputs.traces
            | "mark-traces" >> beam.Map(lambda _: 1)
            | "count-traces" >> beam.CombineGlobally(sum),
            equal_to([2]),
            label="traces",
        )


def test_terminal_output_and_intent_are_separable() -> None:
    # suspend_then_complete_agent emits an intent on suspend, no main output;
    # the resume completes with an output and no further intent.
    with _streaming_pipeline() as p:
        keyed = _keyed_stream(p, _event(b"k", b"go"))
        outputs = keyed | RunAgent(
            suspend_then_complete_agent, config=AgentConfig(provider_factory=make_pong_provider)
        )
        assert_that(outputs.output, equal_to([]), label="no-output-yet")
        tool_names = outputs.intents | "tool-names" >> beam.Map(lambda i: i.tool_name)
        assert_that(tool_names, equal_to(["http.post"]), label="intent-only")


# --- Requirement: configured sink URIs resolve and attach to their tag --------


def test_configured_sink_attaches_and_tag_stays_exposed() -> None:
    stub = _StubSinkResolver()
    config = AgentConfig(
        provider_factory=make_pong_provider,
        intents_to="stub://intents",
        sink_resolver=stub,
    )
    with _streaming_pipeline() as p:
        keyed = _keyed_stream(p, _event(b"k", b"go"))
        outputs = keyed | RunAgent(suspend_then_complete_agent, config=config)
        assert stub.resolved == ["stub://intents"]
        tool_names = outputs.intents | "tool-names" >> beam.Map(lambda i: i.tool_name)
        assert_that(tool_names, equal_to(["http.post"]), label="intents-still-exposed")


def test_each_sink_attaches_only_to_its_own_tag() -> None:
    stub = _StubSinkResolver()
    config = AgentConfig(
        provider_factory=make_pong_provider,
        traces_to="stub://traces",
        errors_to="stub://errors",
        sink_resolver=stub,
    )
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _event(b"k", b"go"))
        keyed | RunAgent(seq_agent, config=config)
    assert sorted(stub.resolved) == ["stub://errors", "stub://traces"]


def test_unset_sinks_attach_nothing() -> None:
    stub = _StubSinkResolver()
    config = AgentConfig(provider_factory=make_pong_provider, sink_resolver=stub)
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _event(b"k", b"go"))
        keyed | RunAgent(seq_agent, config=config)
    assert stub.resolved == []


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
