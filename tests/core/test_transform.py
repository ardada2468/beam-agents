"""Tests for the `run-agent-transform` capability: `AgentConfig` construction-time
validation, the `SinkResolver` seam, `RunAgent`'s KV-input requirement, its four
named `RunAgentOutputs`, and sink-URI attachment.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any
from unittest import mock

import apache_beam as beam
import pytest

# Imported at module scope (unlike the runtime's lazy resolver imports) so the
# writer-configuration tests can assert against the real classes; the poison
# test below patches sys.modules, which module-level bindings don't defeat.
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Aliased: a bare "TestPipeline" name would be mis-collected by pytest.
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, StateSnapshot, TraceEvent
from beam_agents.core.dofn import (
    DETAIL_NO_CONTINUATION,
    REASON_ERROR,
    REASON_ORPHANED,
    ActivationError,
    _AgentDoFn,
)
from beam_agents.core.error_records import activation_error_to_row, serialize_error_envelope
from beam_agents.core.migration import CURRENT_STATE_SCHEMA_VERSION
from beam_agents.core.transform import (
    AgentConfig,
    DefaultSinkResolver,
    RunAgent,
    RunAgentOutputs,
    UnknownSinkSchemeError,
    _validate_kv_input,
    _WriteErrors,
    _WriteSnapshots,
    _WriteTraces,
)
from beam_agents.hitl import HitlPolicy
from beam_agents.memory import DropOldestCompactor, FlushToLongterm
from beam_agents.memory.facade import HARD_CAP_BYTES
from beam_agents.observability import trace_event_to_row, trace_id_for
from beam_agents.observability.exporters import TRACE_TABLE_SCHEMA
from beam_agents.observability.otlp import WriteTracesToOtlp
from beam_agents.tools import ToolRegistry
from tests.core._context_helpers import decode_len_based
from tests.core._dofn_helpers import (
    append_agent,
    make_pong_provider,
    model_then_act_agent,
    seq_agent,
    suspend_then_complete_agent,
)

_TRACE_EVENT = TraceEvent(
    trace_id=bytes(range(16)),
    span_id=bytes(range(8)),
    entity_key=b"key-1",
    seq=7,
    event_type=TraceEvent.LLM_CALL,
    attributes={"gen_ai.request.model": "m-1"},
    start_ms=1_000,
    end_ms=1_000,
)


_ACTIVATION_ERROR = ActivationError(
    entity_key=b"key-1",
    reason=REASON_ERROR,
    detail="RuntimeError('boom') failed_at_step=0 after=ACTIVATION_START",
    event_time_ms=1_000,
)

# The dead letter `_orphaned_result` below produces: a tool result naming an
# intent no continuation ever pended, at the element's own event time.
_ORPHAN_ERROR = ActivationError(
    entity_key=b"k",
    reason=REASON_ORPHANED,
    detail=f"{DETAIL_NO_CONTINUATION}:ghost",
    event_time_ms=1_000,
)


def _event(key: bytes, payload: bytes, t_ms: int = 1000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


def _orphaned_result(key: bytes, t_ms: int = 1000) -> AgentEnvelope:
    """A tool result for an intent nobody is waiting on -> an `.errors` record."""
    envelope = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    envelope.tool_result.intent_id = "ghost"
    return envelope


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


class _RecordingWrite(beam.PTransform):
    """Identity write that keeps the ``PCollection`` it was handed.

    Pipeline construction is synchronous and in-process, so after
    ``RunAgent`` is applied this transform's ``written`` is the very
    collection the resolver's encoding fed to the writer — which is what the
    "not dataclasses" scenario is about.
    """

    def __init__(self) -> None:
        super().__init__()
        self.written: beam.pvalue.PCollection | None = None

    def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
        self.written = pcoll
        return pcoll


class _RecordingErrorsResolver:
    """Resolves ``errors_to`` to the *real* encoder over a recording writer."""

    def __init__(self, *, to_row: bool = False) -> None:
        self.sink = _RecordingWrite()
        self._to_row = to_row

    def validate(self, field_name: str, uri: str) -> None:
        pass

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        return _WriteErrors(self.sink, to_row=self._to_row)


class _RejectingSinkResolver:
    """Fails every validation; proves AgentConfig propagates the resolver's error."""

    def validate(self, field_name: str, uri: str) -> None:
        raise UnknownSinkSchemeError(f"{field_name}: rejected {uri!r}")

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:  # pragma: no cover
        raise AssertionError("resolve() should not be called when validate() rejects")


# --- Requirement: AgentConfig bundles runtime configuration and validates ------
#
# The runner-free half of this requirement — the numeric knobs' positivity
# boundary and immutability — lives in `test_config_validation.py`, which the
# mutation selection reaches; this suite drives TestPipeline/TestStream and is
# deselected under mutmut, so a mutant in the shared `_require_positive`
# validator could never be killed from here. The sink-URI half stays put: see
# that module's docstring.


def test_config_carries_a_default_hitl_policy() -> None:
    config = AgentConfig(provider_factory=make_pong_provider)
    assert config.hitl_policy == HitlPolicy()
    assert config.hitl_policy.max_escalations == 0


def test_config_carries_an_empty_default_tool_registry_or_the_supplied_one() -> None:
    # An unconfigured pipeline refuses every inline `run_tool` by name rather
    # than executing something unregistered; a supplied registry is held as-is.
    default_config = AgentConfig(provider_factory=make_pong_provider)
    assert isinstance(default_config.tool_registry, ToolRegistry)
    assert default_config.tool_registry.tools_schema == []

    registry = ToolRegistry()
    config = AgentConfig(provider_factory=make_pong_provider, tool_registry=registry)
    assert config.tool_registry is registry


def test_config_carries_the_default_drop_oldest_compactor() -> None:
    # Requirement: the default compactor is wired through AgentConfig into every
    # activation. Before this, the compactor parameter was dead and the hard
    # cap's only behavior was MemoryOverflow -> dead letter, forever.
    config = AgentConfig(provider_factory=make_pong_provider)
    assert isinstance(config.compactor, DropOldestCompactor)
    assert config.compactor.target_bytes == HARD_CAP_BYTES // 2
    assert config.compactor.protected_prefixes == ("__langgraph__/",)

    # ...and opting out is expressible, restoring strict-overflow semantics.
    assert AgentConfig(provider_factory=make_pong_provider, compactor=None).compactor is None


def test_the_compactor_and_summarizer_reach_the_dofn() -> None:
    compactor = DropOldestCompactor(target_bytes=99)
    config = AgentConfig(provider_factory=make_pong_provider, compactor=compactor)
    dofn = _AgentDoFn(
        seq_agent,
        provider_factory=config.provider_factory,
        compactor=config.compactor,
        summarizer=config.summarizer,
    )
    assert dofn._compactor is compactor
    assert dofn._summarizer is None


def test_the_summarizer_is_opt_in() -> None:
    assert AgentConfig(provider_factory=make_pong_provider).summarizer is None


def test_on_expire_without_a_long_term_store_is_rejected_at_construction() -> None:
    # The hook writes through the long-term tier; configuring one without the
    # other is a misconfiguration, and it fails at the site of the typo rather
    # than at the first TTL fire in production.
    with pytest.raises(ValueError, match="longterm_memory"):
        AgentConfig(provider_factory=make_pong_provider, on_expire=FlushToLongterm())

    config = AgentConfig(
        provider_factory=make_pong_provider,
        on_expire=FlushToLongterm(),
        longterm_memory="memory://",
    )
    assert config.on_expire is not None


def test_on_expire_is_unset_by_default() -> None:
    # Unset means today's wipe-only behavior, unchanged.
    assert AgentConfig(provider_factory=make_pong_provider).on_expire is None


# --- Requirement: `max_tokens_per_activation` is a validated field ------------


@pytest.mark.parametrize("limit", [0, -1])
def test_a_non_positive_budget_fails_at_construction(limit: int) -> None:
    # Scenario: A non-positive budget fails at construction. The `ValueError`
    # names the field, at the construction site, before any pipeline exists.
    with pytest.raises(ValueError, match="max_tokens_per_activation"):
        AgentConfig(
            provider_factory=make_pong_provider,
            decode=decode_len_based,
            max_tokens_per_activation=limit,
        )


def test_a_budget_without_a_decoder_fails_at_construction() -> None:
    # Scenario: A budget without a decoder fails at construction. Without a
    # decoder the token counts are genuinely unknown, and both readings of
    # unknown are worse than failing here: unknown-is-free meters nothing and is
    # discovered on an invoice, unknown-is-fatal makes the knob unusable.
    with pytest.raises(ValueError, match="decode"):
        AgentConfig(provider_factory=make_pong_provider, max_tokens_per_activation=1_000)


def test_the_budget_is_unset_by_default_and_reaches_the_dofn_when_set() -> None:
    # Scenario: Unset means unlimited -- the config half of it. And when set,
    # the value rides the same `RunAgent -> _AgentDoFn` thread `decode` does.
    assert AgentConfig(provider_factory=make_pong_provider).max_tokens_per_activation is None

    config = AgentConfig(
        provider_factory=make_pong_provider,
        decode=decode_len_based,
        max_tokens_per_activation=5_000,
    )
    dofn = _AgentDoFn(
        seq_agent,
        provider_factory=config.provider_factory,
        decode=config.decode,
        max_tokens_per_activation=config.max_tokens_per_activation,
    )
    assert dofn._max_tokens_per_activation == 5_000


def test_run_agent_expand_passes_the_budget_through_to_the_dofn() -> None:
    # The knob is only worth validating if it actually reaches the runtime:
    # `expand` is the one place that thread is woven, so the real DoFn is built
    # through a recording wrapper rather than a mock that would break `ParDo`.
    config = AgentConfig(
        provider_factory=make_pong_provider, decode=decode_len_based, max_tokens_per_activation=777
    )
    built: list[_AgentDoFn] = []
    real = _AgentDoFn

    def recording(*args: Any, **kwargs: Any) -> _AgentDoFn:
        dofn = real(*args, **kwargs)
        built.append(dofn)
        return dofn

    with mock.patch("beam_agents.core.transform._AgentDoFn", recording), BeamTestPipeline() as p:
        _keyed(p, _event(b"k", b"go")) | RunAgent(seq_agent, config=config)

    assert [dofn._max_tokens_per_activation for dofn in built] == [777]


@pytest.mark.parametrize(
    ("kwargs", "field_name"),
    [
        ({"timeout_ms": 0}, "timeout_ms"),
        ({"timeout_ms": -1}, "timeout_ms"),
        ({"intent_ttl_ms": 0}, "intent_ttl_ms"),
        ({"max_escalations": -1}, "max_escalations"),
        ({"approval_channel": ""}, "approval_channel"),
    ],
)
def test_invalid_hitl_policy_is_rejected_at_config_construction(
    kwargs: dict[str, Any], field_name: str
) -> None:
    # Scenarios: A non-positive timeout / an empty approval channel is rejected
    # at construction, before any pipeline exists.
    with pytest.raises(ValueError, match=field_name):
        policy = HitlPolicy(**kwargs)
        AgentConfig(provider_factory=make_pong_provider, hitl_policy=policy)


def test_config_revalidates_a_policy_that_evaded_its_own_constructor() -> None:
    # A frozen dataclass can still be bypassed via object.__setattr__; the
    # config re-checks rather than trusting the instance it is handed.
    policy = HitlPolicy()
    object.__setattr__(policy, "approval_channel", "")
    with pytest.raises(ValueError, match="approval_channel"):
        AgentConfig(provider_factory=make_pong_provider, hitl_policy=policy)


def test_hitl_policy_reaches_the_dofn() -> None:
    policy = HitlPolicy(timeout_ms=1_234, approval_channel="pager")
    config = AgentConfig(provider_factory=make_pong_provider, hitl_policy=policy)
    dofn = _AgentDoFn(
        seq_agent,
        provider_factory=config.provider_factory,
        hitl_policy=config.hitl_policy,
    )
    assert dofn._hitl_policy is policy


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


# --- Requirement: The traces output is deliverable to a configured sink ------


@pytest.mark.parametrize(
    ("uri", "expect_row"),
    [
        ("kafka://broker:9092/traces", False),
        ("pubsub://my-project/traces", False),
        ("bigquery://my-project/my_dataset/traces", True),
    ],
)
def test_a_traces_sink_encodes_before_writing(uri: str, expect_row: bool) -> None:
    # A raw write transform cannot accept a `TraceEvent` proto, so resolution
    # for `traces_to` must wrap the writer in the scheme's encoder.
    transform = DefaultSinkResolver().resolve("traces_to", uri)

    assert isinstance(transform, _WriteTraces)
    assert transform._to_row is expect_row


@pytest.mark.parametrize(
    ("to_row", "expected"),
    [
        (False, [(b"key-1", _TRACE_EVENT.SerializeToString(deterministic=True))]),
        (True, [trace_event_to_row(_TRACE_EVENT)]),
    ],
)
def test_the_traces_encoder_hands_the_sink_what_its_scheme_accepts(
    to_row: bool, expected: list[Any]
) -> None:
    # The encoder is the whole reason a configured `traces_to` works: run it
    # against a recording sink and assert on what the writer actually receives.
    with BeamTestPipeline() as p:
        written = (
            p | beam.Create([_TRACE_EVENT]) | _WriteTraces(beam.Map(lambda x: x), to_row=to_row)
        )
        assert_that(written, equal_to(expected))


# --- Requirement: An `otlp://` traces sink scheme ------------------------------


@pytest.mark.parametrize(
    "uri",
    [
        "otlp://collector:4318",
        "otlp://collector",  # port defaults
        "otlp://collector:4318?tls=true&batch_size=64&flush_deadline_s=2.5"
        "&queue_batches=4&service_name=my-pipeline",
    ],
)
def test_otlp_uri_validates_for_traces_to(uri: str) -> None:
    DefaultSinkResolver().validate("traces_to", uri)  # must not raise


@pytest.mark.parametrize(
    "uri",
    [
        "otlp://",  # no host
        "otlp://collector:not-a-port",  # unparseable port
        "otlp://collector:4318/some/path",  # the /v1/traces path is implied
        "otlp://collector:4318?batch_size=zero",  # unparseable int
        "otlp://collector:4318?flush_deadline_s=soon",  # unparseable float
        "otlp://collector:4318?batch_size=0",  # not positive
        "otlp://collector:4318?queue_batches=0",  # not positive
        "otlp://collector:4318?flush_deadline_s=0",  # not positive
        "otlp://collector:4318?tls=maybe",  # not a bool
        "otlp://collector:4318?bogus=1",  # unknown option
    ],
)
def test_malformed_otlp_uri_fails_at_construction(uri: str) -> None:
    # Scenario: A malformed OTLP URI fails at construction.
    with pytest.raises(UnknownSinkSchemeError, match=r"otlp://<host>:<port>"):
        DefaultSinkResolver().validate("traces_to", uri)


@pytest.mark.parametrize("field_name", ["intents_to", "errors_to"])
def test_otlp_is_refused_for_the_intents_and_errors_sinks(field_name: str) -> None:
    # Scenario: OTLP is refused for the intents and errors sinks — they are
    # correctness-bearing streams and must not ride a best-effort exporter.
    kwargs: dict[str, Any] = {field_name: "otlp://collector:4318"}
    with pytest.raises(ValueError, match="best-effort"):
        AgentConfig(provider_factory=make_pong_provider, **kwargs)


@pytest.mark.parametrize("field_name", ["intents_to", "errors_to"])
def test_otlp_refusal_also_guards_resolution(field_name: str) -> None:
    # A resolver used directly (not through AgentConfig) must refuse too.
    with pytest.raises(UnknownSinkSchemeError, match="best-effort"):
        DefaultSinkResolver().resolve(field_name, "otlp://collector:4318")


def test_otlp_validation_does_not_import_the_otlp_dependency() -> None:
    # Scenario: Validation does not import the OTLP dependency.
    with mock.patch.dict(sys.modules, {"opentelemetry": None}):
        DefaultSinkResolver().validate("traces_to", "otlp://collector:4318")
        AgentConfig(provider_factory=make_pong_provider, traces_to="otlp://collector:4318")


def test_otlp_resolves_to_the_export_transform_with_knobs_applied() -> None:
    transform = DefaultSinkResolver().resolve(
        "traces_to",
        "otlp://collector:4318?batch_size=64&flush_deadline_s=2.5"
        "&queue_batches=4&service_name=my-pipeline",
    )

    assert isinstance(transform, WriteTracesToOtlp)
    assert transform._endpoint == "http://collector:4318/v1/traces"
    assert transform._batch_size == 64
    assert transform._flush_deadline_s == 2.5
    assert transform._queue_batches == 4
    assert transform._service_name == "my-pipeline"


def test_otlp_resolution_defaults_are_the_documented_ones() -> None:
    transform = DefaultSinkResolver().resolve("traces_to", "otlp://collector:4318")

    assert isinstance(transform, WriteTracesToOtlp)
    assert transform._batch_size == 512
    assert transform._flush_deadline_s == 5.0
    assert transform._queue_batches == 8
    assert transform._service_name == "beam-agents"


def test_otlp_tls_and_default_port_shape_the_endpoint() -> None:
    tls = DefaultSinkResolver().resolve("traces_to", "otlp://collector:4318?tls=true")
    default_port = DefaultSinkResolver().resolve("traces_to", "otlp://collector")

    assert isinstance(tls, WriteTracesToOtlp)
    assert tls._endpoint == "https://collector:4318/v1/traces"
    assert isinstance(default_port, WriteTracesToOtlp)
    assert default_port._endpoint == "http://collector:4318/v1/traces"


# --- Requirement: A self-provisioning BigQuery traces writer -------------------


def test_a_bigquery_traces_sink_carries_schema_and_dispositions() -> None:
    # Scenario: The writer carries schema and dispositions.
    transform = DefaultSinkResolver().resolve(
        "traces_to", "bigquery://my-project/my_dataset/traces"
    )

    assert isinstance(transform, _WriteTraces)
    writer = transform._sink
    assert isinstance(writer, WriteToBigQuery)
    assert writer.schema == TRACE_TABLE_SCHEMA
    assert writer.create_disposition == BigQueryDisposition.CREATE_IF_NEEDED
    assert writer.write_disposition == BigQueryDisposition.WRITE_APPEND
    assert writer.additional_bq_parameters == {
        "timePartitioning": {"type": "DAY", "field": "event_time"},
        "clustering": {"fields": ["trace_id"]},
    }
    assert writer.table_reference.projectId == "my-project"
    assert writer.table_reference.datasetId == "my_dataset"
    assert writer.table_reference.tableId == "traces"


def test_other_bigquery_sinks_are_not_schema_d() -> None:
    # `errors_to`/`intents_to` BigQuery resolution is unchanged: the trace
    # table's schema must not leak onto other streams' writers. `errors_to`
    # rides its own row encoder (`_WriteErrors`), so the assertion is about the
    # writer it wraps.
    resolver = DefaultSinkResolver()

    errors = resolver.resolve("errors_to", "bigquery://my-project/my_dataset/errs")
    assert isinstance(errors, _WriteErrors)
    inner = errors._sink
    assert isinstance(inner, WriteToBigQuery)
    assert inner.schema is None

    intents = resolver.resolve("intents_to", "bigquery://my-project/my_dataset/ints")
    assert isinstance(intents, WriteToBigQuery)
    assert intents.schema is None


def test_other_sinks_are_not_wrapped_in_the_traces_encoder() -> None:
    # `errors_to` carries `ActivationError` dataclasses, not `TraceEvent`s: it
    # gets its own encoder (`_WriteErrors`), never the trace one.
    transform = DefaultSinkResolver().resolve(
        "errors_to", "bigquery://my-project/my_dataset/my_table"
    )
    assert not isinstance(transform, _WriteTraces)


def test_an_unset_traces_sink_leaves_the_output_exposed() -> None:
    # Scenario: An unset traces sink leaves the output exposed.
    config = AgentConfig(provider_factory=make_pong_provider)
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _event(b"k", b"go"))
        outputs = keyed | RunAgent(seq_agent, config=config)
        kinds = outputs.traces | "event-types" >> beam.Map(lambda e: e.event_type)
        assert_that(
            kinds,
            equal_to([TraceEvent.ACTIVATION_START, TraceEvent.ACTIVATION_END]),
            label="raw-trace-events",
        )


def test_traces_flow_through_a_pipeline_end_to_end() -> None:
    # Scenario: Traces flow through a pipeline end to end.
    # The agent calls the model and stages an intent, so the trace should hold
    # the activation bracket plus one child event for each.
    config = AgentConfig(provider_factory=make_pong_provider, decode=decode_len_based)
    with _streaming_pipeline() as p:
        keyed = _keyed_stream(p, _event(b"k", b"go"))
        outputs = keyed | RunAgent(model_then_act_agent, config=config)
        kinds = outputs.traces | "kinds" >> beam.Map(lambda e: e.event_type)
        assert_that(
            kinds,
            equal_to(
                [
                    TraceEvent.ACTIVATION_START,
                    TraceEvent.LLM_CALL,
                    TraceEvent.INTENT_EMITTED,
                    TraceEvent.ACTIVATION_END,
                ]
            ),
            label="one-event-per-step",
        )
        # Every event of one activation belongs to one trace, and the intent
        # carries that same trace onward to the effector.
        trace_ids = outputs.traces | "trace-ids" >> beam.Map(lambda e: e.trace_id)
        assert_that(
            trace_ids,
            equal_to([trace_id_for(b"k", 0)] * 4),
            label="one-trace-per-activation",
        )
        intent_trace_ids = outputs.intents | "intent-trace-ids" >> beam.Map(lambda i: i.trace_id)
        assert_that(
            intent_trace_ids, equal_to([trace_id_for(b"k", 0)]), label="intent-carries-trace"
        )


def test_a_configured_decode_puts_truthful_token_counts_on_the_traces() -> None:
    # The runtime path only reports usage if the provider's decoder reaches it,
    # so this is the end-to-end half of the omit-rather-than-zero rule.
    config = AgentConfig(provider_factory=make_pong_provider, decode=decode_len_based)
    with _streaming_pipeline() as p:
        keyed = _keyed_stream(p, _event(b"k", b"go"))
        outputs = keyed | RunAgent(model_then_act_agent, config=config)
        usage = (
            outputs.traces
            | "llm-only" >> beam.Filter(lambda e: e.event_type == TraceEvent.LLM_CALL)
            | "usage" >> beam.Map(lambda e: dict(e.attributes)["gen_ai.usage.input_tokens"])
        )
        # len(b"pong"), the response the fake provider returns.
        assert_that(usage, equal_to(["4"]), label="truthful-usage")


# --- Requirement: The errors output is deliverable to a configured sink ------


@pytest.mark.parametrize(
    ("uri", "expect_row"),
    [
        ("kafka://broker:9092/errors", False),
        ("pubsub://my-project/errors", False),
        ("bigquery://my-project/my_dataset/errors", True),
    ],
)
def test_an_errors_sink_encodes_before_writing(uri: str, expect_row: bool) -> None:
    # Scenarios: errors_to kafka/bigquery URIs resolve to an encoding writer.
    # A raw write transform cannot accept an `ActivationError` dataclass, so
    # resolution for `errors_to` must wrap the writer in the scheme's encoder.
    transform = DefaultSinkResolver().resolve("errors_to", uri)

    assert isinstance(transform, _WriteErrors)
    assert transform._to_row is expect_row


@pytest.mark.parametrize(
    ("to_row", "expected"),
    [
        (False, [serialize_error_envelope(_ACTIVATION_ERROR)]),
        (True, [activation_error_to_row(_ACTIVATION_ERROR)]),
    ],
)
def test_the_errors_encoder_hands_the_sink_what_its_scheme_accepts(
    to_row: bool, expected: list[Any]
) -> None:
    # The encoder is the whole reason a configured `errors_to` works: run it
    # against a recording sink and assert on what the writer actually receives.
    with BeamTestPipeline() as p:
        written = (
            p
            | beam.Create([_ACTIVATION_ERROR])
            | _WriteErrors(beam.Map(lambda x: x), to_row=to_row)
        )
        assert_that(written, equal_to(expected))


def test_a_configured_errors_sink_receives_encoded_records_not_dataclasses() -> None:
    # Scenario: A configured errors sink receives encoded records, not
    # dataclasses. The resolver hands back the real encoding wrapped around a
    # recording writer, so this exercises the production path rather than a
    # stand-in for it.
    resolver = _RecordingErrorsResolver()
    config = AgentConfig(
        provider_factory=make_pong_provider,
        errors_to="stub://errors",
        sink_resolver=resolver,
    )
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _orphaned_result(b"k"))
        outputs = keyed | RunAgent(seq_agent, config=config)
        # What reached the writer: keyed envelope bytes carrying the record.
        assert_that(
            resolver.sink.written,
            equal_to([serialize_error_envelope(_ORPHAN_ERROR)]),
            label="encoded-at-the-writer",
        )
        # ...and `.errors` still exposes the dataclass to a direct consumer.
        reasons = outputs.errors | "reasons" >> beam.Map(lambda e: e.reason)
        assert_that(reasons, equal_to([REASON_ORPHANED]), label="errors-still-exposed")


# --- Requirement: The snapshots output resolves a sink like traces ------------


def _export_request(key: bytes, request_id: str = "req-1", t_ms: int = 1000) -> AgentEnvelope:
    return AgentEnvelope(
        entity_key=key,
        event_time_ms=t_ms,
        export_request=AgentEnvelope.StateExportRequest(request_id=request_id),
    )


class _RecordingSnapshotsResolver:
    """Resolves ``snapshots_to`` to the *real* encoder over a recording writer."""

    def __init__(self) -> None:
        self.sink = _RecordingWrite()

    def validate(self, field_name: str, uri: str) -> None:
        pass

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        return _WriteSnapshots(self.sink)


def test_a_configured_snapshots_sink_receives_serialized_snapshots_keyed_by_entity() -> None:
    # Scenario: A configured snapshots sink receives serialized snapshots keyed
    # by entity. The resolver hands back the real encoding wrapped around a
    # recording writer, so this exercises the production path.
    resolver = _RecordingSnapshotsResolver()
    config = AgentConfig(
        provider_factory=make_pong_provider,
        snapshots_to="stub://snapshots",
        sink_resolver=resolver,
    )
    expected = StateSnapshot(
        state_schema_version=CURRENT_STATE_SCHEMA_VERSION,
        entity_key=b"k",
        seq=0,
        snapshot_at_ms=1000,
        request_id="req-1",
    )
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _export_request(b"k"))
        outputs = keyed | RunAgent(seq_agent, config=config)
        assert_that(
            resolver.sink.written,
            equal_to([(b"k", expected.SerializeToString(deterministic=True))]),
            label="keyed-serialized-at-the-writer",
        )
        # ...and `.snapshots` still exposes the proto to a direct consumer.
        request_ids = outputs.snapshots | "request-ids" >> beam.Map(lambda s: s.request_id)
        assert_that(request_ids, equal_to(["req-1"]), label="snapshots-still-exposed")


def test_no_snapshots_sink_configured_still_constructs_and_runs() -> None:
    # Scenario: No sink configured still constructs and runs. The tagged output
    # stays exposed and unconsumed; nothing about pipeline construction needs it.
    config = AgentConfig(provider_factory=make_pong_provider)
    with BeamTestPipeline() as p:
        keyed = _keyed(p, _export_request(b"k"), _event(b"k", b"go"))
        outputs = keyed | RunAgent(seq_agent, config=config)
        assert isinstance(outputs.snapshots, beam.pvalue.PCollection)
        # The activation still runs; only the export produces a snapshot.
        assert_that(outputs.output, equal_to([b"0"]), label="activation-unaffected")


def test_a_snapshots_sink_encodes_before_writing() -> None:
    # A raw write transform cannot accept a `StateSnapshot` proto, so resolution
    # for `snapshots_to` must wrap the writer in the serializer — the same shape
    # `traces_to` has.
    for uri in ("kafka://broker:9092/snapshots", "pubsub://my-project/snapshots"):
        transform = DefaultSinkResolver().resolve("snapshots_to", uri)
        assert isinstance(transform, _WriteSnapshots)


def test_the_snapshots_encoder_hands_the_sink_keyed_deterministic_bytes() -> None:
    snapshot = StateSnapshot(state_schema_version=1, entity_key=b"key-1", seq=3, snapshot_at_ms=7)
    with BeamTestPipeline() as p:
        written = p | beam.Create([snapshot]) | _WriteSnapshots(beam.Map(lambda x: x))
        assert_that(
            written,
            equal_to([(b"key-1", snapshot.SerializeToString(deterministic=True))]),
        )


def test_a_bigquery_snapshots_sink_is_refused_at_construction() -> None:
    # A `StateSnapshot` is an opaque per-key state image with no row encoding,
    # so the misconfiguration is refused where it is written rather than
    # failing inside a bundle.
    with pytest.raises(UnknownSinkSchemeError, match="snapshots_to"):
        DefaultSinkResolver().validate("snapshots_to", "bigquery://my-project/my_dataset/snaps")
    with pytest.raises(UnknownSinkSchemeError, match="snapshots_to"):
        AgentConfig(
            provider_factory=make_pong_provider,
            snapshots_to="bigquery://my-project/my_dataset/snaps",
        )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
