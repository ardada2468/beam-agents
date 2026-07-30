"""The DirectRunner leg: every conformance scenario x every registered adapter.

Each scenario body is written once, against runtime-observable behavior only —
``RunAgent``'s ``.output``/``.intents``/``.traces``/``.errors`` collections
(streaming TestPipeline/TestStream, scripted watermark and processing-time
advances) or, for restart-mid-suspension, the committed keyed state a
DoFn-level drive round-trips. Adapters enter only through their registered
factories; a red cell therefore names an adapter that diverged from the
runtime's lifecycle semantics.

Offline by construction: ``semantics`` marker only (no ``integration``), so
these cells ride the required offline semantics CI selection.
"""

from __future__ import annotations

import functools
import json
from typing import Any

import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.agent import intent_id_for
from beam_agents.core.dofn import DETAIL_NO_CONTINUATION, REASON_ORPHANED, _AgentDoFn
from beam_agents.core.loop import ActivationResult
from beam_agents.core.transform import AgentConfig, RunAgent
from beam_agents.testing.chaos import fail_first_matching_commit
from tests.conformance._cells import adapter_params, require_framework
from tests.conformance._registry import (
    LIVE_PROVIDERS,
    AgentBundle,
    LazyCellAgent,
    live_provider_calls,
    provider_for,
    validated_bundle,
)
from tests.conformance._spec import (
    APPROVAL_TIMEOUT_FALLBACK,
    BUNDLE_RETRY_CACHE,
    DIRECT,
    EXECUTED_SIDE_EFFECTS,
    MULTI_TOOL_INLINE,
    RESTART_MID_SUSPENSION,
    SINGLE_SHOT,
    SUSPENSION_RESUME,
    TTL_EXPIRY,
    IntentExpectation,
    ScenarioSpec,
    registry_for,
)
from tests.core._dofn_fakes import FakeBag, FakeSum, FakeTimer, FakeValue, scripted_clock

# LangGraph cells compile a graph per activation on top of the streaming
# DirectRunner's own overhead; the default 30s budget is too tight for the
# slowest cells on a loaded CI runner.
pytestmark = [pytest.mark.semantics, pytest.mark.timeout(120)]

_KEY = b"conformance-key"


@pytest.fixture(autouse=True)
def _clean_cell_state() -> None:
    EXECUTED_SIDE_EFFECTS.clear()
    LIVE_PROVIDERS.clear()


def _streaming_pipeline() -> BeamTestPipeline:
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    return BeamTestPipeline(options=options)


def _keyed(pcoll: Any) -> Any:
    import apache_beam as beam

    return pcoll | beam.WithKeys(lambda e: e.entity_key).with_output_types(
        tuple[bytes, AgentEnvelope]
    )


def _event(t_ms: int, payload: bytes = b"go") -> TimestampedValue:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(envelope, t_ms / 1000)


def _tool_result_envelope(intent_id: str, t_ms: int, payload: bytes = b"ack") -> AgentEnvelope:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=t_ms)
    envelope.tool_result.intent_id = intent_id
    envelope.tool_result.entity_key = _KEY
    envelope.tool_result.payload = payload
    envelope.tool_result.status = ToolResult.OK
    return envelope


def _tool_result(intent_id: str, t_ms: int, payload: bytes = b"ack") -> TimestampedValue:
    return TimestampedValue(_tool_result_envelope(intent_id, t_ms, payload), t_ms / 1000)


def _approval(intent_id: str, t_ms: int, *, approved: bool = True) -> TimestampedValue:
    envelope = AgentEnvelope(entity_key=_KEY, event_time_ms=t_ms)
    envelope.approval.intent_id = intent_id
    envelope.approval.approved = approved
    envelope.approval.decided_at_ms = t_ms
    return TimestampedValue(envelope, t_ms / 1000)


def _cell_config(adapter_name: str, spec: ScenarioSpec) -> AgentConfig:
    return AgentConfig(
        provider_factory=functools.partial(provider_for, adapter_name, spec.name),
        ttl_ms=spec.memory_ttl_ms,
        tool_registry=registry_for(spec),
    )


def _cell_agent(adapter_name: str, spec: ScenarioSpec) -> LazyCellAgent:
    # The pipeline cells hold only this picklable handle; the bundle itself is
    # built (and equivalence-checked, task 1.3) worker-side on first
    # activation. Validate once test-side too, so a spec/factory divergence
    # fails the cell before any pipeline runs.
    validated_bundle(adapter_name, spec.name)
    return LazyCellAgent(adapter_name, spec.name)


def _expected_intent_id(spec: ScenarioSpec, expectation: IntentExpectation) -> str:
    return intent_id_for(_KEY, expectation.seq, expectation.step_index)


# -- assert_that checks (module-level + functools.partial: they must survive
#    Beam's serialization of the assertion DoFn) ----------------------------------


def _check_llm_calls(expected_real: int, actual: object) -> None:
    """Committed LLM_CALL traces: exactly the scripted number of real provider
    calls (``cache_hit == "false"``); cache hits are allowed extras."""
    llm_events = [t for t in actual if t.event_type == TraceEvent.LLM_CALL]  # type: ignore[attr-defined]
    cache_hits = [e.attributes["beam_agents.cache_hit"] for e in llm_events]
    assert cache_hits.count("false") == expected_real, (
        f"expected exactly {expected_real} real provider call(s) in committed traces, "
        f"got cache_hit values {cache_hits!r}"
    )


def _check_single_intent(expected_id: str, expected: IntentExpectation, actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert len(items) == 1, f"expected exactly one committed intent, got {items!r}"
    intent = items[0]
    assert intent.intent_id == expected_id, (
        f"intent_id diverged from the deterministic formula: {intent.intent_id!r} "
        f"!= intent_id_for(key, {expected.seq}, {expected.step_index})"
    )
    assert intent.seq == expected.seq
    assert intent.step_index == expected.step_index
    assert intent.tool_name == expected.tool_name
    assert intent.kind == expected.kind
    # Byte-identity: re-mint the intent from its own scope; a match proves the
    # commit was reproducible, not merely self-consistent.
    reminted = ToolIntent(
        intent_id=expected_id,
        entity_key=_KEY,
        seq=expected.seq,
        step_index=expected.step_index,
        tool_name=intent.tool_name,
        args_json=intent.args_json,
        created_at_ms=intent.created_at_ms,
        expires_at_ms=intent.expires_at_ms,
        attempt=0,
        kind=intent.kind,
        trace_id=intent.trace_id,
    )
    assert intent.SerializeToString(deterministic=True) == reminted.SerializeToString(
        deterministic=True
    )


def _check_orphaned(expected_detail: str, actual: object) -> None:
    items = [(e.reason, e.detail) for e in actual]  # type: ignore[attr-defined]
    assert items == [(REASON_ORPHANED, expected_detail)], (
        f"expected the late decision to surface as orphaned_result, got {items!r}"
    )


# ---------------------------------------------------------------------------------
# Scenario: single-shot fast path
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("adapter_name", adapter_params(SINGLE_SHOT, DIRECT))
def test_single_shot(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = SINGLE_SHOT
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            # Nothing may fire after the fast path: a suspension smuggled in
            # here would produce no terminal at all (Suspend emits nothing),
            # so the single output below also proves the continuation path
            # was never taken.
            .advance_processing_time(5)
            .advance_watermark_to_infinity()
        )
        out = _keyed(p | stream) | RunAgent(
            _cell_agent(adapter_name, spec), config=_cell_config(adapter_name, spec)
        )
        assert_that(out.output, equal_to(list(spec.expected_outputs)), label="output")
        assert_that(out.intents, equal_to([]), label="no-intents")
        assert_that(out.errors, equal_to([]), label="no-errors")
        assert_that(
            out.traces,
            functools.partial(_check_llm_calls, spec.expected_provider_calls),
            label="traces",
        )


# ---------------------------------------------------------------------------------
# Scenario: multi-tool inline (fast path, read-only tools)
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("adapter_name", adapter_params(MULTI_TOOL_INLINE, DIRECT))
def test_multi_tool_inline(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = MULTI_TOOL_INLINE
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            .advance_watermark_to_infinity()
        )
        out = _keyed(p | stream) | RunAgent(
            _cell_agent(adapter_name, spec), config=_cell_config(adapter_name, spec)
        )
        # The terminal embeds both tool results, so inline execution (and its
        # ordering) is proven uniformly for every adapter; the scripted model
        # turns are proven in the committed traces.
        assert_that(out.output, equal_to(list(spec.expected_outputs)), label="output")
        assert_that(out.intents, equal_to([]), label="no-intents")
        assert_that(out.errors, equal_to([]), label="no-errors")
        assert_that(
            out.traces,
            functools.partial(_check_llm_calls, spec.expected_provider_calls),
            label="traces",
        )


# ---------------------------------------------------------------------------------
# Scenario: suspension / resume with deterministic intents
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("adapter_name", adapter_params(SUSPENSION_RESUME, DIRECT))
def test_suspension_resume(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = SUSPENSION_RESUME
    expectation = spec.expected_intents[0]
    intent_id = _expected_intent_id(spec, expectation)
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            .add_elements([_tool_result(intent_id, 2000)])
            .advance_watermark_to_infinity()
        )
        out = _keyed(p | stream) | RunAgent(
            _cell_agent(adapter_name, spec), config=_cell_config(adapter_name, spec)
        )
        assert_that(out.output, equal_to(list(spec.expected_outputs)), label="output")
        assert_that(out.errors, equal_to([]), label="no-errors")
        assert_that(
            out.intents,
            functools.partial(_check_single_intent, intent_id, expectation),
            label="intent",
        )
        assert_that(
            out.traces,
            functools.partial(_check_llm_calls, spec.expected_provider_calls),
            label="traces",
        )
    assert EXECUTED_SIDE_EFFECTS == [], "the side-effect tool's body executed inside the pipeline"


# ---------------------------------------------------------------------------------
# Scenario: approval timeout fallback (fail-closed HITL timer)
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("adapter_name", adapter_params(APPROVAL_TIMEOUT_FALLBACK, DIRECT))
def test_approval_timeout_fallback(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = APPROVAL_TIMEOUT_FALLBACK
    expectation = spec.expected_intents[0]
    intent_id = _expected_intent_id(spec, expectation)
    with _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(0)])  # suspends; deadline = 0 + hitl_timeout_ms
            .advance_processing_time(5)  # past the deadline -> fail-closed fallback
            .add_elements([_approval(intent_id, 100)])  # late decision
            .advance_watermark_to_infinity()
        )
        out = _keyed(p | stream) | RunAgent(
            _cell_agent(adapter_name, spec), config=_cell_config(adapter_name, spec)
        )
        # The fallback terminal is the only output: the continuation was
        # cleared, so the late decision cannot mint a second one.
        assert_that(out.output, equal_to(list(spec.expected_outputs)), label="fallback")
        assert_that(
            out.intents,
            functools.partial(_check_single_intent, intent_id, expectation),
            label="approval-intent",
        )
        assert_that(
            out.errors,
            functools.partial(_check_orphaned, f"{DETAIL_NO_CONTINUATION}:{intent_id}"),
            label="orphaned",
        )


# ---------------------------------------------------------------------------------
# Scenario: bundle retry replay-cache determinism (chaos-forced resume retry)
# ---------------------------------------------------------------------------------


def _is_resume_commit(result: ActivationResult) -> bool:
    # The resume is this scenario's only "completed" commit; the pre-suspend
    # activation commits with status "suspended".
    return result.status == "completed"


@pytest.mark.parametrize("adapter_name", adapter_params(BUNDLE_RETRY_CACHE, DIRECT))
def test_bundle_retry_cache(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = BUNDLE_RETRY_CACHE
    expectation = spec.expected_intents[0]
    intent_id = _expected_intent_id(spec, expectation)
    with fail_first_matching_commit(_is_resume_commit), _streaming_pipeline() as p:
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            .add_elements([_tool_result(intent_id, 2000)])
            .advance_watermark_to_infinity()
        )
        out = _keyed(p | stream) | RunAgent(
            _cell_agent(adapter_name, spec), config=_cell_config(adapter_name, spec)
        )
        assert_that(out.output, equal_to(list(spec.expected_outputs)), label="output")
        assert_that(out.errors, equal_to([]), label="no-errors")
        assert_that(
            out.intents,
            functools.partial(_check_single_intent, intent_id, expectation),
            label="intent-bytes",
        )
        assert_that(
            out.traces,
            functools.partial(_check_llm_calls, spec.expected_provider_calls),
            label="traces",
        )
    # THE claim: the chaos-forced retry added zero real provider calls. The
    # DirectRunner is in-process, so this counts every FakeLLM invocation —
    # including any a discarded bundle attempt made, which committed traces
    # can never show.
    assert live_provider_calls() == spec.expected_provider_calls, (
        f"the retried resume re-hit the provider: {live_provider_calls()} real "
        f"call(s) observed, expected {spec.expected_provider_calls}"
    )
    assert EXECUTED_SIDE_EFFECTS == []


# ---------------------------------------------------------------------------------
# Scenario: TTL expiry (working-memory GC on watermark passage)
# ---------------------------------------------------------------------------------


@pytest.mark.parametrize("adapter_name", adapter_params(TTL_EXPIRY, DIRECT))
def test_ttl_expiry(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = TTL_EXPIRY
    with _streaming_pipeline() as p:
        # Watermark advances are in SECONDS; TTL marks in ms. Event 1 arms the
        # TTL at 1.1s, event 2 (before the mark) re-arms at 1.15s and must see
        # the written memory; the 1.5s advance fires the wipe; event 3 must
        # see empty memory.
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([_event(1000)])
            .add_elements([_event(1050)])
            .advance_watermark_to(1.5)
            .add_elements([_event(2000)])
            .advance_watermark_to_infinity()
        )
        out = _keyed(p | stream) | RunAgent(
            _cell_agent(adapter_name, spec), config=_cell_config(adapter_name, spec)
        )
        assert_that(out.output, equal_to(list(spec.expected_outputs)), label="output")
        assert_that(out.errors, equal_to([]), label="no-errors")
        assert_that(out.intents, equal_to([]), label="no-intents")


# ---------------------------------------------------------------------------------
# Scenario: restart mid-suspension (fresh-DoFn drive over committed state)
# ---------------------------------------------------------------------------------


class _DoFnDrive:
    """One ``_AgentDoFn`` over fake state handles (the ``_dofn_fakes`` drive).

    ``carry_from`` round-trips another drive's committed state through
    serialized proto bytes, so the second instance provably resumes from
    committed protobuf state alone — nothing object-identical survives.
    """

    def __init__(
        self, bundle: AgentBundle, spec: ScenarioSpec, *, carry_from: _DoFnDrive | None = None
    ) -> None:
        self._bundle = bundle
        self.dofn = _AgentDoFn(
            bundle.agent,
            provider_factory=lambda: bundle.provider,
            ttl_ms=spec.memory_ttl_ms,
            tool_registry=bundle.tool_registry,
            monotonic_ns=scripted_clock(),
        )
        if carry_from is None:
            self.memory = FakeValue(MemoryBlob())
            self.continuation = FakeValue(None)
            self.llm_cache = FakeValue(LlmCacheBlob())
            self.pending = FakeBag()
            self.seq = FakeSum(0)
        else:
            self.memory = FakeValue(
                MemoryBlob.FromString(carry_from.memory.value.SerializeToString())
            )
            committed = carry_from.continuation.value
            self.continuation = FakeValue(
                Continuation.FromString(committed.SerializeToString())
                if committed is not None
                else None
            )
            self.llm_cache = FakeValue(
                LlmCacheBlob.FromString(carry_from.llm_cache.value.SerializeToString())
            )
            self.pending = FakeBag(
                [ToolIntent.FromString(i.SerializeToString()) for i in carry_from.pending.items]
            )
            self.seq = FakeSum(carry_from.seq.value)

    def process(self, envelope: AgentEnvelope) -> list[Any]:
        self.dofn.setup()
        try:
            return list(
                self.dofn.process(
                    (_KEY, envelope),
                    memory=self.memory,
                    continuation=self.continuation,
                    llm_cache=self.llm_cache,
                    pending=self.pending,
                    seq=self.seq,
                    ttl_timer=FakeTimer(),
                    hitl_timer=FakeTimer(),
                )
            )
        finally:
            self.dofn.teardown()


def _split(emitted: list[Any]) -> tuple[list[Any], dict[str, list[Any]]]:
    import apache_beam as beam

    main = [e for e in emitted if not isinstance(e, beam.pvalue.TaggedOutput)]
    tagged: dict[str, list[Any]] = {"intents": [], "traces": [], "errors": []}
    for element in emitted:
        if isinstance(element, beam.pvalue.TaggedOutput):
            tagged.setdefault(element.tag, []).append(element.value)
    return main, tagged


def _run_shape(emitted: list[Any]) -> dict[str, Any]:
    """The comparable observable surface of one run's emissions."""
    main, tagged = _split(emitted)
    return {
        "outputs": main,
        "intents": [i.SerializeToString(deterministic=True) for i in tagged["intents"]],
        "errors": tagged["errors"],
        # Trace shape: type/step/span identity. Serialized bytes would also
        # compare attribute maps whose duration values ride a scripted clock,
        # but identity + ordering is the parity claim.
        "traces": [(t.event_type, t.step_index, t.span_id, t.trace_id) for t in tagged["traces"]],
    }


@pytest.mark.parametrize("adapter_name", adapter_params(RESTART_MID_SUSPENSION, DIRECT))
def test_restart_mid_suspension(adapter_name: str) -> None:
    require_framework(adapter_name)
    spec = RESTART_MID_SUSPENSION
    expectation = spec.expected_intents[0]
    intent_id = _expected_intent_id(spec, expectation)
    event = AgentEnvelope(entity_key=_KEY, event_time_ms=1000, external_event=b"go")
    result = _tool_result_envelope(intent_id, 2000)

    # Baseline: the uninterrupted suspend/resume run, same DoFn instance.
    baseline = _DoFnDrive(validated_bundle(adapter_name, spec.name), spec)
    baseline_emitted = baseline.process(event) + baseline.process(result)

    # Restarted: the suspending instance (bridge thread, caches, adapter
    # object) is discarded after its commit; a FRESH DoFn over a fresh agent
    # bundle is built on the committed state contents alone, and the matching
    # result is delivered to it.
    first = _DoFnDrive(validated_bundle(adapter_name, spec.name), spec)
    restarted_emitted = first.process(event)
    second = _DoFnDrive(validated_bundle(adapter_name, spec.name), spec, carry_from=first)
    restarted_emitted += second.process(result)

    baseline_shape = _run_shape(baseline_emitted)
    restarted_shape = _run_shape(restarted_emitted)
    assert restarted_shape["outputs"] == baseline_shape["outputs"] == list(spec.expected_outputs), (
        f"restart changed the terminal output: baseline {baseline_shape['outputs']!r}, "
        f"restarted {restarted_shape['outputs']!r}"
    )
    assert restarted_shape["intents"] == baseline_shape["intents"], (
        "restart changed the committed intent bytes"
    )
    assert restarted_shape["errors"] == baseline_shape["errors"] == []
    assert restarted_shape["traces"] == baseline_shape["traces"], (
        f"restart changed the trace shape: baseline {baseline_shape['traces']!r}, "
        f"restarted {restarted_shape['traces']!r}"
    )
    # The intent minted before the restart is the deterministic one.
    parsed = ToolIntent.FromString(restarted_shape["intents"][0])
    assert parsed.intent_id == intent_id
    assert json.loads(parsed.args_json) == dict(spec.turns[0].args)
    assert EXECUTED_SIDE_EFFECTS == []
