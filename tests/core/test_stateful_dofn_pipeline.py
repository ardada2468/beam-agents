from __future__ import annotations

from dataclasses import dataclass

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    ToolIntent,
    ToolResult,
)
from beam_agents._protos import (
    RuntimeError as RuntimeErrorProto,
)
from beam_agents.core.coders import register_coders
from beam_agents.core.dofn import _ActivationContext, _ActivationInput, _AgentDoFn


@dataclass
class _PipelineDriver:
    async def run(  # noqa: PLR0911
        self, activation_input: _ActivationInput, context: _ActivationContext
    ) -> None:
        if activation_input.kind == "external_event":
            command = activation_input.envelope.external_event.decode("utf-8")
            if command == "emit":
                context.stage_output((context.entity_key, "emit", context.seq))
                return
            if command == "suspend":
                context.stage_output((context.entity_key, "suspend", context.seq))
                z_intent = ToolIntent(
                    intent_id="z",
                    entity_key=context.entity_key,
                    seq=context.seq,
                    expires_at_ms=8_000,
                )
                a_intent = ToolIntent(
                    intent_id="a",
                    entity_key=context.entity_key,
                    seq=context.seq,
                    expires_at_ms=6_000,
                )
                context.replace_pending([z_intent, a_intent])
                context.set_continuation(
                    Continuation(
                        state_schema_version=1,
                        seq=context.seq,
                        pending_intent_ids=["z", "a"],
                        deadline_ms=7_000,
                    )
                )
                return
            if command == "touch-memory":
                context.memory.set("remember", b"1")
                context.stage_output(("memory", context.seq, True))
                return
            if command == "check-memory":
                context.stage_output(
                    ("memory", context.seq, context.memory.get("remember") is not None)
                )
                return
            if command == "fail":
                context.memory.set("remember", b"2")
                raise RuntimeError("forced failure")
        if activation_input.kind == "tool_result":
            assert activation_input.resumed_intent is not None
            if activation_input.resumed_intent.intent_id == "a":
                b_intent = ToolIntent(
                    intent_id="b",
                    entity_key=context.entity_key,
                    seq=context.seq,
                    expires_at_ms=9_000,
                )
                z_intent = ToolIntent(
                    intent_id="z",
                    entity_key=context.entity_key,
                    seq=context.seq,
                    expires_at_ms=10_000,
                )
                context.replace_pending([z_intent, b_intent])
                context.set_continuation(
                    Continuation(
                        state_schema_version=1,
                        seq=context.seq,
                        pending_intent_ids=["z", "b"],
                        deadline_ms=11_000,
                    )
                )
                context.stage_output((context.entity_key, "resume-a", context.seq))
                return
            context.stage_output((context.entity_key, "tool", context.seq))
            return
        if activation_input.kind == "approval":
            context.stage_output(
                (
                    context.entity_key,
                    "resume-b",
                    context.seq,
                    tuple(context.pending_intents),
                )
            )
            context.replace_pending(())
            context.set_continuation(None)
            return
        if activation_input.kind == "hitl_timeout":
            context.stage_output((context.entity_key, "timeout", context.seq))
            context.replace_pending(())
            context.set_continuation(None)


def _decode_envelope(raw: bytes) -> tuple[bytes, AgentEnvelope]:
    envelope = AgentEnvelope()
    envelope.ParseFromString(raw)
    return envelope.entity_key, envelope


def _external(key: bytes, event_time_ms: int, payload: bytes) -> tuple[bytes, AgentEnvelope]:
    return key, AgentEnvelope(entity_key=key, event_time_ms=event_time_ms, external_event=payload)


def _tool_result(key: bytes, event_time_ms: int, intent_id: str) -> tuple[bytes, AgentEnvelope]:
    return key, AgentEnvelope(
        entity_key=key,
        event_time_ms=event_time_ms,
        tool_result=ToolResult(intent_id=intent_id),
    )


def _approval(key: bytes, event_time_ms: int, intent_id: str) -> tuple[bytes, AgentEnvelope]:
    return key, AgentEnvelope(
        entity_key=key,
        event_time_ms=event_time_ms,
        approval=AgentEnvelope.Approval(intent_id=intent_id, approved=True),
    )


def test_stateful_pipeline_sequences_isolation_and_resume() -> None:
    register_coders()
    dofn = _AgentDoFn(
        driver=_PipelineDriver(),
        memory_ttl_ms=5_000,
        now_ms_fn=lambda: 5_000,
    )
    inputs = [
        _external(b"a", 1_000, b"emit"),
        _external(b"b", 1_000, b"emit"),
        _external(b"a", 2_000, b"suspend"),
        _external(b"a", 3_000, b"emit"),
        _tool_result(b"a", 4_000, "a"),
        _approval(b"a", 5_000, "b"),
    ]
    with BeamTestPipeline() as pipeline:
        result = (
            pipeline
            | "create-inputs" >> beam.Create(inputs)
            | "run-agent" >> beam.ParDo(dofn).with_outputs("errors", main="main")
        )
        assert_that(
            result.main,
            equal_to(
                [
                    (b"a", "emit", 1),
                    (b"b", "emit", 1),
                    (b"a", "suspend", 2),
                    (b"a", "resume-a", 2),
                    (b"a", "resume-b", 2, ("z",)),
                ]
            ),
            label="assert-main",
        )
        assert_that(
            result.errors | "err-types" >> beam.Map(lambda err: err.error_type),
            equal_to([RuntimeErrorProto.BUSY_KEY]),
            label="assert-errors",
        )


def test_ttl_timer_watermark_cleanup_and_failed_activation_preserves_deadline() -> None:
    register_coders()
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue((b"k", b"touch-memory"), Timestamp(0))])
        .advance_watermark_to(2)
        .add_elements([TimestampedValue((b"k", b"fail"), Timestamp(2))])
        .advance_watermark_to(6)
        .add_elements([TimestampedValue((b"k", b"check-memory"), Timestamp(6))])
        .advance_watermark_to_infinity()
    )
    dofn = _AgentDoFn(
        driver=_PipelineDriver(),
        memory_ttl_ms=5_000,
        activation_timeout_s=0.1,
    )
    with BeamTestPipeline() as pipeline:
        typed = (
            pipeline
            | "stream" >> stream
            | "to-envelope"
            >> beam.Map(
                lambda kv, ts=beam.DoFn.TimestampParam: (
                    kv[0],
                    AgentEnvelope(
                        entity_key=kv[0],
                        event_time_ms=int(ts.micros / 1000),
                        external_event=kv[1],
                    ),
                )
            )
        )
        result = typed | "ttl-run" >> beam.ParDo(dofn).with_outputs("errors", main="main")
        assert_that(
            result.main,
            equal_to([("memory", 1, True), ("memory", 2, False)]),
            label="assert-ttl-main",
        )
        assert_that(
            result.errors | "ttl-err-types" >> beam.Map(lambda err: err.error_type),
            equal_to([RuntimeErrorProto.ACTIVATION_FAILED]),
            label="assert-ttl-errors",
        )


@pytest.mark.semantics
def test_hitl_timer_timeout_and_late_result_orphaning() -> None:
    register_coders()
    wait_env = AgentEnvelope(entity_key=b"k", event_time_ms=0, external_event=b"suspend")
    late_result = AgentEnvelope(
        entity_key=b"k",
        event_time_ms=0,
        tool_result=ToolResult(intent_id="a"),
    )
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([wait_env.SerializeToString(deterministic=True)])
        .advance_processing_time(9)
        .add_elements([late_result.SerializeToString(deterministic=True)])
        .advance_watermark_to_infinity()
    )
    dofn = _AgentDoFn(
        driver=_PipelineDriver(),
        memory_ttl_ms=5_000,
        now_ms_fn=lambda: 12_000,
    )
    with BeamTestPipeline() as pipeline:
        result = (
            pipeline
            | "hitl-stream" >> stream
            | "decode-hitl" >> beam.Map(_decode_envelope)
            | "hitl-run" >> beam.ParDo(dofn).with_outputs("errors", main="main")
        )
        assert_that(
            result.main,
            equal_to([(b"k", "suspend", 1), (b"k", "timeout", 1)]),
            label="assert-hitl-main",
        )
        assert_that(
            result.errors | "hitl-errors" >> beam.Map(lambda err: err.error_type),
            equal_to([RuntimeErrorProto.ORPHANED_RESULT]),
            label="assert-hitl-errors",
        )
