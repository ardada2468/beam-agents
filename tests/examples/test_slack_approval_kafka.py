"""The compose-Redpanda closed loop for `examples/slack_approval` (`-m integration`).

Requires `make compose-up` (Redpanda on localhost:19092). The demo agent's
approval intent crosses a real Kafka topic, the surface (real Kafka source and
sink, fake gateway scripted to approve) publishes the approval envelope to a
real approvals topic, and the envelope bytes read back off that topic —
byte-for-byte what the surface put on the wire — re-injected into the demo
pipeline on the same key resume the suspended activation with the approved
verdict. FakeLLM only; nothing beyond the existing compose services.

The intent reaches the channel topic through a plain producer keyed by the raw
``entity_key`` — exactly how ``WriteIntents`` keys the outbox — rather than
through ``WriteIntents`` itself: the DirectRunner cross-language Kafka write is
blocked by an upstream Beam defect, root-caused and xfail-tracked in
`tests/actions/test_write_intents_integration.py`. The intent bytes themselves
are the demo activation's own staging: the offline doc-contract leg proves the
derivation used here is byte-identical to what the pipeline commits
(`test_slack_approval.py::test_the_demo_activation_stages_exactly_the_intent_the_surface_consumes`).

This module imports the same example modules `docs/examples/slack-approval.md`
walks through, so the documented demo cannot drift from the example silently.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.core.agent import intent_id_for
from beam_agents.core.transform import RunAgent
from beam_agents.effector.sinks import KafkaMessageSink
from beam_agents.effector.sources import KafkaIntentSource
from beam_agents.observability import trace_id_for
from examples.slack_approval.agent import (
    APPROVED_OUTPUT,
    DEMO_TTL_MS,
    demo_args_json,
    demo_config,
    refund_agent,
)
from examples.slack_approval.slack import FakeSlackGateway
from examples.slack_approval.surface import ApprovalSurface
from tests.core._dofn_helpers import keyed

# The optional Kafka client is installed in the integration lane only; marker
# deselection happens after collection, so a module-level import would break
# collection in the offline lane.
aiokafka = pytest.importorskip("aiokafka")

pytestmark = [pytest.mark.integration, pytest.mark.slow]

BROKERS = "localhost:19092"
_ENTITY_KEY = b"customer-7"
_ORDER = "order-42"
_APPROVER = "U-ALICE"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _demo_intent(t0_ms: int) -> ToolIntent:
    """The intent the demo activation stages for `_ORDER` at `t0_ms`.

    Same deterministic derivation the runtime uses; held byte-identical to the
    pipeline's own staging by the offline doc-contract leg.
    """
    return ToolIntent(
        intent_id=intent_id_for(_ENTITY_KEY, 0, 0),
        entity_key=_ENTITY_KEY,
        seq=0,
        step_index=0,
        tool_name="approval",
        args_json=demo_args_json(_ORDER),
        created_at_ms=t0_ms,
        expires_at_ms=t0_ms + DEMO_TTL_MS,
        attempt=0,
        kind=ToolIntent.APPROVAL,
        trace_id=trace_id_for(_ENTITY_KEY, 0),
    )


async def _produce_intent(topic: str, intent: ToolIntent) -> None:
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=BROKERS)
    await producer.start()
    try:
        # Keyed by the raw entity_key, exactly as WriteIntents writes the outbox.
        await producer.send_and_wait(
            topic,
            value=intent.SerializeToString(deterministic=True),
            key=intent.entity_key,
        )
    finally:
        await producer.stop()


async def _read_one(topic: str, deadline_s: float = 30.0) -> tuple[bytes, bytes]:
    consumer = aiokafka.AIOKafkaConsumer(
        topic, bootstrap_servers=BROKERS, auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=deadline_s)
        return message.key, message.value
    finally:
        await consumer.stop()


async def _eventually(condition: object, deadline_s: float = 30.0) -> None:
    deadline = asyncio.get_running_loop().time() + deadline_s
    while not condition():  # type: ignore[operator]
        assert asyncio.get_running_loop().time() < deadline, "timed out waiting"
        await asyncio.sleep(0.05)


async def _run_surface_leg(channel_topic: str, approvals_topic: str, decided_at_ms: int) -> None:
    """Phase 2: real Kafka source/sink, fake gateway scripted to approve."""
    run_id = uuid.uuid4().hex[:8]
    source = KafkaIntentSource(BROKERS, channel_topic, f"slack-surface-{run_id}")
    sink = KafkaMessageSink(BROKERS, approvals_topic)
    gateway = FakeSlackGateway()
    surface = ApprovalSurface(source=source, sink=sink, gateway=gateway, channel="#approvals")
    run_task = asyncio.create_task(surface.run())
    try:
        await _eventually(lambda: gateway.posts)
        gateway.push(gateway.click(approved=True, approver=_APPROVER, decided_at_ms=decided_at_ms))
        # The verdict edit happens strictly after the envelope publish, so its
        # arrival means the envelope is durable on the approvals topic.
        await _eventually(lambda: gateway.edits)
    finally:
        surface.stop()
        await run_task


def _check_output_approved(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [APPROVED_OUTPUT], f"unexpected output: {items!r}"


def _check_no_errors(actual: object) -> None:
    items = list(actual)  # type: ignore[call-overload]
    assert items == [], f"unexpected errors: {items!r}"


@pytest.mark.timeout(180)
def test_the_closed_loop_over_redpanda_resumes_the_agent() -> None:
    # Scenario: The closed loop over Redpanda resumes the agent.
    run_id = uuid.uuid4().hex[:8]
    channel_topic = f"slack-approval-requests-{run_id}"
    approvals_topic = f"slack-approvals-{run_id}"
    t0_ms = _now_ms()
    decided_at_ms = t0_ms + 5_000
    intent = _demo_intent(t0_ms)

    async def _phases_1_and_2() -> tuple[bytes, bytes]:
        await _produce_intent(channel_topic, intent)
        await _run_surface_leg(channel_topic, approvals_topic, decided_at_ms)
        return await _read_one(approvals_topic)

    key, payload = asyncio.run(_phases_1_and_2())

    # The envelope on the wire is keyed by the raw entity_key and is exactly
    # the deterministic serialization of the surface's verdict.
    assert key == _ENTITY_KEY
    expected = AgentEnvelope(entity_key=_ENTITY_KEY, event_time_ms=decided_at_ms)
    expected.approval.intent_id = intent.intent_id
    expected.approval.approved = True
    expected.approval.approver = _APPROVER
    expected.approval.decided_at_ms = decided_at_ms
    assert payload == expected.SerializeToString(deterministic=True)

    # Phase 3: those exact bytes, re-injected on the same key, resume the
    # suspended activation with the approved verdict.
    envelope = AgentEnvelope()
    envelope.ParseFromString(payload)
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with BeamTestPipeline(options=options) as p:
        event = AgentEnvelope(
            entity_key=_ENTITY_KEY, event_time_ms=t0_ms, external_event=_ORDER.encode()
        )
        stream = (
            TestStream()
            .advance_watermark_to(0)
            .add_elements([TimestampedValue(event, t0_ms / 1000)])
            .add_elements([TimestampedValue(envelope, envelope.event_time_ms / 1000)])
            .advance_watermark_to_infinity()
        )
        out = keyed(p | stream) | RunAgent(refund_agent, config=demo_config())
        assert_that(out.output, _check_output_approved, label="output")
        assert_that(out.errors, _check_no_errors, label="errors")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q", "-m", "integration"]))
