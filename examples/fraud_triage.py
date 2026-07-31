"""Fraud triage with human approval: suspension, resume, and the fail-closed timeout.

Two accounts each send one suspicious transaction. The agent triages via a
scripted model call, stages an approval request, and suspends with an explicit
deadline. Account A's approval arrives in time and the resumed activation
freezes the account; account B's approval never arrives, so when the scripted
clock passes the deadline the runtime's default deny route emits its
deterministic fallback output — a timeout is an explicit outcome, never a
silent drop.

The approvals topic is played by a `TestStream` branch here. In production the
effector consumes the approval intent off `.intents`, routes it to a human, and
publishes the decision back onto the pipeline's approvals topic *already
carrying* the pending `intent_id`; this module computes that id with the
runtime's `intent_id_for` only because there is no effector in an offline
example — the id is a pure function of `(entity_key, seq, step_index)`, which
is the whole effectively-once argument in one line.

Run it offline, with no credentials and no docker:

    uv run python -m examples.fraud_triage

Every function a pipeline references is module-level so the DirectRunner can
pickle it by reference.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from beam_agents import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, match_contains, respond_with

ACCOUNT_A = b"acct-a"
ACCOUNT_B = b"acct-b"

# How long the agent waits for a human decision before failing closed.
APPROVAL_TIMEOUT_MS = 30_000

# Account A suspends on its first activation (seq=0); the triage model call
# consumes step 0, so the approval intent is minted at step 1. In production
# the effector publishes the decision with this id already attached — computing
# it here just demonstrates that the id is deterministic.
APPROVAL_INTENT_ID = intent_id_for(ACCOUNT_A, seq=0, step_index=1)


def make_provider() -> FakeLLM:
    """Scripted triage: wire transfers read as suspicious, everything else clears."""
    return FakeLLM(
        [
            (match_contains("wire-transfer"), respond_with(b"suspicious")),
            (match_any(), respond_with(b"ok")),
        ]
    )


async def triage(ctx: ActivationContext) -> Complete | Suspend:
    """Triage a transaction; suspend for approval when the model flags it.

    On resume the human's decision is on `ctx.resume_approval`, delivered on
    the same key the suspension committed under.
    """
    if ctx.is_resume:
        approval = ctx.resume_approval
        if approval is not None and approval.approved:
            return Complete(output=b"freeze:" + ctx.entity_key)
        return Complete(output=b"release:" + ctx.entity_key)

    verdict = await ctx.call_model(
        LlmRequest(
            model_id="fake-triage-model",
            messages=[ctx.single_event.decode()],
            tools_schema=None,
            sampling_params=None,
        )
    )
    if verdict.response != b"suspicious":
        return Complete(output=b"cleared:" + ctx.entity_key)

    # Stage the approval request (an APPROVAL-kind ToolIntent on `.intents`)
    # and suspend. The runtime persists a continuation and arms the fail-closed
    # HITL timer; nothing external has happened yet.
    ctx.request_approval('{"action":"freeze","reason":"suspicious wire transfer"}')
    return Suspend(snapshot=b"awaiting-approval", timeout_ms=APPROVAL_TIMEOUT_MS)


def _transaction(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    envelope = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(envelope, t_ms / 1000)


def _approval(key: bytes, intent_id: str, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    """A human decision re-entering on the account's key — the approvals topic's job."""
    envelope = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    envelope.approval.intent_id = intent_id
    envelope.approval.approved = True
    envelope.approval.approver = "analyst@example.test"
    envelope.approval.decided_at_ms = t_ms
    return TimestampedValue(envelope, t_ms / 1000)


def scripted_stream() -> TestStream:
    """The harness: two transactions, one in-time approval, one elapsed deadline.

    `TestStream` scripts both clocks. The watermark carries event time; the
    processing-time advance is what fires account B's real-time HITL timer —
    no `sleep()`, no wall clock.
    """
    return (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_transaction(ACCOUNT_A, b'{"kind":"wire-transfer","amount":9500}', 1_000)])
        .add_elements([_transaction(ACCOUNT_B, b'{"kind":"wire-transfer","amount":8200}', 2_000)])
        # The analyst approves account A's freeze well inside the deadline.
        .add_elements([_approval(ACCOUNT_A, APPROVAL_INTENT_ID, t_ms=5_000)])
        # Nobody answers for account B: advance the scripted clock past its
        # 30s deadline so the HITL timer fires the default deny route.
        .advance_processing_time(60)
        .advance_watermark_to_infinity()
    )


def build(pipeline: beam.Pipeline) -> RunAgentOutputs:
    """Wire the scripted stream, keyed by account, into `RunAgent`."""
    keyed = (
        pipeline
        | "Transactions" >> scripted_stream()
        | "KeyByAccount"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )
    return keyed | RunAgent(triage, config=AgentConfig(provider_factory=make_provider))


def main() -> None:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as pipeline:
        outputs = build(pipeline)
        outputs.output | "Print" >> beam.Map(print)


if __name__ == "__main__":
    main()
