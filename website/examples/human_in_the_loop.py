"""Human-in-the-loop: ask a person, and decide what happens when nobody answers.

`ctx.request_approval` stages an approval intent on the configured channel and
`Suspend` parks the activation. If the approval arrives, the agent resumes with
it. If it never arrives, the HITL timer fires and `HitlPolicy.on_timeout`
decides — `Deny` (emit deterministic bytes and end), `Drop` (emit nothing and
record the timeout on `.errors`), or `Escalate` (ask again, louder, bounded by
`max_escalations`).

Timeouts fail closed at both layers. This file shows layer 1, the in-pipeline
timer. Layer 2 is the effector refusing an intent past its `expires_at_ms`, so a
late approval cannot cause an effect after the runtime has already given up.

`on_timeout` must be pure, synchronous, and picklable — a module-level function,
never a lambda. That is a correctness requirement rather than a style rule: a
timer callback re-executes when its bundle is retried, and a fallback that read
a clock or called the model would make the retry diverge from the original.
Every value the policy could need is carried on the `FallbackContext`.

Run it:  python website/examples/human_in_the_loop.py
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents import AgentConfig, Deny, FallbackContext, HitlPolicy, RunAgent
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.hitl import Route
from beam_agents.model.fake import FakeLLM, match_any, respond_with

APPROVAL_TTL_MS = 60_000
SUSPENSION_TIMEOUT_MS = 1_000


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"ok"))])


# region: policy
def deny_on_timeout(fallback: FallbackContext) -> Route:
    """Route an unanswered approval to a deterministic denial.

    Pure and synchronous. It reads only the `FallbackContext` it is handed —
    which carries the suspended `seq`, the persisted snapshot, the elapsed
    deadline, the timer's fire time, and the intent ids nothing answered — so a
    retried timer bundle reaches the same decision.
    """
    return Deny(output=b"denied:no-approval:" + str(fallback.seq).encode())


# endregion: policy


# region: agent
async def large_transfer(ctx: ActivationContext) -> Complete | Suspend:
    """Hold a large transfer until a human approves it."""
    if not ctx.is_resume:
        ctx.request_approval('{"amount": 250000}', ttl_ms=APPROVAL_TTL_MS)
        return Suspend(
            snapshot=b"awaiting-approval",
            adapter="example",
            timeout_ms=SUSPENSION_TIMEOUT_MS,
        )

    approval = ctx.resume_approval
    assert approval is not None
    return Complete(output=b"approved" if approval.approved else b"rejected")


# endregion: agent


def _event(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


# region: approval
def _approval(
    key: bytes, intent_id: str, *, approved: bool, t_ms: int
) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    # `Approval` carries no entity_key of its own: the envelope's key is what
    # routes it, and the intent id is what matches it to a continuation.
    env.approval.intent_id = intent_id
    env.approval.approved = approved
    env.approval.approver = "ops@example.invalid"
    return TimestampedValue(env, t_ms / 1000)


# endregion: approval


def main() -> None:
    # region: intent-id
    # Two keys: one gets an answer, one never does. The answered key's approval
    # can be addressed before the pipeline exists, because the intent id is a
    # pure function of (key, seq, step_index): first activation of the key is
    # seq 0, and the approval it stages is step 0.
    answered = intent_id_for(b"acct-answered", 0, 0)
    # endregion: intent-id

    # region: stream
    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"acct-answered", b"transfer", 1_000)])
        .add_elements([_event(b"acct-silent", b"transfer", 1_000)])
        .add_elements([_approval(b"acct-answered", answered, approved=True, t_ms=1_200)])
        # Push processing time past the suspension timeout so the silent key's
        # HITL timer fires. Scripted, never a sleep: the test controls both
        # clocks, so the outcome is deterministic.
        .advance_processing_time(60)
        .advance_watermark_to_infinity()
    )
    # endregion: stream

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    # region: policy-config
    # `approval_channel` names where the effector routes the request — a queue,
    # a pager — not a registered tool, so nothing is resolved or executed for
    # it in-pipeline. `intent_ttl_ms` is the default expiry stamped onto staged
    # intents, and it is layer 2 of the fail-closed rule: past `expires_at_ms`
    # the effector refuses the intent rather than acting on it.
    policy = HitlPolicy(
        timeout_ms=SUSPENSION_TIMEOUT_MS,
        intent_ttl_ms=APPROVAL_TTL_MS,
        approval_channel="approval",
        on_timeout=deny_on_timeout,
    )
    # endregion: policy-config

    with beam.Pipeline(options=options) as pipeline:
        keyed = (
            pipeline
            | stream
            | "Key"
            >> beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        )
        # region: wiring
        outputs = keyed | "Agent" >> RunAgent(
            large_transfer,
            config=AgentConfig(
                provider_factory=make_provider,
                hitl_policy=policy,
                ttl_ms=1_000_000_000,
            ),
        )

        # One assertion covers both keys: the answered one resumes and
        # completes, the silent one is routed by the policy. Neither outcome is
        # a timing accident — both are the only value the pipeline can produce.
        assert_that(
            outputs.output,
            equal_to([b"approved", b"denied:no-approval:0"]),
            label="approved-and-denied",
        )
        # endregion: wiring

    print("human_in_the_loop: ok")


if __name__ == "__main__":
    main()
