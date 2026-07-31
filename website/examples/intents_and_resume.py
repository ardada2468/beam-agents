"""Side effects: emit an intent, suspend, resume when the result comes back.

An agent never performs an external write itself. It stages a `ToolIntent` —
a declarative request — and suspends. The intent leaves on `.intents`, an
external effector executes it, and the resulting `ToolResult` re-enters the
pipeline on the same key, resuming the activation where it stopped.

The reason this is the only effect path is the intent id. It is
`uuid5(namespace, key|seq|step_index)` — a pure function of the activation's
position, never a clock or a counter — so a replayed bundle that walks the same
path mints byte-identical intents and the effector deduplicates on them. That
is the whole effectively-once argument, and it is why calling a
`side_effect=True` tool directly raises instead of working.

The loop runs through the message bus, not the DAG: Beam DAGs are acyclic, so
resumption is a new element on the same key rather than a cycle in the graph.

This example scripts the result's arrival with `TestStream` so the ordering is
deterministic — the same technique the repository's own timer tests use. In a
real deployment the result arrives from the effector's results topic.

Run it:  python website/examples/intents_and_resume.py
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from beam_agents import AgentConfig, RunAgent
from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.model.fake import FakeLLM, match_any, respond_with

INTENT_TTL_MS = 60_000


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"ok"))])


# region: agent
async def refund(ctx: ActivationContext) -> Complete | Suspend:
    """Request a refund, then report what the effector did.

    Two activations, one logical unit of work. `ctx.act` stages the intent and
    returns its deterministic id; `Suspend` persists the continuation and arms
    the fail-closed timeout. Nothing has been written to the outside world when
    this function returns — the intent is a request, not an effect.
    """
    if not ctx.is_resume:
        ctx.act("payments.refund", '{"amount": 4200}', ttl_ms=INTENT_TTL_MS)
        return Suspend(snapshot=b"awaiting-refund", adapter="example", timeout_ms=30_000)

    # On resume the same activation continues: same key, same seq, and the
    # snapshot it persisted is available as ctx.snapshot.
    assert ctx.resume_result is not None
    return Complete(output=b"refunded:" + ctx.resume_result.payload)


# endregion: agent


def _event(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(env, t_ms / 1000)


# region: result
def _tool_result(key: bytes, intent_id: str, payload: bytes, t_ms: int):
    """One effector result, shaped as it arrives from the results topic."""
    env = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    env.tool_result.intent_id = intent_id
    env.tool_result.entity_key = key
    env.tool_result.payload = payload
    env.tool_result.status = ToolResult.OK
    return TimestampedValue(env, t_ms / 1000)


# endregion: result


def main() -> None:
    # The id is computable ahead of time precisely because it is deterministic:
    # first activation of key b"acct-1" (seq 0), first staged intent (step 0).
    intent_id = intent_id_for(b"acct-1", 0, 0)

    stream = (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([_event(b"acct-1", b"chargeback", 1_000)])
        .add_elements([_tool_result(b"acct-1", intent_id, b"txn-88", 1_500)])
        .advance_watermark_to_infinity()
    )

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        keyed = (
            pipeline
            | stream
            | "Key"
            >> beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        )
        outputs = keyed | "Agent" >> RunAgent(
            refund,
            config=AgentConfig(provider_factory=make_provider, ttl_ms=1_000_000_000),
        )

        # The suspension emits no main output; the resume completes.
        assert_that(outputs.output, equal_to([b"refunded:txn-88"]), label="output")

        staged = outputs.intents | "Describe" >> beam.Map(
            lambda intent: (intent.tool_name, intent.intent_id)
        )
        assert_that(staged, equal_to([("payments.refund", intent_id)]), label="intents")

    print("intents_and_resume: ok")


if __name__ == "__main__":
    main()
