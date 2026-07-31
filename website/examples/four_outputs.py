"""The four outputs: `.output`, `.intents`, `.traces`, and `.errors`.

`RunAgent` returns a `RunAgentOutputs` with four named `PCollection`s, and a
pipeline is expected to consume all of them:

    .output   terminal agent outputs (bytes)
    .intents  ToolIntent side-effect requests, bound for the outbox topic
    .traces   TraceEvent observability records
    .errors   ActivationError dead letters

`.errors` is the one people forget. Element-level failures never fail the
bundle — they are routed here — and a dead letter means the activation
committed *nothing*: no memory write, no intent, no output. The record is the
only evidence the key was touched at all.

This example drives three keys down three different paths in one bounded
pipeline, and asserts on every stream.

Run it:  python website/examples/four_outputs.py
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from beam_agents import AgentConfig, RunAgent
from beam_agents._protos import AgentEnvelope, TraceEvent
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

INTENT_TTL_MS = 60_000


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"assessed"))])


# region: agent
async def route_by_event(ctx: ActivationContext) -> Complete:
    """Take a different path per event so one pipeline exercises each output."""
    if ctx.event == b"BROKEN":
        # Routed to `.errors` as `activation_error`. Nothing this activation
        # staged — including this memory write — reaches durable state.
        ctx.memory.set("scratch", b"never-persisted")
        raise RuntimeError("downstream schema changed")

    if ctx.event == b"NOTIFY":
        ctx.act("slack.post", '{"channel": "#ops"}', ttl_ms=INTENT_TTL_MS)
        return Complete(output=b"notified")

    response = await ctx.call_model(
        LlmRequest(
            model_id="fake-1",
            messages=[ctx.event.decode()],
            tools_schema=None,
            sampling_params=None,
        )
    )
    return Complete(output=response.response)


# endregion: agent


def _event(key: bytes, payload: bytes) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=1_000, external_event=payload)


def main() -> None:
    with beam.Pipeline() as pipeline:
        keyed = (
            pipeline
            | "Events"
            >> beam.Create(
                [
                    _event(b"k-model", b"review"),
                    _event(b"k-notify", b"NOTIFY"),
                    _event(b"k-broken", b"BROKEN"),
                ]
            )
            | "Key"
            >> beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        )

        # region: outputs
        outputs = keyed | "Agent" >> RunAgent(
            route_by_event, config=AgentConfig(provider_factory=make_provider)
        )

        assert_that(outputs.output, equal_to([b"assessed", b"notified"]), label="output")

        intents = outputs.intents | "IntentNames" >> beam.Map(
            lambda intent: (intent.entity_key, intent.tool_name)
        )
        assert_that(intents, equal_to([(b"k-notify", "slack.post")]), label="intents")

        # An ActivationError names the key, why it failed, and the element's
        # event time — never a wall clock.
        errors = outputs.errors | "ErrorShape" >> beam.Map(
            lambda error: (error.entity_key, error.reason, error.event_time_ms)
        )
        assert_that(errors, equal_to([(b"k-broken", "activation_error", 1_000)]), label="errors")
        # endregion: outputs

        # Traces show the atomic-commit rule from the outside. A committed
        # activation emits its whole span set — START, whatever happened in the
        # middle, END. The failed one emits a single ERROR event: the traces it
        # staged before raising were discarded with the rest of its effects,
        # exactly like its memory write.
        traced = outputs.traces | "TraceShape" >> beam.Map(
            lambda event: (event.entity_key, event.event_type)
        )
        assert_that(
            traced,
            equal_to(
                [
                    (b"k-model", TraceEvent.ACTIVATION_START),
                    (b"k-model", TraceEvent.LLM_CALL),
                    (b"k-model", TraceEvent.ACTIVATION_END),
                    (b"k-notify", TraceEvent.ACTIVATION_START),
                    (b"k-notify", TraceEvent.INTENT_EMITTED),
                    (b"k-notify", TraceEvent.ACTIVATION_END),
                    (b"k-broken", TraceEvent.ERROR),
                ]
            ),
            label="traces",
        )

    print("four_outputs: ok")


if __name__ == "__main__":
    main()
