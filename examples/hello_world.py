"""The smallest complete `RunAgent` pipeline: one event, one model call, one output.

One `AgentEnvelope` enters, keyed by `entity_key`; the agent awaits a single
model call (served by a scripted, offline `FakeLLM`) and completes. Everything
here is the fast path — no suspension, no side effects, no state carried
between elements.

Run it offline, with no credentials and no docker:

    uv run python -m examples.hello_world

Every function a pipeline references is module-level so the DirectRunner can
pickle it by reference.
"""

from __future__ import annotations

import apache_beam as beam

from beam_agents import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

# The scripted response, and therefore the pipeline's one documented output.
GREETING = b"Hello from the beam-agents runtime!"


def make_provider() -> FakeLLM:
    """A deterministic stand-in for a real provider: every request gets `GREETING`.

    Swapping this factory for one that returns a real `LLMClient` is the only
    change a production pipeline needs.
    """
    return FakeLLM([(match_any(), respond_with(GREETING))])


async def greeter(ctx: ActivationContext) -> Complete:
    """One model call, then complete: the whole fast path.

    `ctx.call_model` is cache-first — a retried bundle replays the cached
    response instead of paying for a second provider call.
    """
    response = await ctx.call_model(
        LlmRequest(
            model_id="fake-model",
            messages=[ctx.single_event.decode()],
            tools_schema=None,
            sampling_params=None,
        )
    )
    return Complete(output=response.response)


def build(pipeline: beam.Pipeline) -> RunAgentOutputs:
    """Wire the minimal pipeline: one envelope, keyed upstream, into `RunAgent`."""
    envelope = AgentEnvelope(entity_key=b"user-1", event_time_ms=1_000, external_event=b"hello")
    keyed = (
        pipeline
        | "OneEvent" >> beam.Create([envelope])
        # RunAgent takes a pre-keyed PCollection[KV[bytes, AgentEnvelope]]; the
        # caller keys by entity_key, exactly as the documented dataflow shape does.
        | "KeyByEntity"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )
    return keyed | RunAgent(greeter, config=AgentConfig(provider_factory=make_provider))


def main() -> None:
    """Run the example pipeline, printing each decision to stdout."""
    with beam.Pipeline() as pipeline:
        outputs = build(pipeline)
        outputs.output | "Print" >> beam.Map(print)


if __name__ == "__main__":
    main()
