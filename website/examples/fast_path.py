"""Fast path: one event in, one decision out, inside a single activation.

The fast path is the simple case — the agent runs to completion inside one
`process()` call. It reads the event, consults durable per-key working memory,
calls the model, writes memory back, and completes. No suspension, no external
side effect, no second element.

Everything the activation touches is staged and committed atomically with the
Beam bundle: if this function raised halfway through, the memory write below
would not persist and the key's sequence counter would not advance.

Run it:  python website/examples/fast_path.py
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from beam_agents import AgentConfig, RunAgent
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with


# region: provider
def make_provider() -> FakeLLM:
    """The model used throughout these examples.

    `FakeLLM` matches requests against ordered rules and records every call, so
    an example is deterministic and needs no credentials or network. Swapping
    in a real provider is a change to this factory alone.
    """
    return FakeLLM([(match_any(), respond_with(b"escalate"))])


# endregion: provider


# region: agent
async def triage(ctx: ActivationContext) -> Complete:
    """Decide what to do about one event for one entity.

    Module-level, not a closure: the DoFn holding the agent is serialized for
    the runner, so the agent has to pickle by reference.
    """
    ctx.memory.append("recent", ctx.event, max_items=32)
    seen = len(ctx.memory.ring("recent"))

    response = await ctx.call_model(
        LlmRequest(
            model_id="fake-1",
            messages=[f"event={ctx.event.decode()} seen={seen}"],
            tools_schema=None,
            sampling_params=None,
        )
    )
    return Complete(output=b"%s:%d" % (response.response, seen))


# endregion: agent


def build_pipeline(pipeline: beam.Pipeline) -> beam.pvalue.PCollection:
    """Wire the transform. Input must be pre-keyed by `entity_key`."""
    # region: pipeline
    events = pipeline | "Events" >> beam.Create(
        [
            AgentEnvelope(entity_key=b"acct-1", event_time_ms=1_000, external_event=b"login"),
            AgentEnvelope(entity_key=b"acct-1", event_time_ms=2_000, external_event=b"transfer"),
            AgentEnvelope(entity_key=b"acct-2", event_time_ms=1_500, external_event=b"login"),
        ]
    )

    # `RunAgent` does not key elements itself. It validates the input is
    # KV-shaped at pipeline-construction time and raises ValueError otherwise,
    # because a stateful DoFn cannot accept anything else.
    keyed = events | "Key" >> beam.WithKeys(lambda e: e.entity_key).with_output_types(
        tuple[bytes, AgentEnvelope]
    )

    outputs = keyed | "Agent" >> RunAgent(
        triage, config=AgentConfig(provider_factory=make_provider)
    )
    # endregion: pipeline
    return outputs.output


def main() -> None:
    with beam.Pipeline() as pipeline:
        results = build_pipeline(pipeline)
        # region: assertion
        # Per-key serialization means acct-1's two events are ordered relative
        # to each other; the ring is 1 then 2 deep. acct-2 is a separate key
        # with its own memory, so it starts at 1.
        assert_that(results, equal_to([b"escalate:1", b"escalate:2", b"escalate:1"]))
        # endregion: assertion
    print("fast_path: ok")


if __name__ == "__main__":
    main()
