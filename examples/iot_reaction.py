"""IoT reaction over keyed rolling memory: react to a trend, not to every reading.

A stream of per-device temperature readings flows through one agent. Each
activation appends its reading to a bounded per-key ring in working memory and
reads the rolled window back. While the window's average stays below the
threshold the activation completes with no model call — the runtime does not
charge tokens for uninteresting events. When the average crosses the threshold
the agent makes one scripted model call for a reaction decision, emits it, and
resets the window so one sustained excursion produces one reaction.

Run it offline, with no credentials and no docker:

    uv run python -m examples.iot_reaction

Every function a pipeline references is module-level so the DirectRunner can
pickle it by reference.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from beam_agents import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LLMClient, LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with

DEVICE_QUIET = b"sensor-1"
DEVICE_HOT = b"sensor-2"

# The rolling window keeps at most this many readings per device — bounded
# memory by construction, enforced by `Memory.append`'s max_items.
WINDOW_ITEMS = 4
# A window whose average reaches this temperature is a breach.
THRESHOLD_AVG = 100

# One reading: (device key, temperature, event time in ms). sensor-1 stays
# quiet; sensor-2's second reading tips its window average over the threshold.
Reading = tuple[bytes, int, int]
QUIET_READINGS: Sequence[Reading] = (
    (DEVICE_QUIET, 60, 1_000),
    (DEVICE_QUIET, 70, 2_000),
    (DEVICE_QUIET, 80, 3_000),
)
READINGS: Sequence[Reading] = (
    (DEVICE_QUIET, 60, 1_000),
    (DEVICE_HOT, 90, 1_500),
    (DEVICE_QUIET, 70, 2_000),
    (DEVICE_HOT, 120, 2_500),  # window avg (90+120)/2 = 105 -> breach
    (DEVICE_QUIET, 80, 3_000),
    (DEVICE_HOT, 95, 3_500),  # fresh window after the reaction reset
)


def make_provider() -> FakeLLM:
    """Scripted reaction decision: the model always says throttle the device."""
    return FakeLLM([(match_any(), respond_with(b"throttle"))])


async def react(ctx: ActivationContext) -> Complete:
    """Append the reading to the device's rolling window; react on a breach."""
    ctx.memory.append("readings", ctx.event, max_items=WINDOW_ITEMS)
    window = [int(item) for item in ctx.memory.ring("readings")]
    average = sum(window) / len(window)

    if average < THRESHOLD_AVG:
        # Quiet reading: remember it and complete. No model call.
        return Complete(output=f"ok:{ctx.entity_key.decode()}:window={len(window)}".encode())

    # Breach: ask the model what to do about the trend, then reset the window
    # so the same excursion does not re-alarm on every subsequent reading.
    decision = await ctx.call_model(
        LlmRequest(
            model_id="fake-reaction-model",
            messages=[f"overheating trend: {window}"],
            tools_schema=None,
            sampling_params=None,
        )
    )
    ctx.memory.delete("readings")
    return Complete(output=b"reaction:" + ctx.entity_key + b":" + decision.response)


def _reading(device: bytes, temperature: int, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    envelope = AgentEnvelope(
        entity_key=device, event_time_ms=t_ms, external_event=str(temperature).encode()
    )
    return TimestampedValue(envelope, t_ms / 1000)


def build(
    pipeline: beam.Pipeline,
    readings: Sequence[Reading] = READINGS,
    *,
    provider_factory: Callable[[], LLMClient] = make_provider,
) -> RunAgentOutputs:
    """Wire a scripted reading stream, keyed by device, into `RunAgent`.

    `readings` defaults to the module's script; `provider_factory` defaults to
    the scripted FakeLLM — swap it for a real client factory in production.
    """
    stream = TestStream().advance_watermark_to(0)
    for device, temperature, t_ms in readings:
        stream = stream.add_elements([_reading(device, temperature, t_ms)])
    stream = stream.advance_watermark_to_infinity()

    keyed = (
        pipeline
        | "Readings" >> stream
        | "KeyByDevice"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )
    return keyed | RunAgent(react, config=AgentConfig(provider_factory=provider_factory))


def main() -> None:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as pipeline:
        outputs = build(pipeline)
        outputs.output | "Print" >> beam.Map(print)


if __name__ == "__main__":
    main()
