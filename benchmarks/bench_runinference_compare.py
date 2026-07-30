"""RunAgent vs plain RunInference over the same FakeLLM-backed work.

Two DirectRunner pipelines over identical inputs: ``RunAgent`` with a
single-call agent, and ``apache_beam.ml.inference``'s ``RunInference`` with a
minimal ``ModelHandler`` invoking the identical ``FakeLLM`` script (via a
private event loop — the handler API is synchronous). Both use zero-latency
behaviors: provider wait time would only dilute the difference being
measured. The reported quantity is the per-element delta and ratio — what
durable keyed memory, the replay cache, deterministic intents, and the staged
atomic commit cost over raw model invocation (design D7).

This is the one benchmark that runs whole pipelines: the DirectRunner's own
overhead appears on both sides and largely cancels in the difference. The
absolute per-element figures are NOT gated and not comparable to the
fake-handle benchmarks (different measurement surface); the baseline ratchet
tracks the delta only.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import apache_beam as beam
from apache_beam.ml.inference.base import ModelHandler, RunInference

from beam_agents._protos import AgentEnvelope
from beam_agents.core.transform import AgentConfig, RunAgent
from benchmarks._harness import (
    EVENT_TIME_MS,
    bench_request,
    single_call_agent,
    zero_latency_provider,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from beam_agents.model.fake import FakeLLM

# Identical input volume on both sides. Per-element figures are the pipeline's
# wall time divided by this (via pyperf inner_loops), so the runner's fixed
# startup cost is spread identically across both pipelines.
N_ELEMENTS = 100

# Pinned sampling (CI defaults; CLI overrides are local iteration only): each
# value is one whole pipeline run, so a few values from a few processes is the
# budget-honest choice.
PROCESSES = 3
VALUES = 3
WARMUPS = 1


def _payloads() -> list[bytes]:
    return [b"event-%04d" % i for i in range(N_ELEMENTS)]


def _envelope(payload: bytes) -> AgentEnvelope:
    # One key per element: the comparison is per-element cost against a
    # keyless RunInference, not per-key serialization behavior.
    return AgentEnvelope(entity_key=payload, event_time_ms=EVENT_TIME_MS, external_event=payload)


def _keyed(envelope: AgentEnvelope) -> tuple[bytes, AgentEnvelope]:
    return (envelope.entity_key, envelope)


class _FakeLLMHandler(ModelHandler):
    """Minimal ModelHandler over the identical FakeLLM script.

    ``run_inference`` is synchronous by API, so it drives the async client on
    a private event loop — the closest raw-inference analogue of the bridge.
    """

    def load_model(self) -> FakeLLM:
        return zero_latency_provider()

    def run_inference(
        self,
        batch: Sequence[bytes],
        model: FakeLLM,
        inference_args: dict[str, Any] | None = None,
    ) -> Iterable[bytes]:
        loop = asyncio.new_event_loop()
        try:
            return [
                loop.run_until_complete(model.complete(bench_request(payload))).response
                for payload in batch
            ]
        finally:
            loop.close()


def run_runagent_pipeline() -> None:
    with beam.Pipeline() as pipeline:
        _ = (
            pipeline
            | beam.Create(_payloads())
            | beam.Map(_envelope)
            | beam.Map(_keyed).with_output_types(tuple[bytes, AgentEnvelope])
            | RunAgent(
                single_call_agent,
                config=AgentConfig(provider_factory=zero_latency_provider),
            )
        )


def run_runinference_pipeline() -> None:
    with beam.Pipeline() as pipeline:
        _ = pipeline | beam.Create(_payloads()) | RunInference(_FakeLLMHandler())


def time_runagent(loops: int) -> float:
    t0 = time.perf_counter()
    for _ in range(loops):
        run_runagent_pipeline()
    return time.perf_counter() - t0


def time_runinference(loops: int) -> float:
    t0 = time.perf_counter()
    for _ in range(loops):
        run_runinference_pipeline()
    return time.perf_counter() - t0


TIMED: tuple[tuple[str, Callable[[int], float]], ...] = (
    ("runagent_per_element", time_runagent),
    ("runinference_per_element", time_runinference),
)


def main() -> None:
    import pyperf

    runner = pyperf.Runner(
        processes=PROCESSES,
        values=VALUES,
        warmups=WARMUPS,
        metadata={
            "description": (
                "whole DirectRunner pipelines over identical zero-latency FakeLLM "
                "work; per-element figures, baseline tracks the delta only"
            )
        },
        program_args=("-m", "benchmarks.bench_runinference_compare"),
    )
    # inner_loops spreads each pipeline's wall time over its element count, so
    # the recorded values are per-element seconds on both sides.
    runner.bench_time_func("runagent_per_element", time_runagent, inner_loops=N_ELEMENTS)
    runner.bench_time_func("runinference_per_element", time_runinference, inner_loops=N_ELEMENTS)


if __name__ == "__main__":
    main()
