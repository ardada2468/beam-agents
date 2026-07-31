"""Point a pipeline at a running console, and fill it with every kind of record.

Run it offline, with no credentials, no docker, and no console — it prints a
per-scenario summary of everything the round produced:

    uv run python -m examples.console_demo

Run it against a console started with `beam-agents-console`:

    uv run python -m examples.console_demo --console console://localhost:8787

Or keep producing, so a console shows live traffic arriving rather than a static
snapshot (this is what `docker/compose.console.yaml` runs):

    uv run python -m examples.console_demo --console console://localhost:8787 --loop

The scenarios themselves live in `beam_agents.console._demo`, and this module
calls into it rather than forking it. That is deliberate: the Docker image runs
the same generator, and an example that reimplemented the twelve scenarios would
drift away from the one the compose stack actually starts — which is exactly the
kind of drift that leaves a `docker compose up` user staring at a console with
three of its twelve views empty.

What this module adds is the part a reader is looking for: `console_config`, the
whole adoption path in one `AgentConfig`. Every function a pipeline references is
module-level so the DirectRunner can pickle it by reference.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from beam_agents.console import ConsoleSinkResolver
from beam_agents.console._demo import build as build_demo
from beam_agents.console._demo import main as demo_main
from beam_agents.core.transform import AgentConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    import apache_beam as beam

    from beam_agents.model.client import LLMClient

# Where a console started by `beam-agents-console` listens by default.
CONSOLE_URI = "console://localhost:8787"


def console_config(
    provider_factory: Callable[[], LLMClient], *, console: str = CONSOLE_URI
) -> AgentConfig:
    """The whole adoption path: one resolver, three sink URIs.

    `ConsoleSinkResolver` *wraps* the runtime's `DefaultSinkResolver` rather than
    replacing it, so every other scheme a pipeline already uses keeps behaving
    exactly as it does today and no module on the hot path is modified. Unlike
    `otlp://` — which the default resolver refuses for anything but traces,
    because the OTLP encoding cannot represent an error record or a state
    snapshot — `console://` is accepted for all three, since the console's native
    ingest is the protos themselves.

    Copy this into your own pipeline and every activation it runs shows up in the
    console; delete the four keyword arguments and the pipeline is byte-for-byte
    what it was before.
    """
    return AgentConfig(
        provider_factory=provider_factory,
        traces_to=console,
        errors_to=console,
        snapshots_to=console,
        sink_resolver=ConsoleSinkResolver(),
    )


def build(pipeline: beam.Pipeline, *, console: str | None = None, seed: int = 0) -> None:
    """Wire the demo's scenarios onto `pipeline`, delivering to `console`.

    `console=None` runs the same pipeline with its sinks unset, which is what
    makes the example runnable with nothing else started.
    """
    build_demo(pipeline, console=console, seed=seed)


def main(argv: list[str] | None = None) -> int:
    """Run the demo and report what it produced.

    Delegates to `beam_agents.console._demo`'s entry point, so the example and
    `python -m beam_agents.console._demo` accept the same flags and cannot
    disagree about what a round contains.
    """
    return demo_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
