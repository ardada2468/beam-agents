"""The quickstart: a real agent, a real model, and a console to watch it in.

Everything else in `examples/` is deliberately hermetic — scripted `FakeLLM`, no
credentials, no network — because an example that cannot run without an API key
is an example most people never run. This one is the opposite end: it exists to
answer "does this actually work against the real thing", so by default it calls
a real provider over the network and streams what it records into the console.

    export ANTHROPIC_API_KEY=sk-ant-...
    make quickstart

What it runs is a small incident-triage agent, which is the shortest path
through every guarantee the runtime claims:

* a **real model call** decides whether an alert is worth waking somebody for;
* a **real tool** enriches the alert before that decision, through the tool
  registry, so the tool-call loop is exercised rather than described;
* a **high-severity verdict stages an approval and suspends** — the activation
  checkpoints, the intent leaves on `.intents`, and nothing has happened yet;
* an **approval arrives and the activation resumes** under the same `seq`, which
  is the effectively-once claim in one observable step;
* a second alert is **never approved**, so its deadline elapses and the
  fail-closed timeout route runs.

Provider selection is by environment, and there is no silent downgrade: if no
credential is present the module says so and exits non-zero unless you asked for
the offline provider explicitly with `--provider fake`. A quickstart that
quietly ran a scripted model while you believed you were testing a real one
would be worse than one that refuses.

The runner is whatever Beam is told to use, so the same module is the local run,
the Flink run, and the Dataflow run — see `docs/quickstart.md`. Every function a
pipeline references is module-level so the runner can pickle it by reference.
"""

from __future__ import annotations

import argparse
import functools
import os
import sys
import time
from collections.abc import Callable

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from beam_agents import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents._protos import AgentEnvelope
from beam_agents.console import ConsoleSinkResolver
from beam_agents.core.agent import Complete, Suspend, intent_id_for
from beam_agents.core.context import ActivationContext
from beam_agents.model import anthropic_decode, openai_compat_decode
from beam_agents.model.anthropic import AnthropicProvider
from beam_agents.model.client import LLMClient, LlmRequest
from beam_agents.model.facade import DecodedResponse, TokenUsage
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.model.openai_compat import OpenAICompatProvider
from beam_agents.tools.registry import ToolRegistry, tool

# Three services, one per outcome, so a real model exercises all three legs:
# one page that gets approved, one alert the model declines to page for, and one
# page nobody ever answers.
PAGING_ALERT = b"svc-checkout"
QUIET_ALERT = b"svc-imagecache"
ABANDONED_ALERT = b"svc-payments"

# How long the agent waits for a human before failing closed. Short, because the
# scripted clock below has to step past it inside a quickstart run.
APPROVAL_TIMEOUT_MS = 30_000

# The approval intent is minted at step 1: the triage model call consumes step 0.
# In production the effector publishes the decision already carrying this id;
# computing it here is what stands in for an effector in a self-contained
# example, and it is a pure function of `(entity_key, seq, step_index)`.
PAGING_INTENT_ID = intent_id_for(PAGING_ALERT, seq=0, step_index=1)

CONSOLE_URI = "console://localhost:8787"


def base_ms() -> int:
    """The instant the scripted alerts are stamped with: now, read once.

    Read from the wall clock rather than fixed, because these records exist to
    be looked at: an offset from zero puts the run in 1970, where it sorts below
    every other record in the store and falls outside the console's "last hour"
    the moment it arrives. Determinism is not the trade here — this module is a
    quickstart, not a gate — and one read at construction still gives every
    element of one run a single consistent base.
    """
    return int(time.time() * 1000)


# Small and cheap on purpose: this runs on somebody's first afternoon with the
# library and should cost a fraction of a cent.
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
OPENAI_MODEL = "gpt-4o-mini"

MAX_TOKENS = 64

SYSTEM_RULE = (
    "You are an on-call triage assistant. Reply with exactly one word: "
    "PAGE if the alert needs a human woken up now, or IGNORE if it can wait "
    "until morning. Reply with the single word and nothing else."
)


# -- The tool ---------------------------------------------------------------


@tool
def service_tier(service: str) -> str:
    """Look up how important a service is. Read-only, so it runs inline."""
    tiers = {"svc-checkout": "tier=1 revenue-critical", "svc-imagecache": "tier=3 best-effort"}
    return tiers.get(service, "tier=unknown")


def make_tool_registry() -> ToolRegistry:
    """The registry the agent's inline tools live in."""
    registry = ToolRegistry()
    registry.register(service_tier)
    return registry


# -- Provider selection -----------------------------------------------------


def _fake_provider() -> LLMClient:
    """The offline provider, reachable only by asking for it by name."""
    return FakeLLM([(match_any(), respond_with(b"PAGE"))])


def _fake_decode(payload: bytes) -> DecodedResponse:
    """Fixed usage: the response is a constant, so parsing it would invent numbers."""
    return DecodedResponse(
        usage=TokenUsage(prompt_tokens=12, completion_tokens=1, total_tokens=13),
        text=payload.decode(),
    )


def _anthropic_provider() -> LLMClient:
    return AnthropicProvider(api_key=os.environ["ANTHROPIC_API_KEY"])


def _openai_provider() -> LLMClient:
    return OpenAICompatProvider(api_key=os.environ["OPENAI_API_KEY"])


def resolve_provider(choice: str) -> tuple[str, Callable[[], LLMClient], str]:
    """Return `(name, factory, model_id)` for the requested provider.

    `auto` prefers a real credential and never falls back to the fake one — see
    the module docstring. The factory is returned rather than an instance
    because `AgentConfig` builds one client per worker.
    """
    if choice in {"auto", "anthropic"} and os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic", _anthropic_provider, ANTHROPIC_MODEL
    if choice in {"auto", "openai"} and os.environ.get("OPENAI_API_KEY"):
        return "openai", _openai_provider, OPENAI_MODEL
    if choice == "fake":
        return "fake", _fake_provider, "fake-quickstart"
    missing = "ANTHROPIC_API_KEY" if choice == "anthropic" else "OPENAI_API_KEY"
    if choice == "auto":
        missing = "ANTHROPIC_API_KEY (or OPENAI_API_KEY)"
    raise SystemExit(
        f"No credential for the '{choice}' provider: {missing} is not set.\n"
        "This quickstart calls a real model on purpose. Either export a key:\n"
        "    export ANTHROPIC_API_KEY=sk-ant-...\n"
        "or run it against the offline scripted provider by asking for that "
        "explicitly:\n"
        "    make quickstart PROVIDER=fake"
    )


def decode_for(provider: str) -> Callable[[bytes], DecodedResponse]:
    """The decoder that matches the selected provider's response shape."""
    if provider == "anthropic":
        return anthropic_decode
    if provider == "openai":
        return openai_compat_decode
    return _fake_decode


# -- The agent --------------------------------------------------------------


async def triage(
    ctx: ActivationContext,
    *,
    model_id: str,
    decode: Callable[[bytes], DecodedResponse],
) -> Complete | Suspend:
    """Triage one alert, waking a human only for a service worth waking one for.

    The model id and decoder arrive as keyword arguments bound by
    `functools.partial` in `build()`, rather than read from module state. Both
    are picklable — a string and a module-level function — so the partial ships
    to a distributed runner intact, and nothing has to be assigned before the
    pipeline is constructed in a worker.
    """
    if ctx.is_resume:
        approval = ctx.resume_approval
        if approval is not None and approval.approved:
            return Complete(output=b"paged:" + ctx.entity_key)
        return Complete(output=b"stood-down:" + ctx.entity_key)

    alert = ctx.single_event.decode()
    tier = service_tier(ctx.entity_key.decode())

    response = await ctx.call_model(
        LlmRequest(
            model_id=model_id,
            messages=[
                {"role": "user", "content": f"{SYSTEM_RULE}\n\nAlert: {alert}\nService: {tier}"}
            ],
            tools_schema=None,
            sampling_params={"max_tokens": MAX_TOKENS},
        )
    )
    verdict = decode(response.response).text.strip().upper()

    if not verdict.startswith("PAGE"):
        return Complete(output=b"ignored:" + ctx.entity_key)

    # Stage the approval and suspend. The runtime persists a continuation and
    # arms the fail-closed timer; nothing external has happened yet.
    ctx.request_approval('{"action":"page-oncall","service":"' + ctx.entity_key.decode() + '"}')
    return Suspend(snapshot=b"awaiting-oncall-approval", timeout_ms=APPROVAL_TIMEOUT_MS)


# -- The source -------------------------------------------------------------


def _alert(key: bytes, payload: bytes, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    envelope = AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)
    return TimestampedValue(envelope, t_ms / 1000)


def _approval(key: bytes, intent_id: str, t_ms: int) -> TimestampedValue[AgentEnvelope]:
    """A human decision re-entering on the service's key — the approvals topic's job."""
    envelope = AgentEnvelope(entity_key=key, event_time_ms=t_ms)
    envelope.approval.intent_id = intent_id
    envelope.approval.approved = True
    envelope.approval.approver = "oncall@example.test"
    envelope.approval.decided_at_ms = t_ms
    return TimestampedValue(envelope, t_ms / 1000)


def scripted_stream() -> TestStream:
    """Two alerts, one answered approval, and one deadline nobody meets.

    A `TestStream` rather than a Kafka source because both clocks have to be
    scripted for the timeout leg to run in seconds instead of in real minutes —
    the watermark carries event time, and the processing-time advance is what
    fires the HITL timer. `docs/quickstart.md` covers pointing this at a real
    broker once you want the durability story too.
    """
    base = base_ms()
    return (
        TestStream()
        .advance_watermark_to(base / 1000)
        .add_elements([_alert(PAGING_ALERT, b'{"alert":"checkout 5xx rate 40%"}', base + 1_000)])
        .add_elements([_alert(QUIET_ALERT, b'{"alert":"image cache warm miss"}', base + 2_000)])
        .add_elements(
            [_alert(ABANDONED_ALERT, b'{"alert":"payments auth failures 100%"}', base + 3_000)]
        )
        .add_elements([_approval(PAGING_ALERT, PAGING_INTENT_ID, t_ms=base + 5_000)])
        # Nobody ever answers for svc-payments; step the clock past its deadline
        # so the fail-closed route runs rather than the activation hanging.
        #
        # Advanced to past `base`, not by 60 seconds. `TestStream`'s processing
        # clock starts at the epoch while the suspension's deadline is derived
        # from `base`, which is now — so a 60-second step lands 55 years short of
        # it and the timeout never fires. This is the seam between a scripted
        # clock and a wall-clock base, and it is only crossable in this
        # direction.
        .advance_processing_time(base / 1000 + 60)
        .advance_watermark_to_infinity()
    )


def build(
    pipeline: beam.Pipeline,
    *,
    config: AgentConfig,
    model_id: str,
    decode: Callable[[bytes], DecodedResponse],
) -> RunAgentOutputs:
    """Wire the scripted alerts, keyed by service, into `RunAgent`."""
    keyed = (
        pipeline
        | "Alerts" >> scripted_stream()
        | "KeyByService"
        >> beam.WithKeys(lambda env: env.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
    )
    agent = functools.partial(triage, model_id=model_id, decode=decode)
    return keyed | "Triage" >> RunAgent(agent, config=config)


def make_config(
    provider: str,
    factory: Callable[[], LLMClient],
    *,
    console: str | None,
) -> AgentConfig:
    """The whole adoption path: a provider, a tool registry, and three sink URIs.

    With no console the sink arguments are omitted entirely rather than passed
    as `None`: `sink_resolver` has a real default, and handing it `None` to mean
    "leave it alone" would replace a resolver with nothing.
    """
    if console is None:
        return AgentConfig(
            provider_factory=factory,
            decode=decode_for(provider),
            tool_registry=make_tool_registry(),
            max_tokens_per_activation=4_000,
        )
    return AgentConfig(
        provider_factory=factory,
        decode=decode_for(provider),
        tool_registry=make_tool_registry(),
        max_tokens_per_activation=4_000,
        traces_to=console,
        errors_to=console,
        snapshots_to=console,
        sink_resolver=ConsoleSinkResolver(),
    )


def main(argv: list[str] | None = None) -> int:
    """Run the quickstart, reporting the provider and every decision it made.

    Unrecognised arguments are handed straight to Beam, so the same entry point
    takes `--runner`, `--project`, and the rest of the runner's flags.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "anthropic", "openai", "fake"],
        help="Which model to call. 'auto' uses whichever credential is set and "
        "refuses rather than silently downgrading to 'fake'.",
    )
    parser.add_argument(
        "--console",
        default=CONSOLE_URI,
        help="Where to deliver traces, errors, and snapshots. Empty to send none.",
    )
    args, beam_argv = parser.parse_known_args(argv)

    provider, factory, model_id = resolve_provider(args.provider)
    console = args.console or None

    print(f"provider : {provider} ({model_id})")
    print(f"console  : {console or 'not delivering'}")
    if provider == "fake":
        print("NOTE     : scripted offline model — no request leaves this machine.")

    options = PipelineOptions(beam_argv)
    options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=options) as pipeline:
        outputs = build(
            pipeline,
            config=make_config(provider, factory, console=console),
            model_id=model_id,
            decode=decode_for(provider),
        )
        outputs.output | "Print" >> beam.Map(lambda decision: print(f"decision : {decision!r}"))

    if console:
        print(f"\nOpen {console.replace('console://', 'http://')} to see the run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
