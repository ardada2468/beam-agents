"""Read-only tools run inline; side-effecting ones cannot.

Tools split in two by their `side_effect` flag, and the split is enforced
rather than advised:

- A read-only tool executes inline, inside the activation, via
  `ctx.run_tool(name, args)`. It is a lookup — an enrichment read, a cache
  probe — and re-running it on a bundle retry is harmless.
- A `side_effect=True` tool raises if called directly. The only path to an
  external write is `ctx.act(...)`, which stages an intent for the effector.

This example shows both halves, including the refusal. The registry is built
per call rather than kept as a module global, matching the project's
no-global-mutable-state convention; the tools themselves are module-level so
they pickle by reference into the DoFn.

Run it:  python website/examples/read_only_tools.py
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

from beam_agents import AgentConfig, RunAgent
from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete
from beam_agents.core.context import ActivationContext
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.tools import ToolRegistry, tool
from beam_agents.tools.errors import SideEffectToolError


# region: tools
@tool
def risk_band(customer_id: str) -> str:
    """Look up a customer's risk band. Read-only: safe to run inline."""
    return "high" if customer_id.endswith("9") else "normal"


@tool(side_effect=True)
def freeze_account(customer_id: str) -> str:
    """Freeze an account. Side-effecting: never executes inside the pipeline."""
    return f"frozen:{customer_id}"


def make_registry() -> ToolRegistry:
    """A fresh registry per call — no module-level mutable state."""
    registry = ToolRegistry()
    registry.register(risk_band)
    return registry


# endregion: tools


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"ok"))])


# region: agent
async def assess(ctx: ActivationContext) -> Complete:
    """Enrich inline, then request the freeze as an intent."""
    customer = ctx.event.decode()

    # Inline: this runs here, now, in the activation.
    band = await ctx.run_tool("risk_band", {"customer_id": customer})

    if band == "high":
        # Not inline: staged as an intent for the effector to execute.
        ctx.act("freeze_account", f'{{"customer_id": "{customer}"}}', ttl_ms=60_000)
        return Complete(output=b"freeze-requested")

    return Complete(output=b"cleared")


# endregion: agent


def _event(key: bytes, payload: bytes) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=1_000, external_event=payload)


def check_direct_call_is_refused() -> None:
    """Calling a side-effecting tool directly raises. That is the guarantee."""
    # region: refusal
    try:
        freeze_account(customer_id="cust-9")
    except SideEffectToolError as exc:
        refusal = str(exc)
    else:
        raise AssertionError("a side-effect tool must refuse a direct call")
    # endregion: refusal
    print(f"read_only_tools: {refusal}")


def main() -> None:
    check_direct_call_is_refused()

    with beam.Pipeline() as pipeline:
        keyed = (
            pipeline
            | "Events" >> beam.Create([_event(b"c-9", b"cust-9"), _event(b"c-1", b"cust-1")])
            | "Key"
            >> beam.WithKeys(lambda e: e.entity_key).with_output_types(tuple[bytes, AgentEnvelope])
        )
        # region: wiring
        # The registry carries only the read-only tool, because the registry is
        # what `ctx.run_tool` resolves against. `ctx.act` stages an intent by
        # name for the effector to route, so the side-effecting tool never
        # needs to be resolvable inside the pipeline at all.
        outputs = keyed | "Agent" >> RunAgent(
            assess,
            config=AgentConfig(provider_factory=make_provider, tool_registry=make_registry()),
        )

        assert_that(outputs.output, equal_to([b"freeze-requested", b"cleared"]), label="output")

        # Exactly one intent, from the one key whose band came back "high".
        # The cleared key executed a tool inline and staged nothing.
        names = outputs.intents | "Names" >> beam.Map(lambda intent: intent.tool_name)
        assert_that(names, equal_to(["freeze_account"]), label="intents")
        # endregion: wiring

    print("read_only_tools: ok")


if __name__ == "__main__":
    main()
