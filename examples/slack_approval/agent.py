"""The FakeLLM demo agent and its pipeline wiring.

The ONE example module that imports Beam: everything the *surface* needs lives
in the service modules, which run outside the pipeline — the same boundary the
effector draws. This module is the other side of the loop: the agent whose
activation stages the approval intent, suspends, and resumes on the verdict
the surface publishes.

The demo agent guards a pretend refund. On first activation it stages one
approval intent (`request_approval`) and suspends; resumed with an approved
verdict it emits `APPROVED_OUTPUT`, and on a denial — or on the fail-closed
HITL timeout, which resumes nothing and denies by default — the guarded action
never happens (`DENIED_OUTPUT`). FakeLLM is the model (the demo needs no
provider), so the whole loop runs with no credentials of any kind except the
two Slack tokens, and none at all in the offline tests.
"""

from __future__ import annotations

import json

import apache_beam as beam

from beam_agents._protos import AgentEnvelope, ToolIntent
from beam_agents.actions.write_intents import WriteIntents
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.core.transform import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents.model.fake import FakeLLM, match_any, respond_with

# 10 minutes: long enough for a live human demo, short enough to demonstrate
# expiry in one sitting (versus hitl.DEFAULT_INTENT_TTL_MS = 1h). The sweep
# default (30s) fits well under it.
DEMO_TTL_MS = 600_000

# The HITL deadline is dominated by the intent TTL: the runtime suspends with
# deadline = min(timeout_ms, earliest intent expiry), so the demo's wait ends
# fail-closed (deny) when the approval expires.
DEMO_TIMEOUT_MS = 900_000

APPROVED_OUTPUT = b"refund-issued"
DENIED_OUTPUT = b"refund-declined"


def demo_args_json(order: str) -> str:
    """The canonical-JSON arguments the demo approval carries."""
    return json.dumps(
        {"action": "issue-refund", "order": order},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


async def refund_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Request approval for a refund; act only on an explicit approval."""
    if not ctx.is_resume:
        ctx.request_approval(demo_args_json(ctx.single_event.decode()), ttl_ms=DEMO_TTL_MS)
        return Suspend(
            snapshot=b"awaiting-approval", adapter="slack-approval-demo", timeout_ms=DEMO_TIMEOUT_MS
        )
    approval = ctx.resume_approval
    if approval is not None and approval.approved:
        return Complete(output=APPROVED_OUTPUT)
    # Denials, timeouts, and anything unexpected all land here: fail closed.
    return Complete(output=DENIED_OUTPUT)


def make_demo_provider() -> FakeLLM:
    """Module-level (picklable) provider factory; the demo agent never calls it."""
    return FakeLLM([(match_any(), respond_with(b"unused"))])


def demo_config() -> AgentConfig:
    """An ``AgentConfig`` wired to the scripted demo provider."""
    return AgentConfig(provider_factory=make_demo_provider)


def wire_demo(
    envelopes: beam.pvalue.PCollection, *, outbox_uri: str | None = None
) -> RunAgentOutputs:
    """The demo pipeline: key by entity, run the agent, outbox the intents.

    `envelopes` is a `PCollection[AgentEnvelope]` — external events Flattened
    with the approvals topic the surface publishes to. With `outbox_uri` the
    staged intents leave via `WriteIntents` for the effector (or, in the
    minimal demo, for the surface directly); without it the caller wires
    `.intents` itself, as the doc-contract tests do.
    """
    keyed = envelopes | "key-by-entity" >> beam.WithKeys(
        lambda envelope: envelope.entity_key
    ).with_output_types(tuple[bytes, AgentEnvelope])
    out = keyed | "run-demo-agent" >> RunAgent(refund_agent, config=demo_config())
    if outbox_uri is not None:
        _ = (
            out.intents
            | "key-intents"
            >> beam.WithKeys(lambda intent: intent.entity_key).with_output_types(
                tuple[bytes, ToolIntent]
            )
            | "outbox" >> WriteIntents(outbox_uri)
        )
    return out
