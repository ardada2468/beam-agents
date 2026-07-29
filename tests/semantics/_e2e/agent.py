"""The e2e gate's test agent, pipeline builder, and effector tool registry.

Module-level functions only (the DoFn and the HITL policy pickle by module
reference into the beam-sdk-harness container), and key-deterministic
behavior so the populations are known before the run:

- ``t-…`` keys: call the model once, stage one ``charge`` tool intent, and
  suspend; the re-injected ``ToolResult`` resumes to the terminal output
  ``result|<key>|<status>``.
- ``a-…`` keys: same shape but a ``kind=APPROVAL`` intent; the re-injected
  decision resumes to ``decision|<key>|approved|denied``.
- ``late-…`` keys: approval-bearing with a deliberately short HITL deadline
  and a feeder that answers only *after* it — the fail-closed timer emits the
  terminal ``decision|<key>|timeout`` and the late decision must surface on
  ``.errors`` as ``orphaned_result``.

Every event gets its own entity key, so per-key ordering pressure never falls
on the ingest spool: within one key the only sequencing is causal
(event → intent → result/decision → resume).

The effector half (``LEDGER_TOOLS``) lives here too so the whole behavioral
contract sits in one file. The ``charge`` tool records its execution in the
Redis ledger — counting at the side effect itself is the gate's central
measurement — and is configured by environment variables because the effector
is a separate OS process launched by the harness supervisor.
"""

from __future__ import annotations

import json
import os

import apache_beam as beam

from beam_agents._protos import AgentEnvelope, ToolResult
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.core.transform import AgentConfig, RunAgent, RunAgentOutputs
from beam_agents.hitl import Deny, HitlPolicy, Route
from beam_agents.model.client import LlmRequest
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.tools import IntentInfo, ToolRegistry, tool

# Key prefixes decide behavior; the gate derives its expected populations from
# the same prefixes.
TOOL_PREFIX = b"t-"
APPROVAL_PREFIX = b"a-"
LATE_PREFIX = b"late-"

# Terminal-output shapes, parsed by the assertions.
RESULT_OUT = b"result|"
DECISION_OUT = b"decision|"

# Lifetimes for the normal population must outlive the WHOLE run, not just a
# recovery: activations run on event time (dofn: now_ms = event_time_ms), so
# every intent's expiry is stamped relative to when the gate PUBLISHED its
# events — F12 submission-stall retries can burn 10+ minutes before the first
# activation, and phase B redelivers every intent near the run's end. A TTL
# any shorter than the worst-case run turns those redeliveries into EXPIRED
# refusals: a lost tool execution in phase A, or a second distinct result
# beside the earlier OK (both observed on slow CI runners at 10 minutes).
# The gate's pytest timeout is 30 minutes, so 60 minutes can never fire
# mid-run; expiry behavior itself is exercised by the late-… population.
NORMAL_TTL_MS = 60 * 60 * 1000
NORMAL_HITL_TIMEOUT_MS = 60 * 60 * 1000

# The late population's HITL deadline: short enough that the timer demonstrably
# fires mid-run, long enough that the suspend commit and effector routing are
# never racing it under normal scheduling.
LATE_HITL_TIMEOUT_MS = 30 * 1000


def make_provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"pong"))])


def keyed_timeout_deny(fallback: object) -> Route:
    """HITL fallback: a keyed, deterministic terminal decision.

    The runtime's default emits fixed bytes; the gate needs to attribute the
    fail-closed decision to its entity key for the zero-lost-approvals
    balance, so the policy embeds the key. Pure function of the context.
    """
    return Deny(output=DECISION_OUT + fallback.entity_key + b"|timeout")  # type: ignore[attr-defined]


def is_approval_key(key: bytes) -> bool:
    return key.startswith((APPROVAL_PREFIX, LATE_PREFIX))


async def e2e_agent(ctx: ActivationContext) -> Complete | Suspend:
    key = ctx.entity_key
    if not ctx.is_resume:
        await ctx.call_model(
            LlmRequest(
                model_id="fake", messages=[key.hex()], tools_schema=None, sampling_params=None
            )
        )
        args = json.dumps({"key": key.hex()}, sort_keys=True, separators=(",", ":"))
        if is_approval_key(key):
            ctx.request_approval(args, ttl_ms=NORMAL_TTL_MS)
            timeout = (
                LATE_HITL_TIMEOUT_MS if key.startswith(LATE_PREFIX) else NORMAL_HITL_TIMEOUT_MS
            )
            return Suspend(snapshot=b"await-approval", adapter="e2e", timeout_ms=timeout)
        ctx.act("charge", args, ttl_ms=NORMAL_TTL_MS)
        return Suspend(snapshot=b"await-result", adapter="e2e", timeout_ms=NORMAL_HITL_TIMEOUT_MS)

    if ctx.resume_approval is not None:
        verdict = b"approved" if ctx.resume_approval.approved else b"denied"
        return Complete(output=DECISION_OUT + key + b"|" + verdict)
    assert ctx.resume_result is not None
    status = ToolResult.Status.Name(ctx.resume_result.status).encode()
    return Complete(output=RESULT_OUT + key + b"|" + status)


def hitl_policy() -> HitlPolicy:
    return HitlPolicy(on_timeout=keyed_timeout_deny)


def build_run_agent(pcoll: beam.pvalue.PCollection) -> RunAgentOutputs:
    """Key the envelope stream and run the e2e agent over it."""
    keyed = pcoll | "KeyByEntity" >> beam.WithKeys(lambda e: e.entity_key).with_output_types(
        tuple[bytes, AgentEnvelope]
    )
    return keyed | RunAgent(
        e2e_agent,
        config=AgentConfig(
            provider_factory=make_provider,
            hitl_policy=hitl_policy(),
            # Event-time TTL GC stays out of the run: the spool's watermark
            # tracks real wall time, and a GC firing mid-run would wipe live
            # continuations (REASON_TTL_WIPED_SUSPENSION) under the gate.
            ttl_ms=1_000_000_000,
        ),
    )


# -- effector half: the ledger-recording tool registry -------------------------
#
# Imported by `beam-agents-effector --registry tests.semantics._e2e.agent:LEDGER_TOOLS`
# inside the worker subprocesses the supervisor launches. Environment carries
# the run scoping, because the effector is a separate OS process.
#
# The tool declares the keyword-only ``intent: IntentInfo`` parameter and
# records by the injected ``intent_id`` directly — it is the reference
# intent-keyed idempotent consumer (change ``add-intent-info-for-tools``):
# an unconditional attempt increment preserves the raw at-least-once
# measurement, and a first-writer-wins effective write models the idempotent
# downstream that the strong-form assertion (exactly one effective execution
# per minted intent) is stated over. The gate separately asserts, from the
# intents topic, that each key carries exactly one distinct ``intent_id``
# minted by the deterministic formula.


def _build_ledger_tools() -> ToolRegistry:
    run_id = os.environ.get("BEAM_AGENTS_E2E_RUN_ID")
    if not run_id:
        # Imported for pipeline/test use, not as an effector registry: expose
        # an empty registry instead of failing the module import.
        return ToolRegistry()

    from tests.semantics._e2e.ledger import DEFAULT_URL, ExecutionLedger

    ledger = ExecutionLedger(run_id, os.environ.get("BEAM_AGENTS_E2E_REDIS_URL", DEFAULT_URL))
    registry = ToolRegistry()

    @tool(side_effect=True)
    def charge(key: str, *, intent: IntentInfo) -> str:
        attempt = ledger.record_attempt(intent.intent_id)
        won = ledger.record_effective(intent.intent_id, attempt=attempt)
        return f"receipt-{key}-{'effect' if won else 'duplicate'}"

    registry.register(charge)
    return registry


LEDGER_TOOLS = _build_ledger_tools()
