"""A pipeline that produces the runtime's full event vocabulary.

An empty console teaches nothing. Someone evaluating this library runs one
command and needs to land on a screen that shows what the runtime records —
which means the demo has to produce not just successful activations but the
*interesting* ones: a suspension awaiting approval, a tool that fails, a cache
hit, a budget exhausted, a TTL that wipes a live suspension, a dead-lettered
intent. Those are the records the error views and the approval queue exist to
render, and a demo that emits only happy-path completions leaves most of the UI
looking broken.

Runs on ``DirectRunner`` over the fake provider (``model/fake.py``), so it needs
no API key, no broker, and no network. Deterministic by construction — the
runtime's identity is ``uuid5`` over ``(entity_key, seq)`` and the fake provider
replays scripted responses — so the same seed produces the same console every
time, which is what makes the screenshots in the docs reproducible.

Four decisions here are load-bearing and not obvious from the code:

**The scenario is encoded in the entity key.** ``<scenario>|<seed>|<index>``.
One agent function drives every scenario by reading its own key, because
``RunAgent`` binds one agent per transform and twelve transforms would be twelve
stateful DoFns with twelve state namespaces for no gain. It also means the
console's activation list — which is keyed by ``entity_key`` — reads as a
labelled index of the vocabulary rather than as opaque identifiers.

**Two ``RunAgent`` transforms, one ``TestStream``.** ``batch_buffer_overflow`` is
reachable only under ``BatchPolicy.ADAPTIVE``, which is a transform-level policy
that changes the agent-visible contract (``ctx.event`` becomes a list), so the
batching scenario needs its own transform. It does *not* get its own
``TestStream``: two scripted streams in one pipeline share a processing-time
clock on the DirectRunner, and the second one's watermark advance fires the
first one's HITL timers before its approvals are delivered. One stream,
partitioned by scenario, keeps both clocks scripted by a single script.

**Records reach the delivery target through files, not a list.** The DirectRunner
is free to execute bundles outside the calling thread, so a closure appending to
a list silently collects nothing. Each stream is appended to its own
length-framed file — the same technique ``tests/semantics`` uses — and read back
after the pipeline drains. This is an in-process handoff, not a published
interchange, so the framing is a fixed-width prefix rather than the replay
bundle's varint.

**Delivery is injected.** ``run_demo`` writes into a running console over
``console://`` or straight into an in-process ``ConsoleStore``, and both are just
implementations of the same one-method seam. That is what makes the demo usable
as test data with no server standing up, and what lets its own tests assert on
the records the pipeline emits rather than on a store's contents.

Importing this module has no side effects.
"""

from __future__ import annotations

import argparse
import hashlib
import struct
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from beam_agents._protos import (
    ActivationErrorRecord,
    AgentEnvelope,
    StateSnapshot,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.actions.write_intents import DEAD_LETTER_TAG, WriteIntentsResult
from beam_agents.console._ingest import normalize
from beam_agents.console._records import PROVENANCE_NATIVE
from beam_agents.console._sink import ConsoleSinkResolver
from beam_agents.console._store import ConsoleStore
from beam_agents.core.agent import Complete, Outcome, Suspend, intent_id_for
from beam_agents.core.batching import BatchPolicy
from beam_agents.core.dofn import ActivationError
from beam_agents.core.error_records import intent_dead_letter_to_error, serialize_error_envelope
from beam_agents.core.transform import AgentConfig, DefaultSinkResolver, RunAgent
from beam_agents.hitl import Deny, Drop, HitlPolicy
from beam_agents.model.client import LlmRequest
from beam_agents.model.facade import DecodedResponse, TokenUsage
from beam_agents.model.fake import FakeLLM, match_any, respond_with
from beam_agents.tools.errors import ToolError
from beam_agents.tools.registry import ToolRegistry, tool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from beam_agents.core.agent import FallbackContext
    from beam_agents.core.context import ActivationContext
    from beam_agents.core.transform import SinkResolver
    from beam_agents.hitl import Route

__all__ = [
    "DEFAULT_ENTITY_COUNT",
    "SCENARIOS",
    "main",
    "run_demo",
]

# Every scenario the demo drives, each chosen because some part of the UI is
# unreachable without it.
SCENARIOS = (
    "completion",
    "multi_tool",
    "cache_hit",
    "suspension_approved",
    "suspension_denied",
    "suspension_timeout",
    "tool_error",
    "activation_error",
    "budget_exceeded",
    "orphaned_result",
    "intent_dead_letter",
    "batch_overflow",
)

DEFAULT_ENTITY_COUNT = 12

# The model id every LLM_CALL trace reports. One id, so the console's per-model
# panel has a row rather than a scatter of one-call models.
DEMO_MODEL_ID = "fake-console-demo"

# The demo clock. Real epoch milliseconds rather than a small offset, so the
# console's timelines read as dates instead of as 1970 — the scripted
# processing-time advance below is sized against this, not against zero.
DEMO_EPOCH_MS = 1_764_547_200_000  # 2025-12-01T00:00:00Z

# How far apart consecutive rounds sit on the demo clock. A round is an hour, so
# a looping demo fills the console's time buckets instead of stacking every
# activation on one instant.
ROUND_INTERVAL_MS = 3_600_000

# How long a suspension waits before the HITL timer fires. Short relative to the
# scripted processing-time advance, so `suspension_timeout` actually elapses.
APPROVAL_TIMEOUT_MS = 30_000

# How long the batching branch's suspension waits. Long relative to the same
# advance, so that key is still suspended when the round ends and the console's
# approval queue has something pending in it — and inside `DEFAULT_INTENT_TTL_MS`
# (an hour), because the runtime takes the *earlier* of the suspension timeout
# and its staged intents' expiries: past an expiry no result can arrive, so
# waiting out a longer timeout would be a fail-open stall.
LONG_SUSPENSION_MS = 1_800_000

# How far the scripted clock advances after the approvals are delivered. Past
# `APPROVAL_TIMEOUT_MS` and inside `LONG_SUSPENSION_MS`.
PROCESSING_ADVANCE_MS = 60_000

# The tool name the demo outbox refuses to route. `intent_dead_letter` is a
# *delivery* failure, so it cannot be produced from inside an activation; the
# demo's outbox refuses this one name and `RunAgent` maps the refusal onto the
# shared `ActivationError` shape through its own `dead_letter` branch.
UNROUTABLE_TOOL = "publish_to_unreachable_outbox"

# The sink URI the demo's intents outbox answers to. Not a real scheme: it names
# the demo's own writer, which discards everything it can serialize.
DEMO_OUTBOX_URI = "demo://outbox"

# The per-activation token bound. Sized so only `budget_exceeded` (which calls
# the model in a loop) crosses it; every other scenario makes at most two calls.
DEMO_TOKEN_BUDGET = 100

# How many model calls `budget_exceeded` attempts. Each scripted response decodes
# to `DEMO_PROMPT_TOKENS + DEMO_COMPLETION_TOKENS`, so this trips the bound.
BUDGET_SCENARIO_CALLS = 10

DEMO_PROMPT_TOKENS = 11
DEMO_COMPLETION_TOKENS = 7

# The batching branch's bounds. `max_buffered_events == max_batch_size` on
# purpose: overflow is reachable only while a live suspension defers the size
# trigger, and the equality is what makes the burst reach the cap quickly.
BATCH_MAX_SIZE = 2
BATCH_MAX_BUFFERED = 4
BATCH_MAX_WAIT_MS = 600_000
# Events the batching key receives: two flush the first batch (which suspends),
# four fill the deferred buffer, and the rest overflow.
BATCH_EVENT_COUNT = 8

# The scripted response. Opaque provider bytes as far as the runtime is
# concerned; `decode_demo_response` is the paired decoder that turns it into the
# usage the LLM_CALL traces carry.
DEMO_RESPONSE = b'{"text":"triaged","status":"ok"}'

_KEY_SEPARATOR = "|"

# Ordering rank per event type, used to put an activation's events into causal
# order rather than enum order (`ERROR` is 6 and `SUSPENDED` is 7, which reads
# backwards). Children share a rank and break ties on `step_index`.
_EVENT_RANK = {
    TraceEvent.ACTIVATION_START: 0,
    TraceEvent.LLM_CALL: 1,
    TraceEvent.TOOL_CALL: 1,
    TraceEvent.INTENT_EMITTED: 1,
    TraceEvent.SUSPENDED: 2,
    TraceEvent.ACTIVATION_END: 3,
    TraceEvent.ERROR: 4,
}


# --- keys ---------------------------------------------------------------------


def entity_key_for(scenario: str, index: int, seed: int) -> bytes:
    """Return the entity key for one scenario instance.

    ``<scenario>|<seed>|<index>``. The seed rides in the key because trace
    identity is ``uuid5(entity_key, seq)``: without it two rounds would produce
    the same trace ids and the store, which dedups on them, would collapse the
    second round onto the first.
    """
    return f"{scenario}{_KEY_SEPARATOR}{seed:06d}{_KEY_SEPARATOR}{index:03d}".encode()


def scenario_of(entity_key: bytes) -> str:
    """Return the scenario name an entity key was minted for."""
    return entity_key.decode().split(_KEY_SEPARATOR, 1)[0]


def assign_scenarios(scenarios: tuple[str, ...], entities: int) -> tuple[tuple[str, int], ...]:
    """Spread ``entities`` keys over ``scenarios``, round-robin.

    Round-robin rather than proportional: a console that shows nine completions
    and one error is a console whose error views look broken, which is the exact
    failure this module exists to prevent.
    """
    if not scenarios:
        raise ValueError("run_demo needs at least one scenario to drive")
    unknown = [name for name in scenarios if name not in SCENARIOS]
    if unknown:
        raise ValueError(
            f"{unknown[0]!r} is not a demo scenario; expected one of {list(SCENARIOS)}"
        )
    if entities < len(scenarios):
        raise ValueError(
            f"entities={entities} is below the {len(scenarios)} scenarios requested; every "
            "scenario needs at least one entity or the console it produces has a hole in it"
        )
    return tuple((scenarios[index % len(scenarios)], index) for index in range(entities))


# --- the scripted provider, its decoder, and the inline tools -----------------


def make_provider() -> FakeLLM:
    """Return the demo's provider: one rule, one scripted response.

    A single ``match_any`` rule rather than per-scenario rules — the demo varies
    what the *agent* does, not what the model says, and a script that branched
    would make the token numbers in the console depend on prompt text.
    """
    return FakeLLM([(match_any(), respond_with(DEMO_RESPONSE))])


def decode_demo_response(payload: bytes) -> DecodedResponse:
    """Decode the scripted response into the usage the traces report.

    Fixed counts, not a parse of ``payload``: the response is a constant, and
    deriving the numbers from it would only add a way for them to differ between
    rounds. Without this decoder ``AgentConfig`` refuses the token budget and the
    LLM_CALL traces omit their usage attributes entirely (design D4 of
    ``add-trace-events``), which would empty the console's spend panel.
    """
    return DecodedResponse(
        usage=TokenUsage(
            prompt_tokens=DEMO_PROMPT_TOKENS,
            completion_tokens=DEMO_COMPLETION_TOKENS,
            total_tokens=DEMO_PROMPT_TOKENS + DEMO_COMPLETION_TOKENS,
        ),
        text="triaged",
    )


@tool
def lookup_customer(customer_id: str) -> str:
    """Read-only customer lookup, served from a fixed table."""
    return f"tier=gold;id={customer_id}"


@tool
def risk_score(amount_cents: int) -> str:
    """Read-only risk score for a transaction amount."""
    return f"score={min(99, amount_cents // 1000)}"


@tool
def probe_ledger(target: str) -> str:
    """Read-only ledger probe that always refuses, to drive the failure path."""
    raise ToolError(f"demo: the {target} ledger refused the connection")


def make_tool_registry() -> ToolRegistry:
    """Return the registry the demo's inline (``side_effect=False``) tools live in."""
    registry = ToolRegistry()
    registry.register(lookup_customer)
    registry.register(risk_score)
    registry.register(probe_ledger)
    return registry


def demo_timeout_route(fallback: FallbackContext) -> Route:
    """Route an elapsed suspension by the scenario its key names.

    Pure and synchronous, as the policy contract requires: it reads only the
    ``FallbackContext`` it is handed. ``suspension_timeout`` drops to ``.errors``
    (the record the console's timed-out-approval view renders); everything else
    denies with the runtime's deterministic fallback output, so a key that timed
    out incidentally still ends its wait on the main output.
    """
    if scenario_of(fallback.entity_key) == "suspension_timeout":
        return Drop()
    return Deny()


# --- the agents ---------------------------------------------------------------


def _request(message: str) -> LlmRequest:
    return LlmRequest(
        model_id=DEMO_MODEL_ID,
        messages=[message],
        tools_schema=None,
        sampling_params=None,
    )


async def _run_completion(ctx: ActivationContext) -> Outcome:
    """The plain fast path: one model call, then complete.

    Also what ``orphaned_result`` runs — the orphan is produced by the harness,
    which delivers a tool result after this activation has committed.
    """
    await ctx.call_model(_request("summarize this event"))
    return Complete(output=b"done:" + ctx.entity_key)


async def _run_multi_tool(ctx: ActivationContext) -> Outcome:
    """A model call, then two inline read-only tools.

    The model call runs first so the tools land at step 1 and the trace's causal
    order survives sorting: ``run_tool`` deliberately does not advance the step
    cursor (that cursor mints intent IDs), so tools called *before* a model call
    share its step index and cannot be ordered against it after the fact.
    """
    await ctx.call_model(_request("what should I check for this transaction?"))
    await ctx.run_tool("lookup_customer", {"customer_id": "c-4821"})
    await ctx.run_tool("risk_score", {"amount_cents": 42_000})
    return Complete(output=b"triaged:" + ctx.entity_key)


async def _run_cache_hit(ctx: ActivationContext) -> Outcome:
    """The same request twice in one activation.

    The first call reaches the provider and stages the response in the replay
    cache; the second is served from it with zero provider calls, which is the
    property correctness invariant 3 promises and the only way
    ``beam_agents.cache_hit=true`` ever appears.
    """
    request = _request("classify this transaction")
    await ctx.call_model(request)
    await ctx.call_model(request)
    return Complete(output=b"classified:" + ctx.entity_key)


async def _run_tool_error(ctx: ActivationContext) -> Outcome:
    """An inline tool that raises, taking the activation with it.

    An inline tool's ``TOOL_CALL`` span is staged only *after* the call returns,
    so a failing tool leaves the activation failure as the whole record — and
    ``error.type`` naming the tool exception is what tells it apart from an
    ordinary agent bug under the same ``activation_error`` reason.
    """
    await ctx.run_tool("probe_ledger", {"target": "settlement"})
    return Complete(output=b"unreachable")


async def _run_activation_error(ctx: ActivationContext) -> Outcome:
    """A model call, then a raise: the enriched failure route.

    The raise is what produces the ``beam_agents.failure.*`` position scalars,
    and the model call ahead of it is what makes ``failure.llm_calls`` non-zero —
    the console's failure-context panel is empty without both.
    """
    await ctx.call_model(_request("summarize the incident"))
    raise RuntimeError("demo: the downstream ledger is unavailable")


async def _run_budget_exceeded(ctx: ActivationContext) -> Outcome:
    """Call the model until the per-activation token bound trips."""
    for index in range(BUDGET_SCENARIO_CALLS):
        await ctx.call_model(_request(f"expand chunk {index}"))
    return Complete(output=b"unreachable")


async def _run_intent_dead_letter(ctx: ActivationContext) -> Outcome:
    """Stage an effect the outbox will refuse to route, then complete.

    Staged and then completed rather than suspended: the intent is
    fire-and-forget here, so the key does not sit open waiting for a result the
    outbox is about to refuse to deliver.
    """
    await ctx.call_model(_request("draft the settlement instruction"))
    ctx.act(UNROUTABLE_TOOL, '{"amount_cents":42000,"currency":"USD"}')
    return Complete(output=b"dispatched:" + ctx.entity_key)


async def _run_suspension(ctx: ActivationContext) -> Outcome:
    """Triage, ask a human, and suspend with an explicit deadline.

    Whether this key is approved, denied, or left to time out is the harness's
    decision, not the agent's: all three scenarios run this same activation and
    differ only in what re-enters on the key afterwards.
    """
    await ctx.call_model(_request("triage this transaction"))
    ctx.request_approval('{"action":"freeze","reason":"suspicious wire transfer"}')
    return Suspend(snapshot=b"awaiting-approval", timeout_ms=APPROVAL_TIMEOUT_MS)


# Scenario -> the activation it runs. A table rather than a chain of branches so
# each shape carries its own docstring explaining why the console needs it.
_SCENARIO_ACTIVATIONS: dict[str, Callable[[ActivationContext], Awaitable[Outcome]]] = {
    "completion": _run_completion,
    "orphaned_result": _run_completion,
    "multi_tool": _run_multi_tool,
    "cache_hit": _run_cache_hit,
    "tool_error": _run_tool_error,
    "activation_error": _run_activation_error,
    "budget_exceeded": _run_budget_exceeded,
    "intent_dead_letter": _run_intent_dead_letter,
    "suspension_approved": _run_suspension,
    "suspension_denied": _run_suspension,
    "suspension_timeout": _run_suspension,
}


async def demo_agent(ctx: ActivationContext) -> Outcome:
    """Drive one scenario, chosen by the activation's own entity key.

    Every branch is a shape the console has a view for. Nothing here reads a
    clock or a randomness source, so a replayed bundle walks the same path.
    """
    if ctx.is_resume:
        # Every suspending scenario resumes the same way: the human's decision is
        # on `ctx.resume_approval`, delivered on the key the suspension committed
        # under. A denial is an *answer* — it completes, it does not fail.
        approval = ctx.resume_approval
        approved = approval is not None and approval.approved
        prefix = b"approved:" if approved else b"denied:"
        return Complete(output=prefix + ctx.entity_key)
    activate = _SCENARIO_ACTIVATIONS.get(scenario_of(ctx.entity_key), _run_completion)
    return await activate(ctx)


async def demo_batch_agent(ctx: ActivationContext) -> Outcome:
    """Suspend the first flushed batch so the buffer behind it can overflow.

    Overflow is not reachable on an unblocked ``ADAPTIVE`` pipeline: the size
    trigger flushes the buffer before it can reach its cap. A live continuation
    defers both flush triggers (design D6 of adaptive batching), which is what
    lets the burst pile up and the cap bind.
    """
    if ctx.is_resume:
        return Complete(output=b"batch-resumed:" + ctx.entity_key)
    ctx.request_approval('{"action":"hold","batch":true}')
    return Suspend(snapshot=b"awaiting-batch-approval", timeout_ms=LONG_SUSPENSION_MS)


# --- the intents outbox -------------------------------------------------------


class _SerializeOrRefuse(beam.DoFn):
    """The demo's stand-in for ``WriteIntents``' serializer.

    ``WriteIntents`` dead-letters an intent whose *serialization* fails, and a
    valid proto does not fail to serialize — which would make
    ``intent_dead_letter`` the one reason in the closed vocabulary the demo could
    not produce. This refuses one reserved tool name instead, and is otherwise
    the same shape: refusals go to ``DEAD_LETTER_TAG`` as
    ``((entity_key, ToolIntent), reason)``, and everything else is serialized.
    """

    def process(self, element: tuple[bytes, ToolIntent]) -> Any:
        """Serialize the intent, or route it to the dead-letter output."""
        key, intent = element
        if intent.tool_name == UNROUTABLE_TOOL:
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                (element, f"no route configured for tool {intent.tool_name!r}"),
            )
            return
        yield key, intent.SerializeToString(deterministic=True)


class _DemoOutbox(beam.PTransform):
    """A ``WriteIntents``-shaped outbox with no broker behind it.

    Returns a :class:`WriteIntentsResult` so ``RunAgent`` picks up the
    ``dead_letter`` branch and maps it through the runtime's own
    ``intent_dead_letter_to_error``; the serialized intents are discarded,
    because a demo has nowhere to publish them and the console reads telemetry,
    not the outbox.
    """

    def expand(self, pcoll: beam.pvalue.PCollection) -> WriteIntentsResult:
        """Key the intents by entity, then serialize or refuse each one."""
        keyed = pcoll | "KeyIntentsByEntity" >> beam.WithKeys(
            lambda intent: intent.entity_key
        ).with_output_types(tuple[bytes, ToolIntent])
        tagged = keyed | "SerializeIntent" >> beam.ParDo(_SerializeOrRefuse()).with_outputs(
            DEAD_LETTER_TAG, main="serialized"
        )
        tagged.serialized | "DiscardSerializedIntents" >> beam.Map(_discard)
        return WriteIntentsResult(dead_letter=tagged.dead_letter)


def _discard(element: object) -> None:
    return None


class _DemoSinkResolver:
    """Adds the demo's ``demo://`` outbox and delegates every other scheme.

    Wrapping rather than extending, exactly as ``ConsoleSinkResolver`` does: the
    delegate is whatever resolver the round's delivery target needs, so the same
    demo runs with the default resolver (records collected in-process) or with
    ``ConsoleSinkResolver`` (records pushed over ``console://``).
    """

    def __init__(self, delegate: SinkResolver | None = None) -> None:
        self._delegate: SinkResolver = delegate if delegate is not None else DefaultSinkResolver()

    def validate(self, field_name: str, uri: str) -> None:
        """Accept the demo outbox for ``intents_to``; delegate the rest."""
        if uri == DEMO_OUTBOX_URI:
            return
        self._delegate.validate(field_name, uri)

    def resolve(self, field_name: str, uri: str) -> beam.PTransform:
        """Build the demo outbox for ``demo://``; delegate the rest."""
        if uri == DEMO_OUTBOX_URI:
            return _DemoOutbox()
        return self._delegate.resolve(field_name, uri)


# --- the scripted stream ------------------------------------------------------


def _envelope(key: bytes, event_time_ms: int, payload: bytes) -> TimestampedValue:
    envelope = AgentEnvelope(entity_key=key, event_time_ms=event_time_ms, external_event=payload)
    return TimestampedValue(envelope, event_time_ms / 1000)


def _approval(key: bytes, event_time_ms: int, *, approved: bool) -> TimestampedValue:
    """A human decision re-entering on the key that suspended.

    The ``intent_id`` is recomputed with the runtime's own ``intent_id_for``
    because there is no effector in an offline demo. It is a pure function of
    ``(entity_key, seq, step_index)``, and the approval intent is minted at step
    1 — the triage model call consumes step 0.
    """
    envelope = AgentEnvelope(entity_key=key, event_time_ms=event_time_ms)
    envelope.approval.intent_id = intent_id_for(key, 0, 1)
    envelope.approval.approved = approved
    envelope.approval.approver = "analyst@example.test"
    envelope.approval.decided_at_ms = event_time_ms
    return TimestampedValue(envelope, event_time_ms / 1000)


def _stray_result(key: bytes, event_time_ms: int) -> TimestampedValue:
    """A tool result for an activation that already completed.

    Nothing is waiting for it, so resume admission refuses it with
    ``no_continuation`` — one of the four details the ``orphaned_result`` record
    carries so triage does not have to re-derive them.
    """
    envelope = AgentEnvelope(entity_key=key, event_time_ms=event_time_ms)
    envelope.tool_result.intent_id = intent_id_for(key, 99, 0)
    envelope.tool_result.entity_key = key
    envelope.tool_result.seq = 99
    envelope.tool_result.status = ToolResult.OK
    envelope.tool_result.payload = b'{"settled":true}'
    envelope.tool_result.completed_at_ms = event_time_ms
    return TimestampedValue(envelope, event_time_ms / 1000)


def _export_request(key: bytes, event_time_ms: int) -> TimestampedValue:
    """An in-band state export, so the console's snapshot view is not empty."""
    envelope = AgentEnvelope(entity_key=key, event_time_ms=event_time_ms)
    envelope.export_request.request_id = f"demo-export-{event_time_ms}"
    return TimestampedValue(envelope, event_time_ms / 1000)


def scripted_stream(*, scenarios: tuple[str, ...], entities: int, seed: int) -> TestStream:
    """Build the one scripted stream both branches read.

    Both clocks are scripted: the watermark carries event time (and, at
    infinity, fires working-memory GC), and the processing-time advance is what
    fires the real-time HITL timers. No ``sleep()``, no wall clock, so the round
    is as deterministic as the runtime under it.
    """
    base_ms = DEMO_EPOCH_MS + seed * ROUND_INTERVAL_MS
    plan = assign_scenarios(scenarios, entities)

    stream = TestStream().advance_watermark_to(base_ms / 1000)

    # Phase 1: one event per key. Batching keys get their whole burst, because
    # the buffer has to be filled before anything defers it.
    for offset, (scenario, index) in enumerate(plan):
        key = entity_key_for(scenario, index, seed)
        at_ms = base_ms + offset * 10
        if scenario == "batch_overflow":
            for tick in range(BATCH_EVENT_COUNT):
                stream = stream.add_elements([_envelope(key, at_ms + tick, b"tick-%d" % tick)])
            continue
        stream = stream.add_elements([_envelope(key, at_ms, b'{"kind":"wire-transfer"}')])

    # Phase 2: the answers. Separate `add_elements` calls so the suspension that
    # each one resolves has certainly committed first.
    for offset, (scenario, index) in enumerate(plan):
        key = entity_key_for(scenario, index, seed)
        at_ms = base_ms + 4_000 + offset * 10
        if scenario == "suspension_approved":
            stream = stream.add_elements([_approval(key, at_ms, approved=True)])
        elif scenario == "suspension_denied":
            stream = stream.add_elements([_approval(key, at_ms, approved=False)])
        elif scenario == "orphaned_result":
            stream = stream.add_elements([_stray_result(key, at_ms)])
        elif scenario == "completion":
            stream = stream.add_elements([_export_request(key, at_ms)])

    # Phase 3: elapse the short deadlines, then let working-memory GC run. The
    # advance is measured from the demo clock's own origin, not from zero, so it
    # lands past `APPROVAL_TIMEOUT_MS` and inside `LONG_SUSPENSION_MS`.
    stream = stream.advance_processing_time((base_ms + PROCESSING_ADVANCE_MS) / 1000)
    return stream.advance_watermark_to_infinity()


# --- the pipeline -------------------------------------------------------------


class _SplitByBatchPolicy(beam.PartitionFn):
    """Routes each envelope to the ``RunAgent`` whose batch policy it needs.

    ``BatchPolicy`` is a transform-level setting, so the batching scenario cannot
    share a transform with the rest. Partitioning one stream is what keeps them
    sharing a *clock*, which is what the suspension scenarios depend on.
    """

    def partition_for(self, element: Any, num_partitions: int, *args: Any, **kwargs: Any) -> int:
        """Return 1 for the adaptive-batching branch, 0 for the per-event one."""
        return 1 if scenario_of(element.entity_key) == "batch_overflow" else 0


class _FramedAppender:
    """Picklable terminal sink: append each element's bytes, length-framed.

    A file rather than an in-memory list because the DirectRunner is free to
    execute bundles outside the calling thread, where a closure's appends land in
    a copy nobody reads. The framing is a 4-byte big-endian length — an
    in-process handoff between the pipeline and the delivery target, with no
    compatibility surface, so it deliberately does not reuse the replay bundle's
    varint interchange.
    """

    def __init__(self, path: str, kind: str) -> None:
        self._path = path
        self._kind = kind

    def __call__(self, element: Any) -> None:
        """Append one element's encoded bytes to the file."""
        payload = _ENCODERS[self._kind](element)
        with open(self._path, "ab") as handle:
            handle.write(struct.pack(">I", len(payload)) + payload)


def _encode_trace(event: TraceEvent) -> bytes:
    return bytes(event.SerializeToString(deterministic=True))


def _encode_error(error: ActivationError) -> bytes:
    # The runtime's own bus encoding: an `ActivationErrorRecord` inside an
    # `AgentEnvelope`, exactly what a `kafka://` errors sink would carry.
    return serialize_error_envelope(error)[1]


def _encode_snapshot(snapshot: StateSnapshot) -> bytes:
    return bytes(snapshot.SerializeToString(deterministic=True))


def _encode_output(payload: bytes) -> bytes:
    return payload


_ENCODERS: dict[str, Callable[[Any], bytes]] = {
    "traces": _encode_trace,
    "errors": _encode_error,
    "snapshots": _encode_snapshot,
    "outputs": _encode_output,
}

_STREAM_KINDS = ("traces", "errors", "snapshots", "outputs")


def build(
    pipeline: beam.Pipeline,
    *,
    scenarios: tuple[str, ...] = SCENARIOS,
    entities: int = DEFAULT_ENTITY_COUNT,
    seed: int = 0,
    console: str | None = None,
    capture_dir: str | None = None,
) -> None:
    """Wire the demo's two ``RunAgent`` branches onto one scripted stream.

    ``console`` routes ``.traces``/``.errors``/``.snapshots`` straight to a
    running console through ``ConsoleSinkResolver``. ``capture_dir``, when given,
    additionally appends every record to a framed file per stream, which is how
    the in-process delivery targets get their records back.
    """
    delegate: SinkResolver | None = ConsoleSinkResolver() if console is not None else None
    resolver = _DemoSinkResolver(delegate)

    branches = (
        pipeline
        | "DemoEvents" >> scripted_stream(scenarios=scenarios, entities=entities, seed=seed)
        | "SplitByPolicy" >> beam.Partition(_SplitByBatchPolicy(), 2)
    )

    fast_path = branches[0] | "KeyFastPath" >> beam.WithKeys(
        lambda envelope: envelope.entity_key
    ).with_output_types(tuple[bytes, AgentEnvelope])
    fast = fast_path | "RunDemoAgent" >> RunAgent(
        demo_agent,
        config=AgentConfig(
            provider_factory=make_provider,
            decode=decode_demo_response,
            tool_registry=make_tool_registry(),
            hitl_policy=HitlPolicy(on_timeout=demo_timeout_route),
            max_tokens_per_activation=DEMO_TOKEN_BUDGET,
            intents_to=DEMO_OUTBOX_URI,
            traces_to=console,
            errors_to=console,
            snapshots_to=console,
            sink_resolver=resolver,
        ),
    )

    batched_path = branches[1] | "KeyBatchPath" >> beam.WithKeys(
        lambda envelope: envelope.entity_key
    ).with_output_types(tuple[bytes, AgentEnvelope])
    batched = batched_path | "RunDemoBatchAgent" >> RunAgent(
        demo_batch_agent,
        config=AgentConfig(
            provider_factory=make_provider,
            decode=decode_demo_response,
            batch_policy=BatchPolicy.ADAPTIVE,
            max_batch_size=BATCH_MAX_SIZE,
            max_wait_ms=BATCH_MAX_WAIT_MS,
            max_buffered_events=BATCH_MAX_BUFFERED,
            traces_to=console,
            errors_to=console,
            snapshots_to=console,
            sink_resolver=resolver,
        ),
    )

    if capture_dir is None:
        return

    # `.errors` is exposed unflattened by `RunAgent`; the intents dead letter is
    # a second error stream on its own branch. Merging them here is exactly what
    # `RunAgent` does internally when `errors_to` is configured, through the same
    # runtime mapper, so the demo's captured errors are the sink's contents.
    errors: list[beam.pvalue.PCollection] = [fast.errors, batched.errors]
    if fast.dead_letter is not None:
        errors.append(
            fast.dead_letter | "IntentDeadLetterToError" >> beam.Map(intent_dead_letter_to_error)
        )

    merged = {
        "traces": (fast.traces, batched.traces),
        "errors": tuple(errors),
        "snapshots": (fast.snapshots, batched.snapshots),
        "outputs": (fast.output, batched.output),
    }
    for kind in _STREAM_KINDS:
        path = str(Path(capture_dir) / f"{kind}.bin")
        flat = merged[kind] | f"Flatten_{kind}" >> beam.Flatten()
        flat | f"Capture_{kind}" >> beam.Map(_FramedAppender(path, kind))


# --- what one round produced --------------------------------------------------


@dataclass(frozen=True, slots=True)
class DemoRecords:
    """Everything one demo round put on the four streams the console reads.

    Sorted into a canonical order on construction: the runner emits bundles in
    whatever order it likes, and a demo whose output depends on that is not a
    demo whose screenshots reproduce.
    """

    traces: tuple[TraceEvent, ...] = ()
    errors: tuple[ActivationErrorRecord, ...] = ()
    snapshots: tuple[StateSnapshot, ...] = ()
    outputs: tuple[bytes, ...] = ()

    def __len__(self) -> int:
        """Return the total number of records across all four streams."""
        return len(self.traces) + len(self.errors) + len(self.snapshots) + len(self.outputs)

    def activations(self) -> int:
        """Return the number of committed activations this round produced.

        Counted from ``ACTIVATION_START``, which is emitted only on the commit
        path: a failed activation commits nothing and traces only its ``ERROR``.
        """
        return sum(1 for event in self.traces if event.event_type == TraceEvent.ACTIVATION_START)

    def digest(self) -> str:
        """Return a stable hash over every record, for determinism assertions."""
        hasher = hashlib.sha256()
        for event in self.traces:
            hasher.update(event.SerializeToString(deterministic=True))
        for error in self.errors:
            hasher.update(error.SerializeToString(deterministic=True))
        for snapshot in self.snapshots:
            hasher.update(snapshot.SerializeToString(deterministic=True))
        for payload in self.outputs:
            hasher.update(payload)
        return hasher.hexdigest()


class DemoDelivery(Protocol):
    """Where a round's records go. The seam ``run_demo`` is injected through."""

    def deliver(self, records: DemoRecords) -> None:
        """Accept one round's records."""
        ...


class _StoreDelivery:
    """Writes a round straight into an in-process :class:`ConsoleStore`."""

    def __init__(self, store: ConsoleStore) -> None:
        self._store = store

    def deliver(self, records: DemoRecords) -> None:
        """Normalize the round's protos and write them idempotently."""
        self._store.write(
            normalize(
                events=records.traces,
                errors=records.errors,
                snapshots=records.snapshots,
                provenance=PROVENANCE_NATIVE,
            )
        )


class _NullDelivery:
    """Accepts a round and drops it.

    Two callers, for opposite reasons: with ``console://`` configured the sink
    already pushed every record from inside the pipeline, and with no target at
    all the round exists to be summarized to stdout rather than stored.
    """

    def deliver(self, records: DemoRecords) -> None:
        """Do nothing; nothing further is owed for this round."""


# --- reading the captured streams back ----------------------------------------


def _read_framed(path: Path) -> tuple[bytes, ...]:
    if not path.exists():
        return ()
    payload = path.read_bytes()
    records: list[bytes] = []
    offset = 0
    while offset < len(payload):
        (size,) = struct.unpack(">I", payload[offset : offset + 4])
        offset += 4
        records.append(payload[offset : offset + size])
        offset += size
    return tuple(records)


def _trace_sort_key(event: TraceEvent) -> tuple[Any, ...]:
    """Canonical per-activation order: attempt, then rank, then step.

    Two inline tool calls at the same step index are ordered by span id, which is
    deterministic but not causal — ``run_tool`` deliberately does not advance the
    step cursor (it would move the intent IDs the activation goes on to mint), so
    their relative order is genuinely not on the wire to recover.
    """
    return (
        event.entity_key,
        event.seq,
        event.start_ms,
        _EVENT_RANK.get(event.event_type, len(_EVENT_RANK)),
        event.step_index,
        event.span_id,
        event.event_type,
    )


def _read_records(capture_dir: Path) -> DemoRecords:
    traces = [TraceEvent.FromString(raw) for raw in _read_framed(capture_dir / "traces.bin")]
    errors = [
        ActivationErrorRecord.FromString(AgentEnvelope.FromString(raw).external_event)
        for raw in _read_framed(capture_dir / "errors.bin")
    ]
    snapshots = [
        StateSnapshot.FromString(raw) for raw in _read_framed(capture_dir / "snapshots.bin")
    ]
    outputs = _read_framed(capture_dir / "outputs.bin")
    return DemoRecords(
        traces=tuple(sorted(traces, key=_trace_sort_key)),
        errors=tuple(
            sorted(errors, key=lambda e: (e.entity_key, e.event_time_ms, e.reason, e.detail))
        ),
        snapshots=tuple(sorted(snapshots, key=lambda s: (s.entity_key, s.seq, s.snapshot_at_ms))),
        outputs=tuple(sorted(outputs)),
    )


# --- the per-scenario summary -------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScenarioSummary:
    """What one scenario put on the streams, as the demo reports it."""

    scenario: str
    keys: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    input_tokens: int = 0
    output_tokens: int = 0
    cache_hits: int = 0


def summarize(records: DemoRecords) -> dict[str, ScenarioSummary]:
    """Group a round's records by the scenario their entity key names.

    Every number here is read off a ``TraceEvent`` attribute or an
    ``ActivationErrorRecord`` field. Nothing is derived from a duration: spans
    are zero-width by design, so there is no elapsed time in the trace bytes to
    report.
    """
    grouped: dict[str, dict[str, Any]] = {}
    for event in records.traces:
        entry = grouped.setdefault(scenario_of(event.entity_key), _empty_group())
        entry["keys"].add(event.entity_key.decode())
        entry["event_types"].append(TraceEvent.EventType.Name(event.event_type))
        if event.event_type == TraceEvent.ACTIVATION_END:
            entry["statuses"].append(event.attributes["beam_agents.activation.status"])
        entry["input_tokens"] += int(event.attributes.get("gen_ai.usage.input_tokens", "0"))
        entry["output_tokens"] += int(event.attributes.get("gen_ai.usage.output_tokens", "0"))
        if event.attributes.get("beam_agents.cache_hit") == "true":
            entry["cache_hits"] += 1
    for error in records.errors:
        entry = grouped.setdefault(scenario_of(error.entity_key), _empty_group())
        entry["keys"].add(error.entity_key.decode())
        entry["reasons"].append(error.reason)

    return {
        scenario: ScenarioSummary(
            scenario=scenario,
            keys=tuple(sorted(entry["keys"])),
            statuses=tuple(entry["statuses"]),
            event_types=tuple(entry["event_types"]),
            reasons=tuple(sorted(entry["reasons"])),
            input_tokens=entry["input_tokens"],
            output_tokens=entry["output_tokens"],
            cache_hits=entry["cache_hits"],
        )
        for scenario, entry in sorted(grouped.items(), key=lambda item: SCENARIOS.index(item[0]))
    }


def _empty_group() -> dict[str, Any]:
    return {
        "keys": set(),
        "statuses": [],
        "event_types": [],
        "reasons": [],
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_hits": 0,
    }


def render_summary(records: DemoRecords) -> str:
    """Render a round's per-scenario summary as plain text.

    This is what ``python -m beam_agents.console._demo`` prints with no console
    and no store configured, and what ``docs/examples/console-demo.md`` pastes:
    a demo whose only output is "it ran" is not a demo anyone can check.
    """
    grouped = summarize(records)
    lines = [
        f"{len(records)} records over {len(grouped)} scenarios: "
        f"{len(records.traces)} trace events, {len(records.errors)} errors, "
        f"{len(records.snapshots)} snapshots, {len(records.outputs)} outputs "
        f"({records.activations()} committed activations)",
        "",
    ]
    for summary in grouped.values():
        lines.append(f"{summary.scenario}")
        lines.append(f"  keys      {', '.join(summary.keys)}")
        lines.append(f"  status    {', '.join(summary.statuses) or '(none committed)'}")
        lines.append(f"  events    {' > '.join(summary.event_types) or '(none)'}")
        lines.append(f"  reasons   {', '.join(summary.reasons) or '(none)'}")
        lines.append(
            f"  tokens    in={summary.input_tokens} out={summary.output_tokens} "
            f"cache_hits={summary.cache_hits}"
        )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# --- running it ---------------------------------------------------------------


def run_round(
    *,
    scenarios: tuple[str, ...],
    entities: int,
    seed: int,
    console: str | None,
    runner: str | None,
) -> DemoRecords:
    """Run one demo round and return everything it put on the four streams."""
    # `flags=[]` rather than the default: `PipelineOptions()` scrapes `sys.argv`,
    # which under `python -m ...` is the demo's own CLI and produces a page of
    # "unparseable argument" warnings on the way to ignoring it.
    options = PipelineOptions(flags=[], runner=runner) if runner else PipelineOptions(flags=[])
    options.view_as(StandardOptions).streaming = True
    with tempfile.TemporaryDirectory(prefix="beam-agents-console-demo-") as capture_dir:
        with beam.Pipeline(options=options) as pipeline:
            build(
                pipeline,
                scenarios=scenarios,
                entities=entities,
                seed=seed,
                console=console,
                capture_dir=capture_dir,
            )
        return _read_records(Path(capture_dir))


def run_demo(
    *,
    console: str | None = None,
    store: ConsoleStore | None = None,
    entities: int = DEFAULT_ENTITY_COUNT,
    scenarios: tuple[str, ...] = SCENARIOS,
    seed: int = 0,
    loop: bool = False,
    **options: Any,
) -> int:
    """Run the demo pipeline; return the number of activations produced.

    Delivers either to a running console over ``console://`` or straight into an
    in-process ``store``. The second path is what makes the demo usable as test
    data without standing a server up.

    Recognized ``options``:

    - ``delivery``: an object with ``deliver(records)``, used instead of the
      console/store defaults. The seam this module's own tests inject through.
    - ``rounds``: how many rounds to run under ``loop=True``; ``None`` (the
      default) runs until interrupted.
    - ``interval_s``: seconds to wait between rounds (default ``5.0``).
    - ``runner``: a Beam runner name, defaulting to the DirectRunner.
    - ``print_summary``: print each round's summary to stdout.

    Each round runs at ``seed + round``, so a looping demo keeps producing
    records the store has not already seen — it dedups on
    ``(trace_id, span_id, event_type)``, and those are ``uuid5`` over the entity
    key, which carries the seed.
    """
    delivery: DemoDelivery | None = options.pop("delivery", None)
    rounds: int | None = options.pop("rounds", None)
    interval_s: float = options.pop("interval_s", 5.0)
    runner: str | None = options.pop("runner", None)
    print_summary: bool = options.pop("print_summary", False)
    if options:
        raise TypeError(f"run_demo got unexpected options {sorted(options)}")

    if delivery is None:
        if store is not None:
            delivery = _StoreDelivery(store)
        elif console is not None:
            delivery = _NullDelivery()
        else:
            raise ValueError(
                "run_demo needs somewhere to put its records: pass console=<console:// URI>, "
                "store=<ConsoleStore>, or delivery=<object with deliver(records)>"
            )

    # Validated before any pipeline exists, so a typo in `scenarios` fails at the
    # call site rather than as an empty console twenty seconds later.
    assign_scenarios(scenarios, entities)

    activations = 0
    round_index = 0
    while True:
        records = run_round(
            scenarios=scenarios,
            entities=entities,
            seed=seed + round_index,
            console=console,
            runner=runner,
        )
        delivery.deliver(records)
        if print_summary:
            print(render_summary(records))
        activations += records.activations()
        round_index += 1
        if not loop or (rounds is not None and round_index >= rounds):
            return activations
        if interval_s > 0:
            time.sleep(interval_s)


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m beam_agents.console._demo``."""
    parser = argparse.ArgumentParser(
        prog="python -m beam_agents.console._demo",
        description=(
            "Run the beam-agents console demo pipeline: every trace, error, and "
            "snapshot record the runtime can produce, on the DirectRunner over a "
            "scripted fake provider."
        ),
    )
    parser.add_argument(
        "--console",
        default=None,
        help="deliver over console://host:port to a running console",
    )
    parser.add_argument(
        "--store",
        default=None,
        help="deliver into the SQLite ConsoleStore at this path",
    )
    parser.add_argument("--entities", type=int, default=DEFAULT_ENTITY_COUNT)
    parser.add_argument(
        "--scenarios",
        default=",".join(SCENARIOS),
        help="comma-separated subset of the scenarios to drive",
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--loop", action="store_true", help="keep producing rounds")
    parser.add_argument("--rounds", type=int, default=None)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--runner", default=None)
    parser.add_argument(
        "--quiet", action="store_true", help="do not print the per-scenario summary"
    )
    args = parser.parse_args(argv)

    scenarios = tuple(name.strip() for name in args.scenarios.split(",") if name.strip())
    # With neither a console nor a store there is still something worth doing:
    # run the round and print what it produced. An entry point whose only output
    # is an exit code makes a broken demo indistinguishable from a working one,
    # so the summary is on by default and the delivery is explicitly a no-op.
    print_summary = not args.quiet
    store = ConsoleStore(args.store) if args.store is not None else None
    delivery = _NullDelivery() if store is None and args.console is None else None

    try:
        produced = run_demo(
            console=args.console,
            store=store,
            entities=args.entities,
            scenarios=scenarios,
            seed=args.seed,
            loop=args.loop,
            delivery=delivery,
            rounds=args.rounds,
            interval_s=args.interval,
            runner=args.runner,
            print_summary=print_summary,
        )
    except ValueError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        return 0
    finally:
        if store is not None:
            store.close()
    if not args.quiet:
        print(f"{produced} activations produced")
    return 0


if __name__ == "__main__":
    sys.exit(main())
