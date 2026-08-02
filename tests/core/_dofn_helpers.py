"""Shared fixtures and picklable test agents for the stateful-runtime tests.

Agents are module-level (not lambdas/closures) so the ``_AgentDoFn`` that holds
them pickles cleanly for the DirectRunner. Every agent surfaces committed state
into its output/intents so tests can assert persistence, ordering, and seq
progression through pipeline outputs alone.
"""

from __future__ import annotations

import asyncio

import apache_beam as beam

from beam_agents._protos import AgentEnvelope
from beam_agents.core.agent import Complete, FallbackContext, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.hitl import Escalate, Route
from beam_agents.model.client import LlmRequest, ProviderError
from beam_agents.model.facade import TokenUsage
from beam_agents.model.fake import FakeLLM, match_any, raise_error, respond_with
from beam_agents.tools import ToolRegistry, tool

# Tool intents get a fixed TTL in tests; the value only feeds expires_at.
_TTL_MS = 60_000


def keyed(pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
    """Key a ``PCollection[AgentEnvelope]`` by ``entity_key`` for ``RunAgent``.

    ``RunAgent`` no longer keys elements itself (add-runagent-transform); every
    pipeline test keys its envelope stream upstream with this helper.
    """
    return pcoll | beam.WithKeys(lambda e: e.entity_key).with_output_types(
        tuple[bytes, AgentEnvelope]
    )


def make_pong_provider() -> FakeLLM:
    """Provider that answers every request with ``b"pong"`` and no latency."""
    return FakeLLM([(match_any(), respond_with(b"pong"))])


def make_slow_provider() -> FakeLLM:
    """Provider whose single response sleeps 30s, so a small activation timeout
    cancels the awaiting coroutine well before it returns.
    """
    return FakeLLM([(match_any(), respond_with(b"pong", latency_ms=30_000))])


def make_briefly_slow_provider() -> FakeLLM:
    """Provider slow enough to blow a millisecond-scale activation timeout, but
    fast enough to *finish* well inside a test's own patience.

    The distinction matters for the timeout bound itself: against
    ``make_slow_provider``'s 30s, an activation that ignored its timeout would
    simply hang, which is indistinguishable from a slow test. Against this one
    it completes and commits, so a test asserting the timeout error fails
    outright.
    """
    return FakeLLM([(match_any(), respond_with(b"pong", latency_ms=300))])


def make_failing_provider() -> FakeLLM:
    """Provider that always raises a ``ProviderError``."""
    return FakeLLM([(match_any(), raise_error(ProviderError("boom")))])


def request(text: str = "hello") -> LlmRequest:
    return LlmRequest(model_id="m", messages=[text], tools_schema=None, sampling_params=None)


# -- agents ---------------------------------------------------------------------


async def seq_agent(ctx: ActivationContext) -> Complete:
    """Complete with the activation's seq, revealing SEQ progression per key."""
    return Complete(output=str(ctx.seq).encode())


async def append_agent(ctx: ActivationContext) -> Complete:
    """Append the event payload to a memory ring and return ``ring#seq``.

    The joined ring reveals per-key ordering and memory persistence; the ``#seq``
    suffix reveals SEQ. A TTL wipe resets both (empty ring, seq back to 0).
    """
    ctx.memory.append("log", ctx.single_event, max_items=64)
    ring = b",".join(ctx.memory.ring("log"))
    return Complete(output=ring + b"#" + str(ctx.seq).encode())


async def bulk_write_agent(ctx: ActivationContext) -> Complete:
    """Write 150 KiB under ``bulk``, enough to cross the hard cap on a full key.

    Paired with a pre-loaded, nearly-full ``MemoryBlob`` so the write's
    prospective total exceeds the 1 MiB cap: with a compactor configured it
    succeeds after LRU eviction, without one it raises ``MemoryOverflow``.
    """
    ctx.memory.set("bulk", b"b" * 150_000)
    return Complete(output=b"written")


async def model_agent(ctx: ActivationContext) -> Complete:
    """Call the model once and return its response bytes (exercises the cache)."""
    resp = await ctx.call_model(request())
    return Complete(output=resp.response)


async def conditional_append_agent(ctx: ActivationContext) -> Complete:
    """Append the event to a ring and return ``ring#seq`` — but raise (after a
    memory write) when the event is ``b"FAIL"``.

    Lets a test prove a failed activation commits nothing: the failing element
    neither persists its scratch write nor advances SEQ, so the next append
    lands on the pre-failure ring with the next-lower seq.
    """
    if ctx.single_event == b"FAIL":
        ctx.memory.set("scratch", b"should-not-persist")
        raise RuntimeError("conditional failure")
    ctx.memory.append("log", ctx.single_event, max_items=64)
    ring = b",".join(ctx.memory.ring("log"))
    return Complete(output=ring + b"#" + str(ctx.seq).encode())


@tool
def lookup(customer_id: str) -> str:
    """Uppercase a customer id: the read-only test tool for inline execution."""
    return customer_id.upper()


def make_tool_registry() -> ToolRegistry:
    """Registry holding the module-level `lookup` tool.

    A fresh registry per call (never a module-global instance), honoring the
    no-global-mutable-state convention; the `Tool` itself is module-level so it
    pickles by reference into the DoFn for DirectRunner tests.
    """
    registry = ToolRegistry()
    registry.register(lookup)
    return registry


async def inline_tool_agent(ctx: ActivationContext) -> Complete:
    """Run the read-only `lookup` tool inline and complete with its value."""
    value = await ctx.run_tool("lookup", {"customer_id": ctx.single_event.decode() or "x"})
    return Complete(output=str(value).encode())


async def outcome_routing_agent(ctx: ActivationContext) -> Complete:
    """Pick an outcome from the event payload, so one bounded pipeline can
    exercise several of them at once (each on its own key).

    Deliberately has no suspending branch: a suspension left unresumed in a
    *bounded* pipeline meets the end-of-input watermark advance and the
    real-time HITL timer at once, and which fires first is the runner's choice.
    Suspension counting is asserted under `TestStream`, where both clocks are
    scripted (see test_dofn_streaming).
    """
    if ctx.single_event == b"FAIL":
        raise RuntimeError("routed failure")
    if ctx.single_event == b"ACT":
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Complete(output=b"acted")
    if ctx.single_event == b"MODEL":
        response = await ctx.call_model(request())
        return Complete(output=response.response)
    return Complete(output=ctx.single_event)


async def raising_agent(ctx: ActivationContext) -> Complete:
    """Write memory, then raise — the failed activation must commit nothing."""
    ctx.memory.set("scratch", b"should-not-persist")
    raise RuntimeError("agent blew up")


async def hang_agent(ctx: ActivationContext) -> Complete:
    """Await the slow provider so a small activation timeout cancels it."""
    resp = await ctx.call_model(request())
    return Complete(output=resp.response)


async def timeout_or_append_agent(ctx: ActivationContext) -> Complete:
    """Hang (via the slow provider) on ``b"SLOW"`` events; append otherwise.

    Paired with ``make_slow_provider`` and a small activation timeout so the SLOW
    element times out while the surrounding appends commit normally.
    """
    if ctx.single_event == b"SLOW":
        return await hang_agent(ctx)
    return await append_agent(ctx)


async def model_then_act_agent(ctx: ActivationContext) -> Complete:
    """Call the model, then stage an intent, then complete.

    One activation that exercises every child-event kind the fast path can
    produce, so a pipeline test can assert the whole trace shape at once.
    """
    resp = await ctx.call_model(request())
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    return Complete(output=resp.response)


async def model_act_then_fail_agent(ctx: ActivationContext) -> Complete:
    """Call the model, stage an intent, then raise.

    The enriched `activation_error` route sees a failure at step 2 whose last
    staged event is the intent's — the position the failure context must name.
    """
    await ctx.call_model(request())
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    raise RuntimeError("agent blew up")


async def suspend_then_complete_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Suspend on the first activation (emitting an intent); complete on resume.

    On resume, the incoming tool-result payload is echoed as the output, proving
    the continuation was rehydrated and the same logical seq was used.
    """
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=1000)
    assert ctx.resume_result is not None
    return Complete(output=b"resumed:" + ctx.resume_result.payload)


def escalate_once(fallback: FallbackContext) -> Route:
    """Timeout policy that pages a second approver instead of denying.

    Module-level (not a lambda) so the `HitlPolicy` holding it pickles for the
    DirectRunner, exactly as `HitlPolicy` documents.
    """
    return Escalate(tool_name="pager", args_json='{"level":2}', timeout_ms=5_000)


async def approval_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Request a human approval and suspend; on resume, report the decision.

    The suspension's deadline is `event_time + 1000ms`, and the approval
    intent's TTL is far longer, so the deadline under test is the timeout.
    """
    if not ctx.is_resume:
        ctx.request_approval('{"amount":5}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"awaiting-approval", adapter="test", timeout_ms=1000)
    approval = ctx.resume_approval
    if approval is not None:
        return Complete(output=b"approved" if approval.approved else b"rejected")
    assert ctx.resume_result is not None
    return Complete(output=b"result:" + str(ctx.resume_result.status).encode())


async def suspend_then_act_again_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Stage an intent and suspend; on resume, stage a second intent.

    Both intents belong to the same ``seq``, so their IDs only differ if the
    resumed activation continues the suspended one's step index instead of
    restarting at zero.
    """
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"first"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=1000)
    ctx.act("http.post", '{"url":"second"}', ttl_ms=_TTL_MS)
    return Complete(output=b"done")


async def suspend_then_fail_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Suspend on the first activation; raise on resume.

    Lets a test prove `_resume`'s fail-closed path commits nothing on a
    resumed activation's own failure, independent of `_start`'s failure
    handling.
    """
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=1000)
    raise RuntimeError("resume blew up")


async def usage_reporting_agent(ctx: ActivationContext) -> Complete:
    """Report decoded provider usage through the context's staging sink.

    The runtime's own `ctx.call_model` never decodes the opaque response bytes,
    so nothing on the plain runtime path reports usage; the framework adapters
    do (`adapters/pydantic_ai` calls exactly this). Standing in for one of them
    is what lets a DirectRunner pipeline reach the three usage distributions.
    """
    ctx.accumulate_usage(TokenUsage(prompt_tokens=7, completion_tokens=5, total_tokens=12))
    return Complete(output=b"reported")


async def sleep_briefly(ms: int) -> None:  # pragma: no cover - trivial
    await asyncio.sleep(ms / 1000)


# -- adaptive batching ----------------------------------------------------------


async def batch_join_agent(ctx: ActivationContext) -> Complete:
    """Join the activation's events and append its seq: ``a|b#0``.

    Written against `ctx.events`, the uniform accessor, so the same agent runs
    under either policy: one activation per event under `NONE`, one per flush
    under `ADAPTIVE`. The joined payloads reveal the batch's contents and
    arrival order; the ``#seq`` suffix reveals that a flush of N events
    consumed exactly one seq.
    """
    return Complete(output=b"|".join(ctx.events) + b"#" + str(ctx.seq).encode())


async def batch_prior_agent(ctx: ActivationContext) -> Complete:
    """Report the key's committed memory alongside the batch, then write to it.

    ``prior/joined-events``, so a flush that reasoned from a blank working
    memory instead of the key's committed one is visible in the output rather
    than only in the blob nobody asserts on.
    """
    prior = ctx.memory.get("prior") or b"-"
    joined = b"|".join(ctx.events)
    ctx.memory.set("last_batch", joined)
    return Complete(output=prior + b"/" + joined)


async def batch_shape_agent(ctx: ActivationContext) -> Complete:
    """Report the agent-visible shape: ``<is_batch>:<type>:<len(events)>``.

    The list-ness of `ctx.event` is a policy-level contract, so the agent has
    to be able to see it; this is the runtime-visible proof of that.
    """
    kind = "list" if isinstance(ctx.event, list) else "bytes"
    return Complete(output=f"{ctx.is_batch}:{kind}:{len(ctx.events)}".encode())


async def batch_intent_agent(ctx: ActivationContext) -> Complete:
    """Stage one intent, then complete with the joined batch.

    The intent's `created_at_ms`/`expires_at_ms` are derived from the
    activation clock, which for a flush is the batch's own `max(event_time_ms)`
    — so the committed intent pins the batch clock.
    """
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    return Complete(output=b"|".join(ctx.events))


async def batch_suspend_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Suspend the first batch on an intent; on resume, echo the snapshot.

    The snapshot carries the batch's payloads, which is the only thing that
    can: `Continuation` does not persist the batched events, so a resume that
    reports them proves the snapshot-owns-resume-state rule holds at batch
    granularity (design D5). Only the ``seq 0`` activation suspends, so a
    later flush of the buffer that grew during the suspension completes and its
    own seq is observable.
    """
    if ctx.is_resume:
        return Complete(output=b"resumed:" + ctx.snapshot + b"#" + str(ctx.seq).encode())
    if ctx.seq == 0:
        ctx.act("http.post", '{"url":"batch"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"|".join(ctx.events), adapter="test", timeout_ms=1000)
    return await batch_join_agent(ctx)


class FixedClock:
    """Picklable wall-clock double reading one fixed epoch-seconds value.

    `FLUSH_TIMER` is armed at `time_fn() + max_wait_ms`, and the DirectRunner's
    `TestStream` clock starts at epoch zero — so a pipeline test must arm from
    the same timeline it advances, or the mark lands decades in the future and
    no scripted `advance_processing_time` can ever reach it. Never a `sleep()`.
    """

    def __init__(self, now_s: float = 0.0) -> None:
        self._now_s = now_s

    def __call__(self) -> float:
        return self._now_s


class SteppingClock:
    """Picklable wall clock advancing a fixed step per reading.

    Distinguishes "armed once, on the first buffered event" from "re-armed per
    element": under this clock the two produce different marks, so a re-arming
    implementation misses the scripted processing-time advance instead of
    passing on an indistinguishable one.

    The counter is module-instance state carried through pickling by value.
    The classic DirectRunner (which a REAL_TIME timer forces) runs in-process,
    and the DoFn reads this clock exactly once per buffer start, so the reading
    sequence is deterministic for a scripted stream.
    """

    def __init__(self, start_s: float = 0.0, step_s: float = 0.4) -> None:
        self._next_s = start_s
        self._step_s = step_s

    def __call__(self) -> float:
        reading = self._next_s
        self._next_s += self._step_s
        return reading
