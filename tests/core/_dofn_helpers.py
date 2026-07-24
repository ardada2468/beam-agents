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
from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest, ProviderError
from beam_agents.model.fake import FakeLLM, match_any, raise_error, respond_with

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
    ctx.memory.append("log", ctx.event, max_items=64)
    ring = b",".join(ctx.memory.ring("log"))
    return Complete(output=ring + b"#" + str(ctx.seq).encode())


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
    if ctx.event == b"FAIL":
        ctx.memory.set("scratch", b"should-not-persist")
        raise RuntimeError("conditional failure")
    ctx.memory.append("log", ctx.event, max_items=64)
    ring = b",".join(ctx.memory.ring("log"))
    return Complete(output=ring + b"#" + str(ctx.seq).encode())


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
    if ctx.event == b"SLOW":
        return await hang_agent(ctx)
    return await append_agent(ctx)


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


async def sleep_briefly(ms: int) -> None:  # pragma: no cover - trivial
    await asyncio.sleep(ms / 1000)
