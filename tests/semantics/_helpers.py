"""Shared fixtures for the semantics tier.

Module-level (not closures) so agents pickle cleanly for the DirectRunner —
same convention as ``tests/core/_dofn_helpers.py``.
"""

from __future__ import annotations

from beam_agents.core.agent import Complete, Suspend
from beam_agents.core.context import ActivationContext
from beam_agents.model.client import LlmRequest

_TTL_MS = 60_000


def repeated_request() -> LlmRequest:
    """The single request this scenario's agent issues both pre- and post-suspend."""
    return LlmRequest(model_id="m", messages=["hello"], tools_schema=None, sampling_params=None)


async def suspend_then_recall_agent(ctx: ActivationContext) -> Complete | Suspend:
    """Call the model, stage an intent, and suspend; on resume, call the
    identical model request again before completing.

    The resume's repeated call has the same ``entity_key``/``seq``/request
    material as the pre-suspend call, so it reads the ``LLM_CACHE`` committed
    at suspend time (correctness invariant 3) rather than hitting the
    provider again — the scenario the retry-determinism gate verifies.
    """
    resp = await ctx.call_model(repeated_request())
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=_TTL_MS)
    return Complete(output=b"resumed:" + resp.response)


async def budgeted_suspend_then_recall_agent(ctx: ActivationContext) -> Complete | Suspend:
    """:func:`suspend_then_recall_agent` under a configured token budget.

    Reports the meter's running total on the output, so the gate can compare
    the budget decision the original walk made against the one the retried,
    cache-served walk makes. A trip would fail the activation instead; the
    scenario is an activation that committed *within* budget, so the claim
    under test is that the retry charges identically rather than by luck.
    """
    resp = await ctx.call_model(repeated_request())
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"waiting", adapter="test", timeout_ms=_TTL_MS)
    return Complete(output=b"resumed:" + resp.response + b"#" + str(_consumed(ctx)).encode())


def _consumed(ctx: ActivationContext) -> int:
    """This attempt's charged total, read off the context's meter."""
    budget = ctx._budget
    assert budget is not None, "the scenario configures a budget"
    return budget.consumed


async def batch_act_agent(ctx: ActivationContext) -> Complete:
    """Stage one intent and complete with the joined batch.

    The batch's composition rides the output and the intent's ID rides
    ``(entity_key, seq, step_index)``, so a chaos-retried flush bundle that
    re-read a different buffer -- or minted from a different scope -- is
    visible in both.
    """
    ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
    return Complete(output=b"|".join(ctx.events))


async def batch_suspend_then_recall_agent(ctx: ActivationContext) -> Complete | Suspend:
    """The batch-flush form of :func:`suspend_then_recall_agent`.

    The suspending activation is a flush over a buffered batch; the resume
    repeats the identical model request under the same ``(entity_key, seq)``
    scope, so it must read the ``LLM_CACHE`` committed at suspend time rather
    than reach the provider again -- across a chaos-forced retry included.
    """
    resp = await ctx.call_model(repeated_request())
    if not ctx.is_resume:
        ctx.act("http.post", '{"url":"x"}', ttl_ms=_TTL_MS)
        return Suspend(snapshot=b"|".join(ctx.events), adapter="test", timeout_ms=_TTL_MS)
    return Complete(output=b"resumed:" + resp.response)
