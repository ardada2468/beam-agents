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
