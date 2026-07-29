"""The httpx transport hook: chat-model HTTP calls served by the runtime LLMClient.

A recognized chat model (the langchain-openai / langchain-anthropic layout: an
SDK client at a known attribute whose ``_client`` is an ``httpx.AsyncClient``)
gets its transport swapped for :class:`_ReplayTransport`. During an activation
the transport parses the provider request body — provider-shaped JSON, exactly
what ``LlmRequest``'s fields hold — and serves it through
``ActivationContext.call_model``: cache-first (zero provider calls on the
cached path, correctness invariant 3), runtime-provider on a miss, with the
step cursor, replay-cache insert, tally, and trace staging all on the one
existing path. Outside an activation the wrapped original transport handles the
request untouched.

Unrecognized model objects are left alone: the caller logs one warning per
agent instance (naming the model class and the lost replay-cache protection)
via :func:`warn_fallback`, which also increments the
``beam_agents.adapters/transport_fallback`` counter so the degradation is
visible on dashboards, not just in logs.
"""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from typing import TYPE_CHECKING

import httpx
from apache_beam.metrics.metric import Metrics

from beam_agents.model.client import LlmRequest

if TYPE_CHECKING:
    from beam_agents.core.context import ActivationContext

_LOGGER = logging.getLogger(__name__)

_METRIC_NAMESPACE = "beam_agents.adapters"
_FALLBACK_COUNTER = "transport_fallback"

# The activation currently driving the graph. Contextvars propagate into the
# tasks and worker threads LangGraph spawns, and activations run strictly
# sequentially per DoFn instance (per-key serialization + one bridge loop), so
# a single var is race-free.
_current_activation: ContextVar[ActivationContext | None] = ContextVar(
    "beam_agents_langgraph_activation", default=None
)

# Attribute paths probed for the SDK client on a chat-model object. Covers the
# langchain-openai (`root_async_client`, `async_client`) and langchain-anthropic
# (`_async_client`) layouts; the SDK object's `_client` is the httpx client.
_SDK_CLIENT_ATTRS = ("root_async_client", "_async_client", "async_client")

# Body keys that map to dedicated LlmRequest fields; everything else is
# sampling parameters.
_RESERVED_BODY_KEYS = frozenset({"model", "messages", "tools"})


class _ReplayTransport(httpx.AsyncBaseTransport):
    """Serves provider requests through the current activation's model path."""

    def __init__(self, wrapped: httpx.AsyncBaseTransport) -> None:
        self._wrapped = wrapped

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        ctx = _current_activation.get()
        if ctx is None:
            return await self._wrapped.handle_async_request(request)
        body: dict[str, object] = json.loads(request.content) if request.content else {}
        llm_request = LlmRequest(
            model_id=str(body.get("model", "")),
            messages=body.get("messages"),
            tools_schema=body.get("tools"),
            sampling_params={k: v for k, v in body.items() if k not in _RESERVED_BODY_KEYS},
        )
        response = await ctx.call_model(llm_request)
        return httpx.Response(
            200,
            content=response.response,
            headers={"content-type": "application/json"},
            request=request,
        )


def find_async_client(model: object) -> httpx.AsyncClient | None:
    """The model's underlying ``httpx.AsyncClient``, or ``None`` if unrecognized."""
    for attr in _SDK_CLIENT_ATTRS:
        sdk = getattr(model, attr, None)
        if sdk is None:
            continue
        if isinstance(sdk, httpx.AsyncClient):
            return sdk
        inner = getattr(sdk, "_client", None)
        if isinstance(inner, httpx.AsyncClient):
            return inner
    return None


def install_transport(model: object) -> bool:
    """Swap the model's httpx transport for the replay transport (idempotent).

    Returns False when the model has no recognizable httpx client, in which
    case the model is left untouched.
    """
    client = find_async_client(model)
    if client is None:
        return False
    if not isinstance(client._transport, _ReplayTransport):
        client._transport = _ReplayTransport(client._transport)
    return True


def warn_fallback(model: object) -> None:
    """Log the once-per-instance fallback warning and count the degradation."""
    _LOGGER.warning(
        "chat model %s has no recognizable httpx client; its provider calls bypass "
        "the runtime LLMClient and lose replay-cache protection (bundle retries "
        "will re-hit the provider)",
        type(model).__name__,
    )
    Metrics.counter(_METRIC_NAMESPACE, _FALLBACK_COUNTER).inc()
