"""The ADK httpx transport hook: model HTTP calls served by the runtime LLMClient.

The same seam the LangGraph adapter proved out
(``beam_agents.adapters.langgraph.transport``), taught the google-genai client
layout: an ADK model wrapper (``google.adk.models.google_llm.Gemini``) holds a
``google.genai.Client`` at ``api_client``, whose ``_api_client`` (the genai
``BaseApiClient``) owns the ``_async_httpx_client`` — an ``httpx.AsyncClient``.
A recognized model gets that client's transport swapped for
:class:`_ReplayTransport`: during an activation the transport parses the
provider request body and serves it through ``ActivationContext.call_model`` —
cache-first (zero provider calls on the cached path, correctness invariant 3),
runtime-provider on a miss. Outside an activation the wrapped original
transport handles the request untouched.

Unrecognized model objects are left alone: the caller logs one warning per
agent instance via :func:`_warn_fallback`, which also increments the shared
``beam_agents.adapters/transport_fallback`` counter.

NOTE(coordinator): this module is deliberately a local seam. A parallel change
hoists the LangGraph ``_ReplayTransport`` into a shared framework-free
``beam_agents.adapters._transport``; once that lands, this module should
delegate to it (the recognition table below is the only ADK-specific part).
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

# Shared with the LangGraph adapter on purpose: one dashboard counter covers
# the degradation across adapters.
_METRIC_NAMESPACE = "beam_agents.adapters"
_FALLBACK_COUNTER = "transport_fallback"

# The activation currently driving the ADK run. Contextvars propagate into the
# tasks ADK spawns (including its parallel tool `gather`), and activations run
# strictly sequentially per DoFn instance (per-key serialization + one bridge
# loop), so a single var is race-free.
_current_activation: ContextVar[ActivationContext | None] = ContextVar(
    "beam_agents_adk_activation", default=None
)

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


def _genai_httpx_client(candidate: object) -> httpx.AsyncClient | None:
    """The ``httpx.AsyncClient`` inside a ``google.genai.Client``-shaped object."""
    api = getattr(candidate, "_api_client", None)
    if api is None:
        return None
    inner = getattr(api, "_async_httpx_client", None)
    return inner if isinstance(inner, httpx.AsyncClient) else None


def _find_async_client(model: object) -> httpx.AsyncClient | None:
    """The model's underlying ``httpx.AsyncClient``, or ``None`` if unrecognized.

    Probes, in order: the object itself being an ``httpx.AsyncClient``, a
    ``google.genai.Client`` passed directly (``_api_client._async_httpx_client``),
    and the ADK ``Gemini`` wrapper layout (``api_client`` → genai client).
    ``Gemini.api_client`` is a ``cached_property`` that *constructs* the genai
    client and raises without credentials, so the probe is exception-guarded:
    an unconstructable client is an unrecognized model, handled by the
    warning-fallback degradation rather than an activation failure.
    """
    if isinstance(model, httpx.AsyncClient):
        return model
    direct = _genai_httpx_client(model)
    if direct is not None:
        return direct
    try:
        api_client = getattr(model, "api_client", None)
    except Exception:  # cached_property construction may raise (e.g. no API key)
        return None
    if api_client is None:
        return None
    return _genai_httpx_client(api_client)


def _install_transport(model: object) -> bool:
    """Swap the model's httpx transport for the replay transport (idempotent).

    Returns False when the model has no recognizable httpx client, in which
    case the model is left untouched.
    """
    client = _find_async_client(model)
    if client is None:
        return False
    if not isinstance(client._transport, _ReplayTransport):
        client._transport = _ReplayTransport(client._transport)
    return True


def _warn_fallback(model: object) -> None:
    """Log the once-per-instance fallback warning and count the degradation."""
    _LOGGER.warning(
        "ADK model %s has no recognizable httpx client; its provider calls bypass "
        "the runtime LLMClient and lose replay-cache protection (bundle retries "
        "will re-hit the provider)",
        type(model).__name__,
    )
    Metrics.counter(_METRIC_NAMESPACE, _FALLBACK_COUNTER).inc()
