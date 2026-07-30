"""The framework-neutral core of the adapter transport hook.

Hoisted move-only from ``adapters/langgraph/transport.py`` (change
``add-pydantic-ai-adapter``): :class:`_ReplayTransport`, the activation
contextvar, and the generic client probing / fallback-warning helpers contain
nothing framework-specific — they parse a provider-shaped JSON request body
into an ``LlmRequest``, await ``ActivationContext.call_model`` (cache-first,
zero provider calls on the cached path, correctness invariant 3), and
materialize the ``LlmResponse`` bytes as an ``httpx.Response``.

What *is* framework-specific stays in each adapter's own ``transport`` module:
the attribute table that finds the SDK client on a model object, and the
module whose logger/metrics the fallback warning is emitted through (so a
dashboard names the adapter that degraded). Adapters call
:func:`find_async_client`/:func:`install_transport` with their probing table
and :func:`warn_fallback` with their logger.
"""

from __future__ import annotations

import json
from contextvars import ContextVar
from typing import TYPE_CHECKING

import httpx
from apache_beam.metrics.metric import Metrics

from beam_agents.model.client import LlmRequest

if TYPE_CHECKING:
    import logging
    from collections.abc import Sequence

    from beam_agents.core.context import ActivationContext

_METRIC_NAMESPACE = "beam_agents.adapters"
_FALLBACK_COUNTER = "transport_fallback"

# The activation currently driving the framework run. Contextvars propagate
# into the tasks and worker threads a framework spawns, and activations run
# strictly sequentially per DoFn instance (per-key serialization + one bridge
# loop), so a single shared var is race-free — for every adapter at once.
_current_activation: ContextVar[ActivationContext | None] = ContextVar(
    "beam_agents_adapter_activation", default=None
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


def find_async_client(model: object, sdk_client_attrs: Sequence[str]) -> httpx.AsyncClient | None:
    """The model's underlying ``httpx.AsyncClient``, or ``None`` if unrecognized.

    Probes each attribute in ``sdk_client_attrs`` for either the httpx client
    itself or an SDK object whose ``_client`` is one (the official
    anthropic/openai SDK layout every recognized stack shares).
    """
    for attr in sdk_client_attrs:
        sdk = getattr(model, attr, None)
        if sdk is None:
            continue
        if isinstance(sdk, httpx.AsyncClient):
            return sdk
        inner = getattr(sdk, "_client", None)
        if isinstance(inner, httpx.AsyncClient):
            return inner
    return None


def install_transport(model: object, sdk_client_attrs: Sequence[str]) -> bool:
    """Swap the model's httpx transport for the replay transport (idempotent).

    Returns False when the model has no recognizable httpx client, in which
    case the model is left untouched.
    """
    client = find_async_client(model, sdk_client_attrs)
    if client is None:
        return False
    if not isinstance(client._transport, _ReplayTransport):
        client._transport = _ReplayTransport(client._transport)
    return True


def warn_fallback(model: object, *, logger: logging.Logger, metrics: type[Metrics]) -> None:
    """Log the once-per-instance fallback warning and count the degradation.

    ``logger`` and ``metrics`` are the *calling adapter module's* bindings, so
    the warning carries the adapter's logger name and the counter increment
    goes through that module's (patchable) ``Metrics`` — existing tests that
    patch ``adapters.langgraph.transport.Metrics`` keep working unchanged.
    """
    logger.warning(
        "chat model %s has no recognizable httpx client; its provider calls bypass "
        "the runtime LLMClient and lose replay-cache protection (bundle retries "
        "will re-hit the provider)",
        type(model).__name__,
    )
    metrics.counter(_METRIC_NAMESPACE, _FALLBACK_COUNTER).inc()
