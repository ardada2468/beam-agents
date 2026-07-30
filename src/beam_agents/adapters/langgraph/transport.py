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

The framework-neutral core (:class:`_ReplayTransport`, the activation
contextvar, the probing/fallback helpers) lives in
:mod:`beam_agents.adapters._transport`, shared with the Pydantic AI adapter;
this module keeps the LangGraph-specific probing table and re-exports the
moved names unchanged.
"""

from __future__ import annotations

import logging

import httpx
from apache_beam.metrics.metric import Metrics

from beam_agents.adapters import _transport as _shared
from beam_agents.adapters._transport import (
    _FALLBACK_COUNTER,
    _METRIC_NAMESPACE,
    _RESERVED_BODY_KEYS,
    _current_activation,
    _ReplayTransport,
)

__all__ = [
    "_FALLBACK_COUNTER",
    "_METRIC_NAMESPACE",
    "_RESERVED_BODY_KEYS",
    "_ReplayTransport",
    "_current_activation",
    "find_async_client",
    "install_transport",
    "warn_fallback",
]

_LOGGER = logging.getLogger(__name__)

# Attribute paths probed for the SDK client on a chat-model object. Covers the
# langchain-openai (`root_async_client`, `async_client`) and langchain-anthropic
# (`_async_client`) layouts; the SDK object's `_client` is the httpx client.
_SDK_CLIENT_ATTRS = ("root_async_client", "_async_client", "async_client")


def find_async_client(model: object) -> httpx.AsyncClient | None:
    """The model's underlying ``httpx.AsyncClient``, or ``None`` if unrecognized."""
    return _shared.find_async_client(model, _SDK_CLIENT_ATTRS)


def install_transport(model: object) -> bool:
    """Swap the model's httpx transport for the replay transport (idempotent).

    Returns False when the model has no recognizable httpx client, in which
    case the model is left untouched.
    """
    return _shared.install_transport(model, _SDK_CLIENT_ATTRS)


def warn_fallback(model: object) -> None:
    """Log the once-per-instance fallback warning and count the degradation."""
    _shared.warn_fallback(model, logger=_LOGGER, metrics=Metrics)
