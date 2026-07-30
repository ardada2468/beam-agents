"""Transport recognition for Pydantic AI models: HTTP calls served by the
runtime LLMClient.

The framework-neutral interception core lives in
:mod:`beam_agents.adapters._transport`; this module contributes only the
Pydantic AI probing table. The pinned range's provider model classes
(``OpenAIChatModel``/``OpenAIResponsesModel``, ``AnthropicModel``) expose the
official SDK client as a ``client`` property, and that SDK object's
``_client`` is the ``httpx.AsyncClient`` the replay transport rewires — the
identical wire stack the runtime's own provider clients use.

Unrecognized model objects are left alone: the caller logs one warning per
agent instance via :func:`warn_fallback`, which also increments the
``beam_agents.adapters/transport_fallback`` counter so the degradation is
visible on dashboards, not just in logs. Belt-and-braces for exotic setups:
Pydantic AI providers accept a caller-supplied ``http_client`` at
construction, so a user can always hand their model a client the adapter is
guaranteed to recognize.
"""

from __future__ import annotations

import logging

import httpx
from apache_beam.metrics.metric import Metrics

from beam_agents.adapters import _transport as _shared

_LOGGER = logging.getLogger(__name__)

# Attribute paths probed for the SDK client on a Pydantic AI model object:
# the pinned range's Anthropic/OpenAI model classes all expose the SDK client
# as `client`, whose `_client` is the httpx client.
_SDK_CLIENT_ATTRS = ("client",)


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
