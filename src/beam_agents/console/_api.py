"""The read API's route table: a thin mapping onto ``_queries``.

Deliberately thin. Every question lives in ``_queries.py`` where it can be
tested without an HTTP client; this module turns request parameters into a
filter object, calls one query, and returns its model. A route that grows logic
belongs in the query layer instead.

Read-only with respect to agent state: nothing here writes to a running
pipeline, and there is no endpoint that could. Ingest lives in ``_app.py``,
separately, so the distinction is visible in the file layout.

``fastapi`` is imported inside :func:`build_router`, not at module scope, so
``import beam_agents.console`` works with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import APIRouter

    from beam_agents.console._store import ConsoleStore

__all__ = ["DEFAULT_PAGE_SIZE", "MAX_PAGE_SIZE", "build_router"]

DEFAULT_PAGE_SIZE = 50
# A ceiling, not a suggestion: the UI never asks for more, and an unbounded
# `limit` on a store a pipeline is actively writing is a way to hold a read
# connection open long enough to matter.
MAX_PAGE_SIZE = 500


def build_router(store: ConsoleStore, **options: Any) -> APIRouter:
    """Build the ``/api`` router over ``store``.

    Routes: ``/overview``, ``/activations``, ``/activations/{entity_key}/{seq}``,
    ``/traces``, ``/traces/{trace_id}``, ``/errors``, ``/errors/groups``,
    ``/models``, ``/tools``, ``/approvals``, ``/entities``, ``/search``, and
    ``/store``.
    """
    raise NotImplementedError
