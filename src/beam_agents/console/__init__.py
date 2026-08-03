"""The Beam Agents Console: a local viewer for what the runtime already records.

The runtime emits a complete telemetry surface — deterministic ``TraceEvent``s
(``observability/traces.py``), ``ActivationErrorRecord``s over a closed reason
vocabulary (``core/error_records.py``), and ``StateSnapshot``s
(``core/snapshot.py``) — and every existing delivery path ends at a wire
boundary: bytes on a topic, rows in BigQuery, spans at a collector. This package
is the reader that closes that loop without asking for infrastructure: a WAL
SQLite store, an HTTP read API, a live stream, and a bundled UI, in one process
over one file.

Nothing here runs inside an activation. ``console://`` is resolved by
:class:`ConsoleSinkResolver`, which *wraps* the runtime's ``DefaultSinkResolver``
and is opt-in through the existing ``AgentConfig.sink_resolver`` seam, so no
module on the hot path is modified (design D2).

The public surface is deliberately five names. Everything else is underscore-
private and therefore outside the frozen API — the console is a tool driven by a
URI and a CLI, not a library callers compose against, and binding its schema,
route table, and query shapes at 1.0 would freeze decisions that must stay free
to move (design D1).

Optional dependencies are imported inside the function that needs them, never at
module scope: ``import beam_agents.console`` succeeds with no extras installed.

Importing this module has no side effects.
"""

from __future__ import annotations

from beam_agents.console._app import create_app, serve
from beam_agents.console._sink import ConsoleSinkResolver, WriteToConsole
from beam_agents.console._store import ConsoleStore

__all__ = [
    "ConsoleSinkResolver",
    "ConsoleStore",
    "WriteToConsole",
    "create_app",
    "serve",
]
