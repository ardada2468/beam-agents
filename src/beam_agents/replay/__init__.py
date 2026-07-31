"""Local, offline replay of one activation from a snapshot plus its trace.

A tool surface, not part of the public API: nothing here is re-exported from
``beam_agents``. The console script ``beam-agents-replay`` is the entry point;
the modules under it are:

- :mod:`beam_agents.replay.provider` — the cache-only tripwire client. It holds
  no transport and serves nothing: every cached request is answered by
  ``ActivationContext.call_model``'s own cache-first path, so reaching this
  client at all *is* the miss.
- :mod:`beam_agents.replay.bundle` — loading and version-checking the
  ``StateSnapshot``, parsing the framed ``TraceEvent`` stream, selecting the
  target ``(entity_key, seq)``, reconstructing ``run_activation``'s arguments,
  and driving the re-run.
- :mod:`beam_agents.replay.diff` — comparing the re-run against the traced
  record on the trace-comparable surface, and rendering the difference.

Nothing in this package opens a network connection, and nothing writes back to
a snapshot: replay reads.

Importing this module has no side effects.
"""

from __future__ import annotations

__all__: list[str] = []
