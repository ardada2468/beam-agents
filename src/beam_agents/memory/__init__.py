"""Working-memory facade over the keyed ``MemoryBlob`` state value.

Agent code and the activation context reach per-key working memory through
:class:`Memory`, never by touching Beam state or the ``MemoryBlob`` proto
directly. The facade stages mutations on an in-memory blob (the stateful DoFn
loads it before and commits it after each activation), enforces the working
-memory size invariants (incremental accounting, 75% soft-cap warning, 1 MiB
hard cap via :class:`MemoryOverflow`), and exposes a stable :class:`Compactor`
hook so compaction strategies can ship without touching the facade.

Importing this package has no side effects.
"""

from beam_agents.memory.facade import Compactor, Memory, MemoryOverflow

__all__ = ["Compactor", "Memory", "MemoryOverflow"]
