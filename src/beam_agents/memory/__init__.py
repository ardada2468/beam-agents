"""Working-memory facade over the keyed ``MemoryBlob`` state value, plus the
compaction strategies that keep it inside its caps.

Agent code and the activation context reach per-key working memory through
:class:`Memory`, never by touching Beam state or the ``MemoryBlob`` proto
directly. The facade stages mutations on an in-memory blob (the stateful DoFn
loads it before and commits it after each activation), enforces the working
-memory size invariants (incremental accounting, 75% soft-cap warning, 1 MiB
hard cap via :class:`MemoryOverflow`), and exposes a stable :class:`Compactor`
hook so compaction strategies can ship without touching the facade.

Two strategies ship behind that seam, split by where they are allowed to run
(``beam_agents.memory.compaction``): :class:`DropOldestCompactor`, the
synchronous LLM-free default the facade itself invokes at its cap sites, and
:class:`SummarizeCompactor`, which the loop driver runs inside the activation so
its model calls are replay-cached. :class:`FlushToLongterm` is the shipped
``on_expire`` hook demoting a key's final blob to the long-term tier at TTL.

Importing this package has no side effects.
"""

from beam_agents.memory.compaction import (
    DropOldestCompactor,
    ExpireHook,
    ExpiringMemory,
    FlushToLongterm,
    SummarizationView,
    SummarizeCompactor,
    Summarizer,
)
from beam_agents.memory.facade import Compactor, LongtermMemory, Memory, MemoryOverflow

__all__ = [
    "Compactor",
    "DropOldestCompactor",
    "ExpireHook",
    "ExpiringMemory",
    "FlushToLongterm",
    "LongtermMemory",
    "Memory",
    "MemoryOverflow",
    "SummarizationView",
    "SummarizeCompactor",
    "Summarizer",
]
