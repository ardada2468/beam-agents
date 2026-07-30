"""Long-term `MemoryStore` backends for the memory-stores capability.

The second memory tier: durable, per-entity rows behind an async
``load``/``save``/``search`` contract with a seq-guarded idempotent upsert —
the sanctioned invariant-5 exception ("documented idempotent upserts to the
long-term MemoryStore keyed by ``(key, seq)``"). Reached only explicitly, via
``ctx.memory.longterm``; nothing here is consulted implicitly by the working
tier.

Backend client libraries (Bigtable, Redis, Firestore, SQLAlchemy) are the
optional ``memory-stores`` extra and are imported lazily inside the backend
constructors: importing this package — and using the ABC, factory, and
in-memory store — requires none of them.

Nothing here is re-exported from ``beam_agents``'s public API surface.
"""

from beam_agents.memory.stores.base import (
    InMemoryMemoryStore,
    MemoryRecord,
    MemoryStore,
    build_memory_store,
    parse_memory_store_uri,
)

__all__ = [
    "InMemoryMemoryStore",
    "MemoryRecord",
    "MemoryStore",
    "build_memory_store",
    "parse_memory_store_uri",
]
