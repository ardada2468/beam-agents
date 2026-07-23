# Add Memory Facade

## Why

Agent code needs a safe, ergonomic way to read and mutate per-key working memory without touching Beam state primitives or the `MemoryBlob` proto directly. Today only the schema exists (`MemoryBlob` from wire-schemas); there is no API enforcing the size invariants (1 MiB working-memory hard cap, staged-mutation atomicity) that the runtime depends on. The facade is a prerequisite for the activation context (`ctx.memory`) and the stateful DoFn.

## What Changes

- New `beam_agents.memory` package with a `Memory` facade that wraps a `MemoryBlob` held in keyed state:
  - `get(key)` / `set(key, value)` / `delete(key)` for scalar bytes values.
  - `append(key, item)` with **ring semantics**: a key can hold a bounded ring of items; when the ring is full the oldest item is dropped. Ring items are length-prefixed inside the entry's `value` bytes, so no proto schema change is needed.
  - Every access updates `last_access_ms` (caller-supplied clock) so LRU ordering stays representable per the wire-schemas spec.
- **Size accounting**: `total_value_bytes` is maintained incrementally on every mutation and always equals the sum of entry value sizes.
- **Soft cap warning**: when utilization crosses 75% of the hard cap, the facade logs a warning and increments a Beam metric (once per activation, not per write).
- **Hard cap**: a mutation that would push `total_value_bytes` past 1 MiB first runs the compaction hook (if configured); if still over, it raises `MemoryOverflow` and leaves the blob unchanged, so the activation fails cleanly with no partial state.
- **Compaction hook interface**: a `Compactor` protocol (`compact(memory) -> None`) invoked at soft-cap crossing and before hard-cap rejection. Concrete compactors (summarization, etc.) are out of scope — this change ships the interface and a no-op default only.
- The facade mutates an in-memory blob only (staged); committing the blob back to Beam state is the DoFn's job (future change). A `dirty` flag lets the DoFn skip state writes when nothing changed.

## Capabilities

### New Capabilities
- `memory-facade`: get/set/delete/append API over the keyed `MemoryBlob`, ring-buffer append semantics, incremental size accounting, 75% soft-cap warning, 1 MiB hard cap with `MemoryOverflow`, and the compaction hook interface.

### Modified Capabilities

None. `MemoryBlob` already carries `state_schema_version`, ordered entries with `last_access_ms`, and `total_value_bytes`; the ring encoding lives inside opaque entry `value` bytes, so wire-schemas requirements are unchanged.

## Impact

- New code: `src/beam_agents/memory/__init__.py`, `src/beam_agents/memory/facade.py`; tests under `tests/memory/`.
- No new dependencies; no proto changes; no changes to the public root API (`beam_agents/__init__.py` re-exports are untouched — the facade is reached via the future activation context).
- Downstream consumers (future changes): `core/context.py` (`ctx.memory`), `core/dofn.py` (load/commit around activations), `memory/compaction` implementations.
