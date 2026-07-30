# memory-facade Specification (delta)

## ADDED Requirements

### Requirement: Read-only LRU-order enumeration for compaction strategies
`Memory` SHALL expose `keys()` returning a tuple of stored key names in LRU order (least-recently-used first, matching the order `to_blob()` persists) and `entry_size(key)` returning the stored encoded value size in bytes for an existing key (raising `KeyError` for an absent one). Neither call SHALL re-stamp `last_access_ms`, reorder entries, or set `dirty` — a compaction strategy must be able to iterate candidates without perturbing the eviction order it is iterating. Both SHALL reflect staged (uncommitted) mutations, and `entry_size` values SHALL sum to `size_bytes` across all keys.

#### Scenario: keys() reports LRU order without dirtying the facade
- **WHEN** keys `a`, `b`, `c` are loaded from a blob, `b` is read via `get`, and `keys()` is then called on a freshly-loaded copy versus the mutated facade
- **THEN** the fresh facade's `keys()` returns `(a, b, c)` with `dirty` still `False`, and the mutated facade's returns `(a, c, b)`; calling `keys()` itself changes neither ordering nor `dirty`

#### Scenario: entry_size does not perturb eviction order
- **WHEN** `entry_size` is called for the least-recently-used key and a subsequent LRU-order eviction pass runs
- **THEN** that key is still evicted first, and the sum of `entry_size` over `keys()` equals `size_bytes`
