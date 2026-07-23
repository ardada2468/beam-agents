# memory-facade Specification

## Purpose
TBD - created by archiving change add-memory-facade. Update Purpose after archive.
## Requirements
### Requirement: Facade stages mutations over an in-memory MemoryBlob
`beam_agents.memory` SHALL provide a `Memory` facade constructed per activation from an optional `MemoryBlob` and a caller-supplied `now_ms` clock value. The facade SHALL mutate only in-memory data — it MUST NOT perform any Beam state I/O or read wall-clock time — and SHALL expose `to_blob()` returning a `MemoryBlob` with `state_schema_version` set to 1, entries emitted in LRU order (least-recently-used first), and `total_value_bytes` populated. A `dirty` property SHALL be `False` until the first successful mutation or LRU-updating access.

#### Scenario: Blob round-trips through the facade
- **WHEN** a `Memory` is constructed from a blob containing existing entries and `to_blob()` is called without any access
- **THEN** the returned blob has field-equal entries in the same order, the same `total_value_bytes`, `state_schema_version` 1, and `dirty` is `False`

#### Scenario: Fresh facade produces a versioned empty blob
- **WHEN** a `Memory` is constructed with no blob and `to_blob()` is called
- **THEN** the result has `state_schema_version` 1, no entries, and `total_value_bytes` 0

#### Scenario: Rejected mutation leaves staged state unchanged
- **WHEN** any mutation raises (`MemoryOverflow` or `TypeError`)
- **THEN** subsequent reads and `to_blob()` reflect the state as of before the failed call (except compaction effects, per the hard-cap requirement)

### Requirement: Scalar get/set/delete with LRU stamping
`Memory.set(key, value)` SHALL store arbitrary bytes (including empty) under a string key, `get(key)` SHALL return the stored bytes or `None` if absent, and `delete(key)` SHALL remove the entry (idempotent on absent keys). Every `get` hit, `set`, and `append` SHALL stamp the entry's `last_access_ms` with the facade's `now_ms` and move it to most-recently-used position, setting `dirty`.

#### Scenario: Set then get round-trips bytes
- **WHEN** `set("k", b"v")` is followed by `get("k")`
- **THEN** `get` returns `b"v"`, and `get("missing")` returns `None`

#### Scenario: Access order is persisted for LRU
- **WHEN** keys `a`, `b`, `c` are set and then `a` is read via `get`
- **THEN** `to_blob()` emits entries in order `b`, `c`, `a` with `a.last_access_ms` equal to the facade's `now_ms`

#### Scenario: Delete removes and is idempotent
- **WHEN** `set("k", b"v")`, `delete("k")`, `delete("k")` are called in sequence
- **THEN** no error is raised, `get("k")` returns `None`, and `total_value_bytes` reflects the removal

### Requirement: Append maintains a bounded ring per key
`Memory.append(key, item, max_items=64)` SHALL maintain an ordered ring of bytes items under one key, encoded inside the entry's `value` bytes with no proto schema change. When an append would exceed `max_items`, the oldest items SHALL be dropped until the bound holds. `ring(key)` SHALL return the items in oldest-to-newest order (empty for absent keys). Kind mixing SHALL fail fast: `append` on a scalar key and `get` or `ring` on the wrong kind SHALL raise `TypeError`; `set` SHALL be permitted to overwrite a ring with a scalar.

#### Scenario: Appends preserve order and survive blob round-trip
- **WHEN** items `b"1"`, `b"2"`, `b"3"` are appended to `"log"` and the facade's `to_blob()` output is loaded into a new `Memory`
- **THEN** `ring("log")` on the new facade returns `(b"1", b"2", b"3")`

#### Scenario: Ring drops oldest at capacity
- **WHEN** four items are appended to a key with `max_items=3`
- **THEN** `ring` returns the last three items in order and `total_value_bytes` no longer accounts for the dropped item

#### Scenario: Kind mixing raises
- **WHEN** `set("k", b"v")` is followed by `append("k", b"x")`, or `append("r", b"x")` is followed by `get("r")`
- **THEN** each wrong-kind call raises `TypeError` and the stored entry is unchanged

### Requirement: Size accounting is incremental and exact
The facade SHALL maintain `total_value_bytes` as the exact sum of stored entry value sizes (including ring encoding overhead) across every mutation, without rescanning all entries, and SHALL expose it via a `size_bytes` property that always equals a from-scratch recomputation over the current entries.

#### Scenario: Accounting matches recomputation under mixed operations
- **WHEN** an arbitrary (property-based) sequence of `set`, `delete`, and `append` operations is applied
- **THEN** after every operation `size_bytes` equals the sum of stored value lengths, and `to_blob().total_value_bytes` equals `size_bytes`

### Requirement: Soft cap warns and triggers compaction at 75%
When a mutation lands with `size_bytes` at or above 75% of the 1 MiB hard cap (786 432 bytes), the facade SHALL log a `WARNING`, increment the Beam metrics counter `beam_agents.memory:soft_cap_warnings`, and invoke the configured compaction hook. The warning and counter SHALL fire at most once per facade instance; writes at or above the soft cap but under the hard cap SHALL still succeed.

#### Scenario: Crossing the soft cap warns once and compacts
- **WHEN** writes push `size_bytes` from below to at or above 786 432 bytes, followed by further writes still above the threshold
- **THEN** exactly one warning is logged, the counter is incremented once, the compactor's `compact` is invoked, and every write under the hard cap succeeds

#### Scenario: No compactor configured is not an error
- **WHEN** the soft cap is crossed on a facade constructed without a compactor
- **THEN** the warning and counter still fire and the write succeeds

### Requirement: Hard cap raises MemoryOverflow at 1 MiB
A mutation whose prospective `size_bytes` exceeds 1 048 576 bytes SHALL first invoke the compaction hook (if configured) once; if the prospective total still exceeds the cap, the facade SHALL raise `MemoryOverflow` — exported from `beam_agents.memory` and carrying the key, attempted size, and cap — without applying the triggering write. Compaction effects applied before rejection SHALL persist in the staged blob.

#### Scenario: Overflowing write is rejected atomically
- **WHEN** a `set` would push `size_bytes` past 1 048 576 bytes on a facade with no compactor
- **THEN** `MemoryOverflow` is raised, `get` of the target key returns its prior value, and `size_bytes` is unchanged

#### Scenario: Compaction that frees space lets the write succeed
- **WHEN** an overflowing `set` occurs with a compactor configured that deletes enough entries to fit the write
- **THEN** no exception is raised, the write is applied, and the compactor's deletions are reflected in `to_blob()`

#### Scenario: Compaction that frees too little still rejects
- **WHEN** an overflowing `set` occurs and the compactor frees some but not enough space
- **THEN** `MemoryOverflow` is raised, the triggering write is not applied, and the compactor's deletions persist

### Requirement: Compaction hook is a stable protocol with a safe default
`beam_agents.memory` SHALL export a `Compactor` protocol with a single method `compact(memory: Memory) -> None` that receives the facade itself, so compaction strategies mutate memory only through the guarded API. Cap enforcement SHALL be suspended during `compact()` to prevent re-entry, and a facade constructed without a compactor SHALL behave as if a no-op compactor were configured. An exception raised by a compactor SHALL propagate unmodified.

#### Scenario: Compactor mutates through the facade with correct accounting
- **WHEN** a compactor's `compact` deletes entries and rewrites one key via `set` during a soft-cap invocation
- **THEN** `size_bytes` afterwards equals a from-scratch recomputation and no nested compaction is triggered by the compactor's own writes

#### Scenario: Compactor exceptions propagate
- **WHEN** a compactor raises a custom exception during hard-cap handling
- **THEN** that exception (not `MemoryOverflow`) propagates to the caller
