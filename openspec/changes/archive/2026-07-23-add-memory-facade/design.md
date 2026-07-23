# Design: add-memory-facade

## Context

Working memory per key is a single `MemoryBlob` proto in `ReadModifyWriteState` (Python SDK has no MapState, so the bounded map lives inside one blob with explicit LRU ordering). The wire-schemas spec already fixes the schema: `state_schema_version`, ordered `MemoryEntry {key, value bytes, last_access_ms}`, and a cached `total_value_bytes`. Correctness invariant 1 (atomic commit) requires that all memory writes be staged in the activation and applied only on bundle success — so the facade must never touch Beam state itself; it mutates an in-memory blob that the DoFn loads before and commits after the activation.

Constraints that shape this design:
- Working-memory hard cap is 1 MiB per key (project constraint; release-gating).
- State is protobuf, never pickle; entry order must encode LRU so the TTL GC (future change) can evict without extra bookkeeping.
- `mypy --strict`, no `Any` in public signatures; determinism across bundle retries (no wall-clock reads inside the facade).

## Goals / Non-Goals

**Goals:**
- Single ergonomic API (`get`/`set`/`delete`/`append`) that agent code and the future `ctx.memory` use for all working-memory access.
- Enforce size invariants at the API boundary: incremental accounting, 75% soft-cap warning, `MemoryOverflow` at 1 MiB.
- Ring-buffer append without any proto schema change.
- A stable `Compactor` protocol so compaction strategies can ship later without touching the facade.

**Non-Goals:**
- No Beam state I/O, timers, or GC — that is `core/dofn.py` (future change).
- No concrete compaction strategies (summarization etc.); only the hook and a no-op default.
- No long-term MemoryStore (Bigtable/Redis/Firestore) integration.
- No change to `protos/beam_agents.proto`.

## Decisions

### D1: Facade wraps an in-memory blob; clock is injected and frozen per activation

`Memory(blob: MemoryBlob | None, *, now_ms: int, compactor: Compactor | None = None)` is constructed once per activation. It parses entries into an insertion-ordered `dict` for O(1) access; `to_blob()` re-emits entries in LRU order (least-recently-used first) with `state_schema_version = 1` and the maintained `total_value_bytes`. All accesses in one activation stamp the same caller-supplied `now_ms` (activation start time), with LRU ties broken by touch order via dict reinsertion — fully deterministic across bundle retries, no `time.time()` anywhere in the module.

*Alternative considered:* mutating the proto in place. Rejected: repeated-field reordering on every touch is O(n) per access and makes partial-failure states possible; a dict rebuild at `to_blob()` is one O(n) pass at commit.

### D2: Ring entries are self-describing via a 1-byte kind tag inside `value`

Entry `value` bytes start with a kind tag: `0x00` = scalar (rest is the raw value), `0x01` = ring (rest is a sequence of items, each prefixed with a 4-byte big-endian u32 length). This keeps the wire schema untouched (values stay opaque bytes), survives blob round-trips without external metadata, and the encoding is deterministic.

Kind rules: `set` always writes a scalar (overwriting a ring is allowed — it is an explicit replace); `append` on a scalar key raises `TypeError`; `get` on a ring key raises `TypeError`; `ring(key)` returns the items of a ring key (`()` if absent) and raises `TypeError` on a scalar. Strict errors over silent coercion because kind confusion is always an agent-code bug.

*Alternative considered:* key namespacing (`key/0`, `key/1`, …) for ring items. Rejected: pollutes the keyspace, makes item ordering depend on string sorting, and turns one logical mutation into many entries.

### D3: `append` ring bound is per-call `max_items` (default 64)

`append(key, item, *, max_items=64)` drops oldest items until `len ≤ max_items`. Per-call rather than per-facade because different keys legitimately want different depths (recent events vs. observations). The byte cost of a ring still counts fully toward the 1 MiB cap, so `max_items` is a shape control, not a size control.

### D4: Size accounting is over stored bytes, maintained incrementally

`total_value_bytes` = Σ `len(entry.value)` as stored — including kind tags and ring length prefixes, matching the wire-schemas definition ("sum of entry value sizes"). Every mutation adjusts the running total by the delta (old stored size → new stored size); no full rescans on the hot path. A `size_bytes` property exposes it; `to_blob()` writes it into the proto.

### D5: Cap enforcement order — check, compact, then reject; compaction effects persist

Hard cap `HARD_CAP_BYTES = 1_048_576` (1 MiB), soft threshold = 75% = 786 432 bytes.

For a mutation with prospective total > hard cap: invoke the compactor once (if configured), recompute, and if still over raise `MemoryOverflow(key, attempted_bytes, cap_bytes)` **without applying the triggering write**. Compaction changes made before the rejection persist — they are legitimate staged mutations and everything is discarded anyway if the activation fails. After a mutation lands with total ≥ soft threshold, the facade logs one `WARNING` and increments Beam counter `beam_agents.memory:soft_cap_warnings` — at most once per facade instance (= per activation) to avoid log spam — and invokes the compactor (best-effort; failure to shrink below 75% is not an error).

*Alternative considered:* raising at soft cap. Rejected: the whole point of a soft cap is early signal + automatic compaction while writes still succeed.

### D6: Reads update LRU and mark the facade dirty

`get`/`ring` re-stamp `last_access_ms` and move the entry to most-recent, setting the `dirty` flag, because TTL GC correctness depends on persisted access order. `dirty` starts `False` and lets the DoFn skip the state write for activations that never touch memory. Trade-off (accepted): a read-only-memory activation still incurs a state write; activations that use memory at all typically write anyway.

### D7: Public surface and errors

`beam_agents.memory` exports `Memory`, `MemoryOverflow`, `Compactor` (a `typing.Protocol` with `compact(memory: Memory) -> None`). `MemoryOverflow` subclasses `Exception` (not `ValueError`): it is a runtime capacity condition routed to the errors output, not a construction-time misconfiguration. Root `beam_agents/__init__.py` is untouched. The compactor receives the facade itself, so strategies use the same guarded API (its deletes/rewrites keep accounting correct by construction); recursive cap enforcement during compaction is disabled to prevent re-entry.

## Risks / Trade-offs

- [Ring encoding is a private convention inside opaque bytes] → Encoder/decoder isolated in module-private helpers with property-based round-trip tests; a corrupt/truncated ring raises a clear `ValueError` rather than yielding garbage items.
- [`max_items` per call means two call sites can disagree for the same key] → Documented: the bound applied is the one passed on the current call; last writer wins. Acceptable for v0; a per-key policy registry would be framework creep.
- [Reads dirty the blob (D6)] → Extra state writes on read-only activations; measured as negligible against the p50 < 15 ms overhead budget since the blob write is one RMW state op the DoFn performs at most once per activation.
- [Compactor is user code and can itself overflow or loop] → Re-entrant cap enforcement is disabled during `compact()`, but the post-compaction check still rejects the triggering write if the compactor failed to free space; a compactor raising propagates and fails the activation atomically (invariant 1).
- [4-byte length prefix caps a single ring item at 4 GiB] → Irrelevant in practice; the 1 MiB blob cap rejects long before.

## Migration Plan

New leaf package; nothing depends on it yet. Ship, then `core/context.py` and `core/dofn.py` adopt it in their own changes. Rollback = revert the package. No state written by this change exists in any deployed pipeline, so no `--update` compatibility concerns beyond what wire-schemas already guarantees.

## Open Questions

None blocking. The exact metric namespace (`beam_agents.memory`) may be revisited when the observability capability lands; renaming a counter is non-breaking pre-1.0.
