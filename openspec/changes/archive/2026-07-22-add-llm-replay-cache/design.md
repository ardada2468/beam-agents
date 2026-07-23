# Design: add-llm-replay-cache

## Context

Correctness invariant 3 fixes the replay-cache contract: every LLM call is keyed by `sha256(model_id, canonical_json(messages), tools_schema, sampling_params, key, seq)` and cached in keyed state as `LLM_CACHE` (ReadModifyWriteState, bounded blob) with LRU max 64, 6h TTL, and the 100 KiB per-blob cap. Bundle retries must incur zero additional provider calls on the cached path, and the `-m semantics` retry-determinism gate will assert exactly that against FakeLLM.

The memory facade established the pattern this change follows: state is a single proto blob (no MapState in the Python SDK), and a pure in-memory facade stages all mutations — no Beam I/O, no wall-clock reads, caller-injected `now_ms`, `dirty` flag, `to_blob()` at commit. The DoFn (future change) loads the blob before the activation and commits it after, preserving atomic-commit invariant 1.

Constraints that shape this design:
- State is protobuf, never pickle; additive schema evolution only; blobs ≤ 100 KiB.
- Determinism across bundle retries: no `time.time()`, no iteration-order dependence, canonical hashing.
- `mypy --strict`, no `Any` in public signatures; runtime overhead budget p50 < 15 ms per activation.

## Goals / Non-Goals

**Goals:**
- A deterministic, canonical cache-key function usable by every provider client.
- `ReplayCache` facade enforcing LRU-64 / 6h-TTL / 100 KiB-cap semantics entirely in memory.
- Digest-only fallback so oversized responses degrade to nondeterminism *detection* instead of silent divergence.
- `LlmCacheBlob` schema + deterministic coder support, so `core/dofn.py` can declare the `LLM_CACHE` state spec later without further schema work.

**Non-Goals:**
- No LLMClient or provider integrations (anthropic/openai_compat/vertex/vllm) — the code that *consults* the cache before a provider call is the model-client change.
- No Beam state I/O, DoFn wiring, or timers; no cross-activation GC beyond lazy TTL purge.
- No digest-mismatch handling policy (trace event, metric, error routing) — the facade only preserves digests; the model client decides what a mismatch means.
- No caching of streaming/partial responses; the cached unit is one complete serialized response.

## Decisions

### D1: Cache key = sha256 over one canonical JSON document

`compute_cache_key(model_id, messages, tools_schema, sampling_params, entity_key, seq)` builds a single dict `{"model": ..., "messages": ..., "tools": ..., "params": ..., "key": entity_key.hex(), "seq": seq}`, serializes it with `json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)`, and returns the sha256 hex digest of the UTF-8 bytes. `messages`, `tools_schema`, and `sampling_params` are JSON-compatible Python structures (the provider-request shape); `entity_key` is bytes and is hex-encoded before hashing.

Rationale: `sort_keys` + compact separators kills dict-insertion-order and whitespace nondeterminism; `allow_nan=False` rejects the one stdlib-JSON construct with no canonical form; hex digest strings are stable proto map/string keys. Non-serializable inputs fail with `TypeError` at the call site — a loud construction bug, per the error conventions.

*Alternatives considered:* hashing a proto-serialized request message — rejected: no request proto exists and provider payloads are natively JSON-shaped. Hashing components separately and chaining — rejected: a single document with fixed field names is simpler and unambiguous about component boundaries (no concatenation-collision risk).

### D2: `LlmCacheBlob` mirrors `MemoryBlob`'s ordered-blob design

```proto
message LlmCacheBlob {
  message LlmCacheEntry {
    string cache_key = 1;       // 64-char hex sha256 of the request material
    bytes response = 2;         // serialized provider response; empty when digest_only
    bytes response_digest = 3;  // sha256 of the full response bytes, always set
    int64 created_at_ms = 4;
    int64 last_access_ms = 5;
    bool digest_only = 6;
  }
  uint32 state_schema_version = 1;
  repeated LlmCacheEntry entries = 2;  // LRU order, least-recently-used first
  int64 total_response_bytes = 3;
}
```

Repeated field (not `map<>`) so LRU order is representable and encoding order is under our control, exactly like `MemoryBlob`. `response_digest` is populated for *every* entry (32 bytes is noise next to responses) so divergence checks are uniform whether or not the payload was retained. Additive proto change; `state_schema_version = 1`; no version bump.

### D3: Facade holds an insertion-ordered dict; clock injected and frozen per activation

`ReplayCache(blob: LlmCacheBlob | None, *, now_ms: int)` parses entries into a `dict[str, _Entry]` (insertion order = LRU order, least-recent first). `get` re-stamps `last_access_ms = now_ms` and reinserts at the MRU end; `to_blob()` emits one O(n) pass with `state_schema_version = 1` and maintained `total_response_bytes`. `dirty` starts `False`; a miss on `get` does not dirty the facade, a hit (LRU touch) or any `put`/purge does. Same determinism argument as the memory facade's D1: all timestamps in one activation are the caller's `now_ms`, ties broken by touch order — byte-stable across bundle retries.

### D4: TTL is 6h from `created_at_ms`, enforced lazily; expiry is `age > TTL`

`TTL_MS = 21_600_000`. An entry is expired iff `now_ms - created_at_ms > TTL_MS` (boundary value still live). `get` on an expired entry removes it and reports a miss; `put` purges all expired entries before capacity checks. No purge at construction — an untouched facade must stay `dirty == False` so the DoFn can skip the state write.

TTL anchors on `created_at_ms`, not `last_access_ms`: the cache exists to make retries of recent activations free, and access-refreshed TTL would let a hot entry pin a stale provider response indefinitely. Overwriting `put` on an existing key replaces the entry wholesale and resets `created_at_ms` (last writer wins).

### D5: Capacity enforcement order — purge, count-evict, byte-evict, then digest fallback

`MAX_ENTRIES = 64`, `BLOB_CAP_BYTES = 102_400`, enforced on every `put` in this order:

1. Purge expired entries (D4).
2. Insert/replace the new entry at MRU.
3. While entry count > 64: evict LRU.
4. While prospective serialized blob size > 100 KiB and other entries remain: evict LRU.
5. If the new entry *alone* still exceeds the cap: store it digest-only instead (see D6).

Prospective blob size is maintained incrementally: per-entry encoded wire size (entry `ByteSize()` plus field-2 tag and length-varint overhead) summed with the fixed blob header cost — no full re-serialize on the hot path. `to_blob()` output is asserted ≤ 102 400 bytes in property tests as the ground truth for the accounting.

Step 5 is short-circuited up front: if the new entry could never fit even in an empty blob, it goes digest-only *without* evicting anything — a pathological 99 KiB response must not flush 63 useful entries only to fail anyway.

### D6: Digest-only fallback preserves detection, not replay

A digest-only entry stores `response_digest = sha256(response_bytes)`, `digest_only = True`, empty `response` (~60 bytes total — always fits). `get` returns it marked as digest-only with no payload: the caller cannot replay, must re-call the provider, and can compare the fresh response's digest against the stored one to detect provider nondeterminism. Whether a mismatch becomes a trace event, metric, or error is the model client's decision (non-goal here); the facade's contract is only that the digest survives.

*Alternative considered:* rejecting oversized responses outright (like `MemoryOverflow`). Rejected: an oversized LLM response is not an agent bug — it's a legitimate provider outcome, and failing the activation for it would violate the spirit of invariant 3. Degraded caching plus divergence detection is the correct fallback.

### D7: `get` returns a typed entry view; misses are `None`

`get(cache_key) -> ReplayEntry | None`, where `ReplayEntry` is a frozen dataclass: `response: bytes` (empty iff digest-only), `response_digest: bytes`, `digest_only: bool`. `None` means miss (absent or expired). No exceptions on the read path; `put(cache_key, response: bytes)` computes the digest internally. Constants (`MAX_ENTRIES`, `TTL_MS`, `BLOB_CAP_BYTES`) are module-level and exported for the DoFn and tests.

*Alternative considered:* returning raw `bytes | None` and surfacing digest-only via exception or sentinel bytes. Rejected: digest-only is a normal outcome, not an error, and sentinel bytes are indistinguishable from a real response.

### D8: Coder support is a one-line type-registry extension

`beam_agents.core.coders` maintains its supported-message tuple; `LlmCacheBlob` joins it, inheriting deterministic encoding, round-trip, registration, and no-pickle guarantees from the existing machinery. Golden blob fixture and generator entry added under `tests/core/golden/` per the wire-schemas evolution requirement.

### D9: Public surface

`beam_agents.model` exports `ReplayCache`, `ReplayEntry`, `compute_cache_key`, and the three constants. Root `beam_agents/__init__.py` untouched — the cache is reached via the future model client and DoFn, not by agent authors. Everything else in the module is underscore-private.

## Risks / Trade-offs

- [Canonical JSON depends on Python `repr` float formatting] → CPython ≥ 3.1 guarantees shortest-repr floats, identical across supported 3.10–3.12; `allow_nan=False` closes the non-canonical NaN/Infinity hole. Cross-language key stability is explicitly not promised (Python-only v0.x).
- [A burst of large responses can evict most of the cache (D5 step 4)] → Bounded by design: the newest activation's replay is the priority, older entries protect older retries that are increasingly unlikely to replay. The alone-never-fits short-circuit prevents the worst case (flush + fail).
- [Incremental size accounting can drift from real encoded size] → Property-based test serializes `to_blob()` after arbitrary operation sequences and asserts both the ≤ 102 400 invariant and accounting-vs-`ByteSize()` agreement.
- [Digest-only entries spend cache slots without enabling replay] → They count toward the 64-entry bound (they are the record that a call happened and what it returned); at ~60 bytes each their byte cost is negligible, and they age out via the same TTL.
- [`get` dirties the blob on every hit (LRU touch)] → Same trade-off the memory facade accepted (its D6): persisted access order is what makes eviction deterministic across retries; the state write is one RMW op the DoFn already pays when anything changed.
- [TTL purge is lazy, so an idle key's blob can hold expired entries indefinitely] → Bounded staleness (≤ 100 KiB per key) with no correctness impact — expired entries are unreadable through the facade. Global state GC belongs to `TTL_TIMER` in the DoFn change.

## Migration Plan

Additive proto field + new leaf package; nothing depends on either yet. Ship proto + regen first (diff-clean gate), then coders, then the facade. Rollback = revert; no deployed pipeline carries `LlmCacheBlob` state, so no `--update` concerns beyond the existing additive-evolution guarantees. Downstream adoption order: model client change (consult cache around provider calls), then DoFn change (`LLM_CACHE` state spec + load/commit), then the `-m semantics` retry-determinism gate exercises the full path.

## Open Questions

None blocking. The exact provider-response serialization format (what bytes go in `response`) is fixed by the model-client change; this facade treats it as opaque bytes, which is compatible with any choice.
