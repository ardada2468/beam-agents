# Add LLM Replay Cache

## Why

Correctness invariant 3 requires that bundle retries incur ZERO additional provider calls on the cached path, and the retry-determinism semantics gate depends on it. Today nothing implements it: there is no cache schema, no canonical request hashing, and no bounded keyed-state structure for `LLM_CACHE`. The replay cache is a prerequisite for the model client (`model/`) and the stateful DoFn, and follows the same staged-facade pattern the memory facade established.

## What Changes

- New `LlmCacheBlob` message in `protos/beam_agents.proto` (additive; no version bump): ordered repeated entries — `cache_key`, `response` bytes, `response_digest`, `created_at_ms`, `last_access_ms`, `digest_only` flag — plus `state_schema_version` and cached size accounting, mirroring `MemoryBlob`'s no-MapState blob design.
- Deterministic content-hash key derivation: `sha256` over a canonical encoding of `(model_id, canonical_json(messages), tools_schema, sampling_params, key, seq)`. Same request material always produces the same key; `key`+`seq` scope entries to one activation per invariant 3 and the glossary.
- New `beam_agents.model` package with a `ReplayCache` facade over an in-memory `LlmCacheBlob` (no Beam I/O, caller-supplied `now_ms`, `dirty` flag, `to_blob()` — the DoFn loads/commits it in a future change):
  - `get(cache_key)` → hit (cached response bytes), digest-only marker, or miss; hits update LRU order.
  - `put(cache_key, response_bytes)` stages a response.
  - **LRU bound**: max 64 entries; inserting past the bound evicts least-recently-used.
  - **TTL 6h**: entries older than 6h relative to the injected clock are treated as misses and purged lazily.
  - **100 KiB blob cap with digest-only fallback**: inserts evict LRU entries until the serialized blob fits; a response too large to ever fit is stored as a digest-only entry (sha256 of the response, no payload) so retries detect and surface provider nondeterminism instead of silently diverging.
- Extend the deterministic proto coder registry (`beam_agents.core.coders`) to cover `LlmCacheBlob` so the `LLM_CACHE` state spec can use it.

## Capabilities

### New Capabilities
- `llm-replay-cache`: content-hash cache-key derivation over LLM request material, and the `ReplayCache` staged facade with LRU-64 / 6h-TTL / 100 KiB-cap-with-digest-fallback semantics over `LlmCacheBlob`.

### Modified Capabilities
- `wire-schemas`: the schema set grows from six to seven top-level messages — adds the `LlmCacheBlob` requirement (versioned, LRU-orderable cache entries with digest-only support) and extends the package/importability requirement to include it.
- `proto-coders`: coder requirements extend from six to seven message types — `LlmCacheBlob` must encode deterministically, round-trip losslessly, and be registered/dispatched like the existing messages.

## Impact

- Proto: `protos/beam_agents.proto` gains one message (additive-only, regen via `scripts/gen_proto.sh`, diff-clean gate applies); regenerated `src/beam_agents/_protos/` bindings.
- New code: `src/beam_agents/model/__init__.py`, `src/beam_agents/model/replay_cache.py`; tests under `tests/model/` plus additions to `tests/core/` coder/schema property tests and golden blobs.
- Modified code: `src/beam_agents/core/coders.py` (register the new type).
- No new dependencies (`hashlib`/`json` from stdlib); public root API (`beam_agents/__init__.py`) untouched.
- Downstream consumers (future changes): `model/` LLMClient providers (consult cache before provider calls), `core/dofn.py` (`LLM_CACHE` state load/commit), retry-determinism semantics tests.
