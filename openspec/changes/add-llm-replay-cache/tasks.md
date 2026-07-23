# Tasks: add-llm-replay-cache

## 1. Schema and generated bindings

- [x] 1.1 Add `LlmCacheBlob` (nested `LlmCacheEntry`: `cache_key`, `response`, `response_digest`, `created_at_ms`, `last_access_ms`, `digest_only`; blob: `state_schema_version`, repeated `entries`, `total_response_bytes`) to `protos/beam_agents.proto` with doc comments matching the existing style (design D2)
- [x] 1.2 Regenerate bindings via `scripts/gen_proto.sh` and commit `src/beam_agents/_protos/`; confirm regeneration is diff-clean
- [x] 1.3 Extend `tests/core/golden/generate.py` with an `LlmCacheBlob` fixture (including a digest-only entry) and commit the golden blob

## 2. Schema and coder tests first (one test per spec scenario, named after it)

- [x] 2.1 `tests/core/test_wire_schemas.py` — cache entries round-trip in insertion order; digest-only entries representable; importability scenario now covers all seven message classes
- [x] 2.2 `tests/core/test_schema_compat.py` — golden `LlmCacheBlob` decodes with field-level equality; unknown-field tolerance scenarios cover the new type
- [x] 2.3 `tests/core/test_coders.py` — extend property-based strategies so byte-identical repeated encoding, lossless round-trip, registration, and no-pickle TestPipeline scenarios cover `LlmCacheBlob` (seven types)
- [x] 2.4 Run `pytest tests/core` and confirm new coder/registration assertions fail for the right reason before touching `core/coders.py`

## 3. Coder support

- [x] 3.1 Add `LlmCacheBlob` to the supported-message registry in `src/beam_agents/core/coders.py` (design D8)
- [x] 3.2 Run `pytest tests/core` until green without weakening any test

## 4. Facade scaffolding

- [x] 4.1 Create `src/beam_agents/model/__init__.py` exporting `ReplayCache`, `ReplayEntry`, `compute_cache_key`, `MAX_ENTRIES`, `TTL_MS`, `BLOB_CAP_BYTES`, and `tests/model/__init__.py`
- [x] 4.2 Stub `src/beam_agents/model/replay_cache.py` with typed signatures (frozen `ReplayEntry` dataclass, `ReplayCache`, `compute_cache_key`, constants) raising `NotImplementedError`, so spec-derived tests fail for the right reason

## 5. Facade tests first (one test per spec scenario, named after it)

- [x] 5.1 `tests/model/test_cache_key.py` — logically equal requests hash identically (permuted dict orders); every component perturbs the key; non-canonical input rejected (`TypeError` / NaN `ValueError`)
- [x] 5.2 `tests/model/test_replay_cache_staging.py` — untouched blob round-trip with `dirty` `False`; fresh facade emits versioned empty blob; no wall-clock reads (injected `now_ms` only)
- [x] 5.3 `tests/model/test_replay_cache_hits.py` — put-then-get byte-identical replay with digest; hit moves entry to MRU with `now_ms` stamp; miss returns `None` and leaves facade clean
- [x] 5.4 `tests/model/test_replay_cache_ttl.py` — expired entry is a miss and purged; exact-boundary age still live; access does not refresh TTL; re-put resets `created_at_ms`
- [x] 5.5 `tests/model/test_replay_cache_lru.py` — 65th insert evicts LRU; recently read entry survives eviction; digest-only entries count toward the 64 bound
- [x] 5.6 `tests/model/test_replay_cache_caps.py` — hypothesis property: arbitrary put/get sequences keep `to_blob().ByteSize()` ≤ 102,400 and accounting agrees with real encoded size; large inserts evict until fit; oversized response becomes digest-only without collateral eviction; digest-only hits identify themselves
- [x] 5.7 Run `pytest tests/model` and confirm every test fails with `NotImplementedError` (not collection or import errors)

## 6. Facade implementation

- [x] 6.1 Implement `compute_cache_key` canonical JSON hashing (sorted keys, compact separators, `ensure_ascii=False`, `allow_nan=False`, hex-encoded `entity_key`) (design D1)
- [x] 6.2 Implement blob load/emit: insertion-ordered dict, `to_blob()` in LRU order with `state_schema_version=1` and `total_response_bytes`, `dirty` tracking with miss-does-not-dirty (design D3)
- [x] 6.3 Implement `get` hit path (LRU touch, `ReplayEntry` view, digest-only marker) and lazy TTL expiry with inclusive boundary (design D4, D6, D7)
- [x] 6.4 Implement `put` with internal sha256 digest, expired-purge, 64-entry LRU eviction, incremental blob-size accounting against the 102,400-byte cap, and the alone-never-fits digest-only short-circuit (design D5, D6)
- [x] 6.5 Run `pytest tests/model` until green without weakening any test

## 7. Quality gates

- [x] 7.1 `ruff check` and `mypy --strict` clean on `src/beam_agents/model/` and touched `core/` files; no `Any` in public signatures
- [x] 7.2 Full offline suite `pytest` (no docker) passes; coverage does not decrease; mutation gate on touched `core/` files
- [x] 7.3 Verify no import-time side effects (`import beam_agents.model` mutates no registry, logs nothing) and root `beam_agents/__init__.py` untouched
