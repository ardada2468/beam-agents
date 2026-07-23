# Tasks: add-memory-facade

## 1. Scaffolding

- [x] 1.1 Create `src/beam_agents/memory/__init__.py` exporting `Memory`, `MemoryOverflow`, `Compactor`, and `tests/memory/__init__.py`
- [x] 1.2 Stub `src/beam_agents/memory/facade.py` with typed signatures (`Memory`, `MemoryOverflow`, `Compactor` protocol, `HARD_CAP_BYTES`, soft-cap constant) raising `NotImplementedError`, so spec-derived tests fail for the right reason

## 2. Tests first (one test per spec scenario, named after it)

- [x] 2.1 `tests/memory/test_facade_staging.py` — blob round-trip untouched, fresh facade versioned empty blob, rejected mutation leaves staged state unchanged, `dirty` flag transitions
- [x] 2.2 `tests/memory/test_facade_scalars.py` — set/get round-trip, missing key `None`, LRU emit order and `last_access_ms` stamping, delete idempotency and accounting
- [x] 2.3 `tests/memory/test_facade_ring.py` — append order preserved across blob round-trip, oldest dropped at `max_items`, kind-mixing raises `TypeError` with entry unchanged
- [x] 2.4 `tests/memory/test_facade_accounting.py` — hypothesis property: arbitrary set/delete/append sequences keep `size_bytes` equal to from-scratch recomputation and `to_blob().total_value_bytes`
- [x] 2.5 `tests/memory/test_facade_caps.py` — soft-cap warning + `soft_cap_warnings` counter fire once per facade, compactor invoked at soft cap, no-compactor crossing succeeds; hard-cap atomic rejection, compaction-frees-space success, compaction-insufficient rejection with persisted compaction effects
- [x] 2.6 `tests/memory/test_facade_compactor.py` — compactor mutates via facade with exact accounting and no re-entrant cap enforcement; compactor exceptions propagate unmodified
- [x] 2.7 Run `pytest tests/memory` and confirm every test fails with `NotImplementedError` (not collection or import errors)

## 3. Implementation

- [x] 3.1 Implement blob load/emit: parse entries into insertion-ordered dict, `to_blob()` in LRU order with `state_schema_version=1` and `total_value_bytes`, `dirty` tracking (design D1, D6)
- [x] 3.2 Implement scalar `get`/`set`/`delete` with kind tag `0x00`, `now_ms` stamping, move-to-recent on access
- [x] 3.3 Implement ring encoding (tag `0x01` + u32 big-endian length-prefixed items) in module-private helpers, `append` with `max_items` eviction, `ring()` reader, `TypeError` on kind mixing, `ValueError` on corrupt ring bytes (design D2, D3)
- [x] 3.4 Implement incremental size accounting over stored bytes with `size_bytes` property (design D4)
- [x] 3.5 Implement cap enforcement: once-per-instance soft-cap warning + Beam counter + compactor call at ≥75%; hard-cap compact-then-reject with `MemoryOverflow(key, attempted, cap)` and re-entrancy suppression during `compact()` (design D5, D7)
- [x] 3.6 Run `pytest tests/memory` until green without weakening any test

## 4. Quality gates

- [x] 4.1 `ruff check` and `mypy --strict` clean on `src/beam_agents/memory/`; no `Any` in public signatures
- [x] 4.2 Full offline suite `pytest` (no docker) passes; coverage does not decrease
- [x] 4.3 Verify no import-time side effects (no logging/metrics/registry mutation on `import beam_agents.memory`) and root `beam_agents/__init__.py` untouched
