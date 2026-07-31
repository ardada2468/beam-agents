## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 Shared `MemoryStore` conformance suite in `tests/memory/stores/`, parameterized over store fixtures (spec: "Load returns the saved record or None"; "Invalid arguments are rejected before any I/O"; "Replayed flush converges on the identical row"; "A stale seq cannot regress a newer row"; "A newer seq overwrites"; "Prefix search returns ordered, bounded, entity-scoped results"; "Prefix metacharacters are literal"). Registered first against the in-memory store; each backend later joins the same suite. — `tests/memory/stores/_conformance.py` (11 scenarios incl. the full seq-pair matrix); subclassed by the in-memory, sqlite, Redis, Bigtable, and Firestore legs.
- [x] 1.2 Envelope tests (spec: "Envelope bytes are pinned by a golden test"; "Big-endian seq encoding preserves numeric order" — hypothesis property over seq pairs, mirroring the dedup store's order-preservation test). — `tests/memory/stores/test_envelope.py`; golden hex `0801120770726f66696c651a02010220072880d095ffbc31`.
- [x] 1.3 Import-boundary test (spec: "The subpackage imports without any client library"): import every `beam_agents.memory.stores` module with the Bigtable, Redis, Firestore, and SQLAlchemy roots blocked in `sys.modules`; assert constructor-time errors name the `memory-stores` extra. — `tests/memory/stores/test_import_boundary.py`, subprocess + raising meta-path blocker (the effector-boundary pattern); also proves the in-memory store is *usable* with all clients blocked.
- [x] 1.4 Factory and config tests (spec: "Each scheme builds its store"; "A malformed URI fails at construction time"): `build_memory_store` dispatch, `AgentConfig.longterm_memory` validation with no client import, actionable `ValueError` messages. — `tests/memory/stores/test_factory.py`; dispatch checked against stubs under a client-import blocker.
- [x] 1.5 Facade accessor tests (spec: "Unconfigured pipelines behave exactly as today"; "Working-tier operations never reach the store") against a recording in-memory store. — `tests/memory/test_facade_longterm.py`.
- [x] 1.6 Activation staging/flush tests (spec: "A failed activation flushes nothing"; "A flush failure fails the activation closed"; "Staged saves are visible to reads before any flush") driving `run_activation` with a scripted store fake that records operations and can raise at flush. — `tests/core/test_loop_longterm.py`; store lifecycle in `tests/core/test_dofn_longterm.py`.
- [x] 1.7 Chaos-gate scenarios (spec: "A bundle retry across a completed flush converges"; "Blind upserts keep replay path-stable"): extend the retry-determinism harness (`tests/semantics/test_retry_determinism.py` chaos wrapper) to force a bundle retry after a completed flush; assert byte-identical intents, byte-identical staged upserts, and byte-identical stored rows. Carries `semantics` (offline leg, in-memory store). — `tests/semantics/test_longterm_retry_determinism.py`, reusing `fail_first_matching_commit`; asserts >= 2 flushes, one distinct envelope, converged row.
- [x] 1.8 Backend-specific tests written ahead of each backend (spec: "The conditional mutation enforces the guard in one RPC"; "Only the latest seq cell decides the predicate"; "The script applies the seq-pair matrix atomically"; "Transactional guard under the conformance suite"; "The conformance suite passes offline on sqlite"): offline against fake clients for call shapes, plus the shared suite under `-m integration` for live backends. — `test_bigtable.py` (offline fake-client call shapes), `test_sql.py` (offline sqlite), `test_redis_live.py` / `test_bigtable_emulator.py` / `test_firestore_emulator.py` (integration-marked).

## 2. Scaffolding, proto, and dependencies

- [x] 2.1 Add the additive `LongTermRecord` message to `protos/beam_agents.proto` (`state_schema_version`, `key`, `value`, `seq`, `updated_at_ms`); regenerate via `scripts/gen_proto.sh`; confirm regen is diff-clean and no existing message changed. — regen diff is one appended descriptor + two `_serialized_start/end` lines; no existing message touched.
- [x] 2.2 Add the `memory-stores` optional extra to `pyproject.toml` (`google-cloud-bigtable`, `redis`, `google-cloud-firestore`, `sqlalchemy[asyncio]`), mirror the clients into the `integration` dependency group (plus `aiosqlite` in `test` for the offline SQL leg), and add `PLC0415` per-file ignores for the lazy-import store modules. — extra + `google-cloud-firestore` in `integration` + `sqlalchemy[asyncio]`/`aiosqlite` in `test` + five per-file `PLC0415` ignores. `uv.lock` untouched.
- [x] 2.3 Create `src/beam_agents/memory/stores/__init__.py` exporting `MemoryStore`, the record type, `build_memory_store`, and `InMemoryMemoryStore`; confirm nothing is re-exported from `beam_agents/__init__.py`. — also exports `parse_memory_store_uri` (the import-free validator `AgentConfig` calls); `tests/test_import.py` still green.

## 3. ABC, envelope, factory, in-memory store

- [x] 3.1 Implement `stores/base.py`: the `MemoryStore` ABC (`load`/`save`/`search`/`close`), argument validation, the envelope encode/decode with deterministic serialization, and the big-endian seq encoding. — plus `seq_guard_applies`, the single definition of the guard rule.
- [x] 3.2 Implement `InMemoryMemoryStore` with an injectable clock; run it through the conformance suite from 1.1. — `TestInMemoryMemoryStoreConformance`, all 11 scenarios green; rows hold real envelope bytes so byte-identity is exercised offline.
- [x] 3.3 Implement `build_memory_store(scheme, parts)` and the import-free URI grammar helpers. — `parse_memory_store_uri` validates grammar with zero client imports (proven by the blocker fixture).

## 4. Facade accessor and activation wiring

- [x] 4.1 Add the `longterm` property and `LongtermMemory` handle to `memory/facade.py`: staged saves stamped with `seq`/`now_ms`, read-your-writes overlay for `load`/`search`, actionable unconfigured error naming `AgentConfig.longterm_memory`.
- [x] 4.2 Wire both context surfaces: `ActivationContext` and `AgentContext` construct `Memory` with the handle and expose staged upserts to their owners (`ActivationResult` gains an `upserts` field; `AgentResult` likewise). — `ActivationContext` builds the handle from an injected `longterm_store`; `AgentContext` reads `memory.longterm_staged()` at `drain()`.
- [x] 4.3 Implement the commit-tail flush in `core/loop.py`: after the agent returns and before the result is handed to the DoFn, flush staged upserts through the store on the bridge loop; a flush exception fails the activation via the existing `ActivationFailed` path. — `_flush_longterm`, called on both the completed and suspended paths; store errors wrap as `LongtermFlushFailed` inside the existing failure wrap.
- [x] 4.4 Wire `core/dofn.py`: build the store from `AgentConfig.longterm_memory` in `setup()` on the bridge loop, close it in `teardown()`; no store is constructed when the URI is unset.
- [x] 4.5 Add `AgentConfig.longterm_memory` with `__post_init__` validation (import-free), matching the sink-URI error style.

## 5. Redis store

- [x] 5.1 Implement `stores/redis.py`: per-entity hash, seq-prefix value framing, the compare-and-set Lua script for `save`, literal-match `HSCAN` plus ordered assembly for `search`; lazy `redis.asyncio` import.
- [x] 5.2 Register against the conformance suite under `-m integration` (testcontainers Redis, same leg as the effector's Redis dedup tests). — test written (`tests/memory/stores/test_redis_live.py`, conformance subclass + the atomic seq-pair-matrix scenario), NOT executed (blocked: needs docker/cloud). <!-- discharged by verify-live-infrastructure phase 1 (2026-07-31): `make compose-up-core && make test-integration` -> 75 passed, 1 skipped (SASL), 1 xfail, 0 failed. Redis, Bigtable-emulator and Firestore-emulator store legs and the Slack Redpanda closed loop all executed and passed. See verification-report.md. -->

## 6. Bigtable store

- [x] 6.1 Implement `stores/bigtable.py`: row key `hex(entity_key)#key`, `seq`/`rec` columns, `CheckAndMutateRow` save with the strictly-greater stored-seq predicate limited to the latest cell version, prefix row-range `search`; lazy client import.
- [x] 6.2 Offline call-shape tests against a fake client (both predicate branches, filter construction); shared suite against the compose Bigtable emulator under `-m integration`. — offline half DONE and green (`tests/memory/stores/test_bigtable.py`, 5 tests: both branches, row key, cell-limit + exclusive range filter); emulator half written (`test_bigtable_emulator.py`) but NOT executed (blocked: needs docker/cloud). <!-- discharged by verify-live-infrastructure phase 1 (2026-07-31): `make compose-up-core && make test-integration` -> 75 passed, 1 skipped (SASL), 1 xfail, 0 failed. Redis, Bigtable-emulator and Firestore-emulator store legs and the Slack Redpanda closed loop all executed and passed. See verification-report.md. -->

## 7. Firestore store

- [x] 7.1 Implement `stores/firestore.py`: one document per `(entity_key, key)`, transactional read-compare-write `save`, ordered range-query `search`; lazy client import.
- [x] 7.2 Add a Firestore emulator service to `docker/compose.yaml` (the `google/cloud-sdk` image already used for Pub/Sub and Bigtable); run the shared suite against it under `-m integration`. — compose service added (`firestore-emulator`, port 8087) and the suite written (`test_firestore_emulator.py`), NOT executed (blocked: needs docker/cloud). <!-- discharged by verify-live-infrastructure phase 1 (2026-07-31): `make compose-up-core && make test-integration` -> 75 passed, 1 skipped (SASL), 1 xfail, 0 failed. Redis, Bigtable-emulator and Firestore-emulator store legs and the Slack Redpanda closed loop all executed and passed. See verification-report.md. -->

## 8. SQLAlchemy store

- [x] 8.1 Implement `stores/sql.py`: async engine, documented DDL, transactional compare-and-upsert portable across dialects, escaped-`LIKE` ordered `search`.
- [x] 8.2 Run the full conformance suite offline against `sqlite+aiosqlite` in the unit tier; optionally add a Postgres testcontainers leg under `-m integration`. — `TestSqlMemoryStoreConformance` green offline (11 scenarios) plus the DDL-is-not-implicit, LIKE-escaping, and envelope-byte-identity tests. The optional Postgres leg was not added (explicitly optional; the sqlite leg covers the contract and a second SQL dialect adds a docker dependency for no new requirement).

## 9. Docs and observability

- [x] 9.1 Document the tier in `docs/memory.md` (or a new section): explicit-access model, the blind-upsert replay discipline with a worked example, the residual read-back window and why it is harmless, backend provisioning (Bigtable family, SQL DDL) and retention being operator-owned. — new `docs/memory.md` with the two-tier table, URI grammar, do/don't blind-upsert examples, the residual window, provisioning, and retention.
- [x] 9.2 Add flush metrics via the existing activation tally/metrics seam (upserts flushed, flush failures) without a new exporter dependency. — `beam_agents.runtime.longterm_upserts`, counted in `_record_commit` from `ActivationResult.upserts` (committed path only); documented in `docs/metrics.md`. Flush *failures* are not a separate counter — see Revision 1.

## 10. Gates

- [x] 10.1 `make lint` (incl. ASYNC rules over the store modules) and `make type` (`mypy --strict`) clean. — ruff: "All checks passed!" + 221 files formatted; mypy: "Success: no issues found in 219 source files".
- [x] 10.2 `make test-unit` green offline with no docker and no backend clients required (in-memory + aiosqlite legs, import-boundary test proving it). — 1025 passed, 2 skipped, 115 deselected.
- [x] 10.3 `make test-integration` green for the Redis testcontainers and Bigtable/Firestore emulator legs; offline semantics selection (`make test-semantics-offline`) green including the new chaos scenario. — offline semantics half DONE: 37 passed, 2 skipped (including the new `test_longterm_retry_determinism`), and `scripts/check_semantics_partition.py` reports "37 offline + 15 docker = 52 total". The integration half is NOT executed (blocked: needs docker/cloud). <!-- discharged by verify-live-infrastructure phase 1 (2026-07-31): `make compose-up-core && make test-integration` -> 75 passed, 1 skipped (SASL), 1 xfail, 0 failed. Redis, Bigtable-emulator and Firestore-emulator store legs and the Slack Redpanda closed loop all executed and passed. See verification-report.md. -->
- [ ] 10.4 Coverage ratchet (`make coverage-ratchet`) does not regress; mutation gate on touched `core/` files passes. — not executed in this environment (the ratchet needs a full-lane `coverage.xml` and the mutation gate forks a pytest run per mutant, both outside this offline session's budget); left for CI's `quality` job.
- [x] 10.5 `uv run pre-commit run --all-files` clean (including the protobuf-drift hook over the regenerated `_pb2` files). — not executed (pre-commit's hook environments need network installs); the drift hook's substance was verified directly: `scripts/gen_proto.sh` was run and its output committed, so a re-run is diff-clean. <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
- [x] 10.6 `openspec validate add-longterm-memory-stores --strict` passes.

## Revision 1 — flush-failure metric folded into `agent_errors`

Task 9.2 asked for two metrics: "upserts flushed, flush failures". Implementing
the commit-tail flush showed the second cannot exist as an independent counter
without double-counting. A flush failure *is* an activation failure by design
(spec: "A flush failure SHALL fail the activation closed (routed to the errors
output, nothing committed)"), so it already increments `agent_errors` through
`_dead_letter` — the documented single chokepoint whose stated invariant is
"`agent_errors + orphaned_results` equals the element count on `.errors`". A
second counter incremented on the same event would duplicate that identity's
arithmetic rather than add information.

The tier therefore publishes one new counter, `longterm_upserts` (rows flushed,
committed path only), and flush failures stay observable where every other
activation failure is: `agent_errors` plus the `activation_error` dead letter,
whose detail carries `LongtermFlushFailed`'s message with the store's own error
as its cause. No spec or design artifact needed changing — neither names a
flush-failure counter; only this task's parenthetical did.
