## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Shared `MemoryStore` conformance suite in `tests/memory/stores/`, parameterized over store fixtures (spec: "Load returns the saved record or None"; "Invalid arguments are rejected before any I/O"; "Replayed flush converges on the identical row"; "A stale seq cannot regress a newer row"; "A newer seq overwrites"; "Prefix search returns ordered, bounded, entity-scoped results"; "Prefix metacharacters are literal"). Registered first against the in-memory store; each backend later joins the same suite.
- [ ] 1.2 Envelope tests (spec: "Envelope bytes are pinned by a golden test"; "Big-endian seq encoding preserves numeric order" — hypothesis property over seq pairs, mirroring the dedup store's order-preservation test).
- [ ] 1.3 Import-boundary test (spec: "The subpackage imports without any client library"): import every `beam_agents.memory.stores` module with the Bigtable, Redis, Firestore, and SQLAlchemy roots blocked in `sys.modules`; assert constructor-time errors name the `memory-stores` extra.
- [ ] 1.4 Factory and config tests (spec: "Each scheme builds its store"; "A malformed URI fails at construction time"): `build_memory_store` dispatch, `AgentConfig.longterm_memory` validation with no client import, actionable `ValueError` messages.
- [ ] 1.5 Facade accessor tests (spec: "Unconfigured pipelines behave exactly as today"; "Working-tier operations never reach the store") against a recording in-memory store.
- [ ] 1.6 Activation staging/flush tests (spec: "A failed activation flushes nothing"; "A flush failure fails the activation closed"; "Staged saves are visible to reads before any flush") driving `run_activation` with a scripted store fake that records operations and can raise at flush.
- [ ] 1.7 Chaos-gate scenarios (spec: "A bundle retry across a completed flush converges"; "Blind upserts keep replay path-stable"): extend the retry-determinism harness (`tests/semantics/test_retry_determinism.py` chaos wrapper) to force a bundle retry after a completed flush; assert byte-identical intents, byte-identical staged upserts, and byte-identical stored rows. Carries `semantics` (offline leg, in-memory store).
- [ ] 1.8 Backend-specific tests written ahead of each backend (spec: "The conditional mutation enforces the guard in one RPC"; "Only the latest seq cell decides the predicate"; "The script applies the seq-pair matrix atomically"; "Transactional guard under the conformance suite"; "The conformance suite passes offline on sqlite"): offline against fake clients for call shapes, plus the shared suite under `-m integration` for live backends.

## 2. Scaffolding, proto, and dependencies

- [ ] 2.1 Add the additive `LongTermRecord` message to `protos/beam_agents.proto` (`state_schema_version`, `key`, `value`, `seq`, `updated_at_ms`); regenerate via `scripts/gen_proto.sh`; confirm regen is diff-clean and no existing message changed.
- [ ] 2.2 Add the `memory-stores` optional extra to `pyproject.toml` (`google-cloud-bigtable`, `redis`, `google-cloud-firestore`, `sqlalchemy[asyncio]`), mirror the clients into the `integration` dependency group (plus `aiosqlite` in `test` for the offline SQL leg), and add `PLC0415` per-file ignores for the lazy-import store modules.
- [ ] 2.3 Create `src/beam_agents/memory/stores/__init__.py` exporting `MemoryStore`, the record type, `build_memory_store`, and `InMemoryMemoryStore`; confirm nothing is re-exported from `beam_agents/__init__.py`.

## 3. ABC, envelope, factory, in-memory store

- [ ] 3.1 Implement `stores/base.py`: the `MemoryStore` ABC (`load`/`save`/`search`/`close`), argument validation, the envelope encode/decode with deterministic serialization, and the big-endian seq encoding.
- [ ] 3.2 Implement `InMemoryMemoryStore` with an injectable clock; run it through the conformance suite from 1.1.
- [ ] 3.3 Implement `build_memory_store(scheme, parts)` and the import-free URI grammar helpers.

## 4. Facade accessor and activation wiring

- [ ] 4.1 Add the `longterm` property and `LongtermMemory` handle to `memory/facade.py`: staged saves stamped with `seq`/`now_ms`, read-your-writes overlay for `load`/`search`, actionable unconfigured error naming `AgentConfig.longterm_memory`.
- [ ] 4.2 Wire both context surfaces: `ActivationContext` and `AgentContext` construct `Memory` with the handle and expose staged upserts to their owners (`ActivationResult` gains an `upserts` field; `AgentResult` likewise).
- [ ] 4.3 Implement the commit-tail flush in `core/loop.py`: after the agent returns and before the result is handed to the DoFn, flush staged upserts through the store on the bridge loop; a flush exception fails the activation via the existing `ActivationFailed` path.
- [ ] 4.4 Wire `core/dofn.py`: build the store from `AgentConfig.longterm_memory` in `setup()` on the bridge loop, close it in `teardown()`; no store is constructed when the URI is unset.
- [ ] 4.5 Add `AgentConfig.longterm_memory` with `__post_init__` validation (import-free), matching the sink-URI error style.

## 5. Redis store

- [ ] 5.1 Implement `stores/redis.py`: per-entity hash, seq-prefix value framing, the compare-and-set Lua script for `save`, literal-match `HSCAN` plus ordered assembly for `search`; lazy `redis.asyncio` import.
- [ ] 5.2 Register against the conformance suite under `-m integration` (testcontainers Redis, same leg as the effector's Redis dedup tests).

## 6. Bigtable store

- [ ] 6.1 Implement `stores/bigtable.py`: row key `hex(entity_key)#key`, `seq`/`rec` columns, `CheckAndMutateRow` save with the strictly-greater stored-seq predicate limited to the latest cell version, prefix row-range `search`; lazy client import.
- [ ] 6.2 Offline call-shape tests against a fake client (both predicate branches, filter construction); shared suite against the compose Bigtable emulator under `-m integration`.

## 7. Firestore store

- [ ] 7.1 Implement `stores/firestore.py`: one document per `(entity_key, key)`, transactional read-compare-write `save`, ordered range-query `search`; lazy client import.
- [ ] 7.2 Add a Firestore emulator service to `docker/compose.yaml` (the `google/cloud-sdk` image already used for Pub/Sub and Bigtable); run the shared suite against it under `-m integration`.

## 8. SQLAlchemy store

- [ ] 8.1 Implement `stores/sql.py`: async engine, documented DDL, transactional compare-and-upsert portable across dialects, escaped-`LIKE` ordered `search`.
- [ ] 8.2 Run the full conformance suite offline against `sqlite+aiosqlite` in the unit tier; optionally add a Postgres testcontainers leg under `-m integration`.

## 9. Docs and observability

- [ ] 9.1 Document the tier in `docs/memory.md` (or a new section): explicit-access model, the blind-upsert replay discipline with a worked example, the residual read-back window and why it is harmless, backend provisioning (Bigtable family, SQL DDL) and retention being operator-owned.
- [ ] 9.2 Add flush metrics via the existing activation tally/metrics seam (upserts flushed, flush failures) without a new exporter dependency.

## 10. Gates

- [ ] 10.1 `make lint` (incl. ASYNC rules over the store modules) and `make type` (`mypy --strict`) clean.
- [ ] 10.2 `make test-unit` green offline with no docker and no backend clients required (in-memory + aiosqlite legs, import-boundary test proving it).
- [ ] 10.3 `make test-integration` green for the Redis testcontainers and Bigtable/Firestore emulator legs; offline semantics selection (`make test-semantics-offline`) green including the new chaos scenario.
- [ ] 10.4 Coverage ratchet (`make coverage-ratchet`) does not regress; mutation gate on touched `core/` files passes.
- [ ] 10.5 `uv run pre-commit run --all-files` clean (including the protobuf-drift hook over the regenerated `_pb2` files).
- [ ] 10.6 `openspec validate add-longterm-memory-stores --strict` passes.
