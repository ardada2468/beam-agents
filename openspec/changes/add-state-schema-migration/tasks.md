## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/core/test_migration.py` — registry semantics: a registered single-step migration upgrades one version; chained test-double steps (`1 -> 2`, `2 -> 3`) compose in order; a gap raises the typed missing-migration error naming type and `from_version`; a current-version blob is returned as the identical object with no step invoked; version `0` normalizes to `1`; a future version raises the typed from-the-future error naming type, found version, and current version; a step that fails to advance the stamp is rejected. Derived from "A single-step migration upgrades one version", "Chains compose across multiple versions", "A gap in the chain is a hard error", "A current-version blob passes through untouched", "Version zero reads as the baseline", "A future-version blob fails the bundle".
- [ ] 1.2 `tests/core/test_migration.py` — writers stamp from the constant: `WorkingMemory.to_blob`, `ReplayCacheView.to_blob`, and `build_continuation` all produce `state_schema_version == CURRENT_STATE_SCHEMA_VERSION`. Derived from "Writers stamp the current version".
- [ ] 1.3 `tests/core/test_dofn_migration.py` (fake state/timer handles from `tests/core/_dofn_fakes.py`, inside the mutmut selection) — the read-path hook: with a test-double step registered and an old-version blob seeded into each of `MEMORY`, `CONTINUATION`, and `LLM_CACHE`, a committed activation observes the migrated view and the blobs written at commit read the current version; a raising activation leaves the seeded old-version bytes untouched; a refused resume and a stale timer fire mutate nothing; `on_hitl`/`on_ttl` interpret the migrated continuation's `deadline_ms`/`seq`/`escalations`, and an escalation writes the migrated continuation back at the current version. Derived from "An old blob is migrated on read and committed at the current version", "A failed activation leaves old-version bytes untouched", "Timer callbacks interpret only migrated continuations".
- [ ] 1.4 `tests/core/test_dofn_migration.py` — fail fast on the future: a blob stamped `CURRENT + 1` in any of the three specs makes `process` (and each timer callback) raise the typed error with no `.errors` emission and no state mutation; re-driving the same element after lifting the binary's current version (monkeypatched constant plus identity step) processes cleanly. Derived from "A future-version blob fails the bundle", "Rolling forward recovers the key".
- [ ] 1.5 `tests/core/test_schema_compat.py` — corpus replay: parametrize over every committed `tests/core/golden/v<N>/*.bin`; decode at its version, `migrate_to_current`, assert field-level equality against the expected current-version message; keep the explicit non-goal of byte-identical re-encode; keep the pre-field decode assertions (`ToolIntent.kind`, `Continuation.escalations`, `trace_id`) against the moved `v1/` blobs. Derived from "Every historical version replays to current", "Golden blobs decode with current bindings", "Golden corpus is laid out per version".
- [ ] 1.6 `tests/core/test_schema_compat.py` — completeness meta-tests: every version in `1..CURRENT` has a corpus directory; every versioned message type has a fixture per version directory since its introduction; every `(type, n)` for `n < CURRENT` has a registered migration step; simulate a bump (monkeypatched constant) to prove both checks go red naming what is missing. Derived from "The corpus cannot silently shrink", "A version bump without migration functions fails CI", "A version bump without a corpus entry fails CI".
- [ ] 1.7 `tests/core/test_coders.py` — coder migration-invariance: `decode(encode(blob))` of a blob stamped with a non-current version preserves the version and invokes no migration; old-version bytes decode intact under the current descriptor. Derived from "Decoding an old-version blob does not migrate it", "Wire compatibility holds across a version bump".
- [ ] 1.8 Mark the corpus replay and completeness tests with the offline `semantics` marker and confirm `scripts/check_semantics_partition.py` still partitions cleanly (offline selection non-empty, nothing escapes both selections).

## 2. Migration registry

- [ ] 2.1 Create `src/beam_agents/core/migration.py`: `CURRENT_STATE_SCHEMA_VERSION: Final[int] = 1`; typed errors (`MissingMigrationError`, `StateSchemaFromFutureError`) carrying message type and versions; the `(message type, from_version)` registry with decorator registration confined to this module's import; `migrate_to_current` with the version-`0` normalization, identity fast path, chain walk with per-step stamp verification, and fail-fast on future versions (design D2/D4). Import-side-effect-free beyond its own registry; `mypy --strict` clean with no `Any` in public signatures.
- [ ] 2.2 Point the three writers at the constant: `memory/facade.py` `to_blob`, `model/replay_cache.py` `to_blob`, `core/context.py` `build_continuation` (byte-identical output at version 1, so no golden or determinism movement).

## 3. The read-path hook in `_AgentDoFn`

- [ ] 3.1 Route all seven keyed-state reads through `migrate_to_current`: `_start`'s `MEMORY`/`LLM_CACHE` reads, `_resume`'s `CONTINUATION`/`MEMORY`/`LLM_CACHE` reads, and the `CONTINUATION` reads in `on_ttl` and `on_hitl` — each before any field of the blob is interpreted, and with `None` (absent state) passed through untouched.
- [ ] 3.2 Add no write at read time: verify the migrated value reaches state only via the existing commit writes and the escalation `CopyFrom` write, and document the read-path hook in the `dofn.py` module docstring next to the state-spec inventory (design D3).
- [ ] 3.3 Confirm the retry-determinism gate (`tests/semantics/test_retry_determinism.py`) passes unchanged: migration is pure, so a replayed bundle over old-version state produces byte-identical intents and zero extra provider calls.

## 4. The cross-version corpus

- [ ] 4.1 Restructure `tests/core/golden/`: `git mv` the existing `*.bin` baselines into `tests/core/golden/v1/` byte-for-byte; rework `generate.py` into per-version frozen builder maps whose `main()` writes only `v<CURRENT>/`, keeping the single-source-of-truth pairing of committed bytes and expected values (design D5).
- [ ] 4.2 Update every import/path referencing the old flat layout (`tests/core/test_schema_compat.py` and any fixture helpers) and re-run the full unit tier to prove the move is behavior-neutral.

## 5. Policy documentation

- [ ] 5.1 Write `docs/state-migration.md`: the two-tier evolution rule (additive without a bump; semantic/renumbering breaks only for the three versioned blobs via bump + migration + corpus entry; retype/reuse of field numbers never), the bump checklist CI enforces, lazy-migration and next-commit-rewrite semantics, the version-from-the-future failure mode with the "roll forward, don't roll back" operational rule, and the Dataflow `--update` implications (coder and state-spec invariance).
- [ ] 5.2 Update the `protos/beam_agents.proto` header comment to name the gate and point at `docs/state-migration.md` (comment-only: the serialized descriptor and committed bindings are unchanged, so `scripts/check_proto_drift.sh` stays clean — verify with `make proto` + `git diff --exit-code`).

## 6. Gates

- [ ] 6.1 `make lint` and `make type` clean (`mypy --strict`; no `Any` in public signatures).
- [ ] 6.2 `make test-unit` passes offline with no docker.
- [ ] 6.3 `make test-semantics-offline` passes with the newly marked corpus gates included.
- [ ] 6.4 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new module improves it.
- [ ] 6.5 `make mutation` passes over `core/migration.py` and the `dofn.py` edits; document any `mutation-baseline.toml` ceiling movement in the file's comment per precedent.
- [ ] 6.6 `uv run pre-commit run --all-files` clean (including the proto drift hook after 5.2).
- [ ] 6.7 `openspec validate add-state-schema-migration --strict` passes.
