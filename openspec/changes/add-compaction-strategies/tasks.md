## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/memory/test_facade.py` additions: the enumeration surface — "keys() reports LRU order without dirtying the facade" and "entry_size does not perturb eviction order", plus a hypothesis extension of the existing accounting property asserting `sum(entry_size(k) for k in keys()) == size_bytes` after arbitrary operation sequences. — the facade suite is split by requirement (there is no `test_facade.py`); the scenarios landed in a new `tests/memory/test_facade_enumeration.py` and the property extension in `tests/memory/test_facade_accounting.py` (Revision 1). Both failed with `AttributeError: 'Memory' object has no attribute 'keys'` before section 2.
- [x] 1.2 `tests/memory/test_compaction.py`: `DropOldestCompactor` — "Eviction is LRU-first and stops at the target", "Protected prefixes survive even when oldest", "Only protected entries left still over target is not an error" (including the follow-on `MemoryOverflow` from the facade), "Eviction is deterministic across replays" (byte-identical `to_blob()` from equal inputs), and `ValueError` on non-positive `target_bytes`.
- [x] 1.3 `tests/memory/test_compaction.py`: `SummarizeCompactor` unit behavior against a scripted `call_model` fake — fold/keep_recent arithmetic, prior-summary inclusion in `build_request` inputs, the `ValueError` on a non-shrinking extracted summary, and that the surface handed to user callables exposes no `act`/`emit`. — `_FakeView` has neither, and `test_the_summarizer_surface_exposes_no_effect_path` asserts it.
- [x] 1.4 `tests/core/test_loop_summarize.py`: driver invocation — "Crossing the trigger folds old items into a summary" (one `FakeLLM` call, folded ring, summary scalar in the result blob), "Below the trigger no model call happens", "A failing summarizer commits nothing" (`ActivationFailed` propagates; no `ActivationResult` produced), and "A suspending activation's continuation includes the summarizer's cursor advance" (`Continuation.step_index` includes the summarizer's `call_model` step — asserted as `step_index == 2`: `act()` took step 0, the summarizer step 1).
- [x] 1.5 `tests/core/test_dofn_activation.py` additions (fake state/timer handles from `tests/core/_dofn_fakes.py`): "An unconfigured pipeline survives a hard-cap-crossing write" (default `AgentConfig.compactor` reaches the facade through `_activate`; evicted entries absent from the committed blob) and "Opting out restores strict overflow" (`compactor=None` → `.errors` with `activation_error`). — the driver is built with `AgentConfig(...).compactor`, so the default itself is what is under test; `bulk_write_agent` lives in `_dofn_helpers.py`.
- [x] 1.6 `tests/core/test_dofn_expire.py` (fake handles, fake `MemoryStore`): "Expiring memory lands in the long-term tier and state is wiped", "A retried timer bundle deduplicates to one logical write" (drive `on_ttl` twice; assert byte-identical upserts under the same `(key, seq)`), "Flush failure preserves state for retry", "Unset hook preserves today's expiry behavior", and empty-memory expiry performing no store call. — plus `test_the_flush_runs_before_the_wipe`, which observes the un-wiped `MEMORY` handle from inside the store's `_save`.
- [x] 1.7 `tests/semantics/test_retry_determinism.py`: extend the chaos-forced-retry gate with a summarizing agent — "The summarization LLM call replays from cache on bundle retry": zero extra `FakeLLM` calls on the replayed walk, byte-identical committed `MemoryBlob`, byte-identical intents. — the gate shares ONE `FakeLLM` across every DoFn setup (the `test_longterm_retry_determinism` pattern), so the count spans the discarded attempt the traces cannot see. Measured: 3 summarization passes (suspend + discarded resume + Beam's retry), `provider.call_count == 1`, and the two resume attempts' post-fold blobs byte-identical. The agent seeds a stable summary so the resume's `(items, prior_summary)` match the suspend's — without a stable prior the resume issues a genuinely novel request and a retry legitimately repeats it (the same premise `bundle_retry_cache` documents for the ADK cell).
- [x] 1.8 Confirm the `ttl_expiry` conformance scenario (`tests/conformance/_spec.py`) passes unchanged with `on_expire` unset on both offline legs. — green in `make test-semantics-offline` (the FLINK leg is a pre-existing declared skip); `on_ttl`'s pre-change path is taken verbatim when the hook is unset.

## 2. Memory facade enumeration surface

- [x] 2.1 `src/beam_agents/memory/facade.py`: add `keys()` (LRU order, no `_touch`, no `dirty`) and `entry_size(key)` (`KeyError` on absent; counts the stored encoded value bytes, ring framing included), documented as the compaction iteration surface.
- [x] 2.2 `src/beam_agents/memory/__init__.py`: keep exports coherent (facade surface only; strategies export in section 3). — `keys`/`entry_size` are methods on the already-exported `Memory`; the package docstring now names both tiers of compaction.

## 3. DropOldestCompactor and default wiring

- [x] 3.1 Create `src/beam_agents/memory/compaction.py`: `DropOldestCompactor(target_bytes=524_288, protected_prefixes=("__langgraph__/",))` implementing `Compactor` via `keys()`/`entry_size()`/`delete()` only; construction-time `ValueError` on non-positive target; module import side-effect-free. — `entry_size()` is used by the tests' accounting assertions rather than by the eviction loop, which reads `size_bytes` after each delete (equivalent, and it cannot drift from the facade's own accounting).
- [x] 3.2 Export `DropOldestCompactor` (and later `SummarizeCompactor`) from `beam_agents.memory`; note in the docstring that the LangGraph reserved namespace is protected by default, cross-referencing `adapters/langgraph/checkpoint.py`.
- [x] 3.3 `src/beam_agents/core/transform.py`: `AgentConfig.compactor: Compactor | None` keyword-only field, `default_factory=DropOldestCompactor`; forwarded by `RunAgent.expand` into `_AgentDoFn`.
- [x] 3.4 `src/beam_agents/core/dofn.py`: accept `compactor` and pass it in both `_activate` call sites' `run_activation(...)` kwargs, closing the dead parameter; confirm `testing/chaos.py` needs no signature mirror (only `_commit` is wrapped there). — there is one `run_activation(...)` call site (`_activate`, shared by `_start` and `_resume`), and `chaos.py` mirrors only `_commit`/`on_hitl`, neither of which changed: unmodified, and the gate passes.

## 4. SummarizeCompactor in the activation

- [x] 4.1 `src/beam_agents/memory/compaction.py`: `SummarizeCompactor(build_request, extract_summary, source_keys, summary_key="summary", keep_recent=8, trigger_bytes=786_432)` with an async `compact(view)` taking a narrow structural protocol (memory access + `call_model`) defined in this module — no import of `core.context`, avoiding the core↔memory cycle. — `SummarizationView`; the module imports only `memory.facade`, `memory.stores`, and `model.client`.
- [x] 4.2 `src/beam_agents/core/loop.py`: invoke the configured summarizer after `agent(ctx)` returns and before the outcome branch builds `Continuation`/`ActivationResult`, inside the existing failure wrap, gated on `ctx.memory.size_bytes >= trigger_bytes`; thread a `summarizer` parameter through `run_activation`.
- [x] 4.3 `src/beam_agents/core/transform.py` / `core/dofn.py`: `AgentConfig.summarizer` (default `None`) threaded `RunAgent → _AgentDoFn → run_activation`.
- [x] 4.4 Verify the summarizer's `call_model` usage lands in the existing observability surfaces with no new plumbing (LLM_CALL trace event, `llm_calls`/`llm_ms` tally) — assert in 1.4's tests. — asserted in `test_crossing_the_trigger_folds_old_items_into_a_summary`: one `LLM_CALL` trace with `cache_hit=false`, `tally.llm_calls == 1`, one `llm_ms` sample. No observability code changed.

## 5. on_expire flush to the long-term tier (after add-longterm-memory-stores)

- [x] 5.1 `src/beam_agents/memory/compaction.py` (or the C29-designated home): the `on_expire` hook type and its shipped default implementation performing the `(entity_key, seq)`-keyed idempotent upsert of the final `MemoryBlob` with the timer's firing timestamp as the expiry time, via the C29 `MemoryStore` ABC. — `ExpireHook` (protocol), `ExpiringMemory` (the replay-stable payload), `FlushToLongterm` (the shipped hook, storing under key `"working_memory"`).
- [x] 5.2 `src/beam_agents/core/transform.py`: `AgentConfig.on_expire` (default `None`); construction-time `ValueError` when set without a configured long-term store.
- [x] 5.3 `src/beam_agents/core/dofn.py` `on_ttl`: when configured and `MEMORY` is non-empty, read blob + `SEQ`, submit the flush to the async bridge under a bounded timeout, and only then wipe; on flush failure, propagate (bundle retry) without wiping; unset hook takes the exact pre-change path. — `_flush_expiring`, which also routes the blob read through `migrate_to_current` so a future-version blob raises *before* the wipe.
- [x] 5.4 Document the fail-closed trade-off (wedged key during store outage) where the TTL/GC behavior is documented, alongside the existing `ttl_wiped_suspension` note. — `docs/memory.md`, section "`on_expire`: demoting expiring memory".

## 6. Documentation

- [x] 6.1 Document the compaction tiers and knobs where operators will look (alongside `docs/metrics.md`): the default eviction behavior change and its `compactor=None` opt-out, the summarizer's determinism contract for `build_request`, and the `on_expire` durability/wedging trade-off. — `docs/memory.md` (the page that already owns the two-tier memory story), sections "Compaction" and "`on_expire`: demoting expiring memory". `mkdocs build --strict` clean.

## 7. Gates

- [x] 7.1 `make lint` clean (ruff incl. ASYNC rules on the summarizer's awaited call path). — clean.
- [x] 7.2 `make type` clean (`mypy --strict`; no `Any` in public signatures). — "Success: no issues found in 298 source files".
- [x] 7.3 `make test-unit` passes offline with no docker. — 1340 passed, 9 skipped, 159 deselected.
- [x] 7.4 `make test-semantics-offline` passes, including the extended retry-determinism gate and the unchanged `ttl_expiry` conformance cell. — 65 passed, 5 skipped.
- [x] 7.5 Coverage ratchet at or above baseline; raise `coverage-baseline.toml` if improved. — (blocked: the ratchet is already red on the integration branch this change builds on.) Measured on the same tree with this change stashed: branch-rate **0.8984** (805/896) against a `coverage-baseline.toml` of 0.9497 — the C29 store backends contribute 32 branches that are unreachable without docker. With this change: **0.9000** (846/940), i.e. this change *raises* the rate. Left unchanged rather than lowering a ratchet baseline that is not this change's to move (see Revision 2). <!-- discharged by verify-live-infrastructure phase 4/gates (2026-07-31): `make coverage-ratchet` reports `branch coverage 91.64% is at baseline` on the merged tree; the ratchet is no longer red. See verification-report.md. -->
- [ ] 7.6 `make mutation` passes — `core/dofn.py`, `core/loop.py`, `core/transform.py` are touched; re-check `mutation-baseline.toml` ceilings and document any move in the file's comment. — (deferred: mutation gate runs in CI.)
- [x] 7.7 `uv run pre-commit run --all-files` clean. — (deferred: the `pre-commit` group is not part of this environment's sync; its ruff/mypy hooks are covered by 7.1/7.2.) <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
- [x] 7.8 `openspec validate add-compaction-strategies --strict` passes.

## Revision 1: the facade suite has no `test_facade.py`

Task 1.1 named `tests/memory/test_facade.py`. That file does not exist: the
`memory-facade` suite is split one file per requirement group
(`test_facade_scalars.py`, `test_facade_ring.py`, `test_facade_caps.py`,
`test_facade_accounting.py`, `test_facade_compactor.py`,
`test_facade_staging.py`, `test_facade_longterm.py`). The enumeration
requirement's two scenarios therefore landed in a new file following that
convention, `tests/memory/test_facade_enumeration.py`, and the accounting
property extension went into `test_facade_accounting.py`, whose hypothesis
property it extends. No spec or design text changes.

## Revision 2: the coverage ratchet is red before this change

Task 7.5 assumes the ratchet is green on the base. It is not, on the
integration branch this change builds on: `coverage-baseline.toml` still holds
0.9497 (set by `add-state-schema-migration`), while the merged tree measures
0.8984 — `add-longterm-memory-stores` added four store backends whose 32
branches are only reachable with docker, and the baseline was not re-measured
at that merge. This change moves the rate *up* (0.9000), so it is not the
regression and cannot be the fix: lowering the ratchet is a deliberate act that
belongs to whoever owns the C29 merge, and raising it is impossible while the
tree is below the recorded number. Task 7.5 is left unchecked with the measured
numbers on both sides rather than silently rewriting the baseline file.

## Revision 3: docs live in `docs/memory.md`, which the Impact section did not name

The proposal's Impact section lists modified code but no documentation file,
while task 6.1 requires operator-facing documentation. It was written into
`docs/memory.md` — the page that already owns the working-tier/long-term-tier
split, the `TTL_TIMER` lifetime row, and the invariant-5 carve-out this
change's `on_expire` hook extends — rather than a new page, so the two memory
tiers and the compaction between them stay documented in one place.
