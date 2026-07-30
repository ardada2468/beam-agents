## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/memory/test_facade.py` additions: the enumeration surface — "keys() reports LRU order without dirtying the facade" and "entry_size does not perturb eviction order", plus a hypothesis extension of the existing accounting property asserting `sum(entry_size(k) for k in keys()) == size_bytes` after arbitrary operation sequences.
- [ ] 1.2 `tests/memory/test_compaction.py`: `DropOldestCompactor` — "Eviction is LRU-first and stops at the target", "Protected prefixes survive even when oldest", "Only protected entries left still over target is not an error" (including the follow-on `MemoryOverflow` from the facade), "Eviction is deterministic across replays" (byte-identical `to_blob()` from equal inputs), and `ValueError` on non-positive `target_bytes`.
- [ ] 1.3 `tests/memory/test_compaction.py`: `SummarizeCompactor` unit behavior against a scripted `call_model` fake — fold/keep_recent arithmetic, prior-summary inclusion in `build_request` inputs, the `ValueError` on a non-shrinking extracted summary, and that the surface handed to user callables exposes no `act`/`emit`.
- [ ] 1.4 `tests/core/test_loop_summarize.py`: driver invocation — "Crossing the trigger folds old items into a summary" (one `FakeLLM` call, folded ring, summary scalar in the result blob), "Below the trigger no model call happens", "A failing summarizer commits nothing" (`ActivationFailed` propagates; no `ActivationResult` produced), and "A suspending activation's continuation includes the summarizer's cursor advance" (`Continuation.step_index` includes the summarizer's `call_model` step).
- [ ] 1.5 `tests/core/test_dofn_activation.py` additions (fake state/timer handles from `tests/core/_dofn_fakes.py`): "An unconfigured pipeline survives a hard-cap-crossing write" (default `AgentConfig.compactor` reaches the facade through `_activate`; evicted entries absent from the committed blob) and "Opting out restores strict overflow" (`compactor=None` → `.errors` with `activation_error`).
- [ ] 1.6 `tests/core/test_dofn_expire.py` (fake handles, fake `MemoryStore`): "Expiring memory lands in the long-term tier and state is wiped", "A retried timer bundle deduplicates to one logical write" (drive `on_ttl` twice; assert byte-identical upserts under the same `(key, seq)`), "Flush failure preserves state for retry", "Unset hook preserves today's expiry behavior", and empty-memory expiry performing no store call.
- [ ] 1.7 `tests/semantics/test_retry_determinism.py`: extend the chaos-forced-retry gate with a summarizing agent — "The summarization LLM call replays from cache on bundle retry": zero extra `FakeLLM` calls on the replayed walk, byte-identical committed `MemoryBlob`, byte-identical intents.
- [ ] 1.8 Confirm the `ttl_expiry` conformance scenario (`tests/conformance/_spec.py`) passes unchanged with `on_expire` unset on both offline legs.

## 2. Memory facade enumeration surface

- [ ] 2.1 `src/beam_agents/memory/facade.py`: add `keys()` (LRU order, no `_touch`, no `dirty`) and `entry_size(key)` (`KeyError` on absent; counts the stored encoded value bytes, ring framing included), documented as the compaction iteration surface.
- [ ] 2.2 `src/beam_agents/memory/__init__.py`: keep exports coherent (facade surface only; strategies export in section 3).

## 3. DropOldestCompactor and default wiring

- [ ] 3.1 Create `src/beam_agents/memory/compaction.py`: `DropOldestCompactor(target_bytes=524_288, protected_prefixes=("__langgraph__/",))` implementing `Compactor` via `keys()`/`entry_size()`/`delete()` only; construction-time `ValueError` on non-positive target; module import side-effect-free.
- [ ] 3.2 Export `DropOldestCompactor` (and later `SummarizeCompactor`) from `beam_agents.memory`; note in the docstring that the LangGraph reserved namespace is protected by default, cross-referencing `adapters/langgraph/checkpoint.py`.
- [ ] 3.3 `src/beam_agents/core/transform.py`: `AgentConfig.compactor: Compactor | None` keyword-only field, `default_factory=DropOldestCompactor`; forwarded by `RunAgent.expand` into `_AgentDoFn`.
- [ ] 3.4 `src/beam_agents/core/dofn.py`: accept `compactor` and pass it in both `_activate` call sites' `run_activation(...)` kwargs, closing the dead parameter; confirm `testing/chaos.py` needs no signature mirror (only `_commit` is wrapped there).

## 4. SummarizeCompactor in the activation

- [ ] 4.1 `src/beam_agents/memory/compaction.py`: `SummarizeCompactor(build_request, extract_summary, source_keys, summary_key="summary", keep_recent=8, trigger_bytes=786_432)` with an async `compact(view)` taking a narrow structural protocol (memory access + `call_model`) defined in this module — no import of `core.context`, avoiding the core↔memory cycle.
- [ ] 4.2 `src/beam_agents/core/loop.py`: invoke the configured summarizer after `agent(ctx)` returns and before the outcome branch builds `Continuation`/`ActivationResult`, inside the existing failure wrap, gated on `ctx.memory.size_bytes >= trigger_bytes`; thread a `summarizer` parameter through `run_activation`.
- [ ] 4.3 `src/beam_agents/core/transform.py` / `core/dofn.py`: `AgentConfig.summarizer` (default `None`) threaded `RunAgent → _AgentDoFn → run_activation`.
- [ ] 4.4 Verify the summarizer's `call_model` usage lands in the existing observability surfaces with no new plumbing (LLM_CALL trace event, `llm_calls`/`llm_ms` tally) — assert in 1.4's tests.

## 5. on_expire flush to the long-term tier (after add-longterm-memory-stores)

- [ ] 5.1 `src/beam_agents/memory/compaction.py` (or the C29-designated home): the `on_expire` hook type and its shipped default implementation performing the `(entity_key, seq)`-keyed idempotent upsert of the final `MemoryBlob` with the timer's firing timestamp as the expiry time, via the C29 `MemoryStore` ABC.
- [ ] 5.2 `src/beam_agents/core/transform.py`: `AgentConfig.on_expire` (default `None`); construction-time `ValueError` when set without a configured long-term store.
- [ ] 5.3 `src/beam_agents/core/dofn.py` `on_ttl`: when configured and `MEMORY` is non-empty, read blob + `SEQ`, submit the flush to the async bridge under a bounded timeout, and only then wipe; on flush failure, propagate (bundle retry) without wiping; unset hook takes the exact pre-change path.
- [ ] 5.4 Document the fail-closed trade-off (wedged key during store outage) where the TTL/GC behavior is documented, alongside the existing `ttl_wiped_suspension` note.

## 6. Documentation

- [ ] 6.1 Document the compaction tiers and knobs where operators will look (alongside `docs/metrics.md`): the default eviction behavior change and its `compactor=None` opt-out, the summarizer's determinism contract for `build_request`, and the `on_expire` durability/wedging trade-off.

## 7. Gates

- [ ] 7.1 `make lint` clean (ruff incl. ASYNC rules on the summarizer's awaited call path).
- [ ] 7.2 `make type` clean (`mypy --strict`; no `Any` in public signatures).
- [ ] 7.3 `make test-unit` passes offline with no docker.
- [ ] 7.4 `make test-semantics-offline` passes, including the extended retry-determinism gate and the unchanged `ttl_expiry` conformance cell.
- [ ] 7.5 Coverage ratchet at or above baseline; raise `coverage-baseline.toml` if improved.
- [ ] 7.6 `make mutation` passes — `core/dofn.py`, `core/loop.py`, `core/transform.py` are touched; re-check `mutation-baseline.toml` ceilings and document any move in the file's comment.
- [ ] 7.7 `uv run pre-commit run --all-files` clean.
- [ ] 7.8 `openspec validate add-compaction-strategies --strict` passes.
