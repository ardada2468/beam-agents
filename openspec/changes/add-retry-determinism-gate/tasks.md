## 1. Chaos commit-failure helper (`beam_agents/testing/chaos.py`)

- [x] 1.1 Create the `beam_agents.testing` package (test infrastructure; not re-exported from the root `beam_agents` API).
- [x] 1.2 Implement `fail_first_matching_commit(matcher)`: a context manager that replaces `_AgentDoFn._commit` with a wrapper raising a designated `ChaosBundleFailure` the first time an `ActivationResult` satisfies `matcher`, delegating to the original `_commit` for every other call (including Beam's own retry of the failed bundle); restore the original `_commit` on exit. Provide a `match_any()` convenience matcher.
- [x] 1.3 Unit test: with the helper active and a matcher matching one specific `ActivationResult`, the first matching commit raises and every subsequent commit (matching or not) proceeds normally; a non-matching commit is never failed. (Drive this directly against `_AgentDoFn` via a small `TestPipeline`, not a mocked object — the assertion is on real commit behavior.)
- [x] 1.4 Unit test: after the `with` block exits, `_AgentDoFn._commit` is restored to the original (no leakage into subsequent tests/pipelines).

## 2. Retry-determinism semantics test (write first, TDD)

- [x] 2.1 Create `tests/semantics/` (new tier directory, `__init__.py` + module) and `tests/semantics/test_retry_determinism.py`, marked `-m semantics`.
- [x] 2.2 Add a module-level test agent (`tests/semantics/_helpers.py`, following the `tests/core/_dofn_helpers.py` convention: real module-level functions, not closures/`__main__`, so the DoFn pickles cleanly) that: calls the model once, stages one intent via `ctx.act(...)`, and suspends; on resume, calls the model again with the identical request, then completes.
- [x] 2.3 Build the test pipeline (`TestStream`, `streaming=True`, matching `tests/core/test_dofn_streaming.py`'s pattern, including `.advance_watermark_to_infinity()`) sending one event then one matching `tool_result`, with `fail_first_matching_commit(lambda r: r.status == "completed")` active around `pipeline.run()`.
- [x] 2.4 Assert on `.traces`: exactly one `LLM_CALL` event with `cache_hit == "false"` and exactly one with `cache_hit == "true"`, proving the chaos-forced retry added zero real provider calls beyond the original, unavoidable first call. (Learned mid-implementation: `assert_that` matchers must assert-and-raise on the collected values directly — a closure copying values into an outer list for post-hoc assertions does not reliably survive Beam's own serialization of the assertion DoFn, even on the in-process DirectRunner.)
- [x] 2.5 Assert on `.intents`: exactly one committed `ToolIntent` whose `intent_id` equals `intent_id_for(entity_key, seq, step_index)` computed from the test's own known call sequence.
- [x] 2.6 Add a negative-path unit test proving the trace assertion actually catches a broken invariant: build a hand-constructed trace list with two `cache_hit == "false"` events (simulating a regression that re-calls the provider on resume) and show the same assertion helper fails on it.

## 3. Wire as a required, offline CI check

- [x] 3.1 Add a `test-semantics-offline` Makefile target selecting `-m "semantics and not integration"` with NO exit-5 tolerance (an empty collection fails).
- [x] 3.2 Add a required `semantics` job/step to `.github/workflows/ci.yml` running `make test-semantics-offline` on the offline matrix (no docker, no compose-up).
- [x] 3.3 Keep the docker-backed `make test-semantics` in `.github/workflows/integration.yml` for future worker-kill / golden-blob gates (which additionally carry the `integration` marker).
- [x] 3.4 Update `project.md`'s testing-tier / required-checks notes to record the offline retry-determinism gate and the `semantics and not integration` split.

## 4. Verify

- [x] 4.1 Run `make test-semantics-offline` locally and confirm the retry-determinism test passes offline with no docker.
- [x] 4.2 Run `ruff` and `mypy --strict` clean on the new `testing` module; confirm coverage does not decrease.
- [x] 4.3 `openspec validate add-retry-determinism-gate --strict` passes.
