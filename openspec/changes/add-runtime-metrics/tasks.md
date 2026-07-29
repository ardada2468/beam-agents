## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/observability/test_metrics.py`: the metric surface itself — `RuntimeMetrics` builds a handle per declared name under `beam_agents.runtime`, `incr`/`observe` reach the right handle, handles are built once and reused across calls, `NullMetrics` records nothing, and constructing either outside a Beam context raises nothing. Derived from "The runtime publishes a fixed metric surface" and "The recorder is an injectable seam".
- [x] 1.2 `tests/core/test_context.py`: the per-activation tally — a provider-reached `call_model` adds one `llm_calls` and one `llm_ms` sample measured from the injected `monotonic_ns`; a replay-cache hit adds neither; `accumulate_usage` sums decoded totals; `iterations` equals the step-cursor delta for both a fresh and a step-index-seeded (resumed) context; `AgentContext.run_tool` adds one `tool_calls`.
- [x] 1.3 `tests/core/test_loop.py`: `ActivationResult` carries the tally out of `run_activation` for both the completing and the suspending outcome, and the tally is absent from `memory_blob`/`continuation` (byte-identical blobs with and without recorded counts). Derived from "the tally never reaches keyed state".
- [x] 1.4 `tests/core/test_dofn_metrics.py` (fake-handle unit tests, inside the mutmut selection, over the shared handles in `tests/core/_dofn_fakes.py`): with a recording fake sink — a committed activation records `activations`, `suspensions` when suspended, `intents_emitted` by intent count, and the four commit-path distributions; a raising activation and a timing-out activation record `agent_errors` and an `activation_ms` sample and nothing else; a refused resume records only `orphaned_results` and no `activation_ms`; `on_ttl` over a live continuation records one `agent_errors`; the HITL `Drop` route records one `agent_errors` and no `activations`; an `Escalate` records one `intents_emitted`.
- [x] 1.5 `tests/core/test_dofn_pipeline.py`: DirectRunner `TestPipeline` runs — one exercising completing, intent-staging, failing, and orphaned elements, and a second covering a suspension (split because a suspension's timer fires drag in the runner's metric under-reporting; see design Risks) — then queries `result.metrics()` for the `beam_agents.runtime` namespace and asserts `intents_emitted == len(.intents)`, `agent_errors + orphaned_results == len(.errors)`, and `suspensions <= activations`. This is the only test that proves updates actually reach a real Beam metrics container — the fake-sink tests cannot.
- [x] 1.6 `tests/core/test_dofn_pipeline.py`: an activation whose model calls happen on the bridge thread reports a non-zero `llm_calls` through `result.metrics()`. This is the regression test for the thread-locality trap (a naive implementation reports zero here while every fake-sink test still passes).
- [x] 1.7 Confirm the retry-determinism semantics gate (`tests/semantics/test_retry_determinism.py`) still passes unchanged, and extend it with an assertion that a chaos-forced bundle retry leaves the re-minted intents byte-identical and the provider call count unchanged while the counters may have moved — pinning "measurement is not decision".

## 2. The metrics module

- [x] 2.1 Create `src/beam_agents/observability/__init__.py` with the capability overview docstring (module map's "traces, metrics, exporters" home) and no re-exports beyond the metrics surface.
- [x] 2.2 Create `src/beam_agents/observability/metrics.py`: `NAMESPACE = "beam_agents.runtime"`, the seven counter names and five distribution names as module constants, the `MetricsSink` protocol (`incr(name, n=1)` / `observe(name, value)`), `RuntimeMetrics` (handles pre-built once, no per-call allocation), and `NullMetrics`. Import-side-effect-free.

## 3. Staging the tally

- [x] 3.1 Add the tally type (counts plus the per-call `llm_ms` durations) in `observability/metrics.py` alongside the metric names it maps to — one module owning the vocabulary — with the read-back shape `ActivationResult`/`AgentResult` expose.
- [x] 3.2 `ActivationContext`: accept an injected `monotonic_ns` callable (default `time.monotonic_ns`), bracket the provider await in `call_model` to add one `llm_calls` and one `llm_ms` sample on a miss only, and leave the cache-hit branch uncounted.
- [x] 3.3 `ActivationContext.accumulate_usage`: replace the no-op stub with real accumulation into the tally, and update the stale comment that says the runtime keeps no usage tally.
- [x] 3.4 `ActivationContext`: expose the activation's `iterations` as the step-cursor delta against the seeded starting index, so a resume reports only its own steps.
- [x] 3.5 `AgentContext`: count `tool_calls` in `run_tool` and carry the same tally shape out through `AgentResult.drain()`, sharing the tally type with `ActivationContext` rather than duplicating it.
- [x] 3.6 `core/loop.py`: carry the tally on `ActivationResult` for both outcomes, keeping the dataclass frozen and the field defaulted so existing construction sites in tests still build.

## 4. Recording in the DoFn

- [x] 4.1 `_AgentDoFn.__init__`: private `metrics` parameter defaulting to `RuntimeMetrics()`, plus the injected `monotonic_ns` used for `activation_ms`.
- [x] 4.2 Add the counting dead-letter method that increments `orphaned_results` for `orphaned_result` and `agent_errors` for every other reason, returning the tagged output; route every `.errors` emission in `_start`, `_resume`, `on_ttl`, and `on_hitl` through it, leaving the pure `_error` builder in place for the chaos helper and tests.
- [x] 4.3 Bracket `self._activate(...)` in `_start` and `_resume` with the monotonic clock and record `activation_ms` on all three exits — success, `ActivationTimeout`, and other exceptions.
- [x] 4.4 `_commit`: record `activations` (adjacent to `seq.add(1)`), `suspensions`, `intents_emitted`, `llm_calls`, `tool_calls`, and the `memory_bytes`, `iterations`, `tokens` (only when usage was decoded) and `llm_ms` samples — placed after the timers and before the emits, and document the extended commit order in the docstring/comment that currently states it.
- [x] 4.5 `_escalate`: count the escalation intent as one `intents_emitted`, so the counter still equals the `.intents` element count.
- [x] 4.6 Update `testing/chaos.py` if any monkeypatched signature moved (`_commit` is wrapped there and mirrors its parameters exactly).

## 5. Documentation

- [x] 5.1 Document the metric surface where an operator will look for it — the twelve names, what each counts, the attempted-vs-committed caveat, and the `activation_ms` vs. runtime-overhead distinction — in `docs/` alongside the existing `effector.md`/`ci.md`.

## 6. Gates

- [x] 6.1 `make lint`, `make type` clean (`mypy --strict`, no `Any` in public signatures, ruff ASYNC rules on the bracketed await).
- [x] 6.2 Full unit tier passes offline with no docker; both offline semantics gates still pass.
- [x] 6.3 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new module improves it.
- [x] 6.4 `make mutation` passes; re-check `mutation-baseline.toml`'s `dofn.py` and `context.py` ceilings and document any move in the file's comment, following the precedent set by the previous two changes. **Driving the element path with fake handles pulled ~264 previously unreachable mutants into the selection; killing them needed `tests/core/test_dofn_activation.py` (routing, activation inputs, commit semantics, failure exits) and two renumbered `mutation-exclusions.toml` entries. `dofn.py` 267 -> 3.**
- [x] 6.5 `uv run pre-commit run --all-files` clean.
