## 1. Scaffolding & types

- [x] 1.1 Create `src/beam_agents/model/facade.py` with a no-side-effect module docstring referencing this change's design; import from `model.client` and `model.replay_cache`.
- [x] 1.2 Define the frozen value types with full type hints (no `Any`): `TokenUsage(prompt_tokens, completion_tokens, total_tokens)`, `DecodedResponse(usage: TokenUsage, text: str)`, and `FacadeResult(response: LlmResponse, parsed: BaseModel | None, usage: TokenUsage, cache_hit: bool, attempts: int)`.
- [x] 1.3 Define facade errors: `CircuitOpenError(Exception)` (NOT a `ProviderError`) and `OutputSchemaError(Exception)` (NOT a `ProviderError`); add docstrings stating why each is deliberately outside the retryable taxonomy.
- [x] 1.4 Add `CircuitState` enum (`CLOSED`/`OPEN`/`HALF_OPEN`) and the `RetryPolicy(max_attempts, base_ms, max_ms)` config type.
- [x] 1.5 Re-export the new public names from `model/__init__.py` for intra-package use (leave `beam_agents/__init__.py` untouched — facade stays internal).

## 2. Circuit breaker (worker-local, per-endpoint)

- [x] 2.1 Write failing tests `tests/model/test_facade_breaker.py` from the "Per-endpoint circuit breaker" scenarios: threshold trips to OPEN, OPEN fast-fails with `CircuitOpenError` and no provider call, cooldown → HALF_OPEN single trial, half-open success → CLOSED, half-open failure → OPEN. Use an injected clock, no `sleep`.
- [x] 2.2 Implement `CircuitBreaker(threshold, cooldown_ms)` with consecutive-failure counting, `now_ms`-driven cooldown, and a guarded `before_call()`/`record_success()`/`record_failure()` (or equivalent) surface; keep it worker-local with no Beam state.
- [x] 2.3 Verify `CircuitOpenError` is neither a `ProviderError` nor retried; assert the breaker never reads wall-clock.

## 3. Retry policy with jittered Retry-After backoff

- [x] 3.1 Write failing tests `tests/model/test_facade_retry.py` from the retry + backoff scenarios: transient failure retried to success, attempt cap re-raises the last error, non-`ProviderError` not retried, backoff = `min(base*2^(n-1), max)` with injected `rng`, Retry-After as a floor, no sleep after the final attempt. Use `FakeLLM` `fail_then_succeed`/`raise_error` and a recording fake `sleep`.
- [x] 3.2 Implement the retry loop: classify by `ProviderError` subclass, compute full-jitter backoff via injected `rng`, raise the delay to at least `RateLimitError.retry_after_ms`, await the injected `sleep`, cap attempts, and re-raise the last error.
- [x] 3.3 Confirm `attempts` counting matches provider-call counts and that ASYNC-lint stays clean (no `asyncio.sleep`, no `random`, no `time`).

## 4. Replay-cache integration

- [x] 4.1 Write failing tests `tests/model/test_facade_cache.py` from the cache scenarios: hit → zero provider calls / `attempts==0` / `cache_hit True`, miss → one call + response staged, hit replays while breaker OPEN, `output_schema` perturbs the cache key.
- [x] 4.2 Implement cache-key computation over the effective request (including `output_schema` contribution) via `compute_cache_key`, the cache-first ordering (checked BEFORE the breaker), and staging the response into `ReplayCache` on a successful miss.
- [x] 4.3 Assert correctness invariant 3 directly: a second `complete` for the same key makes zero additional provider calls even with the breaker OPEN.

## 5. Token usage accounting

- [x] 5.1 Write failing tests `tests/model/test_facade_usage.py`: provider calls accumulate billed usage (sum), a cache hit reports usage but does not bill, and a failed activation stages nothing extra.
- [x] 5.2 Implement per-call `TokenUsage` from the injected `decode`, billed-accumulation into the staging sink on misses only, and per-`FacadeResult` usage on every path.

## 6. Constrained JSON via output_schema

- [x] 6.1 Write failing tests `tests/model/test_facade_output_schema.py`: valid JSON parses into the Pydantic model, invalid/violating output raises `OutputSchemaError` without transport retry, and no schema → `parsed is None` with no parsing.
- [x] 6.2 Implement effective-request construction folding `model_json_schema()` into the request, `output_schema.model_validate_json(decoded.text)` parsing, and `OutputSchemaError` on `ValidationError`/JSON decode failure.

## 7. Trace emission

- [x] 7.1 Write failing tests `tests/model/test_facade_traces.py`: exactly one `LLM_CALL` `TraceEvent` per call with `gen_ai.request.model` + usage + attempts + cache-outcome + breaker-state attributes; cache hit records hit + zero attempts; a failed call still stages a failure trace.
- [x] 7.2 Implement staging of one `LLM_CALL` `TraceEvent` per `complete` with `entity_key`/`seq`/`step_index`, `start_ms`/`end_ms` from the injected clock, OTel-GenAI string attributes, and a failure trace on the raising path.

## 8. Facade assembly & end-to-end

- [x] 8.1 Write failing tests `tests/model/test_facade_complete.py` from the "Resilient async facade" scenarios: successful call returns a populated `FacadeResult`; assert no wall-clock/unseeded-randomness usage across hit/miss/retry/open-breaker paths.
- [x] 8.2 Implement `LlmFacade.__init__` (wrapped `LLMClient`, `ReplayCache`, `now_ms`, `rng`, `sleep`, `CircuitBreaker`, `RetryPolicy`, `decode`, staging sink) and `async def complete(...)` composing cache → breaker → retry → decode → usage → parse → trace in that order.
- [x] 8.3 Add a semantics-style test proving determinism under bundle retry: re-running `complete` for the same key replays byte-identically with zero extra provider calls and identical staged effects.

## 9. Quality gates

- [x] 9.1 `ruff` (incl. ASYNC) and `mypy --strict` clean on `src/beam_agents/model/facade.py`; no `Any` in public signatures.
- [x] 9.2 Full `pytest` suite green offline (no docker); confirm the coverage ratchet does not decrease.
- [x] 9.3 Run `openspec validate --change async-llmclient-facade` (or the repo's `scripts/check_openspec_change.sh`) and confirm each new test's name traces to a spec scenario.
