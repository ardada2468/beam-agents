# Tasks: add-fake-llm-provider

## 1. Model-client seam scaffolding

- [x] 1.1 Create `src/beam_agents/model/client.py` with typed signatures only (design D1–D3): frozen `LlmRequest(model_id, messages, tools_schema, sampling_params)`, frozen `LlmResponse(response, response_digest)`, `@runtime_checkable` `LLMClient` `Protocol` with `async def complete(request) -> LlmResponse`, and the error taxonomy `ProviderError` → `RateLimitError(retry_after_ms)` / `ServerError(status)` / `ProviderTimeout`. Bodies raise `NotImplementedError` where applicable so spec-derived tests fail for the right reason.
- [x] 1.2 Extend `src/beam_agents/model/__init__.py` `__all__` to re-export the seam names (`LlmRequest`, `LlmResponse`, `LLMClient`, `ProviderError`, `RateLimitError`, `ServerError`, `ProviderTimeout`); leave root `beam_agents/__init__.py` untouched.

## 2. Model-client seam tests first (one test per spec scenario, named after it)

- [x] 2.1 `tests/model/test_client_types.py` — `LlmRequest`/`LlmResponse` carry their fields and are immutable (reassignment raises); `LlmResponse.response_digest` equals sha256 of `response`.
- [x] 2.2 `tests/model/test_client_protocol.py` — a class defining `async def complete` structurally satisfies `LLMClient` without subclassing; `complete` returns an awaitable resolving to `LlmResponse`.
- [x] 2.3 `tests/model/test_provider_errors.py` — `RateLimitError` exposes `retry_after_ms`, `ServerError` exposes `status`, `ProviderTimeout` is distinct; all four scenarios incl. single `except ProviderError` catches every subclass.
- [x] 2.4 Run `pytest tests/model/test_client_*.py tests/model/test_provider_errors.py` and confirm each fails for the right reason (assertion/`NotImplementedError`, not collection/import error).

## 3. Model-client seam implementation

- [x] 3.1 Implement the frozen value types (`slots=True`, `frozen=True`), `response_digest` derivation, the `Protocol`, and the exception classes with their structured attributes (design D1–D3).
- [x] 3.2 Run `pytest tests/model/test_client_*.py tests/model/test_provider_errors.py` until green without weakening any test.

## 4. FakeLLM scaffolding

- [x] 4.1 Create `src/beam_agents/model/fake.py` with typed stubs (design D4–D8): `FakeLLM` (`add_rule`/constructor script, `complete`, recorded-log accessor, `call_count`, `calls_for`), the `(matcher, behavior)` rule model, behavior constructors (respond-bytes, raise-error, fail-N-then-succeed, `latency_ms`), and convenience matchers (`match_model_id`, `match_contains`, `match_any`). Stubs raise `NotImplementedError`.
- [x] 4.2 Re-export `FakeLLM` and the matcher/behavior helpers from `src/beam_agents/model/__init__.py`; keep root API untouched.

## 5. FakeLLM tests first (one test per spec scenario, named after it)

- [x] 5.1 `tests/model/test_fake_matching.py` — FakeLLM satisfies `LLMClient`; first matching rule wins; match-by-`model_id`; scripted bytes returned verbatim with correct digest.
- [x] 5.2 `tests/model/test_fake_unmatched.py` — unmatched request raises a descriptive non-`ProviderError` naming the request; empty FakeLLM raises on first call.
- [x] 5.3 `tests/model/test_fake_recording.py` — requests recorded in call order; a failing call is still recorded; the exposed log is read-only to callers.
- [x] 5.4 `tests/model/test_fake_latency.py` — injected delay hook awaited exactly once with the configured `latency_ms` (instant hook, no wall-clock wait); real-`asyncio.sleep` path is cancellable so the response never resolves (use `asyncio.wait_for`/cancel, no `sleep()` in the test).
- [x] 5.5 `tests/model/test_fake_failures.py` — rule raises configured `ServerError(503)`; fail-twice-then-succeed yields two `RateLimitError`s then the response, all three recorded.
- [x] 5.6 `tests/model/test_fake_counting.py` — `call_count` increments per invocation incl. a raising call; per-key count groups dict-order-permuted-equal requests; a replay that does not invoke FakeLLM leaves the per-key count unchanged (zero-additional-calls assertion).
- [x] 5.7 `tests/model/test_fake_determinism.py` — same script + same request sequence gives identical responses/errors/log/counts across two runs; `import beam_agents.model` performs no network/logging/global-state mutation.
- [x] 5.8 Run `pytest tests/model/test_fake_*.py` and confirm every test fails with `NotImplementedError` (not collection/import errors).

## 6. FakeLLM implementation

- [x] 6.1 Implement ordered first-match-wins rule evaluation and the fail-closed unmatched-request error (design D4).
- [x] 6.2 Implement `complete`: record request + increment counters FIRST, then apply `latency_ms` via the injected async hook, then serve/raise the behavior (design D5, D6); default hook awaits `asyncio.sleep`, overridable at construction.
- [x] 6.3 Implement per-key counting via `compute_cache_key`'s request derivation and the `calls_for(request)` accessor; expose the recorded log as a read-only ordered view (design D6, D7).
- [x] 6.4 Implement the behavior constructors (respond-bytes → `LlmResponse`, raise-error, fail-N-then-succeed with internal per-rule counter) and convenience matchers (design D4).
- [x] 6.5 Run `pytest tests/model/test_fake_*.py` until green without weakening any test.

## 7. Quality gates

- [x] 7.1 `ruff check` (incl. ASYNC rules) and `mypy --strict` clean on `src/beam_agents/model/`; no `Any` in public signatures; matchers typed `Callable[[LlmRequest], bool]`.
- [x] 7.2 Full offline suite `pytest` (no docker) passes; coverage does not decrease.
- [x] 7.3 Confirm no proto/coder/golden changes crept in (`git status` clean under `protos/`, `src/beam_agents/_protos/`, `tests/core/golden/`) and root `beam_agents/__init__.py` is unchanged.
- [x] 7.4 Run `openspec validate --change add-fake-llm-provider` and confirm the change is clean.
