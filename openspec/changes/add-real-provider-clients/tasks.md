## 1. Taxonomy extension (model-client delta)

- [x] 1.1 Write the failing unit test for `ProviderRequestError`: it exposes `status`, is NOT an instance of `ProviderError`, and is not caught by `except ProviderError` (derives from the model-client `Non-retryable request error is outside the retryable base` scenario).
- [x] 1.2 Add `ProviderRequestError(Exception)` (status-carrying, deliberately not a `ProviderError` subclass) to `src/beam_agents/model/client.py`; re-export it from `beam_agents/model/__init__.py` and add to `__all__`.
- [x] 1.3 Confirm the existing three retryable-taxonomy tests still pass unchanged (no regression to `ProviderError`/`RateLimitError`/`ServerError`/`ProviderTimeout`).

## 2. Shared HTTP→taxonomy mapper

- [x] 2.1 Write failing tests over `httpx.MockTransport` for the mapping helper: 429 (+`Retry-After: 2`) → `RateLimitError(retry_after_ms=2000)`; 429 without header → `retry_after_ms=None`; 5xx → `ServerError(status)`; `httpx.TimeoutException` → `ProviderTimeout`; non-429 4xx (400/401/403/404/422) → `ProviderRequestError(status)`.
- [x] 2.2 Implement `src/beam_agents/model/_http.py`: `raise_for_status_taxonomy(response)` (status→taxonomy) and a timeout-wrapping helper that converts `httpx.TimeoutException` into `ProviderTimeout`. Seconds-only `Retry-After` parsing per design D5 (non-numeric/absent → `None`).
- [x] 2.3 Assert the mapper never string-matches messages — classification is by status code / exception type only.

## 3. Anthropic provider

- [x] 3.1 Write failing tests (mock transport): a 200 Messages body → `LlmResponse.response` equals the raw body bytes; exactly one non-streaming POST is issued with `x-api-key`/`anthropic-version` headers and `stream:false`; `LlmRequest` carries no credential field.
- [x] 3.2 Write failing decode tests: the Anthropic `Decode` extracts `TokenUsage` (input/output tokens) and response text from a fixture body; an undecodable 200 body → `ProviderRequestError`.
- [x] 3.3 Implement `src/beam_agents/model/anthropic.py`: the `LLMClient` (`complete` building the Messages request from `LlmRequest`, POST via the lazy loop-bound `httpx.AsyncClient` per design D4, returning raw bytes, routing failures through the shared mapper) and its `Decode`.
- [x] 3.4 Re-export the Anthropic provider + its `Decode` from `beam_agents/model/__init__.py`.

## 4. OpenAI-compatible provider

- [x] 4.1 Write failing tests (mock transport): a 200 chat-completions body → raw bytes; exactly one non-streaming POST to `<base_url>/chat/completions` with `Authorization: Bearer`; a non-default base URL retargets the endpoint with no code change.
- [x] 4.2 Write failing decode tests: the OpenAI-compatible `Decode` extracts `usage` tokens and the first choice's message content; undecodable 200 → `ProviderRequestError`.
- [x] 4.3 Implement `src/beam_agents/model/openai_compat.py`: the `LLMClient` and its `Decode`, reusing the shared mapper and the lazy loop-bound client.
- [x] 4.4 Re-export the OpenAI-compatible provider + its `Decode` from `beam_agents/model/__init__.py`.

## 5. Bridge-loop / httpx pool behavior

- [x] 5.1 Write a failing test that two `complete` calls on one provider instance reuse the same underlying `httpx.AsyncClient` (shared pools), and that the client is instantiated lazily on first `complete` (on the loop), not in the factory.
- [x] 5.2 Ensure both providers satisfy the async-only path (ruff ASYNC rules clean; no synchronous/blocking HTTP call) and compose with `AsyncBridge` — a small test running `complete` through the bridge loop.

## 6. Smoke tier wiring (nightly only)

- [x] 6.1 Register the `smoke` marker in `pyproject.toml` `[tool.pytest.ini_options].markers` ("smoke: nightly-only real-provider smoke tests against live endpoints").
- [x] 6.2 Update `Makefile`: add `and not smoke` to `test-unit`'s selection; add a `test-smoke` target running `-m smoke` (with the exit-5 no-tests tolerance).
- [x] 6.3 Add a credential-gated smoke job to `.github/workflows/nightly.yml` running `make test-smoke` (skips/no-ops when provider API-key secrets are absent).
- [x] 6.4 Add `tests/smoke/test_real_providers.py`: `-m smoke` tests that run one real `complete` per provider and skip when credentials are absent; assert a decodable non-empty response.

## 7. Verification & docs

- [x] 7.1 Run `make lint`, `make type`, and `make test-unit` — all clean; confirm no `smoke` test ran in the unit tier and coverage did not decrease.
- [x] 7.2 Run `openspec validate --change add-real-provider-clients` and fix any spec/scenario formatting issues.
- [x] 7.3 Confirm `beam_agents/__init__.py` public surface is unchanged (only `model` package re-exports grow); update `project.md`'s provider/testing notes if the smoke marker changes the documented tier list.
