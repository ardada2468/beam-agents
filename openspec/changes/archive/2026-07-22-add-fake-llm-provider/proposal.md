## Why

Conventions make FakeLLM "the default model in all tests," and three release-gating semantics rely on it: retry-determinism asserts zero extra FakeLLM calls on the cached path, effectively-once asserts one execution per intent, and the loop driver needs a way to force `429`/`5xx`/timeout without real providers. None of that exists yet — there is no model-client seam for a provider to implement and no test double to script. The replay cache (just landed) has no client in front of it, so nothing can be tested end-to-end until FakeLLM and the interface it implements exist.

## What Changes

- Introduce the async **model-client seam** all providers (FakeLLM now; anthropic/openai_compat/vertex/vllm later) implement:
  - `LlmRequest` frozen value type carrying `(model_id, messages, tools_schema, sampling_params)` — the same request material `compute_cache_key` hashes (minus the activation-scoped `key`/`seq`).
  - `LlmResponse` frozen value type wrapping the canonical provider `response` bytes (what the replay cache stores) plus a `response_digest`.
  - `LLMClient` `Protocol` with a single `async def complete(request) -> LlmResponse`.
  - A typed provider-error taxonomy the loop driver classifies for retry/backoff: `ProviderError` base with `RateLimitError` (HTTP 429, optional `retry_after_ms`), `ServerError` (5xx, carries `status`), and `ProviderTimeout`.
- Introduce **`FakeLLM`**, a deterministic in-process `LLMClient` for tests:
  - **Scripted responses via matchers**: an ordered list of `(matcher, behavior)` rules where `matcher` is a `Callable[[LlmRequest], bool]` (plus convenience constructors: match by `model_id`, by substring in the request, match-any); first matching rule serves the request. An unmatched request **fails closed** (raises) so missing scripting surfaces loudly instead of returning a silent default.
  - **Request recording**: every `complete` call appends its `LlmRequest` to an ordered, queryable log for assertions.
  - **Injectable latency**: a rule may carry `latency_ms` applied through an injected async delay hook (no wall-clock `sleep`), so tests can drive a provider slow enough to trip `activation_timeout` deterministically.
  - **Injectable failures**: a rule may raise `RateLimitError`/`ServerError`/`ProviderTimeout`, including a "fail N times then succeed" behavior for exercising retry paths.
  - **Call counting for determinism assertions**: total and per-request-key provider-invocation counts, so the retry-determinism gate can assert the cached path adds zero calls.
- No wire-schema or proto changes: request material and the recording log are provider-shaped in-memory Python; response payloads are opaque bytes.

## Capabilities

### New Capabilities
- `model-client`: the async `LLMClient` protocol, the `LlmRequest`/`LlmResponse` value types, and the provider-error taxonomy (`ProviderError`, `RateLimitError`, `ServerError`, `ProviderTimeout`) that every provider raises and the loop driver classifies.
- `fake-llm`: the deterministic scripted `FakeLLM` test double — matcher-ordered responses, fail-closed on unmatched, request recording, injectable latency and failures, and provider-call counting.

### Modified Capabilities
<!-- None: no existing spec's requirements change. FakeLLM implements the new model-client seam and consumes no existing capability's behavior. -->

## Impact

- New code: `src/beam_agents/model/client.py` (seam: `LlmRequest`, `LlmResponse`, `LLMClient`, error taxonomy), `src/beam_agents/model/fake.py` (`FakeLLM` + matcher/behavior helpers); both re-exported from `src/beam_agents/model/__init__.py`. Tests under `tests/model/`.
- No proto/coder changes (`protos/`, `src/beam_agents/_protos/`, `core/coders.py` untouched); no golden-blob changes.
- No new dependencies (stdlib `dataclasses`/`hashlib` + existing `pytest-asyncio`); public root API (`beam_agents/__init__.py`) untouched — `FakeLLM` is test infrastructure, not public surface.
- Downstream consumers (future changes): the model client/loop driver (calls `LLMClient.complete`, consults the replay cache in front of it, classifies provider errors), the retry-determinism / effectively-once semantics gates (assert on `FakeLLM` call counts), and real providers (implement `LLMClient`).
