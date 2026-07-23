## Why

The `model/` seam today gives us only a raw provider protocol (`LLMClient.complete(request) -> LlmResponse`), a `ReplayCache`, and a `FakeLLM`. Nothing ties them together: the loop driver would have to hand-roll cache lookup, retry/backoff, endpoint health, token accounting, structured-output parsing, and trace emission at every call site, and get every correctness invariant right each time. We need one resilient, provider-neutral facade that owns that orchestration once — so activations get replay-cache short-circuiting, typed retry with Retry-After-honoring jittered backoff, per-endpoint circuit breaking, usage accounting, constrained-JSON outputs, and OTel-shaped trace points behind a single call — while preserving determinism under bundle retry.

## What Changes

- Add a resilient async **`LLMClient` facade** that wraps a raw provider `LLMClient` plus an activation's `ReplayCache` and staging context, exposing a higher-level `complete(messages, tools, output_schema=..., ...)` entry point.
- **Replay-cache integration:** every call computes the activation cache key, returns a cached hit with ZERO provider calls, and stages the provider response on a miss — preserving correctness invariant 3 (bundle retries incur no extra provider calls).
- **Retry policy with jittered backoff honoring Retry-After:** classify `ProviderError` subclasses by type (retryable: `RateLimitError`, `ServerError`, `ProviderTimeout`); compute exponential backoff with deterministic (injected-RNG) jitter, capped attempts, and honor `RateLimitError.retry_after_ms` as a lower bound on the delay.
- **Per-endpoint circuit breaker:** a worker-local, per-endpoint breaker (closed → open → half-open) that trips on consecutive failures and fails fast while open, kept out of keyed state so it never violates atomic-commit.
- **Token usage accounting:** parse provider-reported prompt/completion/total token counts per call, accumulate them in the activation context, and expose them for trace attributes and metrics.
- **Constrained JSON via `output_schema`:** accept an optional Pydantic v2 model as `output_schema`, drive the provider toward structured output, and validate/parse the response into a typed instance (raising a typed error on invalid JSON / schema-violating output).
- **Trace emission points:** emit an OTel-GenAI-shaped `LLM_CALL` `TraceEvent` per call (model, cache hit/miss, attempt count, usage, latency, breaker state) into the staging context, staged like every other activation effect.

## Capabilities

### New Capabilities
- `model-facade`: The resilient async LLM facade orchestrating replay-cache lookup/stage, typed retry with jittered Retry-After-honoring backoff, per-endpoint circuit breaking, token-usage accounting, `output_schema`-constrained JSON parsing, and per-call trace emission over a wrapped provider `LLMClient`.

### Modified Capabilities
<!-- No existing spec requirements change; the facade composes the model-client protocol, llm-replay-cache, and wire-schemas TraceEvent without altering them. -->

## Impact

- **New code:** `src/beam_agents/model/facade.py` (facade, retry policy, circuit breaker, usage accounting, output-schema parsing helpers) and its tests under `tests/model/`.
- **Composes (unchanged):** `model/client.py` (`LlmRequest`/`LlmResponse`/`LLMClient`/error taxonomy), `model/replay_cache.py` (`ReplayCache`/`compute_cache_key`), `_protos.TraceEvent` (OTel GenAI attributes), `model/fake.py` (`FakeLLM` for tests including `fail_then_succeed`).
- **Dependencies:** `pydantic` v2 (already a project dep) for `output_schema`; no new runtime dependencies.
- **Determinism / invariants:** circuit breaker is a documented worker-local singleton (allowed by the no-global-mutable-state rule); jitter uses an injected RNG and backoff sleeps use an injected async sleeper so retry paths stay test-deterministic and never read wall-clock; all traces/usage are staged, applied only on activation success.
- **Consumers:** the future `core/` loop driver calls the facade instead of a raw provider; no public `beam_agents/__init__.py` surface changes (facade is internal).
