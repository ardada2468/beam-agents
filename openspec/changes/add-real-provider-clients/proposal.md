## Why

The runtime has a complete provider seam — the `LLMClient` protocol, `LlmRequest`/`LlmResponse` value types, the `Decode` callable the facade needs, and the typed provider-error taxonomy — but the only implementation is `FakeLLM`. Nothing calls a real model. An agent cannot yet run against Anthropic or an OpenAI-compatible endpoint, so the async bridge, replay cache, and resilient facade have never been exercised against a live HTTP provider or its failure modes. This change delivers the first two real providers and, critically, the mapping from raw HTTP outcomes onto the retryable/non-retryable taxonomy the `LlmFacade` already keys its retry and circuit-breaker decisions on.

## What Changes

- Introduce an **Anthropic provider** (`model/anthropic.py`): an `LLMClient` issuing a single non-streaming POST to the Messages API over a worker-local shared `httpx.AsyncClient`, returning the raw response body as opaque `LlmResponse` bytes, plus its `Decode` callable extracting `TokenUsage` and the response JSON text from Anthropic's response shape.
- Introduce an **OpenAI-compatible provider** (`model/openai_compat.py`): an `LLMClient` for any `/chat/completions`-shaped endpoint (base-URL configurable, `Authorization: Bearer`), same non-streaming discipline, with its own `Decode` for the OpenAI usage/choices shape. This one client covers OpenAI, vLLM's OpenAI server, and other compatible gateways.
- **Streaming is disabled for v0.** Both providers send `stream: false` (or omit streaming) and read the complete response body in one shot. Streaming/token-incremental delivery is explicitly out of scope; the opaque-bytes `LlmResponse` contract and the replay cache both assume a whole-response payload.
- **Map HTTP outcomes onto the provider-error taxonomy** so the facade's by-type retry logic works unchanged: `429` → `RateLimitError(retry_after_ms=...)` (parsed from `Retry-After`), `5xx` → `ServerError(status=...)`, request/read timeout → `ProviderTimeout` (all three retryable); non-retryable `4xx` (400/401/403/404/422) and unparseable success bodies → a **new typed non-retryable error** that is deliberately *not* a `ProviderError`, so the facade propagates it immediately without retry (matching how `CircuitOpenError`/`UnmatchedRequestError` sit outside the retryable base).
- **Credentials never travel on `LlmRequest`.** Each provider is constructed (via the `AgentConfig.provider_factory` seam) with its own base URL, API key, and per-request timeout; the request value type stays credential-free and cacheable exactly as today.
- **Nightly smoke marker only for live traffic.** Add a registered `smoke` pytest marker for the handful of tests that hit a real endpoint; exclude it from the offline unit tier and run it only in the nightly workflow. The taxonomy-mapping behavior itself is verified **offline** with `httpx.MockTransport` — no network, no credentials, in the default `ci` tier.

## Capabilities

### New Capabilities
- `model-providers`: the Anthropic and OpenAI-compatible `LLMClient` implementations and their paired `Decode` callables — non-streaming HTTP over worker-local shared `httpx.AsyncClient` pools on the bridge loop, credential-free `LlmRequest`, exhaustive HTTP-outcome → provider-error-taxonomy mapping, and the nightly-only `smoke` tier for live-endpoint verification.

### Modified Capabilities
- `model-client`: extend the provider-error taxonomy with one **non-retryable** typed error (a sibling of `ProviderError`, not a subclass) so a provider can classify a client-side `4xx`/malformed-body failure as a typed, non-retryable outcome the facade propagates rather than retries. No change to the three existing retryable subclasses.

## Impact

- **Depends on C05** — the `model-client` seam (the `LLMClient` async protocol, the `LlmRequest`/`LlmResponse` value types, and the `ProviderError`/`RateLimitError`/`ServerError`/`ProviderTimeout` taxonomy) established by the FakeLLM provider change. These real providers are structural subtypes of that protocol and raise exactly that taxonomy; they add mechanism, not a new contract.
- **New code**: `src/beam_agents/model/anthropic.py`, `src/beam_agents/model/openai_compat.py` (each: the client `complete`, the `Decode`, and the HTTP→taxonomy mapper), and one new non-retryable error type in `src/beam_agents/model/client.py`. New re-exports from `beam_agents/model/__init__.py`.
- **Consumes (unchanged)**: the async bridge (`core/bridge.py`, one loop per DoFn with shared httpx pools), `LlmFacade`/`ReplayCache`/`Decode` (`model/facade.py`), and the `AgentConfig.provider_factory` construction seam (`core/transform.py`). No production wiring in `core/dofn.py` or `core/loop.py` changes — a real provider is dropped in wherever `FakeLLM` is today via `provider_factory`.
- **Dependencies**: `httpx[http2]` is already a project dependency; no new third-party packages. The Anthropic/OpenAI response shapes are decoded by hand (no vendor SDKs), keeping the opaque-bytes cache contract intact.
- **Build/CI**: `pyproject.toml` registers a `smoke` marker; `Makefile` `test-unit` excludes `smoke` (alongside `integration`/`semantics`/`dataflow`) and a `test-smoke` target runs `-m smoke`; `nightly.yml` gains a credential-gated smoke job. `project.md`'s "real providers only in nightly smoke" note is realized by this marker.
- **Verification**: offline unit tests over `httpx.MockTransport` assert every HTTP outcome maps to the correct taxonomy member (retryable vs non-retryable) for both providers, plus round-trip tests that a decoded response yields correct `TokenUsage`/text and that responses stay cacheable opaque bytes. Live smoke tests carry `-m smoke` and skip without credentials.
- **No breaking changes** to any existing public type; the model-client taxonomy grows by one additive, non-retryable error class.
