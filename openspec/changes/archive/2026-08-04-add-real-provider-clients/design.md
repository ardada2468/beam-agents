## Context

The `model-client` seam (C05) is complete: `LLMClient` is an async structural `Protocol` returning opaque `LlmResponse` bytes, `LlmRequest` carries only the four cache-relevant components, and the `ProviderError`/`RateLimitError`/`ServerError`/`ProviderTimeout` taxonomy drives the facade's retry and circuit-breaker logic. The `LlmFacade` decodes usage/text through an injected `Decode` callable so `facade.py` stays provider-neutral. The async bridge (`core/bridge.py`) runs exactly one asyncio loop per DoFn instance and expects providers to hold worker-local shared httpx pools. `AgentConfig.provider_factory: Callable[[], LLMClient]` is the construction seam the DoFn calls in `setup()`.

The only implementation today is `FakeLLM`. This change adds the first two real providers — Anthropic Messages and the OpenAI-compatible `/chat/completions` shape — as drop-in `LLMClient`s, plus the HTTP→taxonomy mapping the facade needs. Both decode by hand from `httpx` responses; no vendor SDKs, because the opaque-bytes cache contract requires the *raw body* to be what we store and re-decode.

## Goals / Non-Goals

**Goals:**
- Two production `LLMClient`s (Anthropic, OpenAI-compatible) usable via `provider_factory` with zero changes to `dofn.py`/`loop.py`/`facade.py`.
- Exhaustive, type-based mapping of every terminal HTTP outcome onto the retryable/non-retryable taxonomy, verified offline with `httpx.MockTransport`.
- Non-streaming, single-shot request/response preserving the opaque-bytes replay-cache contract.
- Worker-local shared `httpx.AsyncClient` pools bound to the bridge loop.
- Live-endpoint verification isolated behind a nightly `smoke` marker; the `ci` tier needs no network or credentials.

**Non-Goals:**
- Streaming / token-incremental delivery (deferred; the cache and `LlmResponse` assume a whole payload).
- Vertex and vLLM-native providers (separate changes; the OpenAI-compatible client already covers vLLM's OpenAI server by base URL).
- Prompt/message construction or tool-schema translation beyond passing through `LlmRequest.messages`/`tools_schema` in each provider's request shape.
- Retry/backoff/circuit-breaking policy — owned by `LlmFacade`; providers only classify errors by type.
- Wiring real providers into any default config; `FakeLLM` remains the test default.

## Decisions

### D1: Hand-rolled HTTP + manual decode over vendor SDKs
Each provider builds its request dict, POSTs via `httpx`, and returns `response.content` (raw bytes) as `LlmResponse.response`. The paired `Decode` re-parses those same bytes. **Why:** correctness invariant 3 makes the *raw response body* the cache payload; a vendor SDK returning parsed objects would force re-serialization and risk a non-byte-identical round-trip, breaking replay. Hand-rolling also avoids two heavy transitive dependency trees. **Alternative rejected:** `anthropic`/`openai` SDKs — they own the transport and hide the raw bytes.

### D2: One shared mapping helper, two provider-specific decoders
The HTTP-status→taxonomy mapping is identical across providers (429/5xx/timeout/4xx/undecodable), so it lives in one helper (e.g. `model/_http.py` `raise_for_status_taxonomy(response)` + a timeout-wrapping context). Each provider supplies only its endpoint/headers/request-shape and its `Decode`. **Why:** the mapping is the load-bearing, spec-verified behavior; duplicating it per provider invites drift. **Alternative rejected:** a provider base class — composition of a free function keeps the `LLMClient` structural protocol clean and avoids inheritance.

### D3: `ProviderRequestError` is a sibling of `ProviderError`, not a subclass
The facade retries via `except ProviderError` (facade.py:323), catching *all* subclasses. A non-retryable 4xx therefore cannot be a `ProviderError` subclass or it would be retried. `ProviderRequestError(Exception)` sits outside the base — exactly like `CircuitOpenError`/`UnmatchedRequestError` — so the facade's "non-`ProviderError` exceptions propagate immediately" path handles it. It carries `status` for typed inspection. **Why:** preserves the existing retry classifier untouched. **Alternative rejected:** adding a `retryable: bool` flag on `ProviderError` — would require changing the facade's classifier and every existing call site.

### D4: Lazy, loop-bound `httpx.AsyncClient` reused across activations
`httpx.AsyncClient` binds its connection pool to the running loop on first use. The provider constructs the client lazily on the first `complete` (which runs on the bridge loop) and reuses it for the DoFn's lifetime. **Why:** the bridge starts the loop in `setup()` on a background thread; creating the client at `provider_factory()` time (potentially off-loop) risks binding to the wrong/no loop. Lazy init on first `complete` guarantees loop affinity. Cleanup rides on the daemon loop-thread teardown; an optional `aclose()` is exposed but not required by v0. **Alternative rejected:** constructing the client in the factory — brittle across the setup/threading boundary.

### D5: `Retry-After` parsing is seconds-only for v0
`RateLimitError.retry_after_ms` is populated from a numeric `Retry-After` header (seconds → ms); an HTTP-date form or a missing header yields `None`, and the facade falls back to its own backoff. **Why:** both target APIs emit integer-seconds `Retry-After`; HTTP-date parsing is unneeded complexity. **Alternative rejected:** full RFC 7231 date parsing — no observed need.

### D6: `smoke` marker, excluded from the unit tier, run only in nightly
Register `smoke` in `pyproject.toml` markers (`--strict-markers` requires it). `make test-unit` adds `and not smoke` to its selection so a live test never runs offline; a new `make test-smoke` runs `-m smoke`; `nightly.yml` gains a credential-gated smoke job that `skip`s without keys. **Why:** `project.md` mandates "real providers only in nightly smoke," and `test-unit` currently excludes only integration/semantics/dataflow — an unmarked-excluded smoke test would leak into `ci`. **Alternative rejected:** reusing the `dataflow` marker — semantically wrong and coupled to GCP gating.

## Risks / Trade-offs

- **`httpx.AsyncClient` created off the bridge loop** → D4's lazy-on-first-`complete` init guarantees the client is built while a coroutine is executing on the bridge loop; a unit test asserts the client is instantiated once and reused.
- **Non-byte-identical raw body across retries breaking replay** → the provider stores `response.content` verbatim and never re-encodes; a round-trip test asserts cached bytes decode identically to the fresh response.
- **Provider response shape drift (usage field renames)** → decoders are covered by fixture-body unit tests; a decode failure maps to the non-retryable `ProviderRequestError` rather than a silent zero-usage result, so drift surfaces loudly.
- **A `smoke` test accidentally running in `ci`** → belt-and-suspenders: the marker is excluded in `test-unit`'s selection *and* the smoke tests skip without credentials, so even a mis-selected run is a skip, not a network call.
- **Undecodable 200 body masking a real success** → v0 treats an undecodable success as non-retryable (`ProviderRequestError`); this is a fail-loud choice, accepted because a body the `Decode` cannot parse is unusable to the facade anyway.

## Migration Plan

Additive only. New modules (`model/anthropic.py`, `model/openai_compat.py`, an internal `model/_http.py`) and one new error class in `model/client.py`; new re-exports from `model/__init__.py`. No existing type changes shape or behavior, so no call site migrates. Rollout is "land and it's available via `provider_factory`"; rollback is deleting the modules and the marker wiring. The `smoke` job is credential-gated, so nightly stays green until real keys are configured as repo secrets.

## Open Questions

- Should `ProviderRequestError` also capture a bounded slice of the response body for diagnostics, or status-only? (Leaning status-only to avoid leaking secrets/PII into error text and traces.)
- Do we expose provider `aclose()` to the DoFn `teardown()` now, or defer explicit pool shutdown to daemon-thread teardown until a leak is observed? (Leaning defer.)
