## Context

The replay cache landed but has no client in front of it. Conventions make FakeLLM "the default model in all tests" and three release-gating `-m semantics` tiers depend on it (retry-determinism: zero extra FakeLLM calls; effectively-once: one execution per intent; failure injection for the loop driver). Before FakeLLM can exist, providers need a seam to implement: today `model/` has only `compute_cache_key`/`ReplayCache`, no `LLMClient`, no request/response types, no error taxonomy.

This change delivers that seam plus its first (test) implementation. Two capabilities ship together because the seam is untestable without an implementation and FakeLLM is meaningless without the seam. Real providers and the loop driver that calls `complete()` and wraps it with the replay cache and retry logic are deliberately later changes.

Request material is the four cache-key components minus the activation-scoped `key`/`seq`: `(model_id, messages, tools_schema, sampling_params)`. `messages`/`tools_schema`/`sampling_params` are typed `object` — provider-shaped Python, exactly as `compute_cache_key` already treats them.

## Goals / Non-Goals

**Goals:**
- A provider-neutral async `LLMClient` protocol and immutable `LlmRequest`/`LlmResponse` value types, where `LlmResponse.response` is the byte payload the replay cache stores unchanged.
- A typed provider-error taxonomy (`ProviderError` → `RateLimitError`/`ServerError`/`ProviderTimeout`) the loop driver can classify by type, not by message string.
- A deterministic, offline `FakeLLM`: ordered matcher rules, fail-closed on unmatched, request recording (recorded before latency/failure), injectable latency via an injected async hook, injectable failures incl. fail-N-then-succeed, and total + per-request-key call counts.

**Non-Goals:**
- No loop driver / DoFn wiring, no cache-consultation-before-call logic (that change puts `ReplayCache` in front of `complete()`).
- No real providers (anthropic/openai_compat/vertex/vllm) and no retry/backoff policy — only the error *taxonomy* they'll use.
- No FakeLLM-over-HTTP server (the nightly `-m dataflow` transport) — a later change wraps this in-process `FakeLLM` behind HTTP.
- No proto/wire-schema or coder changes; `LlmRequest`/`LlmResponse` are in-process Python, not on the wire. No public root API surface.
- No response *templating* or auto-generation — responses are scripted opaque bytes; FakeLLM does not model tokens or streaming.

## Decisions

### D1. Request material excludes `key`/`seq`; response is opaque bytes
`LlmRequest` holds exactly `(model_id, messages, tools_schema, sampling_params)`. The activation-scoped `key`/`seq` that also feed `compute_cache_key` are not provider inputs — they scope the *cache*, not the *call* — so the caller (future loop driver) supplies them to the cache, not to the client. `LlmResponse.response` is opaque `bytes` (plus `response_digest = sha256(response)`) rather than a parsed message: it is precisely what `ReplayCache.put` stores, so a real provider's response is cacheable with no re-serialization, and FakeLLM's scripted bytes are indistinguishable from a real provider's on the cached path. Parsing into text/tool-calls belongs to adapters, not the client seam. *Alternative rejected:* a structured `LlmResponse` (text + tool_calls) — it would force every provider to canonicalize into our shape and make the cache store a re-encoding rather than provider bytes, weakening the byte-identical-replay guarantee.

### D2. `LLMClient` as a `typing.Protocol`, async-only
Providers conform structurally (`@runtime_checkable` Protocol with `async def complete`), so FakeLLM and real providers need no shared base class and no import coupling. Async-only (no sync path) matches the bridge-thread architecture — the runtime never calls a provider off the async loop. *Alternative rejected:* an ABC base class — forces inheritance and an import dependency on the seam module, and invites a sync escape hatch.

### D3. Error taxonomy is exceptions, typed, retryability by class
`ProviderError` base with `RateLimitError(retry_after_ms)`, `ServerError(status)`, `ProviderTimeout`. The loop driver decides retry/backoff by `isinstance`, never by parsing a message. `retry_after_ms` and `status` are carried as structured attributes. Timeout is modelled as an error type (what the provider *raises* when its own deadline trips), distinct from the runtime's `activation_timeout` (which the caller enforces by cancelling the coroutine — see D5). *Alternative rejected:* returning an error union from `complete` — mixing success values and failures defeats `await` ergonomics and forces every call site to branch.

### D4. Ordered `(matcher, behavior)` rules, first-match-wins, fail-closed
Matching is an ordered list of predicates `Callable[[LlmRequest], bool]`, evaluated in registration order; first match serves. This is trivially deterministic and lets specific rules precede general ones. Unmatched requests raise a distinct non-`ProviderError` "unmatched request" error naming the offending request, so a forgotten script line fails the test loudly instead of silently returning a canned default. Convenience constructors (`match_model_id`, `match_contains`, `match_any`) keep tests off raw lambdas. *Alternative rejected:* a dict keyed by exact request — brittle against provider-shaped nested structures and dict ordering; a silent default response — hides missing scripting, the opposite of what a test double should do.

### D5. Latency via an injected async hook, never a wall clock
A behavior's `latency_ms` is realized by awaiting an injected `delay(ms)` hook (default `asyncio.sleep(ms/1000)`, overridable at construction to a recording/instant stub). FakeLLM reads no wall clock and never calls blocking `sleep`, honoring the project's "timer behavior via scripted advances, never `sleep()`" rule and keeping unit tests instant. To simulate a provider that outlasts `activation_timeout`, a test uses the real `asyncio.sleep` hook with a large `latency_ms` and lets the caller cancel the coroutine — the cancellation *is* the timeout behavior. *Alternative rejected:* `time.sleep`/threaded delays — blocks the event loop (ruff ASYNC violation) and makes tests slow and nondeterministic.

### D6. Recording happens before latency and failure
`complete` appends the `LlmRequest` to the log and increments counters as its first actions, before awaiting latency or raising a failure behavior. Thus a request that ultimately times out or 429s is still recorded and still counted — which is exactly what determinism/retry assertions need (they count provider *invocations*, including failed ones). The log is exposed as a read-only ordered view (tuple/`Sequence`) so a test cannot mutate FakeLLM's history. *Alternative rejected:* record on success only — would undercount retried/failed calls and make retry-path assertions wrong.

### D7. Per-key counts reuse the cache-key request derivation
The per-key counter groups requests by the same canonical-JSON derivation `compute_cache_key` uses for its request portion (sorted keys, compact separators, `allow_nan=False`), so logically equal requests that differ only in dict ordering share a key. This makes "the cached path added zero provider calls" a one-liner assertion (`fake.calls_for(request) == 1`) that lines up exactly with what the replay cache considers the same request. FakeLLM depends on `compute_cache_key` for this — reusing the existing function rather than reimplementing canonicalization. *Alternative rejected:* counting by `id()` or raw dict identity — two logically identical requests would count separately and the determinism assertion would be meaningless.

### D8. Module layout and exports
`src/beam_agents/model/client.py` holds the seam (`LlmRequest`, `LlmResponse`, `LLMClient`, `ProviderError` + subclasses); `src/beam_agents/model/fake.py` holds `FakeLLM` and the matcher/behavior helpers. Both are re-exported from `beam_agents/model/__init__.py` (extending the existing `__all__`). The root `beam_agents/__init__.py` is untouched: FakeLLM is test infrastructure, and the client seam is internal until a provider ships. Fail-closed and no-import-side-effects are enforced by test (mirroring the replay-cache change's import-purity test).

## Risks / Trade-offs

- **Two new capabilities in one change vs. the "one capability each" convention** → Accepted: the seam is untestable without an implementation and FakeLLM is undefined without the seam. They are one indivisible unit of value; splitting would produce a spec with no tests. Real providers, HTTP transport, and loop wiring remain separate future changes.
- **Opaque-bytes `LlmResponse` pushes parsing to adapters** → Mitigation: this is the same boundary the replay cache already assumes (it stores `response` bytes); adapters/observability decode when they need structure. Keeps byte-identical replay honest.
- **`FakeLLM` depending on `compute_cache_key` couples test infra to the cache module** → Mitigation: it is the *right* coupling — per-key counts must agree with what the cache treats as identical, or determinism assertions would silently drift from cache behavior. `compute_cache_key` is already public in `beam_agents.model`.
- **Injected-hook latency can't, by itself, prove real timeout behavior** → Mitigation: the deadline is the caller's (`activation_timeout` cancels the coroutine); FakeLLM's job is only to be slow on demand. The spec covers both the instant-hook path (assert hook awaited) and the real-sleep-plus-cancel path.
- **Fail-N-then-succeed adds per-rule mutable state, a determinism hazard** → Mitigation: the counter is internal to the rule and advances only on matching invocations, so a fixed rule set + fixed request sequence is reproducible; the "repeated runs identical" scenario guards it.

## Open Questions

- Should convenience matchers include a match-by-`seq`/turn-index helper for multi-turn scripts, or is `match_contains` + explicit ordering enough? Leaning enough for now; add later if multi-turn tests get verbose.
- Exact spelling of the recorded-log accessor (`requests` property returning a tuple vs. a `recorded()` method) — cosmetic, resolved during implementation to match repo naming once the first test is written.
