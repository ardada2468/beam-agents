## Context

The `model/` seam already provides the primitives this facade composes:

- `model/client.py` — `LlmRequest`, opaque-bytes `LlmResponse`, the async-only `LLMClient` `Protocol`, and the `ProviderError` taxonomy (`RateLimitError.retry_after_ms`, `ServerError.status`, `ProviderTimeout`).
- `model/replay_cache.py` — `ReplayCache` (per-activation, in-memory, injected `now_ms`, no wall clock) and `compute_cache_key(model_id, messages, tools_schema, sampling_params, entity_key, seq)`.
- `model/fake.py` — `FakeLLM` with `respond_with`/`raise_error`/`fail_then_succeed` behaviors and an injected `delay` hook.
- `_protos.TraceEvent` — OTel-GenAI-attribute wire schema with an `LLM_CALL` event type.

What is missing is the orchestration that a loop driver would otherwise duplicate at every call site: cache short-circuit, typed retry with Retry-After-honoring jittered backoff, per-endpoint circuit breaking, usage accounting, `output_schema` structured output, and trace emission. This change adds `model/facade.py`.

The binding constraints come from `openspec/project.md`: correctness invariant 3 (replay cache → zero extra provider calls on retry), the atomic-commit/staging invariant (all effects staged, applied only on success), "no global mutable state except documented worker-local singletons (circuit breakers …)", async-first internals that never block the bridge loop, `mypy --strict` with no `Any` in public signatures, and the mandatory scenario→test→code TDD chain with `FakeLLM` as the default model.

## Goals / Non-Goals

**Goals:**

- One provider-neutral `LlmFacade.complete(...)` that composes cache + retry + breaker + usage + structured output + tracing behind a single async call.
- Full determinism under bundle retry: every non-determinism source (clock, RNG, sleep) is injected; a replayed bundle hits the cache and reproduces byte-identical behavior with no provider calls and no wall-clock reads.
- Typed, by-class control flow: retry/breaker decisions key off the `ProviderError` taxonomy; distinct facade errors (`CircuitOpenError`, `OutputSchemaError`) for the non-transport failure modes.
- Keep the facade internal (no `beam_agents/__init__.py` surface change) and provider-neutral (no Anthropic/OpenAI-specific fields) — provider specifics enter only through the injected `decode` callable.

**Non-Goals:**

- No real provider clients (Anthropic/OpenAI/Vertex/vLLM) — those are later changes; `FakeLLM` drives all tests here.
- No changes to `LLMClient`, `LlmRequest`/`LlmResponse`, `ReplayCache`, or the `TraceEvent` proto.
- No wiring into `core/dofn.py` / the loop driver, and no OTLP/BigQuery exporter — the facade only *stages* effects; transport of staged traces/usage is a separate concern.
- No prompt templating or output-repair loops (project non-goal: no agent-authoring abstractions). `output_schema` validates; it does not re-prompt.

## Decisions

### D1 — Facade wraps request material and returns a rich `FacadeResult`

`complete(request: LlmRequest, *, entity_key: bytes, seq: int, step_index: int, output_schema: type[BaseModel] | None = None) -> FacadeResult`. Taking a full `LlmRequest` (rather than loose `messages`/`tools`) reuses the existing value type and the exact tuple `compute_cache_key` hashes. The result is a frozen `FacadeResult(response, parsed, usage, cache_hit, attempts)` so callers get the cache/attempt/usage facts without re-decoding.

*Alternative considered:* returning a bare `LlmResponse` and making callers re-decode usage — rejected: it duplicates provider `decode` and hides the cache/attempt outcome the loop driver needs for its own tracing/metrics.

### D2 — Provider neutrality via an injected `decode` callable

`LlmResponse.response` is opaque bytes by design (model-client D1), so the facade cannot read token usage or output JSON itself. A `decode: Callable[[bytes], DecodedResponse]` is injected, where `DecodedResponse` exposes `usage: TokenUsage` and `text: str`. This keeps `facade.py` free of any provider-specific parsing; each real provider ships its own decoder later. Tests supply a trivial decoder over `FakeLLM` payloads.

*Alternative considered:* a second protocol method on `LLMClient` (e.g. `decode`) — rejected: it widens the provider protocol and couples response-shape knowledge into every client; a plain callable is narrower and easier to fake.

### D3 — `output_schema` folds into the effective request before hashing

When `output_schema` is present the facade builds an *effective* `LlmRequest` whose `sampling_params`/`tools_schema` incorporate the schema's JSON Schema (`model_json_schema()`), and computes the cache key over that effective request. This guarantees a schema change invalidates the cache and that structured and unstructured variants never alias (spec: "output schema perturbs the cache key"). Parsing is `output_schema.model_validate_json(decoded.text)`; failure raises `OutputSchemaError`.

*Alternative considered:* hashing the original request and parsing after — rejected: two requests differing only by schema would collide in the cache and replay the wrong shape.

### D4 — Retry policy: full-jitter exponential backoff with a Retry-After floor

`RetryPolicy(max_attempts, base_ms, max_ms)`. Pre-jitter delay for attempt *n* (1-indexed) is `min(base_ms * 2^(n-1), max_ms)`; the awaited delay is `rng`-drawn full jitter over `[0, pre_jitter]`, then raised to at least `retry_after_ms` when the error is a `RateLimitError`. Only `ProviderError` subclasses are retryable; the loop re-raises the last error at the cap and awaits no sleep after the final attempt. Sleep is the injected `sleep(ms)` (mirrors `FakeLLM`'s `delay` hook), so ASYNC-lint stays clean and tests never wall-sleep.

*Alternatives considered:* equal (no-jitter) backoff — rejected, thundering-herd on shared endpoints; decorrelated jitter — deferred, full jitter is simpler to specify and test and adequate for our fan-out. Honoring Retry-After as an *override* rather than a *floor* — rejected: a server asking for 30 s must not be undercut by a smaller local backoff.

### D5 — Circuit breaker is a worker-local, per-endpoint singleton

The breaker lives outside keyed Beam state (explicitly allowed by the project's global-state rule) because endpoint health is a worker property, not per-key activation state — putting it in keyed state would both violate atomic-commit determinism and lose cross-key health signal. States CLOSED/OPEN/HALF_OPEN with a consecutive-failure `threshold` and a `cooldown_ms` measured against injected `now_ms`. Crucially, the cache-hit path is checked *before* the breaker, so an open breaker never blocks replay (correctness invariant 3). `CircuitOpenError` is deliberately **not** a `ProviderError`: it must be neither retried by `RetryPolicy` nor counted as a provider failure that re-trips the breaker.

*Alternative considered:* per-key breaker in keyed state — rejected on both determinism (staged state can't carry live wall-clock health) and signal-dilution grounds.

### D6 — Everything observable is staged, never applied inline

Usage accumulation and the `LLM_CALL` `TraceEvent` are written to an injected staging sink, consistent with correctness invariant 1: a failed/timed-out activation must mutate nothing. Billed usage counts cache misses only (a hit made no provider call); each `FacadeResult` still reports its own per-call usage. A `complete` that ultimately raises still stages a failure-describing trace before propagating, so failures are observable without breaking fail-closed semantics (the staged effects are simply discarded if the whole activation is rolled back).

### D7 — Module & export layout

All new types live in `src/beam_agents/model/facade.py`: `LlmFacade`, `FacadeResult`, `TokenUsage`, `DecodedResponse`, `RetryPolicy`, `CircuitBreaker`, `CircuitState`, `CircuitOpenError`, `OutputSchemaError`. Importing the module has no side effects (matches the rest of `model/`). Nothing is re-exported from `beam_agents/__init__.py` — the facade is internal, consumed by the future loop driver. `model/__init__.py` may re-export for intra-package use.

## Risks / Trade-offs

- **[Injected `decode` must be provider-correct]** → A wrong decoder silently mis-accounts tokens or mis-parses output. Mitigation: `DecodedResponse` is a tiny typed contract; real-provider changes ship the decoder with scenario tests, and `FakeLLM` tests pin the facade's own accounting logic independent of any real decoder.
- **[Worker-local breaker state escapes staging]** → Breaker transitions are not rolled back with a failed bundle, so a bundle retry sees post-failure breaker state. Mitigation: this is intended (health is a worker property); the cache-first ordering guarantees replay correctness regardless of breaker state, and breaker mutations never touch keyed state.
- **[Full jitter widens latency variance]** → occasional longer individual backoffs. Mitigation: bounded by `max_ms`; acceptable for a batch/streaming runtime (not sub-second interactive), and Retry-After remains a hard floor.
- **[`output_schema` steering is provider-shaped]** → how the schema is injected into `sampling_params`/`tools_schema` is provider-specific in reality. Mitigation: for this change the facade only needs the schema to (a) perturb the cache key and (b) validate the response; provider-specific steering wiring lands with each real provider. Documented as an open question below.
- **[Trace attribute cardinality]** → free-form attributes could bloat. Mitigation: fixed OTel-GenAI key set, string values, deterministic (sorted) map serialization per the proto comment.

## Migration Plan

Additive only — a new module and tests, no changes to existing specs, protos, or public API, so no migration or rollback machinery is required. Delivered TDD per project convention: write scenario-named failing tests against `FakeLLM` first, then implement `facade.py` until green; `ruff` (incl. ASYNC) and `mypy --strict` clean; coverage ratchet holds. Rollback is deletion of the new module and tests.

## Open Questions

- Exact mapping of `output_schema` into provider-native structured-output controls (Anthropic tool-use vs OpenAI `response_format`) — deferred to each real-provider change; the facade contract here only requires schema-in-cache-key and response validation.
- Whether billed usage should also surface a *cached-tokens* counter for cache hits (observability nicety) — defaulting to no; revisit when the metrics exporter lands.
- Whether `CircuitOpenError` should carry an estimated `retry_after_ms` (time until half-open) to help the loop driver schedule — plausible; left out of the initial contract to keep the breaker minimal.
