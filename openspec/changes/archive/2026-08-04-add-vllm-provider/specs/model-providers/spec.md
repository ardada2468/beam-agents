## ADDED Requirements

### Requirement: vLLM endpoint mode is a preset over the OpenAI-compatible provider

The system SHALL provide a vLLM endpoint provider in `model/vllm.py` that is a structural subtype of `LLMClient` and delegates its HTTP behavior to the OpenAI-compatible provider: exactly one non-streaming POST to `<base_url>/chat/completions` per `complete` call, raw response body returned as opaque `LlmResponse` bytes, and the shared HTTP-outcome→taxonomy mapping unchanged. Unlike the general OpenAI-compatible provider, the vLLM preset SHALL require `base_url` at construction (no OpenAI default) and SHALL make the API key optional: when no key is configured, the request MUST NOT carry an `Authorization` header; when a key is configured, it SHALL be sent as `Authorization: Bearer <api_key>`. Credentials and endpoint configuration SHALL remain provider state, never `LlmRequest` fields. Endpoint mode MUST NOT require the `vllm` extra.

#### Scenario: Endpoint call is a non-streaming chat-completions POST returning raw bytes

- **WHEN** `complete` is called on a vLLM endpoint provider constructed with a base URL and the server returns HTTP 200 with a chat-completions body
- **THEN** exactly one non-streaming POST is issued to `<base_url>/chat/completions` and the returned `LlmResponse.response` equals the raw body bytes unchanged

#### Scenario: No API key means no Authorization header

- **WHEN** the vLLM endpoint provider is constructed without an API key and `complete` sends a request
- **THEN** the request carries no `Authorization` header, and constructing the provider requires an explicit `base_url` (there is no default endpoint)

#### Scenario: Configured API key is sent as a bearer token

- **WHEN** the vLLM endpoint provider is constructed with an API key
- **THEN** requests carry `Authorization: Bearer <api_key>` and no credential field exists on `LlmRequest`

#### Scenario: Taxonomy mapping is inherited from the shared mapper

- **WHEN** the vLLM server returns HTTP 429, HTTP 503, or the request times out
- **THEN** `complete` raises `RateLimitError`, `ServerError(status=503)`, or `ProviderTimeout` respectively, exactly as the OpenAI-compatible provider does

### Requirement: vLLM sidecar engine is a per-worker-process singleton acquired via Shared

The system SHALL provide a vLLM GPU-worker sidecar provider that is a structural subtype of `LLMClient` and holds the in-process vLLM engine as a worker-local singleton acquired through `apache_beam.utils.shared.Shared`. The `Shared` handle SHALL be created once at pipeline-construction time (by the sidecar factory helper) so that every DoFn instance deserialized in one worker process acquires the same engine, and the acquisition SHALL use a tag derived from the engine configuration so a changed configuration yields a fresh engine rather than silently reusing a stale one. The engine handle SHALL own a dedicated background thread running its own asyncio event loop; `complete` SHALL submit generation to that loop from the DoFn's bridge loop without blocking the bridge loop and without sharing loop-bound objects across loops. Each provider instance SHALL hold a strong reference to the engine handle for its whole lifetime, since `Shared` retains the singleton only weakly.

#### Scenario: Two DoFn instances share one engine

- **WHEN** two provider instances are constructed from the same sidecar factory (same `Shared` handle and tag) in one process
- **THEN** the engine constructor runs exactly once and both providers use the same engine instance

#### Scenario: A changed engine configuration yields a distinct engine

- **WHEN** a provider is constructed with an engine configuration whose derived tag differs from the currently held engine's tag
- **THEN** a new engine is constructed for the new tag rather than reusing the previous engine

#### Scenario: Generation is submitted cross-loop without blocking the bridge

- **WHEN** `complete` runs on a DoFn bridge loop while the engine lives on the engine handle's own loop
- **THEN** the generation request is submitted to the engine loop and awaited asynchronously, and the bridge loop is never blocked by a synchronous engine call

### Requirement: Sidecar engine construction is health-checked during DoFn setup and fails fast

Sidecar provider construction — performed by `provider_factory()` inside `_AgentDoFn.setup` — SHALL run a bounded readiness probe against the acquired engine before the provider is returned: a trivial engine round-trip submitted to the engine loop and awaited with a configurable deadline. If the engine fails to load, the probe fails, or the deadline elapses, construction SHALL raise with an actionable message so `setup` fails and the runner replaces the worker before any element is processed. A healthy, already-constructed engine SHALL make subsequent acquisitions cheap: the probe verifies readiness and MUST NOT re-load model weights.

#### Scenario: An unhealthy engine fails setup before any element

- **WHEN** the engine cannot initialize or its readiness probe raises during provider construction
- **THEN** the constructor raises out of `provider_factory()` and therefore out of `setup`, and no element is ever processed against the broken engine

#### Scenario: A probe deadline overrun raises instead of hanging

- **WHEN** the readiness probe does not complete within the configured health deadline
- **THEN** provider construction raises with a message naming the deadline, rather than blocking `setup` indefinitely

#### Scenario: A second instance's probe reuses the live engine

- **WHEN** a second provider instance is constructed while the shared engine is already initialized and healthy
- **THEN** its readiness probe succeeds against the existing engine without constructing a new engine or re-loading weights

### Requirement: Sidecar responses are cacheable chat-completions-shaped bytes decoded by the OpenAI-compatible Decode

The sidecar provider SHALL serialize each engine output into a canonical chat-completions-shaped JSON body — a `usage` object carrying `prompt_tokens`/`completion_tokens`/`total_tokens` from the engine's token accounting, and `choices[0].message.content` carrying the generated text — returned as opaque `LlmResponse` bytes. The existing OpenAI-compatible `Decode` SHALL decode sidecar responses without modification, so both vLLM modes share one decoder and the facade's usage/trace path is unchanged. The bytes SHALL satisfy the replay-cache contract: storing and re-reading them requires no re-serialization, and a bundle retry served from the replay cache makes zero engine calls.

#### Scenario: A sidecar response decodes with the shared Decode

- **WHEN** the sidecar's `complete` returns and its response bytes are passed to the OpenAI-compatible `Decode`
- **THEN** the decode yields a `DecodedResponse` whose `TokenUsage` matches the engine's token accounting and whose text equals the generated content

#### Scenario: Cached sidecar bytes replay without engine calls

- **WHEN** a facade call is served from the replay cache after a sidecar response was stored
- **THEN** the cached bytes decode identically to the fresh response and the engine receives zero additional generation requests

### Requirement: Sidecar engine failures map onto the provider-error taxonomy

The sidecar provider SHALL map every terminal engine outcome onto the existing provider-error taxonomy so the facade classifies retries by exception type with no facade changes: engine admission/queue saturation SHALL raise `RateLimitError` (with `retry_after_ms=None`); an engine internal failure SHALL raise `ServerError(status=500)`; a generation exceeding its per-request deadline SHALL raise `ProviderTimeout`; request material the engine rejects as invalid (e.g. malformed sampling parameters or a prompt over the model's maximum length) SHALL raise the non-retryable `ProviderRequestError(status=400)`. The conventional `500`/`400` statuses SHALL be documented as stand-ins for in-process failures that carry no HTTP status. All mapping SHALL be verifiable offline through the engine seam with no GPU and no `vllm` installation.

#### Scenario: Engine saturation is retryable as a rate limit

- **WHEN** the engine refuses a generation because its queue or KV-cache admission is saturated
- **THEN** `complete` raises `RateLimitError`, which the facade classifies as retryable

#### Scenario: Engine internal failure is a retryable server error

- **WHEN** the engine raises an internal error while generating
- **THEN** `complete` raises `ServerError` with `status == 500`, which the facade classifies as retryable

#### Scenario: Generation deadline maps to ProviderTimeout

- **WHEN** a generation does not complete within the provider's per-request deadline
- **THEN** `complete` raises `ProviderTimeout`, which the facade classifies as retryable

#### Scenario: Invalid request material is non-retryable

- **WHEN** the engine rejects the request as invalid (bad sampling parameters or over-length prompt)
- **THEN** `complete` raises `ProviderRequestError` with `status == 400`, which is not a `ProviderError` and propagates without retry

### Requirement: The sidecar engine shuts down gracefully when the last holder releases it

Engine shutdown SHALL be tied to reference release, not to any single DoFn's teardown: each provider holds a strong reference to the engine handle, `_AgentDoFn.teardown` drops it by nulling the provider, and the engine handle SHALL register a finalizer that — when the last strong reference in the process is released — stops the engine loop, joins the engine thread, and releases the engine so GPU memory is freed. An engine still referenced by a sibling DoFn instance MUST survive any other instance's teardown untouched. Hard process death is exempt: no graceful path is required when the worker is killed, and no correctness property may depend on engine shutdown running.

#### Scenario: The engine survives a sibling's teardown

- **WHEN** one of two provider instances sharing the engine is released
- **THEN** the engine keeps running and the remaining provider's `complete` calls still succeed

#### Scenario: Last release shuts the engine down exactly once

- **WHEN** the final provider instance holding the engine is released
- **THEN** the shutdown finalizer runs exactly once, stopping the engine loop and joining the engine thread

### Requirement: The vllm extra gates the sidecar and verification is offline via an engine seam

The `vllm` dependency SHALL be an optional extra in `pyproject.toml` following the existing `effector`/`langgraph`/`otlp` pattern: `import beam_agents.model` and every endpoint-mode code path SHALL work without the extra installed, and `vllm` SHALL be imported lazily inside the engine constructor only. Constructing the sidecar without the extra SHALL raise an actionable error naming the extra to install. The sidecar SHALL drive the engine through a narrow internal seam so all sidecar behavior in this capability is verifiable in the default offline tier with an injected fake engine — no GPU, no `vllm` installation, no docker. Real-engine verification SHALL be confined to `smoke`-marked tests that skip when the extra or a GPU is absent.

#### Scenario: The model package imports without the extra

- **WHEN** `beam_agents.model` is imported in an environment without the `vllm` extra
- **THEN** the import succeeds with no side effects and endpoint mode is fully usable

#### Scenario: Sidecar construction without the extra fails actionably

- **WHEN** the sidecar provider is constructed in an environment without the `vllm` extra
- **THEN** construction raises an error whose message names the `vllm` extra as the fix

#### Scenario: Sidecar behavior is verified offline with a fake engine

- **WHEN** the default offline unit tier runs
- **THEN** singleton acquisition, health probing, response-byte shape, taxonomy mapping, and shutdown are all exercised against an injected fake engine with no GPU, no network, and no `vllm` installed

#### Scenario: Real-engine tests are smoke-marked and hardware-gated

- **WHEN** a test exercises a real vLLM engine
- **THEN** it carries the `smoke` marker, is excluded from the offline unit selection, and skips when the `vllm` extra or a GPU is unavailable
