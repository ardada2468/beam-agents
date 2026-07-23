## ADDED Requirements

### Requirement: LLM request value type

The system SHALL provide an `LlmRequest` frozen, hashable value type carrying the provider-neutral request material: `model_id` (`str`), `messages`, `tools_schema`, and `sampling_params`. These are the same four request components `beam_agents.model.compute_cache_key` hashes (the activation-scoped `key`/`seq` are supplied separately by the caller, not by the request). Instances MUST be immutable and MUST NOT carry provider connection details, credentials, or transport state.

#### Scenario: Request carries the four request-material components

- **WHEN** an `LlmRequest` is constructed with `model_id`, `messages`, `tools_schema`, and `sampling_params`
- **THEN** those four fields are readable back unchanged and no additional required field exists

#### Scenario: Request is immutable

- **WHEN** code attempts to reassign any field of a constructed `LlmRequest`
- **THEN** the attempt raises (frozen dataclass) and the instance is unchanged

### Requirement: LLM response value type

The system SHALL provide an `LlmResponse` frozen value type wrapping the canonical provider `response` bytes and its `response_digest` (lowercase-hex or bytes sha256 of `response`). The `response` bytes are exactly the payload the replay cache stores, so a response produced by any `LLMClient` is directly cacheable without re-serialization.

#### Scenario: Response exposes cacheable bytes and digest

- **WHEN** an `LlmResponse` is constructed from provider response bytes
- **THEN** its `response` field returns those bytes unchanged and its `response_digest` is the sha256 of those bytes

#### Scenario: Response is immutable

- **WHEN** code attempts to reassign a field of a constructed `LlmResponse`
- **THEN** the attempt raises and the instance is unchanged

### Requirement: Async LLMClient protocol

The system SHALL define an `LLMClient` typing `Protocol` with a single coroutine method `async def complete(request: LlmRequest) -> LlmResponse`. Every provider (FakeLLM now; anthropic, openai_compat, vertex, vllm later) SHALL be a structural subtype of this protocol. The protocol MUST be provider-neutral (no anthropic/openai-specific fields) and MUST NOT expose a synchronous call path, so the async bridge is the only invocation route.

#### Scenario: A conforming client structurally satisfies the protocol

- **WHEN** a class defines `async def complete(self, request: LlmRequest) -> LlmResponse`
- **THEN** an instance of it is accepted anywhere an `LLMClient` is annotated, with no explicit subclassing required

#### Scenario: complete is a coroutine

- **WHEN** `complete(request)` is called
- **THEN** it returns an awaitable that resolves to an `LlmResponse`, never a plain (already-computed) value

### Requirement: Typed provider-error taxonomy

The system SHALL define a provider-error taxonomy the loop driver classifies for retry and backoff decisions: a `ProviderError` base and three subclasses — `RateLimitError` (provider signalled HTTP 429, with an optional `retry_after_ms`), `ServerError` (provider signalled 5xx, carrying the numeric `status`), and `ProviderTimeout` (the provider did not respond within its deadline). All are exceptions raised out of `LLMClient.complete`; none is returned as a value. The taxonomy MUST let a caller distinguish retryable transport failures from other errors without string-matching messages.

#### Scenario: Rate-limit error carries 429 semantics

- **WHEN** a provider raises `RateLimitError` with `retry_after_ms=1500`
- **THEN** it is an instance of `ProviderError`, exposes `retry_after_ms == 1500`, and is distinguishable by type from `ServerError` and `ProviderTimeout`

#### Scenario: Server error carries its status

- **WHEN** a provider raises `ServerError(status=503)`
- **THEN** it is an instance of `ProviderError` and exposes `status == 503`

#### Scenario: Timeout is its own type

- **WHEN** a provider raises `ProviderTimeout`
- **THEN** it is an instance of `ProviderError` and is neither a `RateLimitError` nor a `ServerError`

#### Scenario: Base type catches all provider failures

- **WHEN** any of `RateLimitError`, `ServerError`, or `ProviderTimeout` is raised
- **THEN** a single `except ProviderError` handler catches it
