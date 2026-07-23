## MODIFIED Requirements

### Requirement: LLM request value type

The system SHALL provide an `LlmRequest` frozen, hashable value type carrying the provider-neutral request material for facade completion: `model_id` (`str`), `messages`, `tools_schema`, `output_schema`, and `sampling_params`. These are the request components `beam_agents.model.compute_cache_key` hashes (the activation-scoped `key`/`seq` are supplied separately by the caller, not by the request). Instances MUST be immutable and MUST NOT carry provider connection details, credentials, or transport state.

#### Scenario: Request carries all request-material components

- **WHEN** an `LlmRequest` is constructed with `model_id`, `messages`, `tools_schema`, `output_schema`, and `sampling_params`
- **THEN** those five fields are readable back unchanged and no additional required field exists

#### Scenario: Request is immutable

- **WHEN** code attempts to reassign any field of a constructed `LlmRequest`
- **THEN** the attempt raises (frozen dataclass) and the instance is unchanged

### Requirement: LLM response value type

The system SHALL provide an `LlmResponse` frozen value type wrapping the canonical provider `response` bytes, its `response_digest` (lowercase-hex or bytes sha256 of `response`), and usage/provenance metadata required by the facade (`input_tokens`, `output_tokens`, `total_tokens`, and `from_replay_cache`). The `response` bytes are exactly the payload the replay cache stores, so a response produced by any `LLMClient` is directly cacheable without re-serialization.

#### Scenario: Response exposes cacheable bytes, digest, and usage metadata

- **WHEN** an `LlmResponse` is constructed from provider response bytes and usage values
- **THEN** its `response` field returns those bytes unchanged, `response_digest` is the sha256 of those bytes, and usage/provenance fields are readable without mutation

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
