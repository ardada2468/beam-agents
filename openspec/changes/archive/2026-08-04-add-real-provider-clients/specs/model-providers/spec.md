## ADDED Requirements

### Requirement: Anthropic provider is a conforming non-streaming LLMClient

The system SHALL provide an Anthropic provider that is a structural subtype of `LLMClient`: `async def complete(request: LlmRequest) -> LlmResponse`. It SHALL issue exactly one non-streaming HTTP POST to the Anthropic Messages endpoint per `complete` call, sending `stream: false` (or omitting streaming), with the `x-api-key` and `anthropic-version` headers, and SHALL return the raw response body as the opaque `LlmResponse.response` bytes without re-serialization. The provider SHALL be constructed with its own base URL, API key, `anthropic-version`, and per-request timeout — none of which appear on `LlmRequest`. Streaming/token-incremental delivery is out of scope for v0.

#### Scenario: A successful call returns the raw body as opaque response bytes

- **WHEN** `complete` is called and the endpoint returns HTTP 200 with a Messages response body
- **THEN** the provider issues exactly one non-streaming POST and returns an `LlmResponse` whose `response` equals the raw response body bytes unchanged

#### Scenario: Credentials are provider state, not request material

- **WHEN** the provider is constructed with an API key and a `complete` request is built
- **THEN** the API key is carried on the provider/HTTP headers and no credential field exists on the `LlmRequest`

#### Scenario: The request is single-shot and non-streaming

- **WHEN** `complete` sends its request
- **THEN** the request disables streaming and the whole response body is read in one shot before `complete` returns

### Requirement: OpenAI-compatible provider is a conforming non-streaming LLMClient

The system SHALL provide an OpenAI-compatible provider that is a structural subtype of `LLMClient` targeting any `/chat/completions`-shaped endpoint. It SHALL issue exactly one non-streaming HTTP POST per `complete` call (`stream: false`) with an `Authorization: Bearer <api_key>` header to a configurable base URL, and SHALL return the raw response body as the opaque `LlmResponse.response` bytes. It SHALL be constructed with its base URL, API key, and per-request timeout; the same client SHALL work against OpenAI, a vLLM OpenAI server, or any compatible gateway by base-URL alone.

#### Scenario: A successful call returns the raw body as opaque response bytes

- **WHEN** `complete` is called and the endpoint returns HTTP 200 with a chat-completions body
- **THEN** the provider issues exactly one non-streaming POST to `<base_url>/chat/completions` and returns an `LlmResponse` whose `response` equals the raw body bytes unchanged

#### Scenario: Base URL selects the endpoint

- **WHEN** the provider is constructed with a non-default base URL (e.g. a vLLM server)
- **THEN** the POST targets that base URL's `/chat/completions` path and no code change is needed to switch compatible providers

### Requirement: Each provider ships a Decode for token usage and response text

Each provider SHALL ship a `Decode` callable (`Callable[[bytes], DecodedResponse]`) that parses its own response shape into a `DecodedResponse` carrying `TokenUsage` (prompt/completion/total tokens) and the response's JSON text, keeping `facade.py` provider-neutral. The `Decode` SHALL operate purely on the opaque response bytes with no additional network I/O, so the facade can decode a cached response identically to a fresh one.

#### Scenario: Anthropic decode extracts usage and text

- **WHEN** the Anthropic `Decode` is called with a Messages response body
- **THEN** it returns a `DecodedResponse` whose `TokenUsage` reflects the body's input/output token counts and whose `text` is the response's content

#### Scenario: OpenAI-compatible decode extracts usage and text

- **WHEN** the OpenAI-compatible `Decode` is called with a chat-completions response body
- **THEN** it returns a `DecodedResponse` whose `TokenUsage` reflects the body's `usage` fields and whose `text` is the first choice's message content

#### Scenario: Decode is pure over the cached bytes

- **WHEN** the same response bytes are decoded twice (once fresh, once from the replay cache)
- **THEN** both decodes yield identical `DecodedResponse` values and neither performs network I/O

### Requirement: HTTP outcomes map onto the retryable/non-retryable taxonomy

Each provider SHALL map every terminal HTTP outcome onto the provider-error taxonomy so the facade classifies retries by exception type, never by string-matching. A `429` response SHALL raise `RateLimitError` with `retry_after_ms` parsed from the `Retry-After` header when present (else `None`); a `5xx` response SHALL raise `ServerError(status=<code>)`; a request or read timeout SHALL raise `ProviderTimeout`. These three are retryable. A non-`429` `4xx` response (e.g. 400, 401, 403, 404, 422) and a `2xx` body that cannot be decoded SHALL raise the non-retryable provider-request error (a sibling of `ProviderError`, not a subclass), so the facade propagates it immediately without retry. All mapping SHALL be verifiable offline with a mock transport — no live endpoint.

#### Scenario: 429 maps to a retryable rate-limit error with Retry-After

- **WHEN** the endpoint returns HTTP 429 with `Retry-After: 2`
- **THEN** `complete` raises `RateLimitError` whose `retry_after_ms` reflects the 2-second delay and which the facade classifies as retryable

#### Scenario: 5xx maps to a retryable server error carrying its status

- **WHEN** the endpoint returns HTTP 503
- **THEN** `complete` raises `ServerError` with `status == 503`, which the facade classifies as retryable

#### Scenario: A transport timeout maps to ProviderTimeout

- **WHEN** the underlying `httpx` request exceeds its configured timeout
- **THEN** `complete` raises `ProviderTimeout`, which the facade classifies as retryable

#### Scenario: A non-429 4xx maps to the non-retryable error

- **WHEN** the endpoint returns HTTP 400 or HTTP 401
- **THEN** `complete` raises the non-retryable provider-request error (not a `ProviderError`), carrying the status, and the facade does not retry it

#### Scenario: An undecodable success body maps to the non-retryable error

- **WHEN** the endpoint returns HTTP 200 with a body the provider's `Decode` cannot parse
- **THEN** the failure surfaces as the non-retryable provider-request error and the facade does not retry it

### Requirement: Providers use worker-local shared httpx pools on the bridge loop

Each provider SHALL perform its HTTP I/O over a worker-local shared `httpx.AsyncClient` bound to the DoFn's single bridge event loop, reusing connection pools across activations rather than constructing a client per `complete`. The provider SHALL never call blocking or synchronous HTTP and SHALL never create or use its client off the bridge loop, so it composes with the one-loop-per-DoFn async bridge without threading violations.

#### Scenario: The async client is reused across calls

- **WHEN** two `complete` calls run on the same provider instance on the bridge loop
- **THEN** both use the same underlying `httpx.AsyncClient` (shared pools) rather than a freshly constructed client per call

#### Scenario: No synchronous HTTP path exists

- **WHEN** `complete` performs its request
- **THEN** it awaits async `httpx` I/O only, with no synchronous/blocking HTTP call that would stall the bridge loop

### Requirement: Live-endpoint verification is nightly-smoke only

Real-endpoint verification of the providers SHALL be confined to tests carrying a registered `smoke` marker that run only in the nightly workflow and skip without provider credentials. The offline unit tier (`ci`) SHALL NOT require network or credentials: the taxonomy mapping, decode, and non-streaming behavior are verified against a mock transport. The `smoke` marker SHALL be excluded from the offline unit selection so a live test never runs in the default tier.

#### Scenario: Taxonomy and decode are verified offline

- **WHEN** the offline unit tier runs
- **THEN** provider behavior is exercised through a mock transport with no network access and no credentials, and all taxonomy-mapping and decode scenarios pass

#### Scenario: Live smoke tests are marked and credential-gated

- **WHEN** a real-endpoint provider test runs
- **THEN** it carries the `smoke` marker, is excluded from the offline unit selection, and skips when the required provider credentials are absent
