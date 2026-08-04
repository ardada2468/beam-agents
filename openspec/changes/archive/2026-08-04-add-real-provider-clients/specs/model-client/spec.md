## REMOVED Requirements

### Requirement: Typed provider-error taxonomy

## ADDED Requirements

### Requirement: Typed provider-error taxonomy for retry decisions

The system SHALL define a provider-error taxonomy the loop driver classifies for retry and backoff decisions. A `ProviderError` base collects the three **retryable** subclasses — `RateLimitError` (provider signalled HTTP 429, with an optional `retry_after_ms`), `ServerError` (provider signalled 5xx, carrying the numeric `status`), and `ProviderTimeout` (the provider did not respond within its deadline). The taxonomy SHALL additionally define one **non-retryable** typed error, `ProviderRequestError` (carrying the numeric `status`), for client-side failures — a non-`429` `4xx` response or an undecodable success body — that a caller MUST NOT retry. `ProviderRequestError` SHALL NOT be a subclass of `ProviderError`, so an `except ProviderError` retry handler does not catch it and it propagates immediately (mirroring how `CircuitOpenError`/`UnmatchedRequestError` sit deliberately outside the retryable base). All are exceptions raised out of `LLMClient.complete`; none is returned as a value. The taxonomy MUST let a caller distinguish retryable transport failures from non-retryable client failures by type, without string-matching messages.

#### Scenario: Rate-limit error carries 429 semantics

- **WHEN** a provider raises `RateLimitError` with `retry_after_ms=1500`
- **THEN** it is an instance of `ProviderError`, exposes `retry_after_ms == 1500`, and is distinguishable by type from `ServerError` and `ProviderTimeout`

#### Scenario: Server error carries its status

- **WHEN** a provider raises `ServerError(status=503)`
- **THEN** it is an instance of `ProviderError` and exposes `status == 503`

#### Scenario: Timeout is its own type

- **WHEN** a provider raises `ProviderTimeout`
- **THEN** it is an instance of `ProviderError` and is neither a `RateLimitError` nor a `ServerError`

#### Scenario: Base type catches all retryable provider failures

- **WHEN** any of `RateLimitError`, `ServerError`, or `ProviderTimeout` is raised
- **THEN** a single `except ProviderError` handler catches it

#### Scenario: Non-retryable request error is outside the retryable base

- **WHEN** a provider raises `ProviderRequestError(status=400)`
- **THEN** it exposes `status == 400`, it is NOT an instance of `ProviderError`, and an `except ProviderError` retry handler does not catch it (so it propagates without retry)
