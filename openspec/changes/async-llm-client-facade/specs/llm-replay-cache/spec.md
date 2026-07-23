## MODIFIED Requirements

### Requirement: Deterministic content-hash cache key over LLM request material
`beam_agents.model` SHALL provide `compute_cache_key(model_id, messages, tools_schema, output_schema, sampling_params, entity_key, seq)` returning the lowercase-hex sha256 of a canonical UTF-8 JSON encoding of all seven components (sorted keys, compact separators, `ensure_ascii=False`, NaN/Infinity rejected, `entity_key` bytes hex-encoded). Logically equal inputs MUST always produce the same key regardless of dict insertion order, and changing any single component MUST change the key. Non-JSON-serializable input SHALL raise `TypeError`.

#### Scenario: Logically equal requests hash identically
- **WHEN** `compute_cache_key` is called twice with `messages`, `tools_schema`, `output_schema`, and `sampling_params` structures that are equal but built with different dict insertion orders
- **THEN** both calls return the identical 64-character lowercase hex key

#### Scenario: Every component perturbs the key
- **WHEN** a baseline key is computed and then recomputed seven times, each time changing exactly one of `model_id`, `messages`, `tools_schema`, `output_schema`, `sampling_params`, `entity_key`, `seq`
- **THEN** each of the seven recomputed keys differs from the baseline key

#### Scenario: Non-canonical input is rejected loudly
- **WHEN** `compute_cache_key` is called with a non-JSON-serializable object or a float NaN inside `sampling_params`
- **THEN** the call raises `TypeError` (or `ValueError` for NaN) and no key is produced

## ADDED Requirements

### Requirement: Facade-integrated cache contract for complete flow
The completion facade SHALL treat replay cache as read-through/write-through around provider invocation: lookup by canonical cache key before first provider attempt, return cached response on hit, and insert successful provider responses on miss. Cache-originated responses MUST remain byte-identical to stored payloads and preserve response digest semantics.

#### Scenario: Replay hit suppresses provider retries
- **WHEN** a cache entry exists for a completion request whose upstream endpoint is temporarily unhealthy
- **THEN** `complete()` returns the cached response without opening provider retries for that request

#### Scenario: Fresh successful completion is persisted for replay
- **WHEN** a cache miss is followed by a successful provider completion
- **THEN** the facade stores the exact response bytes and digest under the computed key before returning
