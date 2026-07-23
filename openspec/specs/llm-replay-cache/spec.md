# llm-replay-cache Specification

## Purpose
TBD - created by archiving change add-llm-replay-cache. Update Purpose after archive.

## Requirements

### Requirement: Deterministic content-hash cache key over LLM request material
`beam_agents.model` SHALL provide `compute_cache_key(model_id, messages, tools_schema, sampling_params, entity_key, seq)` returning the lowercase-hex sha256 of a canonical UTF-8 JSON encoding of all six components (sorted keys, compact separators, `ensure_ascii=False`, NaN/Infinity rejected, `entity_key` bytes hex-encoded). Logically equal inputs MUST always produce the same key regardless of dict insertion order, and changing any single component MUST change the key. Non-JSON-serializable input SHALL raise `TypeError`.

#### Scenario: Logically equal requests hash identically
- **WHEN** `compute_cache_key` is called twice with `messages`, `tools_schema`, and `sampling_params` structures that are equal but built with different dict insertion orders
- **THEN** both calls return the identical 64-character lowercase hex key

#### Scenario: Every component perturbs the key
- **WHEN** a baseline key is computed and then recomputed six times, each time changing exactly one of `model_id`, `messages`, `tools_schema`, `sampling_params`, `entity_key`, `seq`
- **THEN** each of the six recomputed keys differs from the baseline key

#### Scenario: Non-canonical input is rejected loudly
- **WHEN** `compute_cache_key` is called with a non-JSON-serializable object or a float NaN inside `sampling_params`
- **THEN** the call raises `TypeError` (or `ValueError` for NaN) and no key is produced

### Requirement: Facade stages lookups and inserts over an in-memory LlmCacheBlob
`beam_agents.model` SHALL provide a `ReplayCache` facade constructed per activation from an optional `LlmCacheBlob` and a caller-supplied `now_ms` clock value. The facade SHALL mutate only in-memory data — it MUST NOT perform Beam state I/O or read wall-clock time — and SHALL expose `to_blob()` returning an `LlmCacheBlob` with `state_schema_version` 1, entries in LRU order (least-recently-used first), and `total_response_bytes` populated. A `dirty` property SHALL be `False` until the first mutation or LRU-updating access; a miss on `get` MUST NOT dirty the facade.

#### Scenario: Blob round-trips through an untouched facade
- **WHEN** a `ReplayCache` is constructed from a blob containing existing entries and `to_blob()` is called without any access
- **THEN** the returned blob has field-equal entries in the same order, the same `total_response_bytes`, `state_schema_version` 1, and `dirty` is `False`

#### Scenario: Fresh facade produces a versioned empty blob
- **WHEN** a `ReplayCache` is constructed with no blob and `to_blob()` is called
- **THEN** the result has `state_schema_version` 1, no entries, `total_response_bytes` 0, and `dirty` is `False`

### Requirement: Cache hits return the staged response and update LRU order
`get(cache_key)` on a live, fully-stored entry SHALL return the exact response bytes and their sha256 digest, re-stamp the entry's `last_access_ms` with the injected `now_ms`, and move it to most-recently-used position. `get` on an absent key SHALL return `None` and leave the facade unchanged. `put(cache_key, response)` SHALL stage the response with an internally computed sha256 `response_digest` and `created_at_ms`/`last_access_ms` set to the injected clock.

#### Scenario: Put-then-get replays the identical bytes
- **WHEN** a response is staged with `put` and retrieved with `get` under the same key
- **THEN** the returned entry carries byte-identical response bytes, `digest_only` is `False`, and the digest equals sha256 of the response

#### Scenario: A hit moves the entry to most-recently-used
- **WHEN** a facade holds entries `[a, b, c]` in LRU order and `get(a)` succeeds
- **THEN** `to_blob()` emits the order `[b, c, a]` and `a.last_access_ms` equals the injected `now_ms`

#### Scenario: A miss leaves the facade clean
- **WHEN** `get` is called with a key that has never been stored
- **THEN** the result is `None` and `dirty` remains `False`

### Requirement: Entries expire 6 hours after creation
An entry SHALL be expired when `now_ms - created_at_ms` exceeds 21,600,000 ms; expiry compares against `created_at_ms`, never `last_access_ms`. `get` on an expired entry SHALL return `None` and remove the entry; `put` SHALL purge all expired entries before enforcing capacity bounds. Re-`put` of an existing key SHALL replace the entry and reset `created_at_ms`.

#### Scenario: An expired entry is a miss and is purged
- **WHEN** a facade is constructed with `now_ms` more than 6 hours after an entry's `created_at_ms` and `get` is called for that key
- **THEN** the result is `None` and the entry is absent from `to_blob()`

#### Scenario: The TTL boundary is inclusive
- **WHEN** `get` is called for an entry whose age equals exactly 21,600,000 ms
- **THEN** the entry is returned as a live hit

#### Scenario: Access does not refresh TTL
- **WHEN** an entry is read via `get` shortly before its expiry and read again after 6 hours from `created_at_ms` have elapsed
- **THEN** the second read returns `None`

### Requirement: LRU bound of 64 entries
The facade SHALL hold at most 64 entries. An insert that would exceed the bound SHALL evict least-recently-used entries (by access order, ties broken by staging order) until the bound holds. Digest-only entries count toward the bound.

#### Scenario: The 65th insert evicts the least-recently-used entry
- **WHEN** 65 distinct keys are staged with `put` and none are read
- **THEN** `to_blob()` contains exactly 64 entries and the first-staged key is absent

#### Scenario: A recently read entry survives eviction
- **WHEN** 64 keys are staged, the first-staged key is read via `get`, and a 65th key is staged
- **THEN** the first-staged key is still present and the second-staged key is evicted

### Requirement: Serialized blob never exceeds 100 KiB; oversized responses degrade to digest-only
`to_blob().ByteSize()` SHALL never exceed 102,400 bytes. An insert that would exceed the cap SHALL evict least-recently-used entries until the blob fits. A response whose entry could not fit even in an otherwise empty blob SHALL be stored as a digest-only entry — `digest_only` `True`, empty `response`, `response_digest` = sha256 of the full response — without evicting any existing entries. `get` on a digest-only entry SHALL return it marked digest-only with the preserved digest and no response bytes.

#### Scenario: The cap holds under arbitrary operation sequences
- **WHEN** property-based sequences of `put` and `get` operations with hypothesis-generated keys and response sizes are applied
- **THEN** after every operation `to_blob().ByteSize()` is at most 102,400 bytes

#### Scenario: Large inserts evict until the blob fits
- **WHEN** responses are staged whose combined size exceeds the cap while each fits individually
- **THEN** least-recently-used entries are evicted, the newest entry is fully stored, and the serialized blob is within the cap

#### Scenario: An oversized response becomes digest-only without collateral eviction
- **WHEN** a response too large to ever fit within the cap is staged into a facade holding other live entries
- **THEN** the new entry is digest-only with `response_digest` equal to sha256 of the response, and every pre-existing entry is still present

#### Scenario: Digest-only hits identify themselves
- **WHEN** `get` is called for a digest-only entry
- **THEN** the returned entry has `digest_only` `True`, empty response bytes, and the original digest, enabling the caller to detect provider nondeterminism after re-calling
