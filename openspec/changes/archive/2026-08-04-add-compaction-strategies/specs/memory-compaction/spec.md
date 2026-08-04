# memory-compaction Specification (delta)

## ADDED Requirements

### Requirement: DropOldestCompactor evicts least-recently-used entries to a target and never touches protected prefixes
`beam_agents.memory` SHALL export a `DropOldestCompactor` implementing the existing `Compactor` protocol. Its `compact(memory)` SHALL delete entries in least-recently-used-first order (the facade's persisted LRU order) until `memory.size_bytes` is at or below a configurable `target_bytes` (default 524 288, half the hard cap) or no evictable entries remain. Keys matching any configured protected prefix (default `("__langgraph__/",)`) MUST NOT be deleted; when only protected entries remain above the target, `compact` SHALL return without error and leave the facade's existing cap contract (raise `MemoryOverflow` on a still-over-cap write) in force. `compact` MUST be a pure function of the staged entries and the compactor's frozen configuration — it MUST NOT read a clock, use randomness, or perform I/O — so a replayed activation evicts an identical set. Constructing with a non-positive `target_bytes` SHALL raise `ValueError`.

#### Scenario: Eviction is LRU-first and stops at the target
- **WHEN** a facade holds entries whose LRU order is `a, b, c, d` and a `DropOldestCompactor(target_bytes=t)` runs with `size_bytes` above `t` such that deleting `a` and `b` reaches `t`
- **THEN** `a` and `b` are deleted, `c` and `d` remain, and `size_bytes` is at or below `t`

#### Scenario: Protected prefixes survive even when oldest
- **WHEN** the least-recently-used entry's key starts with `__langgraph__/` and compaction runs
- **THEN** that entry is skipped, eviction proceeds to the next-oldest unprotected entry, and the protected entry is present in `to_blob()` afterwards

#### Scenario: Only protected entries left still over target is not an error
- **WHEN** every remaining entry is protected and `size_bytes` still exceeds `target_bytes`
- **THEN** `compact` returns without raising, no protected entry is deleted, and a subsequent hard-cap-crossing write raises `MemoryOverflow` per the memory-facade contract

#### Scenario: Eviction is deterministic across replays
- **WHEN** `compact` runs twice against two facades constructed from the same `MemoryBlob` with the same `now_ms`
- **THEN** both facades' `to_blob()` outputs are byte-identical afterwards

### Requirement: The default compactor is wired through AgentConfig into every activation
`AgentConfig` SHALL gain a keyword-only `compactor: Compactor | None` field defaulting to a `DropOldestCompactor` with default configuration, and `RunAgent`'s DoFn SHALL pass it through `run_activation` into the `ActivationContext` so the facade's soft-cap and hard-cap hook sites invoke it. Setting `compactor=None` SHALL restore the prior behavior (no compaction; hard-cap-crossing writes raise `MemoryOverflow`).

#### Scenario: An unconfigured pipeline survives a hard-cap-crossing write
- **WHEN** an agent running under a default `AgentConfig` performs a write whose prospective total exceeds the 1 MiB hard cap while unprotected LRU entries exist
- **THEN** the write succeeds, the evicted entries are absent from the committed `MemoryBlob`, and the activation commits normally instead of dead-lettering

#### Scenario: Opting out restores strict overflow
- **WHEN** the same write occurs under `AgentConfig(compactor=None)`
- **THEN** `MemoryOverflow` propagates, the activation fails closed, and the element is routed to `.errors` with reason `activation_error`

### Requirement: SummarizeCompactor runs inside the activation and calls the model only through the activation's cache-first path
`beam_agents.memory` SHALL provide a `SummarizeCompactor` configured with `build_request` (maps the folded items and any prior summary to an `LlmRequest`), `extract_summary` (maps opaque response bytes to summary bytes), the source ring keys, a `summary_key` (default `"summary"`), a `keep_recent` count (default 8), and a `trigger_bytes` threshold (default 786 432, the soft cap). The loop driver SHALL invoke it inside `run_activation` — after the agent's outcome is returned and before any `Continuation` or `ActivationResult` is assembled, within the existing failure wrap — if and only if the staged `memory.size_bytes` is at or above `trigger_bytes`; the trigger decision MUST depend only on staged memory state. Its model calls SHALL go exclusively through `ActivationContext.call_model`; it MUST NOT stage intents or outputs, and it SHALL be handed a surface that exposes only memory access and `call_model`. On success it SHALL replace each source ring's items older than the newest `keep_recent` with nothing, write the extracted summary under `summary_key` as a scalar, and leave newer items verbatim. An `extract_summary` result whose size is not smaller than the total size of the items it replaces SHALL raise `ValueError`. Any exception it raises SHALL fail the activation atomically (nothing commits).

#### Scenario: Crossing the trigger folds old items into a summary
- **WHEN** an activation ends with `size_bytes` at or above `trigger_bytes` and a source ring `"log"` holds 20 items with `keep_recent=8`
- **THEN** one model call is made through `call_model`, the committed blob's `"log"` ring holds exactly the newest 8 items, the summary scalar is stored under `summary_key`, and `size_bytes` decreased

#### Scenario: Below the trigger no model call happens
- **WHEN** an activation ends with `size_bytes` below `trigger_bytes`
- **THEN** the summarizer performs zero model calls and the committed blob equals what the agent staged

#### Scenario: The summarization LLM call replays from cache on bundle retry
- **WHEN** a bundle containing a summarizing activation is retried by the chaos wrapper after its first committed walk (the retry-determinism gate's forced-retry harness) with a scripted FakeLLM
- **THEN** the replay incurs zero additional FakeLLM calls — the summarizer's request is served from the replay cache keyed by `(content, key, seq)` — and the committed `MemoryBlob` is byte-identical to the first walk's

#### Scenario: A failing summarizer commits nothing
- **WHEN** `extract_summary` raises during the summarization pass of an otherwise successful activation
- **THEN** the activation fails closed (`ActivationFailed` → `.errors` with reason `activation_error`), keyed state is unchanged, and no partial summary or fold is observable on the next activation

#### Scenario: A suspending activation's continuation includes the summarizer's cursor advance
- **WHEN** a summarization pass runs during an activation that suspends
- **THEN** the persisted `Continuation.step_index` reflects the step cursor after the summarizer's `call_model` advances, so the resume mints no intent ID at a step the suspension already consumed

### Requirement: on_expire flushes expiring working memory to the long-term tier before the TTL wipe
`AgentConfig` SHALL gain a keyword-only `on_expire` hook (default `None`). When it is unset, `TTL_TIMER` expiry SHALL behave exactly as today (dead-letter a live suspension, wipe all keyed state). When set — which requires the long-term `MemoryStore` from `add-longterm-memory-stores` to be configured — the `on_ttl` callback SHALL, before wiping, read the key's committed `MemoryBlob` and `SEQ` value and perform one idempotent upsert of the blob to the long-term tier keyed by `(entity_key, seq)`, executed on the DoFn's async bridge under a bounded timeout, with the timer's firing timestamp recorded as the expiry time. The wipe SHALL proceed only after the flush succeeds. A flush failure SHALL propagate out of the timer callback (failing the bundle so the runner retries it) and MUST NOT wipe state. A key with empty working memory SHALL be wiped without a store call. The upsert content MUST be a pure function of committed keyed state and the timer's firing timestamp, so a retried timer bundle produces an identical upsert.

#### Scenario: Expiring memory lands in the long-term tier and state is wiped
- **WHEN** `TTL_TIMER` fires for a key with non-empty working memory under a config with `on_expire` set and a fake long-term store
- **THEN** the store receives exactly one upsert carrying the key's final `MemoryBlob` keyed by `(entity_key, seq)`, and afterwards all five state specs are cleared

#### Scenario: A retried timer bundle deduplicates to one logical write
- **WHEN** the `on_ttl` flush executes twice for the same expiry (a retried timer bundle) against an idempotent store
- **THEN** both upserts are byte-identical and keyed identically, so the store holds one record for `(entity_key, seq)`

#### Scenario: Flush failure preserves state for retry
- **WHEN** the store raises during the `on_ttl` flush
- **THEN** the exception propagates, no state spec is cleared, and a subsequent firing performs the same flush against unchanged state

#### Scenario: Unset hook preserves today's expiry behavior
- **WHEN** `TTL_TIMER` fires under a config with `on_expire` unset
- **THEN** no store interaction occurs and the wipe (and any `ttl_wiped_suspension` dead letter) matches the pre-change behavior, keeping the `ttl_expiry` conformance scenario green
