## ADDED Requirements

### Requirement: The replay CLI reconstructs an activation and re-runs it locally

The package SHALL ship a `beam-agents-replay` console script that loads a `StateSnapshot` file and a varint-length-delimited `TraceEvent` stream, imports the agent by `module:attribute` path (with optional tool-registry and decode import paths), reconstructs the `run_activation` inputs — `entity_key` and `seq` from the snapshot and trace (highest traced `seq` by default, `--seq` to override), `now_ms` from the target activation's traced `ACTIVATION_START.start_ms`, the triggering event or resume payload from an operator-supplied envelope file, and `step_index`/adapter snapshot from the embedded `Continuation` for a resume — and SHALL re-run the activation by calling `run_activation` directly, outside any Beam pipeline. The CLI SHALL refuse, with an actionable error, an envelope whose `entity_key` does not match the snapshot's, and SHALL exit with code `2` on any usage or configuration error.

#### Scenario: A replayed activation reproduces the traced outcome

- **WHEN** an activation committed in a pipeline (its model responses therefore present in the snapshot's cache blob), its requests and intent walk did not read memory entries it itself overwrote, and the CLI replays it from a post-commit snapshot, the trace stream, and the original envelope
- **THEN** the re-run completes with the traced status, its staged `TraceEvent`s serialize byte-identical to the traced events of that attempt, its staged intents carry the traced `intent_id`s, and the CLI exits `0`

#### Scenario: A failed activation replays to its traced failure position

- **WHEN** a traced activation failed in agent logic before any provider-reached model call, and the CLI replays it from a snapshot taken after the failure (state unchanged, per the atomic-commit invariant)
- **THEN** the re-run raises the same failure at the traced step position, the CLI matches it against the traced `ERROR` event's failure attributes, and exits `0`

#### Scenario: Replay makes zero provider calls

- **WHEN** a replay runs an activation whose model calls are all present in the snapshot's cache blob
- **THEN** the injected provider's `complete` is never invoked and no network connection is attempted

#### Scenario: A resume replay is reconstructed from the continuation

- **WHEN** the snapshot contains a `Continuation` and the supplied envelope carries a `ToolResult` for one of its pending intents
- **THEN** the CLI seeds `run_activation` with the continuation's `step_index` and adapter snapshot bytes and the envelope's resume payload, and the re-run resumes rather than starting fresh

#### Scenario: A mismatched envelope is refused

- **WHEN** the supplied envelope's `entity_key` differs from the snapshot's
- **THEN** the CLI refuses to run, names both keys in the error, and exits `2`

### Requirement: The replay provider serves only from the replay cache and fails loudly on miss

Replay SHALL inject a provider implementing the `LLMClient` protocol whose `complete` unconditionally raises a cache-miss error naming the request's cache-key material; cached responses are served by the context's cache-first path from the loaded blob, so the provider is reached only on a genuine miss or a `digest_only` entry. The replay provider MUST NOT hold or construct any transport: no code path in the replay package opens a network connection. A miss SHALL abort the replay with exit code `3` and a report distinguishing "irreproducible: pre-activation state not captured or entry evicted" from a divergence; a `digest_only` entry SHALL abort likewise, reporting the stored `response_digest`.

#### Scenario: A cache miss aborts loudly instead of calling a provider

- **WHEN** a replayed activation issues a model request whose cache key is absent from the loaded cache blob
- **THEN** the replay aborts with an error naming the cache key, no network is touched, and the CLI exits `3`

#### Scenario: A digest-only entry is not silently refetched

- **WHEN** a replayed activation's request resolves to a `digest_only` cache entry
- **THEN** the replay aborts, reports the entry's stored `response_digest` hex, and exits `3`

#### Scenario: Cache entries do not expire at replay time

- **WHEN** a snapshot is replayed long after its cache entries' 6-hour TTL would have elapsed in wall-clock terms
- **THEN** entries are evaluated against the traced activation clock, remain live exactly as they were at activation time, and are served normally

### Requirement: Snapshots migrate on load and newer schemas are refused

The CLI SHALL check the `state_schema_version` of the snapshot and of every embedded blob before use. A version older than the current schema SHALL be migrated in memory using the same per-blob migration functions the pipeline applies lazily (the `state-schema-migration` capability); the migrated result is what replay runs against, and nothing is written back. A version newer than the installed package SHALL be refused with an error naming the snapshot's version, the supported version, and the remedy, exiting `2`.

#### Scenario: An older-schema snapshot replays after migration

- **WHEN** a snapshot whose blobs carry an older `state_schema_version` is loaded
- **THEN** each blob is passed through the registered migration to the current version before reconstruction, and the replay proceeds against the migrated blobs

#### Scenario: A newer-schema snapshot fails closed

- **WHEN** a snapshot carries a `state_schema_version` greater than the installed package supports
- **THEN** the CLI refuses to load it, names both versions in the error, and exits `2`

### Requirement: Traced and replayed outcomes are diffed, and divergence is a distinct exit code

The CLI SHALL compare the re-run's `ActivationResult` against the loaded trace on the trace-comparable surface: activation status against the traced terminal event; the staged `TraceEvent` sequence, deterministically serialized, byte-for-byte against the traced events of the target attempt after applying a closed, documented normalization (a replayed call served from cache reports `beam_agents.cache_hit = true` and `beam_agents.billed = false` even where the original reached the provider); and each staged intent's `intent_id`, `tool_name`, `kind`, and `expires_at_ms` against the traced `INTENT_EMITTED` attributes. On any mismatch the CLI SHALL print a structured diff identifying the first diverging event and each differing field, and exit `1`. Outputs and the post-activation memory blob, which have no traced counterpart, SHALL be reported as sha256 digests and sizes, and MUST NOT be treated as divergence.

#### Scenario: A divergent re-run produces a diff and exit code 1

- **WHEN** a replayed activation stages an intent whose `intent_id` differs from the traced `INTENT_EMITTED` attribute at the same position
- **THEN** the CLI prints a diff naming the position and both `intent_id`s and exits `1`

#### Scenario: Cache-hit normalization does not report false divergence

- **WHEN** the original activation's `LLM_CALL` reached the provider (`cache_hit = false`, `billed = true`) and the replay serves the same call from the cache blob
- **THEN** the normalized comparison treats the two events as equal and no divergence is reported for the cache-hit and billed attributes

#### Scenario: Unrepresented fields are reported, not diffed

- **WHEN** a replay completes and the traced record carries no output payloads or memory contents
- **THEN** the CLI reports the replayed outputs and memory blob as digests and sizes without marking the run divergent
