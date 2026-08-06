# wire-schemas Specification

## Purpose
TBD - created by archiving change add-wire-schemas-and-coders. Update Purpose after archive.
## Requirements
### Requirement: Proto package and committed generation
The project SHALL define all wire and state message schemas in `protos/beam_agents.proto` with `syntax = "proto3"` and package `beam_agents.v1`. Generated Python bindings SHALL be committed under `src/beam_agents/_protos/` and regenerating them via `scripts/gen_proto.sh` MUST produce no diff against the committed files.

#### Scenario: Regeneration is diff-clean
- **WHEN** `scripts/gen_proto.sh` is run on a clean checkout
- **THEN** `git diff --exit-code` over `src/beam_agents/_protos/` reports no changes

#### Scenario: Bindings are importable from the installed package
- **WHEN** a test imports the message classes from `beam_agents._protos`
- **THEN** all eight top-level message classes (`MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `Continuation`, `LlmCacheBlob`, `ActivationErrorRecord`) are available without any `sys.path` manipulation

### Requirement: MemoryBlob carries versioned, LRU-orderable working memory
`MemoryBlob` SHALL contain a `state_schema_version` field (uint32, set to 1 for this schema), a repeated `MemoryEntry` field where each entry has a string `key`, bytes `value`, and int64 `last_access_ms`, and a `total_value_bytes` field caching the sum of entry value sizes. Entry order SHALL be preserved exactly as written so LRU ordering is representable without a map field.

#### Scenario: Entries round-trip in insertion order
- **WHEN** a `MemoryBlob` is built with entries `["a", "b", "c"]` in that order, serialized, and parsed back
- **THEN** the parsed entries appear in the order `["a", "b", "c"]` with identical keys, values, and `last_access_ms`

#### Scenario: Schema version defaults are explicit
- **WHEN** a new `MemoryBlob` is constructed by runtime code
- **THEN** `state_schema_version` is set to 1, and a parsed blob with `state_schema_version` 0 is distinguishable as pre-versioned/uninitialized

### Requirement: ToolIntent carries deterministic identity and expiry
`ToolIntent` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), `tool_name` (string), `args_json` (string holding canonical JSON), `created_at_ms`, `expires_at_ms`, `attempt` (uint32), `kind` (a `Kind` enum with values `TOOL_KIND_UNSPECIFIED = 0`, `TOOL`, `APPROVAL`), and `trace_id` (bytes, field 11). The schema SHALL NOT compute or default `intent_id`; it only transports the caller-computed uuid5 value. `trace_id` is likewise transported, not computed: it carries the emitting activation's 16-byte trace identifier so the effector can execute the intent inside the pipeline's trace. `trace_id` is additive under `state_schema_version = 1` — an intent written before the field existed decodes with `trace_id` empty, which readers treat as "no trace correlation available" rather than an error. The `kind` field SHALL be additive under the existing `state_schema_version`: a reader SHALL treat `TOOL_KIND_UNSPECIFIED` and `TOOL` identically, so an intent written before `kind` existed is read as a tool call, and only an explicit `APPROVAL` SHALL be routed to a human approval channel rather than executed.

`ToolIntent` SHALL additionally carry a signature envelope as three additive fields taking the next free field numbers, above every existing field: `signature_scheme` (an enum with `SIGNATURE_SCHEME_UNSPECIFIED = 0` and `HMAC_SHA256 = 1`), `signing_key_id` (string), and `signature` (bytes). Like `intent_id`, these fields are transported, never computed by the schema; the signature is defined over the deterministic serialization of the message with all three signature fields cleared. Any future `ToolIntent` field SHALL take a new field number above all existing ones (the project's additive-evolution rule), which keeps the cleared-fields signing input stable across schema skew: an older verifier preserves the newer field as an unknown field and re-serializes it after the known fields, matching the signer's sorted-by-number layout. An intent whose `signature_scheme` is unspecified and whose `signature` is empty SHALL read as unsigned. This is an additive change under `state_schema_version = 1`; no version bump.

#### Scenario: All identity fields round-trip
- **WHEN** a `ToolIntent` with every field populated is serialized and parsed back
- **THEN** every field compares equal, including exact preservation of the `args_json` string, the `kind` value, and the 16 bytes of `trace_id`

#### Scenario: Expiry is representable for fail-closed enforcement
- **WHEN** an intent is created with `expires_at_ms` set
- **THEN** the parsed message exposes `expires_at_ms` so the effector can refuse expired intents without consulting external state

#### Scenario: An intent written without trace_id still decodes
- **WHEN** a golden `ToolIntent` blob predating the `trace_id` field is parsed with the current bindings
- **THEN** parsing succeeds, `trace_id` reads as empty bytes, and every other field compares equal to its expected value

#### Scenario: Approval intents are distinguishable on the wire
- **WHEN** an intent created for a human approval request is serialized and parsed back
- **THEN** its `kind` is `APPROVAL`, distinguishable from a `TOOL` intent without inspecting `tool_name`

#### Scenario: An intent written before kind existed reads as a tool call
- **WHEN** a `ToolIntent` blob serialized without the `kind` field is parsed
- **THEN** `kind` reads as `TOOL_KIND_UNSPECIFIED` and is treated as a tool call, not as an approval request

#### Scenario: Signature fields round-trip
- **WHEN** a `ToolIntent` carrying `signature_scheme = HMAC_SHA256`, a non-empty `signing_key_id`, and 32 signature bytes is serialized and parsed back
- **THEN** the scheme, key id, and exact signature bytes compare equal

#### Scenario: A pre-signature intent decodes as unsigned
- **WHEN** a blob serialized before the signature fields existed is parsed with the current bindings
- **THEN** parsing succeeds, `signature_scheme` reads as `SIGNATURE_SCHEME_UNSPECIFIED`, `signature` is empty, and the intent is treated as unsigned

#### Scenario: A future-field intent still yields a stable signing input
- **WHEN** a blob carrying an unknown field with a number above the signature fields is parsed, its signature fields cleared, and the message re-serialized deterministically
- **THEN** the re-encoded bytes match the signer's cleared-fields serialization, so verification succeeds across the schema skew

### Requirement: ToolResult correlates outcomes with terminal statuses
`ToolResult` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), a `Status` enum with values `STATUS_UNSPECIFIED = 0`, `OK`, `ERROR`, `EXPIRED`, `REJECTED`, a `payload` (bytes), `error_message` (string), and `completed_at_ms`.

#### Scenario: Every status value is representable
- **WHEN** a `ToolResult` is constructed with each declared status value in turn and round-tripped
- **THEN** each parses back to the same status, and an unset status reads as `STATUS_UNSPECIFIED`

### Requirement: TraceEvent aligns with OTel GenAI conventions
`TraceEvent` SHALL contain `trace_id`, `span_id`, `parent_span_id` (bytes), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), an `EventType` enum (`EVENT_TYPE_UNSPECIFIED = 0`, `ACTIVATION_START`, `LLM_CALL`, `TOOL_CALL`, `INTENT_EMITTED`, `ACTIVATION_END`, `ERROR`, `SUSPENDED = 7`), a `map<string, string> attributes` field for OTel GenAI semantic-convention attributes, and `start_ms`/`end_ms`. `trace_id` SHALL be 16 bytes and `span_id`/`parent_span_id` 8 bytes, matching the W3C trace-context and OTel wire formats. `SUSPENDED` is additive under `state_schema_version = 1`: a reader that predates it decodes the value as an unrecognized enum number rather than failing.

#### Scenario: GenAI attributes survive round-trip
- **WHEN** a `TraceEvent` carrying attributes such as `gen_ai.request.model` and `gen_ai.usage.input_tokens` is serialized and parsed back
- **THEN** all attribute keys and values compare equal

#### Scenario: Correlation identifiers round-trip at their wire widths
- **WHEN** a `TraceEvent` is built with a 16-byte `trace_id` and 8-byte `span_id` and `parent_span_id`, serialized, and parsed back
- **THEN** all three compare equal byte-for-byte at their original widths

#### Scenario: The suspension event type round-trips
- **WHEN** a `TraceEvent` with `event_type = SUSPENDED` is serialized and parsed back
- **THEN** the parsed event reports `SUSPENDED`, and the same bytes parsed by bindings that predate the value expose it as an unrecognized enum number without a parse error

### Requirement: Continuation persists framework-opaque resume state
`Continuation` SHALL contain `state_schema_version` (uint32, set to 1), `seq` (int64), `step_index` (uint32), repeated `pending_intent_ids` (string), `adapter` (string discriminator), `snapshot` (opaque bytes), `suspended_at_ms`, `deadline_ms`, and `escalations` (uint32, the number of timeout escalations already performed for this suspension). The runtime SHALL treat `snapshot` as opaque — no parsing, no schema. The `escalations` field SHALL be additive under the existing `state_schema_version`, defaulting to `0` for a blob written before it existed.

#### Scenario: Suspension state round-trips
- **WHEN** a `Continuation` with multiple `pending_intent_ids`, a non-empty `snapshot`, and a `deadline_ms` is serialized and parsed back
- **THEN** all fields compare equal and `snapshot` bytes are byte-identical

#### Scenario: Escalation count round-trips and defaults to zero
- **WHEN** a `Continuation` blob serialized without `escalations` is parsed, and a `Continuation` with `escalations` set is serialized and parsed back
- **THEN** the first reads `escalations == 0` and the second preserves its value exactly

### Requirement: LlmCacheBlob carries versioned, LRU-orderable replay-cache entries
`LlmCacheBlob` SHALL contain a `state_schema_version` field (uint32, set to 1 for this schema), a repeated `LlmCacheEntry` field where each entry has a string `cache_key`, bytes `response`, bytes `response_digest`, int64 `created_at_ms`, int64 `last_access_ms`, and bool `digest_only`, and a `total_response_bytes` field caching the sum of stored response sizes. Entry order SHALL be preserved exactly as written so LRU ordering is representable without a map field.

#### Scenario: Cache entries round-trip in insertion order
- **WHEN** an `LlmCacheBlob` is built with entries keyed `["a", "b", "c"]` in that order, serialized, and parsed back
- **THEN** the parsed entries appear in the order `["a", "b", "c"]` with identical field values for every entry field

#### Scenario: Digest-only entries are representable
- **WHEN** an entry is written with `digest_only` `True`, an empty `response`, and a 32-byte `response_digest`, then round-tripped
- **THEN** the parsed entry preserves `digest_only` `True`, empty `response` bytes, and the exact digest bytes

### Requirement: Schema evolution is additive and golden-blob guarded
Schema changes SHALL be additive by default (new fields with new numbers; removals use `reserved` statements). A breaking change is permitted ONLY for the three versioned keyed-state messages (`MemoryBlob`, `Continuation`, `LlmCacheBlob`) and ONLY through a `state_schema_version` bump accompanied by a registered migration function for every versioned message and a frozen golden corpus entry for the outgoing version (see `state-migration`); the unversioned wire messages (`ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope`, `ActivationErrorRecord`) remain additive-only with no bump escape hatch. Every schema change, breaking or not, MUST keep previously written bytes parseable by the current descriptor: an existing field number SHALL never be retyped or reused, because migration operates on decoded messages and runs only after a successful parse. Golden serialized blobs SHALL be committed per schema version under `tests/core/golden/v<N>/`, and a compat test SHALL decode every golden blob with the current bindings, migrate historical versions to current, and assert field-level equality against expected values. Golden tests SHALL NOT require byte-identical re-encoding.

#### Scenario: Golden blobs decode with current bindings
- **WHEN** the compat test parses each committed golden blob under `tests/core/golden/v<N>/`
- **THEN** every blob decodes without error and, after migration to the current version, all expected field values compare equal

#### Scenario: Golden corpus is laid out per version
- **WHEN** the compat suite collects its fixtures
- **THEN** every committed blob lives under a `v<N>` directory matching the `state_schema_version` regime its builders were frozen at, and no fixture sits outside a version directory

#### Scenario: Unknown fields are tolerated on decode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings
- **THEN** parsing succeeds and all known fields compare equal, demonstrating forward-compatible decode

#### Scenario: Unknown fields survive re-encode
- **WHEN** a blob containing an unknown (future) field number is parsed with the current bindings and then re-serialized
- **THEN** the re-encoded bytes still contain the unknown field's data, so an older reader rewriting state during pipeline `--update` never drops fields written by a newer schema

### Requirement: ActivationErrorRecord carries dead-letter triage fields
`ActivationErrorRecord` SHALL be a top-level message in `protos/beam_agents.proto` containing `entity_key` (bytes), `reason` (string), `detail` (string), and `event_time_ms` (int64). It is the wire form of a `.errors` dead letter; when published to a message-bus errors sink it SHALL travel inside `AgentEnvelope.external_event` as deterministically serialized bytes. The `AgentEnvelope` schema itself SHALL NOT change: `external_event` remains opaque bytes, and carrying an `ActivationErrorRecord` there is a documented convention, not a schema constraint.

#### Scenario: All fields round-trip
- **WHEN** an `ActivationErrorRecord` with every field populated is serialized and parsed back
- **THEN** `entity_key`, `reason`, `detail`, and `event_time_ms` all compare equal

#### Scenario: Deterministic serialization is byte-stable
- **WHEN** the same `ActivationErrorRecord` is serialized twice with deterministic serialization
- **THEN** both byte strings are identical

### Requirement: AgentEnvelope is the single keyed input type with four payload variants
`AgentEnvelope` SHALL contain `entity_key` (bytes), `event_time_ms` (int64), and a `oneof payload` with exactly four variants: `external_event` (opaque bytes), `tool_result` (`ToolResult`), `approval` (a nested `Approval` message with `intent_id`, `approved` bool, `approver` string, `decided_at_ms`), and `export_request` (a nested `StateExportRequest` message carrying a `request_id` string). The runtime SHALL NOT impose any schema on `external_event` bytes. `export_request` is additive under `state_schema_version = 1`: an envelope written before the variant existed decodes unchanged, and a reader that predates it sees the new variant as an unknown field rather than a parse failure.

#### Scenario: Exactly one payload variant is set

- **WHEN** an `AgentEnvelope` is constructed with a `tool_result` payload and then assigned an `approval` payload
- **THEN** the `oneof` reports only `approval` as set and the `tool_result` field is cleared

#### Scenario: All four variants round-trip

- **WHEN** one envelope of each payload variant is serialized and parsed back
- **THEN** each parsed envelope reports the correct `oneof` case with field-equal contents

#### Scenario: An envelope written before export_request still decodes

- **WHEN** a golden `AgentEnvelope` blob predating the `export_request` variant is parsed with the current bindings
- **THEN** parsing succeeds and every field compares equal to its expected value

### Requirement: StateSnapshot carries a versioned per-key state image

`StateSnapshot` SHALL contain `state_schema_version` (uint32, set to 1), `entity_key` (bytes), `seq` (int64, the key's `SEQ` counter at export), `snapshot_at_ms` (int64, the export request's `event_time_ms`), `memory` (`MemoryBlob`), `llm_cache` (`LlmCacheBlob`), `continuation` (`Continuation`, present only when the key is suspended), repeated `pending` (`ToolIntent`), and `request_id` (string, echoed from the originating `StateExportRequest`). Embedded blobs SHALL be carried verbatim as committed — the snapshot SHALL NOT re-version, migrate, or reorder their contents at export time.

#### Scenario: A populated snapshot round-trips

- **WHEN** a `StateSnapshot` with a populated memory blob, cache blob, continuation, and two pending intents is serialized deterministically and parsed back
- **THEN** every field compares equal, including entry order within the embedded blobs

#### Scenario: Embedded blobs keep their own schema versions

- **WHEN** a snapshot embeds a `MemoryBlob` whose `state_schema_version` differs from the snapshot envelope's
- **THEN** the round-tripped blob reports its original version unchanged, leaving migration to the loader
