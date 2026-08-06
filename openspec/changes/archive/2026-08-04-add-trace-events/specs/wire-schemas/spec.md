## MODIFIED Requirements

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
