## MODIFIED Requirements

### Requirement: ToolIntent carries deterministic identity and expiry
`ToolIntent` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), `tool_name` (string), `args_json` (string holding canonical JSON), `created_at_ms`, `expires_at_ms`, `attempt` (uint32), and a `Kind` enum with values `TOOL_KIND_UNSPECIFIED = 0`, `TOOL`, `APPROVAL`. The schema SHALL NOT compute or default `intent_id`; it only transports the caller-computed uuid5 value. The `kind` field SHALL be additive under the existing `state_schema_version`: a reader SHALL treat `TOOL_KIND_UNSPECIFIED` and `TOOL` identically, so an intent written before `kind` existed is read as a tool call, and only an explicit `APPROVAL` SHALL be routed to a human approval channel rather than executed.

`ToolIntent` SHALL additionally carry a signature envelope as three additive fields taking the next free field numbers, above every existing field: `signature_scheme` (an enum with `SIGNATURE_SCHEME_UNSPECIFIED = 0` and `HMAC_SHA256 = 1`), `signing_key_id` (string), and `signature` (bytes). Like `intent_id`, these fields are transported, never computed by the schema; the signature is defined over the deterministic serialization of the message with all three signature fields cleared. Any future `ToolIntent` field SHALL take a new field number above all existing ones (the project's additive-evolution rule), which keeps the cleared-fields signing input stable across schema skew: an older verifier preserves the newer field as an unknown field and re-serializes it after the known fields, matching the signer's sorted-by-number layout. An intent whose `signature_scheme` is unspecified and whose `signature` is empty SHALL read as unsigned. This is an additive change under `state_schema_version = 1`; no version bump.

#### Scenario: All identity fields round-trip
- **WHEN** a `ToolIntent` with every field populated is serialized and parsed back
- **THEN** every field compares equal, including exact preservation of the `args_json` string and the `kind` value

#### Scenario: Expiry is representable for fail-closed enforcement
- **WHEN** an intent is created with `expires_at_ms` set
- **THEN** the parsed message exposes `expires_at_ms` so the effector can refuse expired intents without consulting external state

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

### Requirement: Continuation persists framework-opaque resume state
`Continuation` SHALL contain `state_schema_version` (uint32, set to 1), `seq` (int64), `step_index` (uint32), repeated `pending_intent_ids` (string), `adapter` (string discriminator), `snapshot` (opaque bytes), `suspended_at_ms`, `deadline_ms`, and `escalations` (uint32, the number of timeout escalations already performed for this suspension). The runtime SHALL treat `snapshot` as opaque — no parsing, no schema. The `escalations` field SHALL be additive under the existing `state_schema_version`, defaulting to `0` for a blob written before it existed.

#### Scenario: Suspension state round-trips
- **WHEN** a `Continuation` with multiple `pending_intent_ids`, a non-empty `snapshot`, and a `deadline_ms` is serialized and parsed back
- **THEN** all fields compare equal and `snapshot` bytes are byte-identical

#### Scenario: Escalation count round-trips and defaults to zero
- **WHEN** a `Continuation` blob serialized without `escalations` is parsed, and a `Continuation` with `escalations` set is serialized and parsed back
- **THEN** the first reads `escalations == 0` and the second preserves its value exactly
