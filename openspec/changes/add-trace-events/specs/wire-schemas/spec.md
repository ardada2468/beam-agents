## MODIFIED Requirements

### Requirement: ToolIntent carries deterministic identity and expiry
`ToolIntent` SHALL contain `intent_id` (string), `entity_key` (bytes), `seq` (int64), `step_index` (uint32), `tool_name` (string), `args_json` (string holding canonical JSON), `created_at_ms`, `expires_at_ms`, `attempt` (uint32), `kind` (the `Kind` enum), and `trace_id` (bytes, field 11). The schema SHALL NOT compute or default `intent_id`; it only transports the caller-computed uuid5 value. `trace_id` is likewise transported, not computed: it carries the emitting activation's 16-byte trace identifier so the effector can execute the intent inside the pipeline's trace. `trace_id` is additive under `state_schema_version = 1` — an intent written before the field existed decodes with `trace_id` empty, which readers treat as "no trace correlation available" rather than an error.

#### Scenario: All identity fields round-trip
- **WHEN** a `ToolIntent` with every field populated is serialized and parsed back
- **THEN** every field compares equal, including exact preservation of the `args_json` string and the 16 bytes of `trace_id`

#### Scenario: Expiry is representable for fail-closed enforcement
- **WHEN** an intent is created with `expires_at_ms` set
- **THEN** the parsed message exposes `expires_at_ms` so the effector can refuse expired intents without consulting external state

#### Scenario: An intent written without trace_id still decodes
- **WHEN** a golden `ToolIntent` blob predating the `trace_id` field is parsed with the current bindings
- **THEN** parsing succeeds, `trace_id` reads as empty bytes, and every other field compares equal to its expected value

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
