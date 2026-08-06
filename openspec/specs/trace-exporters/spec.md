# trace-exporters Specification

## Purpose
TBD - created by archiving change add-trace-exporters. Update Purpose after archive.
## Requirements
### Requirement: An `otlp://` traces sink scheme

`AgentConfig.traces_to` SHALL accept `otlp://<host>:<port>` URIs, resolving to an OTLP/HTTP (protobuf) trace exporter that POSTs `ExportTraceServiceRequest` messages to the collector's `/v1/traces` endpoint. Optional query parameters SHALL tune the exporter — `tls` (https transport), `batch_size`, `flush_deadline_s`, `queue_batches`, `service_name` — each with a documented default. Validation of the URI SHALL run at `AgentConfig` construction, import-free; the OTLP proto dependency SHALL be imported only at sink resolution. The `otlp` scheme SHALL be rejected for `intents_to` and `errors_to` at validation time with an actionable error, because intents and errors are correctness-bearing streams that MUST NOT ride a lossy exporter.

#### Scenario: A malformed OTLP URI fails at construction

- **WHEN** an `AgentConfig` is constructed with `traces_to = "otlp://"` (no host) or an unparseable query parameter
- **THEN** construction raises a `ValueError` naming the field and the expected `otlp://<host>:<port>` grammar

#### Scenario: OTLP is refused for the intents and errors sinks

- **WHEN** an `AgentConfig` is constructed with `intents_to` or `errors_to` set to an `otlp://` URI
- **THEN** construction raises a `ValueError` stating that the OTLP exporter is best-effort and only valid for `traces_to`

#### Scenario: Validation does not import the OTLP dependency

- **WHEN** an `AgentConfig` with a valid `otlp://` traces sink is constructed in an environment without the `otlp` extra installed
- **THEN** construction succeeds, and only resolving the sink at pipeline expansion raises an actionable import error naming the extra

### Requirement: Non-blocking batched export

The OTLP export DoFn SHALL perform no network I/O in `process()`: elements are mapped to OTLP spans and accumulated into batches, and full batches are handed to a single background sender thread through a bounded queue without blocking. `finish_bundle()` SHALL enqueue the partial batch and wait for the queue to drain up to the configured flush deadline, then return regardless of drain completion. Export delivery SHALL be best-effort at-least-once; the `.traces` PCollection remains the lossless record.

#### Scenario: The element path performs no network I/O

- **WHEN** `process()` handles a trace event while the collector is unreachable
- **THEN** the call completes without any network operation on the calling thread, and the event's span is batched or dropped according to queue capacity

#### Scenario: A full queue drops rather than blocks

- **WHEN** a batch becomes ready while the bounded queue is at capacity
- **THEN** the batch is dropped, the dropped-span count increases by the batch's size, and `process()` returns without blocking

#### Scenario: Bundle completion is bounded by the flush deadline

- **WHEN** `finish_bundle()` runs while the sender cannot reach the collector
- **THEN** the call returns within the configured flush deadline, counting still-undelivered spans as dropped

### Requirement: Export failures never fail the pipeline

The exporter SHALL treat delivery failure as telemetry loss, never as element failure: a failed POST is retried with bounded backoff only within the flush deadline, then dropped and counted. A non-retryable HTTP status (4xx other than 429) SHALL drop immediately. The DoFn SHALL NOT raise for any delivery problem, and a bundle processed under a permanently unavailable collector SHALL commit normally.

#### Scenario: A dead collector does not fail bundles

- **WHEN** a pipeline runs with an `otlp://` traces sink whose collector refuses every connection
- **THEN** every bundle commits successfully, and the export-failure and dropped-span counters reflect the loss

#### Scenario: A client error is not retried

- **WHEN** the collector responds 400 to an export request
- **THEN** the batch is dropped without retry and the failure is counted

#### Scenario: Export counters are recorded on the Beam thread

- **WHEN** a bundle finishes after background sends have succeeded, failed, or dropped
- **THEN** `spans_exported`, `spans_dropped`, `export_failures`, and `batches_sent` are recorded as Beam metrics from `finish_bundle()`, not from the sender thread

### Requirement: Deterministic TraceEvent-to-OTLP mapping

The mapping from `TraceEvent` to OTLP span SHALL be a pure function of the event: `trace_id`, `span_id`, and `parent_span_id` pass through byte-for-byte at their native 16/8/8-byte widths; `start_ms`/`end_ms` convert to unix nanoseconds; every attribute maps to a string `KeyValue`; the span name is the lowercase event-type name; `ERROR` events carry OTLP error status; and the resource carries `service.name` (default `beam-agents`, overridable via the URI). Mapping the same event twice SHALL produce byte-identical serialized spans, so at-least-once export dedups exactly on `(trace_id, span_id)`.

#### Scenario: IDs survive the mapping unchanged

- **WHEN** a trace event with a 16-byte `trace_id` and 8-byte `span_id`/`parent_span_id` is mapped
- **THEN** the OTLP span carries those exact bytes in its `trace_id`, `span_id`, and `parent_span_id` fields

#### Scenario: Mapping is deterministic

- **WHEN** one trace event is mapped to an OTLP span twice, in separate processes
- **THEN** the serialized span bytes are identical

#### Scenario: An ERROR event maps to an error-status span

- **WHEN** a trace event with `event_type = ERROR` is mapped
- **THEN** the OTLP span's status code is `STATUS_CODE_ERROR` and the `beam_agents.reason` attribute is present on the span

### Requirement: The activation span is exported once

Because `ACTIVATION_START` and `ACTIVATION_END` share one span ID and OTLP names a span by `(trace_id, span_id)`, the exporter SHALL export `ACTIVATION_END` as the activation span and SHALL NOT export `ACTIVATION_START`. All other event types SHALL each export as their own span. The `.traces` stream itself SHALL be unaffected: both events remain on the PCollection for every other consumer.

#### Scenario: START is skipped, END is exported

- **WHEN** an activation's `ACTIVATION_START` and `ACTIVATION_END` events pass through the exporter
- **THEN** exactly one OTLP span with the activation's span ID is exported, carrying `ACTIVATION_END`'s status and kind attributes

#### Scenario: Non-activation events all export

- **WHEN** `LLM_CALL`, `TOOL_CALL`, `INTENT_EMITTED`, `SUSPENDED`, and `ERROR` events pass through the exporter
- **THEN** each is exported as one OTLP span

### Requirement: A published BigQuery trace-table schema

The system SHALL publish the trace table's schema beside the row encoder: every key produced by `trace_event_to_row` SHALL appear in the schema with a matching type and mode, and the schema SHALL contain no field the encoder does not produce. Rows SHALL include `event_time`, a TIMESTAMP derived deterministically from `start_ms`, as the partition column.

#### Scenario: Schema and row encoder agree

- **WHEN** the schema's field set is compared with the keys of an encoded row
- **THEN** they are equal, and each field's declared type accepts the encoded value

#### Scenario: event_time derives from start_ms

- **WHEN** a trace event with a given `start_ms` is encoded twice
- **THEN** both rows carry the identical `event_time`, equal to `start_ms` interpreted as epoch milliseconds UTC

### Requirement: A self-provisioning BigQuery traces writer

A `bigquery://` traces sink SHALL write with the published schema, `CREATE_IF_NEEDED` create disposition, `WRITE_APPEND` write disposition, day partitioning on `event_time`, and clustering on `trace_id`, so that pointing `traces_to` at a dataset with no pre-existing table provisions a correctly shaped table.

#### Scenario: The writer carries schema and dispositions

- **WHEN** `traces_to` is a `bigquery://` URI and the sink is resolved
- **THEN** the resolved writer is configured with the published schema, `CREATE_IF_NEEDED`, `WRITE_APPEND`, and day partitioning on `event_time` with clustering on `trace_id`

#### Scenario: Rows still reach the writer encoded

- **WHEN** trace events flow to a resolved `bigquery://` sink
- **THEN** they arrive as encoded rows (hex identifiers, event-type name, sorted key/value attributes, `event_time`), not as proto messages
