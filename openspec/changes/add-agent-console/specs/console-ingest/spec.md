## ADDED Requirements

### Requirement: A `console://` sink delivers traces, errors, and snapshots

`ConsoleSinkResolver` SHALL implement the runtime's `SinkResolver` protocol, resolve the
`console://host[:port]` scheme, and delegate every other scheme to the default resolver unchanged,
so adopting it never removes an existing sink. It SHALL accept `console://` for `traces_to`,
`errors_to`, and `snapshots_to`. Its `validate` SHALL remain import-free.

The runtime's own modules SHALL NOT be modified to add this scheme.

#### Scenario: A console URI is accepted on all three outputs

- **WHEN** a config sets `traces_to`, `errors_to`, and `snapshots_to` to `console://` URIs with the
  console resolver installed
- **THEN** validation passes and each output resolves to a transform delivering that record type

#### Scenario: Other schemes are unaffected

- **WHEN** a config using the console resolver sets a sink to a Kafka, Pub/Sub, BigQuery, or OTLP
  URI
- **THEN** the sink resolves exactly as it would under the default resolver

#### Scenario: A malformed console URI is rejected before the pipeline runs

- **WHEN** a config sets a sink to a `console://` URI with no host
- **THEN** validation fails with an error naming the URI, without importing any HTTP client

### Requirement: The console sink never fails or slows an activation

`WriteToConsole` SHALL batch records, hand them to a background sender, and drop-and-count when the
console is unreachable, slow, or absent — never raising, never retrying indefinitely, and never
applying backpressure to the pipeline. It SHALL publish counters for records exported, records
dropped, export failures, and batches sent, under a namespace distinct from the runtime's own.

Unlike the OTLP exporter, it SHALL transmit `ACTIVATION_START` events, because the native record
carries `event_type` as a first-class field and the start event distinguishes a fresh attempt from
a resume.

#### Scenario: An unreachable console does not fail the pipeline

- **WHEN** a pipeline writes to a `console://` sink whose endpoint refuses every connection
- **THEN** the pipeline completes successfully, and the dropped-record and export-failure counters
  are non-zero

#### Scenario: Start events are delivered

- **WHEN** an activation's events are written through the console sink
- **THEN** the `ACTIVATION_START` event is among the records the console receives

### Requirement: The console accepts records over HTTP

The console SHALL accept records at native endpoints for traces, errors, and snapshots, and SHALL
additionally accept OTLP trace export requests at the path an OTLP exporter posts to, so a pipeline
already configured with an `otlp://` sink can target the console by changing only the host.

Records arriving by any endpoint SHALL be normalized through the same path as records arriving by
any other source, so no field is interpreted differently by delivery route.

#### Scenario: An existing OTLP exporter reaches the console unchanged

- **WHEN** a pipeline configured with an `otlp://` traces sink is pointed at the console's host
- **THEN** its spans are stored and appear as activations and trace events in the API

#### Scenario: OTLP's known loss is reported, not hidden

- **WHEN** an activation's records arrive only over OTLP, which does not carry `ACTIVATION_START`
- **THEN** the activation is stored, and it is marked as having incomplete provenance rather than
  being presented as a complete record

#### Scenario: A malformed payload is rejected without affecting stored records

- **WHEN** a payload that is not a valid record for its endpoint is posted
- **THEN** the request is rejected with a client error naming the problem, and no partial write
  occurs

### Requirement: The console reads an existing Kafka trace topic

The console SHALL be able to consume a topic that a pipeline already writes with a
`traces_to="kafka://…"` sink, decoding each message as a `TraceEvent` and storing it, without
requiring any change to that pipeline. It SHALL default to reading from the end of the topic, offer
reading from the beginning, and SHALL NOT commit offsets, so a restart never blocks on a consumer
group.

A message that fails to decode SHALL be counted and skipped, not fatal to the consumer.

#### Scenario: Traces on an existing topic appear in the console

- **WHEN** the console is started against a topic carrying trace events written by a running
  pipeline
- **THEN** those events are stored and visible through the API, with the pipeline unmodified

#### Scenario: An undecodable message does not stop the consumer

- **WHEN** a message that is not a valid `TraceEvent` appears on the topic
- **THEN** it is counted as a decode failure and skipped, and subsequent valid messages are stored

### Requirement: The console reads an existing BigQuery trace table

The console SHALL be able to read a trace table written by a `bigquery://` traces sink, reversing
the published row encoding — hexadecimal identifiers, the enum *name* for the event type, and
key/value attribute records — into `TraceEvent` records. It SHALL pull incrementally by event time
so repeated reads do not re-scan the whole table, and re-reading an overlapping window SHALL be
harmless because ingest is idempotent.

#### Scenario: Rows are reversed into the records they encoded

- **WHEN** rows written by the BigQuery trace encoder are read by the console
- **THEN** the resulting stored events carry the same identifiers, event types, timestamps, and
  attributes as the events that produced those rows

#### Scenario: Re-reading an overlapping window changes nothing

- **WHEN** a time window that has already been read is read again
- **THEN** the store's contents and every query result are unchanged

### Requirement: The console imports a captured replay bundle

The console SHALL import the files the replay CLI already consumes — a varint-length-delimited
`TraceEvent` stream and a serialized `StateSnapshot` — from a local path or an upload, reusing the
runtime's existing framing parser rather than reimplementing it. An imported run SHALL be
queryable with no pipeline running and no network access.

#### Scenario: A captured run is inspectable offline

- **WHEN** a trace stream file and a snapshot file produced for the replay CLI are imported
- **THEN** the run's activations, traces, spans, and errors are queryable through the API with no
  pipeline running

#### Scenario: A truncated stream reports what it read

- **WHEN** a trace stream file ends mid-record
- **THEN** the import reports how many records were read and that the stream was truncated, and the
  records that were read remain stored
