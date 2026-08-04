## Context

`add-trace-events` shipped a `.traces` stream whose events are already OTLP-shaped on purpose: 16-byte trace IDs, 8-byte span IDs (`observability/traces.py`), OTel GenAI attribute names, and deterministic identity so at-least-once emission dedups exactly. Delivery, though, stops at `observability/exporters.py`'s two encoders and `core/transform.py`'s `_WriteTraces`: Kafka/Pub/Sub get keyed deterministic bytes, and BigQuery gets `WriteToBigQuery(table=...)` with no schema, no create disposition, and no layout definition anywhere — a sink that only works against a table someone shaped by hand. There is no OTLP path at all, which is the delivery mode every actual trace backend (Jaeger, Tempo, Cloud Trace, any collector) speaks.

Constraints that shape everything here:

- **Telemetry must never be a source of unavailability.** A collector outage must not fail bundles, stall `process()`, or backpressure the pipeline. The durable, lossless record is the `.traces` PCollection; an exporter is a best-effort tap.
- **No blocking I/O on the element path.** The runtime's latency budget (p50 < 15 ms overhead) has no room for a network round-trip per trace event.
- **No global mutable state; no OTel SDK global tracer.** The repo bans globals outside documented worker-local singletons, and the OTel SDK is built around a process-global `TracerProvider` plus its own daemon threads.
- **Offline unit lane.** Core install and default `pytest` run must not require any OTLP dependency; heavy imports stay lazy behind `resolve()`, mirroring how Kafka/Pub/Sub/BigQuery IO is already handled.
- **Beam metric updates off the Beam thread are silently discarded** (documented in `observability/metrics.py`) — export counts accumulated on a sender thread must be recorded from the Beam thread.

## Goals / Non-Goals

**Goals:**

- An `otlp://` scheme for `AgentConfig.traces_to` that ships trace events to an OTLP/HTTP collector.
- A batched export DoFn whose `process()` does zero network I/O and whose failure mode is drop-and-count, never raise.
- A deterministic, documented `TraceEvent` → OTLP span mapping that preserves the stream's native IDs.
- A published BigQuery table schema for trace rows, and a `bigquery://` sink that creates and shapes its own table (partitioned, clustered).
- Export observability: exported/dropped/failed counts as Beam metrics.

**Non-Goals:**

- OTLP/gRPC transport. One transport (OTLP/HTTP protobuf) is enough for every mainstream collector; gRPC would add `grpcio` and a second failure surface.
- Guaranteed trace delivery to OTLP. Lossless retention is what the Kafka/Pub/Sub/BigQuery sinks are for; the spec says so explicitly.
- Metrics or logs export (OTLP signals other than traces).
- Sampling. Unchanged from `add-trace-events`: a Non-Goal until volume proves it necessary.
- Exporting from anywhere but the `.traces` sink path — no tracing of the effector or the DoFn internals beyond what `.traces` already carries.

## Decisions

### D1. OTLP/HTTP protobuf over `httpx` + `opentelemetry-proto`; no OTel SDK

The exporter builds `ExportTraceServiceRequest` messages from `opentelemetry-proto`'s generated classes and POSTs them (`content-type: application/x-protobuf`) to the collector's `/v1/traces`. Alternatives considered:

- **OTel SDK (`opentelemetry-sdk` + `opentelemetry-exporter-otlp-proto-http`)**: brings a global `TracerProvider`, its own `BatchSpanProcessor` daemon thread with atexit hooks, and an API designed for *generating* spans, not forwarding pre-built ones. We would fight it to inject our own IDs (SDK spans mint their own) and violate the no-globals rule. Rejected.
- **OTLP/gRPC**: adds `grpcio` (a heavy native dependency the runtime otherwise avoids at run time) for no reach advantage — every collector speaks OTLP/HTTP. Rejected.
- **Hand-rolled JSON encoding of OTLP**: avoids the proto dependency but re-implements a wire format the `opentelemetry-proto` package already generates correctly, in a protobuf-native repo. Rejected.

`opentelemetry-proto` is pure generated code (protobuf only, no SDK, no transitive grpc) and ships as the optional `otlp` extra. `httpx` is already a core dependency and the client lives on the DoFn instance — worker-local, no globals.

### D2. Batch in `process()`, send from one background thread, flush in `finish_bundle()`

The DoFn:

- `setup()`: create the `httpx.Client` and start one daemon sender thread consuming a bounded `queue.Queue` of encoded batches.
- `process()`: map the event to an OTLP span, append to the current batch; when the batch reaches `batch_size`, enqueue it with `put_nowait`. **No network I/O, no lock contention beyond the queue.** If the queue is full, the batch is dropped and counted — overflow means the collector is slower than the pipeline, and blocking here would convert telemetry lag into pipeline backpressure.
- `finish_bundle()`: enqueue the partial batch, then wait for the queue to drain up to `flush_deadline_s`. A drain that times out counts the still-queued spans as dropped (they may still send — the count is conservative) and returns. Then record the tallied counters via Beam metrics **from this Beam-thread call**, per the discarded-off-thread-updates rule.
- `teardown()`: stop the sender (sentinel), close the client.

Why one thread and not async-over-the-bridge: the bridge belongs to activations inside `_AgentDoFn`; this DoFn is a separate downstream transform with no bridge, and one thread with a bounded queue is the smallest thing that makes `process()` non-blocking. Why flush per bundle at all: bundle completion is the only externally visible progress marker; flushing there bounds how much telemetry is in memory and gives the at-least-once story a clean shape (a retried bundle re-sends its spans; deterministic IDs make that a collector-side dedup, not duplication).

### D3. Failure policy: drop and count, never raise, bounded retry inside the deadline

The sender retries a failed POST with short exponential backoff only while the bundle's flush deadline allows, then drops the batch and increments `export_failures` and `spans_dropped`. HTTP 4xx (other than 429) drops immediately — a malformed-request loop would never succeed. No retry state survives the bundle. The DoFn never raises for delivery problems; the only exceptions that propagate are programming errors (mapping bugs), which should fail loudly.

Alternative — fail the bundle so Beam retries delivery — rejected: it makes the collector a pipeline availability dependency, exactly the inversion the Context forbids, and bundle retry would also re-run the activation-side transforms' committed work distribution for nothing.

### D4. Mapping: one event, one span; `ACTIVATION_END` elects the activation span

Each `TraceEvent` maps to one OTLP span: IDs pass through byte-for-byte, `start_ms`/`end_ms` × 10⁶ become `start_time_unix_nano`/`end_time_unix_nano`, `attributes` become string `KeyValue`s, span `name` is the lowercase event-type name (`llm_call`, `tool_call`, …), and `ERROR` events get `status.code = STATUS_CODE_ERROR`. Resource attributes: `service.name = beam-agents` (overridable via URI query param).

`ACTIVATION_START` and `ACTIVATION_END` deliberately share one span ID (they bracket one activation attempt), but OTLP treats `(trace_id, span_id)` as naming a single span — exporting both would send the same span twice with different attributes. `ACTIVATION_END` is strictly more informative (it carries `activation.status` and the same `activation.kind`), and every committed activation emits exactly one (`completed` or `suspended`; failed activations commit nothing except the synthesized `ERROR` event, which has its own span). So: export `ACTIVATION_END` as the activation span, skip `ACTIVATION_START`. The event itself stays on `.traces` untouched — this is an export-mapping rule, not a stream change.

Alternative — pair START/END in the DoFn and merge — rejected: pairing needs cross-element state for something derivable from END alone.

### D5. URI grammar: `otlp://<host>:<port>[?opts]`, valid for `traces_to` only

`otlp://collector:4318` → `http://collector:4318/v1/traces`. Query params (all optional, all with defaults): `tls=true` to switch to https, `batch_size` (default 512 spans), `flush_deadline_s` (default 5), `queue_batches` (default 8), `service_name`. Parsing and validation happen in `DefaultSinkResolver.validate` at `AgentConfig` construction, import-free, like the existing schemes; `opentelemetry-proto` is imported only inside `resolve()`.

`validate` rejects `otlp` for `intents_to` and `errors_to` with an actionable message: intents are a correctness-bearing outbox and errors are a dead letter — neither may ride a lossy exporter. The resolver's existing `field_name` parameter exists for exactly this kind of special-casing.

### D6. BigQuery: schema published beside the row encoder; writer configured for self-service

`observability/exporters.py` gains `TRACE_TABLE_SCHEMA` (the `WriteToBigQuery`-compatible dict form) defined next to `trace_event_to_row` so the pair can be asserted equal in one test: every row key appears in the schema with the matching type and mode, and vice versa. The row gains `event_time` — an RFC 3339 UTC timestamp derived from `start_ms` — because BigQuery cannot column-partition on an INT64 epoch-millis field and ingestion-time partitioning would decouple partition from event semantics under replay/backfill.

The `bigquery://` traces sink becomes a writer configured with: `schema=TRACE_TABLE_SCHEMA`, `CREATE_IF_NEEDED`, `WRITE_APPEND`, day partitioning on `event_time`, clustering on `trace_id` (the documented join/dedup key). `additional_bq_parameters` carries the partitioning/clustering, applied at table creation; for a pre-existing table they are inert. Insert method stays Beam's default (streaming inserts for unbounded input) — choosing Storage Write API is left as an operator concern via pipeline options, not baked in.

Existing hand-made tables: additive — they need the one nullable `event_time` column added; rows without it never existed since the writer previously could not have worked without exactly matching guesswork. Called out in proposal Impact.

### D7. Counter surface joins `beam_agents.runtime` metrics conventions

New counters in the exporter's own namespace (`beam_agents.otlp`): `spans_exported`, `spans_dropped`, `export_failures`, `batches_sent`. Tallied in plain ints on the DoFn/sender, recorded via `Metrics.counter` only from `finish_bundle` on the Beam thread. These are attempted values like every other runner metric here; the trace stream remains the authoritative record.

## Risks / Trade-offs

- **[Dropped telemetry under collector outage]** → By design and by spec: the lossy contract is stated in the `trace-exporters` spec, counted in `spans_dropped`, and the lossless alternatives (Kafka/Pub/Sub/BigQuery sinks) are one URI away. An operator who needs both fan-out and OTLP can consume `.traces` themselves.
- **[Conservative drop counting: a drain timeout counts spans that may still deliver]** → Accepted; the count answers "can I trust this backend's completeness" and over-reporting drops is the safe direction. Exact accounting would need sender-side acknowledgment bookkeeping that outlives the bundle.
- **[`ACTIVATION_START` invisible to OTLP backends]** → Its only unique payload is the activation's beginning timestamp, which equals `ACTIVATION_END.start_ms` on the zero-width-span design anyway; nothing is lost today. If spans ever gain real width, this mapping decision must be revisited — noted in the spec scenario.
- **[Per-bundle flush deadline adds up to `flush_deadline_s` to bundle completion under a dead collector]** → Bounded and configurable; with the default 5 s and Beam's bundle sizing this is a visible-but-safe worst case, and the queue drops rather than accumulates once full, so steady-state cost under a dead collector converges to fast `put_nowait` failures, not repeated deadline waits.
- **[`opentelemetry-proto` version drift against collectors]** → OTLP is backward-compatible by protobuf discipline and the trace signal is stable/frozen; pin a floor, not a ceiling.
- **[Duplicate spans on bundle retry]** → Inherent to at-least-once export; deterministic IDs mean a collector/backend that dedups on `(trace_id, span_id)` collapses them exactly — the same argument the trace stream itself makes.
- **[`event_time` widens the BigQuery row]** → Additive, derived, deterministic; the schema↔row agreement test pins it. Existing tables need one nullable column.

## Migration Plan

1. Land `observability/otlp.py` (mapping + DoFn + transform) with the `otlp` extra and offline tests; nothing is reachable until a URI uses the scheme.
2. Land the resolver arm (`validate`/`resolve` for `otlp`, rejection on non-traces fields).
3. Land the BigQuery schema + writer change. This is the only step with a deployed-surface effect: new tables are created shaped; pre-existing tables get `event_time` added (nullable, additive) before rollout or rows fail schema validation on insert.
4. Docs: URI grammar and the lossy-OTLP contract beside the existing sink documentation.

Rollback is per-step: reverting the resolver arm makes `otlp://` URIs fail validation loudly at construction; reverting the schema change reverts to the prior (broken-by-default) BigQuery behavior with no data loss — `event_time` in already-written rows is simply an extra column.

## Open Questions

- Should the OTLP path eventually expose collector-side batching hints (compression, `gzip`)? Deferred: `httpx` supports it in one line when a real deployment shows the need.
- Storage Write API as the default BigQuery method once Beam's support is uniformly stable across the supported runners — revisit when the runner matrix says so.
