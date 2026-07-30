# Trace delivery

`RunAgent` exposes every trace event on its `.traces` tagged output — a
`PCollection[TraceEvent]` you can consume yourself — and `AgentConfig.traces_to`
ships it to a sink for you:

```python
config = AgentConfig(
    provider_factory=make_client,
    traces_to="otlp://collector:4318",  # or kafka://, pubsub://, bigquery://
)
```

| Scheme | What the sink receives | Delivery |
|---|---|---|
| `kafka://<brokers>/<topic>` | deterministic proto bytes, keyed by `entity_key` | lossless (at-least-once) |
| `pubsub://<project>/<topic>` | deterministic proto bytes, keyed by `entity_key` | lossless (at-least-once) |
| `bigquery://<project>/<dataset>/<table>` | flat rows (see layout below) | lossless (at-least-once) |
| `otlp://<host>[:<port>][?opts]` | OTLP/HTTP protobuf spans | **best-effort** |

Trace events carry deterministic identity (`trace_id`/`span_id` are pure
functions of activation scope), so at-least-once duplicates from bundle retries
collapse exactly under downstream dedup on `(trace_id, span_id, event_type)`.

## Consuming `.traces` downstream

A `kafka://`/`pubsub://` traces topic carries deterministic `TraceEvent` bytes,
so an ordinary Beam pipeline can consume it with nothing but the published
proto bindings — no runtime imports, no adapter.
[continuous_eval.md](continuous_eval.md) is a worked example: it joins exported
traces with lagging business outcomes in a deadline-bounded stateful DoFn,
scores each joined record with an LLM-as-judge through the `LLMClient` seam,
and emits per-scenario quality metrics. Its code is held verbatim by
`tests/examples/test_continuous_eval.py`, which runs it offline against bytes
from this page's own encoder.

## The OTLP exporter (`otlp://`)

Sends spans to any OTLP/HTTP collector (an OTel Collector, Jaeger, Tempo,
Cloud Trace via a collector, ...) at the standard `/v1/traces` endpoint.
Requires the `otlp` extra:

```bash
pip install 'beam-agents[otlp]'
```

URI options, all optional:

| Option | Default | Meaning |
|---|---|---|
| `tls=true` | `false` (http) | POST over https |
| `batch_size` | `512` | spans per export request |
| `flush_deadline_s` | `5` | max wait at each bundle boundary, and each batch's total retry budget |
| `queue_batches` | `8` | bounded hand-off queue between the pipeline and the sender thread |
| `service_name` | `beam-agents` | the OTLP resource's `service.name` |

The port defaults to `4318`; the URI carries no path (`/v1/traces` is implied).
Example: `otlp://collector:4318?tls=true&service_name=fraud-triage`.

### The delivery contract: lossy by design

OTLP export must never make telemetry a source of pipeline unavailability, so
its failure mode is **drop and count, never block, never raise**:

- The element path does no network I/O. Spans are batched and handed to one
  background sender thread through a bounded queue; a full queue (the collector
  is slower than the pipeline) drops the batch rather than backpressuring.
- A failed POST is retried with backoff only inside `flush_deadline_s`, then
  dropped. A non-retryable response (4xx other than 429) drops immediately.
- Each bundle boundary flushes, waiting at most `flush_deadline_s`; whatever
  cannot drain is dropped and counted. A dead collector costs dropped
  telemetry — bundles keep committing.

Loss is visible in Beam counters under the **`beam_agents.otlp`** namespace:
`spans_exported`, `spans_dropped`, `export_failures`, `batches_sent`. If you
need lossless trace retention, point `traces_to` at Kafka/Pub/Sub/BigQuery
instead (or alongside, by consuming `.traces` yourself).

### Mapping notes

- IDs pass through byte-for-byte: `TraceEvent` already uses OTel wire widths
  (16-byte trace, 8-byte span IDs).
- Span names are the lowercase event-type name (`llm_call`, `tool_call`,
  `intent_emitted`, `suspended`, `error`, `activation_end`); `ERROR` events get
  OTLP error status. Attributes map to string key/values.
- `ACTIVATION_START` is **not exported**. It shares its span ID with
  `ACTIVATION_END` (they bracket one activation attempt) and OTLP names a span
  by `(trace_id, span_id)`; `ACTIVATION_END` carries strictly more (the
  `activation.status` outcome alongside the same `activation.kind`), so it *is*
  the activation span. Both events remain on `.traces` for other consumers.
- Spans are zero-width by design (both timestamps come from the activation
  clock); latency lives in the `beam_agents.runtime` metrics (see
  `docs/metrics.md`), not in trace bytes.

## The BigQuery trace table (`bigquery://`)

A `bigquery://` traces sink provisions its own table: the writer carries the
published schema (`beam_agents.observability.exporters.TRACE_TABLE_SCHEMA`),
`CREATE_IF_NEEDED`/`WRITE_APPEND`, **day partitioning on `event_time`**, and
**clustering on `trace_id`** — pointing `traces_to` at an empty dataset just
works.

| Column | Type | Notes |
|---|---|---|
| `trace_id` | STRING | hex; one trace per `(entity_key, seq)`; the cluster key |
| `span_id` | STRING | hex |
| `parent_span_id` | STRING | hex; empty for the trace root |
| `entity_key` | STRING | hex |
| `seq` | INT64 | per-key activation counter |
| `step_index` | INT64 | |
| `event_type` | STRING | the enum name (`LLM_CALL`, `ERROR`, ...) |
| `start_ms` / `end_ms` | INT64 | epoch millis from the activation clock |
| `event_time` | TIMESTAMP | `start_ms` as RFC 3339 UTC; the partition column |
| `attributes` | REPEATED RECORD(`key` STRING, `value` STRING) | sorted by key |

Partitioning uses `event_time` (a derived TIMESTAMP) rather than ingestion
time so replays and backfills land in the partitions their events belong to.
A table created by hand before this writer existed needs the one nullable
`event_time` column added; everything else is unchanged.

Example — token spend by day, cache hits separated from billed calls:

```sql
SELECT DATE(event_time) AS day,
       (SELECT value FROM UNNEST(attributes) WHERE key = 'beam_agents.billed') AS billed,
       SUM(CAST((SELECT value FROM UNNEST(attributes)
                 WHERE key = 'gen_ai.usage.input_tokens') AS INT64)) AS input_tokens
FROM `my-project.my_dataset.traces`
WHERE event_type = 'LLM_CALL'
GROUP BY day, billed
```
