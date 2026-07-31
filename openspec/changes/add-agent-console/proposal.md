## Why

The runtime emits a complete, deliberately-designed telemetry surface that nobody can look at.

`add-trace-events` gave every activation a deterministic `uuid5` trace identity and a `TraceEvent`
vocabulary carrying OTel GenAI semantic-convention attributes. `add-trace-exporters` gave those
events three encodings and four sink schemes. `add-runtime-metrics` published eleven counters and
nine distributions. `add-failure-context` gave a failed activation position scalars describing
where it died. `add-errors-sink-encoding` made `.errors` routable. Every one of those changes ends
at a *wire boundary*: bytes on a topic, rows in BigQuery, spans at a collector.

Seeing any of it requires standing up infrastructure the library does not ship. The
[docs](../../../docs/traces.md) tell an evaluator to provision a BigQuery dataset or run an OTel
collector before they can answer "what did my agent just do?". For the local `DirectRunner` loop
that [`docs/examples/hello-world.md`](../../../docs/examples/hello-world.md) opens with, there is
no answer at all — the `.traces` PCollection is discarded unless the caller writes their own
`beam.Map` to print protos.

The gap is widest exactly where adoption happens. A first-time user runs the hello-world example,
gets `b"..."` on stdout, and has no way to see the six trace events, the token counts, the tool
call, or the `ERROR` record that the run just produced. A user debugging a real failure has
`reason="activation_error"` and a `detail` string, while the `beam_agents.failure.step` /
`failure.last_event` / `failure.staged_intents` / `failure.llm_calls` scalars that
`add-failure-context` computed for precisely this purpose sit unread on a topic.

There is no HTTP surface anywhere in the tree — no FastAPI, no Starlette, no `http.server`, no
inbound endpoint of any kind — and no local store: nothing in the library retains a run after the
pipeline drains. `httpx` appears only as an outbound client.

## What Changes

- **A console service.** A new `beam_agents.console` package: a WAL SQLite store, a FastAPI read
  API, an SSE live feed, and a static-asset mount, started by a new `beam-agents-console`
  console_script. Self-contained — one process, one file, no broker and no cloud project.
- **Five ways in, covering the paths that already exist.** A native `console://host:port` sink
  scheme pushes `.traces`/`.errors`/`.snapshots` directly; an OTLP-compatible `POST /v1/traces`
  accepts what `WriteTracesToOtlp` already emits, so an existing `otlp://` user changes nothing but
  the host; a Kafka consumer reads an existing `traces_to="kafka://…"` topic; a BigQuery reader
  pulls an existing trace table through the published `TRACE_TABLE_SCHEMA`; and a bundle importer
  loads the varint-framed `TraceEvent` files and `StateSnapshot` blobs `beam-agents-replay`
  already consumes, so a captured run can be inspected with no pipeline running at all.
- **A UI that renders what the runtime actually records.** Activations, traces and span trees,
  errors grouped by the closed `reason` vocabulary, per-model token spend and cache-hit ratio,
  per-tool call volume and failure rate, the HITL approval queue, and per-entity-key timelines —
  every field sourced from `TraceEvent.attributes`, `ActivationErrorRecord`, or `StateSnapshot`,
  never invented.
- **A Docker distribution.** A multi-stage image and a compose stack that boots the console
  alongside a demo pipeline generating the full event vocabulary, so `docker compose up` lands on a
  populated console rather than an empty one.
- **Nothing in the hot path.** `console://` is added by a `ConsoleSinkResolver` that *wraps*
  `DefaultSinkResolver` and is opt-in through the existing `AgentConfig.sink_resolver` seam.
  `core/transform.py`, `core/dofn.py`, and `core/loop.py` are not modified.

## Capabilities

### New Capabilities
- `agent-console`: a self-contained local service that stores, queries, and serves the runtime's
  trace, error, and snapshot records over an HTTP API, an SSE live feed, and a bundled UI.
- `console-ingest`: five delivery paths into that store — a `console://` Beam sink, an
  OTLP-compatible endpoint, a Kafka/Redpanda consumer, a BigQuery reader, and a replay-bundle
  importer — each preserving the runtime's at-least-once, byte-identical-under-replay contract.
- `console-ui`: the browser surface over that API — activations, traces, errors, models, tools,
  approvals, and entity timelines.

## Impact

- **New code only.** `src/beam_agents/console/` (public surface: `ConsoleStore`,
  `ConsoleSinkResolver`, `WriteToConsole`, `create_app`, `serve` — everything else underscore-
  private and outside the frozen surface), `frontend/` (the repo's first JS/TS), `tests/console/`,
  `docker/console.Dockerfile`, `docker/compose.console.yaml`, `examples/console_demo/`,
  `docs/console.md`.
- **No existing module is modified.** No proto edit, no `state_schema_version` implication, no
  change to `AgentConfig`'s fields, no new required dependency. `import beam_agents` keeps working
  with no extras installed: the console's dependencies live behind a `console` extra and a
  `console-ingest` extra, and every optional client is imported inside the function that needs it.
- **Post-1.0 API addition.** `beam_agents.console` adds five names to `public-surface.toml` and
  five contract entries to `docs/api.md`. Additive under the deprecation policy — nothing frozen
  is renamed or removed.
- **Zero-width spans are rendered honestly.** `add-trace-events` D7 makes every span
  `start_ms == end_ms`, because measuring elapsed time would need a wall-clock read in the hot
  path. The UI therefore draws a waterfall of *sequence*, not duration, and sources real durations
  from the `ActivationTally` numbers that reach it as attributes. Scaling bars from zero-width
  spans would fabricate the one quantity the runtime deliberately declines to measure.
- **At-least-once is the store's problem, not the pipeline's.** A retried bundle re-emits
  byte-identical events; the store deduplicates on `(trace_id, span_id, event_type)`, the same key
  `docs/traces.md` already tells downstream consumers to use.
- **Rejected alternative: ship a Grafana/Jaeger compose stack instead.** `otlp://` already reaches
  Jaeger, and the change would be a docs page. It was rejected because the OTLP encoding is lossy
  by contract — `ACTIVATION_START` is not exported at all, `.errors` and `.snapshots` have no OTLP
  representation, and `entity_key`/`seq` are span attributes rather than first-class dimensions.
  A generic trace viewer cannot show an activation list keyed by `(entity_key, seq)`, cannot group
  by the `reason` vocabulary, and cannot render a suspend→resume cycle as one trace. The console
  is worth building precisely because it knows this runtime's shape.
- **Rejected alternative: reuse the effector's service skeleton.** `EffectorService` is a
  poll-execute-commit loop over a dedup store with no inbound listener; a read-heavy HTTP service
  over a query store shares no structure with it beyond both being processes.
- **Tests:** `tests/console/` mirrors the package layout. The store, decoders, query layer, API,
  and sink are driven offline with real SQLite files, real proto bytes, and `httpx`'s ASGI
  transport. The Kafka and BigQuery sources are driven by fakes offline and marked `integration`
  against the existing `redpanda` compose service.
- **Gates:** `openspec validate add-agent-console --strict`; `make lint`/`make type` over the new
  package (mypy `--strict` applies); `make coverage-ratchet` — a large new package lands below the
  0.9164 branch-rate baseline unless its tests carry it, which is the point of writing them first.
  `core/` is untouched, so the mutation gate does not select.
