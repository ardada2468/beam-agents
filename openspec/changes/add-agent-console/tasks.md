## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Store: the same event ingested twice yields one row; a later copy carrying more
      attributes merges rather than being discarded; opening a non-existent path creates a usable
      schema. Must fail with "no such table", not with an assertion about row counts.
- [ ] 1.2 Rollups: a partial arrival reports in flight and counts only what arrived; the remaining
      events in any order converge on the all-at-once rollup; a suspend/resume under one
      `(entity_key, seq)` is one activation with two attempts.
- [ ] 1.3 Decoders: a varint-framed `TraceEvent` stream, an OTLP `ExportTraceServiceRequest`, an
      `ActivationErrorRecord`, an `AgentEnvelope`-wrapped error, and a `StateSnapshot` each decode
      to the records that produced them. Round-trip through the real encoders in
      `observability/exporters.py`, `observability/otlp.py`, and `replay/bundle.py` — never a
      hand-written fixture of what those are assumed to emit.
- [ ] 1.4 Queries: every filter narrows the activation list correctly and composes with the others;
      the cursor resumes the same total order across a page boundary with concurrent inserts;
      grouping by `reason` covers the closed vocabulary; time bucketing is stable at bucket edges.
- [ ] 1.5 API: every endpoint answers a well-formed empty result against an empty store; the
      liveness endpoint is healthy before any ingest. Driven through `httpx`'s ASGI transport, no
      live socket.
- [ ] 1.6 Ingest endpoints: a native post and an OTLP post of the same activation converge on the
      same stored records; an OTLP-only activation is marked incomplete-provenance; a malformed
      payload is a client error with no partial write.
- [ ] 1.7 Live stream: an ingested record reaches a connected client; a disconnected client does
      not block ingest and does not affect the remaining clients.
- [ ] 1.8 Sink: an unreachable endpoint leaves the pipeline successful with non-zero drop and
      failure counters; `ACTIVATION_START` is among the delivered records; `validate` rejects a
      hostless `console://` URI without importing an HTTP client.
- [ ] 1.9 Resolver delegation: Kafka, Pub/Sub, BigQuery, and OTLP URIs resolve identically under
      `ConsoleSinkResolver` and `DefaultSinkResolver`.
- [ ] 1.10 Kafka source: valid messages are stored; an undecodable message is counted and skipped
      without stopping the consumer. Offline against a fake consumer; `integration`-marked against
      the existing `redpanda` compose service.
- [ ] 1.11 BigQuery source: rows produced by `trace_event_to_row` reverse into equal `TraceEvent`s;
      re-reading an overlapping window leaves every query result unchanged. Offline against a fake
      client.
- [ ] 1.12 Bundle import: a run captured for `beam-agents-replay` is queryable with no pipeline
      running; a stream truncated mid-record reports the truncation and retains what it read.
- [ ] 1.13 CLI: a database path alone starts the service; a malformed ingest URI and an unwritable
      database path each exit `2` naming the rejected value; every flag falls back to its
      environment variable.
- [ ] 1.14 Import boundary: `import beam_agents` and `import beam_agents.console` both succeed with
      no console extras installed; constructing a source whose client is missing raises an error
      naming the extra. Mirrors the existing adapter and memory-store boundary tests.
- [ ] 1.15 Retention: records outside the window are pruned and records inside it are retained;
      counts and the effective window are reported through the API.

## 2. Implementation

### Wave 0 — shared foundation

- [ ] 2.0 OpenSpec change, `console` package skeleton with the frozen public surface, `pyproject`
      extras and console_script, `public-surface.toml` and `docs/api.md` entries, changelog
      fragment, and the frontend scaffold: build config, design tokens, primitives, app shell,
      typed API client, live-stream hook, and the deterministic fixture interceptor.

### Backend

- [ ] 2.1 `_schema.py` / `_store.py` — WAL SQLite store, self-migrating schema, idempotent upsert
      on `(trace_id, span_id, event_type)` with attribute merge, derived activation rollups,
      retention pruning, and an index behind every filter the query layer exposes.
- [ ] 2.2 `_ingest.py` — the single bytes-to-rows normalizer, with one decoder per source:
      framed trace stream (reusing `replay.bundle.parse_trace_stream`), OTLP request,
      `ActivationErrorRecord`, `AgentEnvelope`-wrapped error, and `StateSnapshot`.
- [ ] 2.3 `_queries.py` — activation list with composed filters and keyset pagination, trace detail
      and span-tree assembly, error grouping, time-bucketed aggregates, model/tool/reason facets,
      and attribute search.
- [ ] 2.4 `_dto.py` / `_api.py` — response models and routers for overview, activations, traces,
      errors, models, tools, approvals, entities, and search.
- [ ] 2.5 `_app.py` / `_sse.py` — `create_app`/`serve`, the native and OTLP ingest endpoints, the
      live stream, the static mount with the documented resolution order, and liveness.
- [ ] 2.6 `_sink.py` — `WriteToConsole` and `ConsoleSinkResolver`, copying the OTLP exporter's
      batching, background-sender, and drop-and-count contract, and delegating every other scheme.
- [ ] 2.7 `_sources/_kafka.py` — background consumer over an existing trace topic, lazy client
      import, read-from-end default, no committed offsets, decode failures counted and skipped.
- [ ] 2.8 `_sources/_bigquery.py` — incremental reader reversing the published row encoding, lazy
      client import.
- [ ] 2.9 `_sources/_bundle.py` — replay-bundle importer over the runtime's existing framing
      parser, backing the import endpoint.
- [ ] 2.10 `__main__.py` — the `beam-agents-console` CLI, env-var fallback per flag, exit `2` on
      configuration error.
- [ ] 2.11 `_demo.py` / `examples/console_demo/` — a `DirectRunner` pipeline over the fake provider
      producing the full event vocabulary: completions, suspensions, approvals, tool errors, cache
      hits, budget exhaustion, TTL wipes, and dead letters.

### Frontend

- [ ] 2.12 Overview — headline figures, throughput/error/token series, live feed, recent errors,
      top models and tools.
- [ ] 2.13 Activations — the dense list with composed filters and paging, and the detail view with
      the sequence waterfall, attribute inspector, staged intents, attempts, and a copy-ready
      replay command.
- [ ] 2.14 Traces — trace search, the span tree, attempt comparison across a suspend/resume, and
      the raw record view.
- [ ] 2.15 Errors — grouping by reason and error type, occurrence series, drill-down, and the
      failure-position panel.
- [ ] 2.16 Models, tools, and approvals — token spend, cache-hit ratio, circuit state, tool volume
      and failure rate, and the approval queue.
- [ ] 2.17 Entities, search, and settings — the per-key timeline, attribute search, retention and
      theme settings, the connect page with a snippet per ingest path, and the bundle importer.

### Packaging

- [ ] 2.18 `docker/console.Dockerfile`, `docker/compose.console.yaml`, `make console-*` targets,
      `docs/console.md`, mkdocs nav, and the README quickstart.

## 3. Gates

- [ ] 3.1 `openspec validate add-agent-console --strict` reports valid.
- [ ] 3.2 `make lint` and `make type` clean over the new package, with mypy `--strict` applying to
      `src/beam_agents/console/`.
- [ ] 3.3 `make test-unit` passes offline with no docker and no console extras beyond the test
      group.
- [ ] 3.4 `make coverage-ratchet` at or above baseline; re-measure `coverage-baseline.toml` on the
      combined tree and raise it if it improved. Never pick a side.
- [ ] 3.5 `uv run pre-commit run --all-files` clean, including the public-surface, docstring, and
      changelog-fragment gates.
- [ ] 3.6 `make docs` (`mkdocs build --strict`) clean with the new page in the nav.
- [ ] 3.7 The console compose stack builds and becomes healthy from a clean checkout, and the demo
      pipeline's records are visible through the API and the UI.
- [ ] 3.8 Every UI page screenshotted in light theme, dark theme, and at a narrow mobile width.
