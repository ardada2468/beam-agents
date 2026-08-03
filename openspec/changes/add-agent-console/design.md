## Context

Everything the console displays already exists on the wire. The change is a *reader*, and the
design work is almost entirely about not distorting what the runtime went to some trouble to make
exact.

Four properties of the existing telemetry constrain every decision below:

| Property | Where it is established | Consequence for a viewer |
|---|---|---|
| Trace identity is `uuid5(entity_key, seq)` — one trace per activation *scope*, so a suspend → effector → resume cycle is **one** trace with multiple attempts | `observability/traces.py`, `add-trace-events` D1/D2 | The primary list object is an **activation**, not a "run"; a trace detail must show attempts, not assume one |
| Spans are **zero-width**: `start_ms == end_ms`, both the injected activation clock | `traces.py` D7 | No duration can be derived from trace bytes; a waterfall scaled by span width would be fabricated |
| Trace records are **at-least-once and byte-identical under replay**; dedup key is `(trace_id, span_id, event_type)` | `docs/traces.md`, `exporters.py` | Ingest must be idempotent, and re-ingesting a replayed run must not double-count |
| Beam user metrics carry **no labels**, and are **attempted, not committed** | `observability/metrics.py` | Every dimensioned number (per-model, per-tool, per-reason, cache-hit) must come from `TraceEvent.attributes`, never from the metrics surface |

The error vocabulary is closed and small — `activation_timeout`, `activation_error`,
`orphaned_result`, `ttl_wiped_suspension`, `ttl_wiped_batch`, `batch_buffer_overflow`,
`budget_exceeded`, `intent_dead_letter`, `hitl_timeout` (`core/dofn.py:152-186`, `hitl.py`) — which
is what makes grouping by `reason` a real navigation axis rather than a string histogram.

## Goals / Non-Goals

**Goals:**
- One command from a clean machine to a populated console: `docker compose up`.
- Every field on screen traceable to a `TraceEvent` attribute, an `ActivationErrorRecord` field, or
  a `StateSnapshot` field. No derived number without a stated derivation.
- Ingest from every path the library already writes to, so adopting the console never requires
  changing how a deployed pipeline exports.
- Zero blast radius on the runtime: no existing module modified, no new required dependency, no
  proto change.

**Non-Goals:**
- Replacing an APM. `otlp://` still exists and still reaches Jaeger/Tempo/Datadog; the console is
  the runtime-shaped view, not the generic one.
- Long-horizon storage. SQLite with time-based retention is the target; a deployment that needs
  months of history should keep using BigQuery, which the console can *read*.
- Multi-tenancy, authentication, or write-back to a running pipeline. The console is read-only over
  telemetry and is scoped to a trusted local/dev network.
- Measuring durations the runtime does not measure. See D4.

## Decisions

### D1. A tiny public surface over a private package

`beam_agents.console` exports exactly five names — `ConsoleStore`, `ConsoleSinkResolver`,
`WriteToConsole`, `create_app`, `serve` — and every implementation module is underscore-prefixed
(`_store.py`, `_schema.py`, `_ingest.py`, `_queries.py`, `_api.py`, `_dto.py`, `_app.py`,
`_sse.py`, `_sink.py`, `_sources/`, `_demo.py`), which puts them outside `public-surface.toml`,
outside the `docs/api.md` drift test, and outside the post-1.0 deprecation policy. The precedent is
`model/_http.py`, `core/_dofn_fakes.py`, and `_protos/`.

This is not cosmetic. The console is a *tool*, driven by a URI and a CLI, not a library users
compose against. Freezing its internals at 1.0 would bind the change to a schema, an ORM shape, and
a route table that will move, and the API-freeze policy has no cheap escape hatch. Five names is
what a user actually needs: a store to open, a resolver to pass to `AgentConfig`, a transform to
apply, and two ways to start the server.

### D2. `console://` wraps the resolver instead of extending it

`ConsoleSinkResolver` implements the existing `SinkResolver` Protocol (`core/transform.py:186`) and
delegates every scheme but `console://` to a `DefaultSinkResolver` it holds. `core/transform.py` is
not edited.

Extending `DefaultSinkResolver` in place would put a `console` scheme — and, through it, an
`httpx` POST target — inside the module every pipeline imports, for a feature most pipelines do not
use. Worse, `SinkResolver.validate` is required to be **import-free** so that config validation
does not pull optional dependencies; a scheme in the default resolver would have to honour that
while its `resolve` reaches for the console client. Wrapping keeps the whole thing opt-in at the
one line where the user already chooses their sinks:

```python
AgentConfig(..., traces_to="console://localhost:8787", sink_resolver=ConsoleSinkResolver())
```

`console://` is accepted for `traces_to`, `errors_to`, and `snapshots_to` — unlike `otlp://`, which
`DefaultSinkResolver` refuses for anything but traces because the OTLP encoding cannot represent
them. The console's native ingest is the protos themselves, so there is nothing to lose.

### D3. The sink copies `WriteTracesToOtlp`'s contract exactly

`WriteToConsole` batches in `process()`, hands batches to one daemon sender thread through a
bounded queue, and **drops-and-counts rather than raising or backpressuring** — the same contract,
the same failure posture, and the same counter shape as `observability/otlp.py`, under a
`beam_agents.console` namespace (`records_exported`, `records_dropped`, `export_failures`,
`batches_sent`).

Any other posture is unsound here. Telemetry delivery failing must never fail an activation, and a
blocked sender must never become backpressure on the agent's own work — a console that a developer
left running on a laptop is exactly the kind of endpoint that goes away mid-pipeline. Reusing the
contract also means the drop behaviour is auditable the same way, and a reader who understands the
OTLP exporter already understands this one.

Unlike the OTLP exporter, `WriteToConsole` transmits `ACTIVATION_START`. The OTLP path drops it
because it shares a span ID with `ACTIVATION_END` and OTLP has no representation for two events on
one span; the native path carries `event_type` as a first-class column, so the start event is both
representable and load-bearing — it is what distinguishes a `start` attempt from a `resume`.

### D4. The waterfall renders sequence, and durations come from attributes

Because every span satisfies `start_ms == end_ms` (D7 of `add-trace-events`, so the hot path never
reads a wall clock), a conventional waterfall is impossible: scaling bars by span width would
render every span as a zero-width tick, and scaling them by *anything else* would invent the one
quantity the runtime deliberately declines to measure.

The UI therefore draws spans as ordered rules of uniform weight — position and nesting are real,
width is not encoded — and shows durations as explicit numbers wherever a real one exists: the
`ActivationTally` figures (`llm_ms`, `tool_ms`, `iterations`, token counts) that reach the store as
attributes, and the wall-clock delta between an activation's `ACTIVATION_START` and
`ACTIVATION_END` `start_ms` values, which *is* meaningful because those two events are stamped by
different clock reads. Anywhere no measurement exists, the UI says so rather than drawing a bar.

### D5. Idempotent ingest keyed on `(trace_id, span_id, event_type)`

The store's event table is keyed on exactly the tuple `docs/traces.md` publishes as the dedup key,
with `INSERT … ON CONFLICT DO UPDATE`. A retried bundle, a replayed run, and a Kafka consumer that
restarts from an earlier offset all converge to the same rows.

`DO UPDATE` rather than `DO NOTHING`: an event's *attributes* can legitimately grow across ingest
paths — the OTLP form of an event carries fewer attributes than the native form, and a run
imported from a bundle may be re-ingested live — so a later, richer copy of the same event should
win rather than be discarded. The identity tuple never changes; only the payload merges.

Activation rollups (status, kind, token totals, tool count, model, error count) are **derived** in
the store from the events that make up an activation, not written by the producer, so they are
correct after any subset of an activation's events has arrived and stay correct when the rest do.

### D6. SQLite, WAL, one file

The store is a single WAL-mode SQLite file. It is the only choice that satisfies "one `docker run`
and it works" without a second container, survives a restart, and still supports the grouping and
time-bucketing the UI needs. Writes are serialized through a single connection on a worker thread;
reads use their own connections, which WAL makes concurrent with the writer.

The obvious alternative — an in-memory ring buffer — was rejected because losing every run on
restart makes the console useless for the failure it was opened to investigate. Postgres was
rejected because it doubles the compose surface and the deployment story for a tool whose whole
premise is that it needs no infrastructure. A deployment that outgrows SQLite already has BigQuery,
and the console reads it (D7).

### D7. Four ingest sources behind one normalizer

`_ingest.py` owns the only path from bytes to store rows. Every source — the native endpoint, the
OTLP endpoint, the Kafka consumer, the BigQuery reader, the bundle importer — decodes to
`TraceEvent` / `ActivationErrorRecord` / `StateSnapshot` protos and hands them to the same
normalizer.

That keeps the surprising parts in one place. The BigQuery reader must reverse
`trace_event_to_row`'s hex-and-enum-name encoding (`exporters.py`); the OTLP reader must reverse
the span-name-and-attribute mapping in `otlp.py` and accept that `ACTIVATION_START` will never
arrive; the bundle importer must reuse `replay.bundle.parse_trace_stream` rather than reimplement
varint framing. Each of those is one function returning protos, and none of them can drift from the
store's understanding of a record, because they do not touch the store.

The Kafka and BigQuery clients are imported **inside** the constructor of the source that needs
them, matching `memory/stores/` and `effector/`. `import beam_agents.console` must work with
neither installed.

### D8. Fixtures are part of the frontend, not a mock server

`frontend/src/lib/fixtures.ts` ships a deterministic dataset covering the full event vocabulary,
installed as a `fetch` interceptor in dev mode. The UI is therefore buildable, screenshot-able, and
reviewable with no backend running.

This is a development affordance, not a product feature: the interceptor is compiled out of the
production bundle, and the fixture data is generated from the same proto vocabulary the store uses,
so a field that exists in fixtures but not in the API is a build-time type error rather than a
runtime blank.

### D9. The Docker image bundles the built UI; the wheel does not require it

The image is multi-stage — Node builds `frontend/` into `src/beam_agents/console/static/`, then a
Python slim stage installs the wheel with the `console` extra and copies the bundle in. Hatchling
includes `console/static/` **if present**, so a wheel built from a tree where the frontend has been
built ships the UI, and one built from a clean checkout does not.

`serve()` resolves its static directory as: `--static-dir`, then `$BEAM_AGENTS_CONSOLE_STATIC`,
then the packaged `console/static/`, and when none exists it serves the API and returns an
actionable message at `/` naming the Docker command. A `pip install beam-agents[console]` user gets
a working API and a clear pointer; a `docker compose up` user gets everything. Committing a built
bundle to the repo — the alternative that would make `pip install` fully self-contained — was
rejected: minified JS in git is unreviewable, and it would put a build artifact under the same
pre-commit gates as source.

## Risks / Trade-offs

- **A large new package lands under `make coverage-ratchet`.** `coverage-baseline.toml` holds a
  single tree-wide `branch_rate = 0.9164`; a package this size drags it down unless its own tests
  clear that bar. Mitigation: tests-first per the repo convention, and the store/decoders/queries
  are pure enough to test exhaustively offline. The baseline is re-measured on the combined tree at
  integration, never picked from one side.
- **The repo's first JS/TS.** `frontend/` adds a Node toolchain, a lockfile, and a build step to a
  pure-Python tree. It is deliberately walled off: no Python target depends on it, `make test-unit`
  never invokes it, and only the Docker build and an explicit `make console-build` touch it. CI is
  not extended to lint or build it in this change.
- **SQLite under sustained high-throughput ingest.** A production-rate pipeline pointed at
  `console://` will outrun a single-writer SQLite file. Mitigation is scope, not engineering: the
  sink drops-and-counts by contract (D3), the drop counter is visible in the UI, and the documented
  answer for production volume is to keep exporting to Kafka/BigQuery and let the console *read* a
  window of it.
- **OTLP ingest is lossy in, as it is lossy out.** A user who points an existing `otlp://` pipeline
  at the console gets no `ACTIVATION_START` events, so `start`-vs-`resume` and attempt boundaries
  are unavailable. The UI must show this as a stated limitation on those activations rather than
  silently rendering them as complete.
- **Derived rollups can be transiently wrong.** An activation whose `ACTIVATION_END` has not
  arrived yet shows as in-flight, and at-least-once means an event may arrive long after. The
  rollup is recomputed on every write to the activation, so it is eventually correct; the UI labels
  a rollup with no `ACTIVATION_END` as `in flight` rather than guessing a status.

## Open Questions

- Should the console expose the `beam_agents.runtime` Beam metrics at all? They are unlabelled and
  attempted-not-committed, so they disagree with the trace-derived numbers by construction under
  retry. Current answer: no — show trace-derived numbers only, and document why the two differ.
- Does the Kafka source need consumer-group offset management, or is "read from the end, best
  effort" the right default for a dev tool? Current answer: start from the end by default, with a
  flag to start from the beginning; no committed offsets, so restarting never blocks on a group.
- Should `WriteToConsole` sign or authenticate its POSTs? `add-effector-security` established HMAC
  intent signing for a path that causes side effects; telemetry ingest causes none. Current answer:
  no auth, bind to localhost by default, and document the console as a trusted-network tool.

## Migration Plan

Nothing to migrate — this is additive, and no existing behaviour changes.

Adoption is one of five opt-ins, in increasing order of intrusiveness:

1. **Already exporting to OTLP:** point `traces_to="otlp://localhost:8787"` at the console. No code
   change; accepts the loss described above.
2. **Already exporting to Kafka or BigQuery:** start the console with `--kafka-traces-from` or
   `--bigquery-traces-from`. No pipeline change at all.
3. **Have a captured run:** `beam-agents-console --import-bundle`, or drag the files onto the
   Connect page. No pipeline needed.
4. **Want the full record:** pass `sink_resolver=ConsoleSinkResolver()` and set `traces_to`,
   `errors_to`, and `snapshots_to` to `console://…`. One constructor argument.
5. **Just want to see it work:** `docker compose -f docker/compose.console.yaml up`.

Rollback is deleting the container and the SQLite file. No pipeline that adopted paths 1–3 is
affected; a pipeline on path 4 reverts by removing the `sink_resolver` argument.
