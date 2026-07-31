# The console

The runtime already records everything you would want to look at. `RunAgent`
emits `TraceEvent`s with deterministic identity ([trace delivery](traces.md)),
`ActivationErrorRecord`s over a closed reason vocabulary
([errors and dead letters](errors.md)), and `StateSnapshot`s for replay
([state export and replay](replay.md)). Every one of those paths ends at a wire
boundary — bytes on a topic, rows in BigQuery, spans at a collector — and
looking at any of it has meant provisioning something first.

The console is the reader that closes that loop: a WAL SQLite store, an HTTP
read API, a live stream, and a browser UI, in one process over one file. No
broker, no cloud project, no collector.

## Quickstart

```sh
docker compose -f docker/compose.console.yaml up -d --build
open http://localhost:8787
```

or, equivalently:

```sh
make console-up      # build + start; the UI is at http://localhost:8787
make console-logs    # follow the console and the demo pipeline
make console-down    # stop, keeping the database volume
```

That stack is two containers. `console` serves the API and the UI. `console-demo`
runs a `DirectRunner` pipeline over the fake provider — no API key, no broker, no
network egress — pushing records over `console://` in a loop, so you land on a
console with data in it and traffic still arriving rather than an empty one. It
drives the scenarios the interesting screens need: completions, multi-tool runs,
cache hits, suspensions that are approved, denied, and timed out, tool errors,
activation errors, budget exhaustion, orphaned results, dead-lettered intents,
and batch overflow.

The database lives on a named volume, so `make console-down && make console-up`
lands on the records from last time. `docker compose -f docker/compose.console.yaml
down -v` is the deliberate way to throw them away.

Without Docker:

```sh
uv pip install 'beam-agents[console]'
make console-frontend         # build the UI bundle into the package (needs Node)
beam-agents-console --db ./beam-agents-console.db
```

`make console-frontend` is optional. Skip it and you get a working API with a
page at `/` telling you how to get the UI — the bundle is a build artifact and is
never committed, so a wheel built from a clean checkout does not carry one.

## Getting records in

Five paths, in increasing order of intrusiveness. Pick the first one that
already describes your deployment.

### 1. Already exporting to OTLP

The console accepts `POST /v1/traces` in the same OTLP/HTTP protobuf encoding
`WriteTracesToOtlp` already emits. Change the host, change nothing else:

```python
config = AgentConfig(
    provider_factory=make_client,
    traces_to="otlp://localhost:8787",
)
```

Lossy on the way in exactly as it is on the way out. `ACTIVATION_START` has no
OTLP representation (it shares a span ID with `ACTIVATION_END`), so activations
that arrive this way cannot distinguish a fresh attempt from a resume, and the
UI labels them as such rather than pretending they are complete.

### 2. Already exporting to Kafka

Point the console at the topic. No pipeline change at all — not even a restart.

```sh
beam-agents-console \
  --db ./console.db \
  --kafka-traces-from kafka://broker:9092/beam-agents-traces
```

It reads from the end by default (`--kafka-from-beginning` to replay a retained
topic) and commits no offsets, so two consoles can watch one topic and a restart
never waits on a consumer-group rebalance.

The compose stack has a Redpanda service behind an opt-in profile:

```sh
BEAM_AGENTS_CONSOLE_KAFKA_TRACES_FROM=kafka://redpanda:9092/beam-agents-traces \
  docker compose -f docker/compose.console.yaml --profile kafka up -d --wait
```

The broker is published on `localhost:29092` — deliberately not the `19092` the
integration stack uses, so both can run at once. Redpanda auto-creates the topic
on first produce; to create it up front:

```sh
docker compose -f docker/compose.console.yaml exec redpanda \
  rpk topic create beam-agents-traces --brokers redpanda:9092
```

### 3. Already exporting to BigQuery

The console reverses the published row encoding
(`beam_agents.observability.exporters.TRACE_TABLE_SCHEMA`) and pulls incrementally
by `event_time`, the table's partition column.

```sh
beam-agents-console \
  --db ./console.db \
  --bigquery-traces-from bigquery://my-project/my_dataset/traces
```

This is also the answer for volume the console's SQLite store cannot hold: keep
BigQuery as the system of record and let the console read a window of it.

### 4. Have a captured run

A replay bundle — the varint-framed `TraceEvent` file and `StateSnapshot` blob
`beam-agents-replay` already consumes — imports with no pipeline running at all.

```sh
beam-agents-console --db ./console.db \
  --import-traces ./run-traces.bin \
  --import-snapshot ./run-snapshot.bin
```

The Connect page in the UI accepts the same files by drag and drop.

### 5. Want the full record

`console://` is the native path: the protos themselves, `ACTIVATION_START`
included, and the only path that carries errors and snapshots as well as traces.
It is one constructor argument, because `ConsoleSinkResolver` wraps the runtime's
own resolver rather than replacing it — every other scheme keeps behaving exactly
as it does today.

```python
from beam_agents.console import ConsoleSinkResolver

config = AgentConfig(
    provider_factory=make_client,
    traces_to="console://localhost:8787",
    errors_to="console://localhost:8787",
    snapshots_to="console://localhost:8787",
    sink_resolver=ConsoleSinkResolver(),
)
```

Delivery is best-effort by contract, the same posture as the OTLP exporter: the
sink batches, hands batches to one background sender, and **drops and counts**
rather than raising or applying backpressure. A console someone closed their
laptop on must never fail an activation. The drop counters live under the
`beam_agents.console` metrics namespace and are visible in the UI.

## CLI reference

```sh
beam-agents-console --db ./beam-agents-console.db
```

Every flag falls back to an environment variable, matching the
[effector CLI](effector.md)'s convention. The console exits `2` on a
configuration error — a malformed ingest URI, an unwritable database path —
naming the value it rejected, and `0` on a clean shutdown.

| Flag | Environment variable | Default | Notes |
|---|---|---|---|
| `--db` | `BEAM_AGENTS_CONSOLE_DB` | `beam-agents-console.db` | The SQLite file. Created with its schema if absent. |
| `--host` | `BEAM_AGENTS_CONSOLE_HOST` | `127.0.0.1` | Loopback on purpose — see the caveats below. |
| `--port` | `BEAM_AGENTS_CONSOLE_PORT` | `8787` | |
| `--static-dir` | `BEAM_AGENTS_CONSOLE_STATIC` | the packaged bundle | Resolution order: flag, then variable, then `console/static/`. |
| `--retention-hours` | `BEAM_AGENTS_CONSOLE_RETENTION_HOURS` | unbounded | Records older than the window are pruned. |
| `--kafka-traces-from` | `BEAM_AGENTS_CONSOLE_KAFKA_TRACES_FROM` | unset | `kafka://<brokers>/<topic>` — the same URI your `traces_to` uses. |
| `--kafka-from-beginning` | `BEAM_AGENTS_CONSOLE_KAFKA_FROM_BEGINNING` | off | Replay a retained topic instead of reading from the end. |
| `--bigquery-traces-from` | `BEAM_AGENTS_CONSOLE_BIGQUERY_TRACES_FROM` | unset | `bigquery://<project>/<dataset>/<table>`. |
| `--import-traces` | — | unset | A replay bundle's trace stream. |
| `--import-snapshot` | — | unset | A replay bundle's state snapshot. |
| `--cors-origin` | `BEAM_AGENTS_CONSOLE_CORS_ORIGIN` | none | For running the Vite dev server against a real console. |
| `--log-level` | `BEAM_AGENTS_CONSOLE_LOG_LEVEL` | `info` | |

The Kafka and BigQuery sources need the `console-ingest` extra
(`uv pip install 'beam-agents[console,console-ingest]'`); constructing one
without its client installed raises an error naming the extra. The Docker image
ships both.

## Spans have no width, and the UI says so

Every span the runtime emits satisfies `start_ms == end_ms`. That is deliberate:
measuring elapsed time would put a wall-clock read in the hot path, and the
runtime declines to (`add-trace-events`, D7). So the console **cannot** draw a
conventional waterfall — scaling bars by span width would render every span as a
zero-width tick, and scaling them by anything else would fabricate the one
quantity the runtime refuses to measure.

The trace view therefore draws spans as ordered rules of uniform weight. Position
and nesting are real; **width encodes nothing**. Durations appear as explicit
numbers only where a real measurement exists: the `ActivationTally` figures
(`llm_ms`, `tool_ms`, `iterations`, token counts) that arrive as attributes, and
the delta between an activation's `ACTIVATION_START` and `ACTIVATION_END`
timestamps, which is meaningful because those two are separate clock reads.
Where nothing was measured, the UI says so instead of drawing a bar.

The same discipline applies to every dimensioned number on screen. Beam user
metrics carry no labels and are *attempted*, not committed, so they disagree with
trace-derived numbers under retry by construction. Every per-model, per-tool,
per-reason, and cache-hit figure in the console comes from `TraceEvent.attributes`,
never from the metrics surface.

## What this is not

**Not authenticated.** There are no users, no tokens, and no authorization.
Telemetry ingest causes no side effects, so it is not signed the way intents are
([security](security.md)), and the compensating control is that the service binds
to loopback by default. The Docker image sets `0.0.0.0` because loopback inside a
container namespace is reachable from nothing — which means the compose stack
belongs on a machine you trust, and publishing port 8787 to a shared network is
publishing your traces to it.

**Not writable.** Every endpoint is read-only with respect to agent state.
Nothing in the console can approve a suspension, retry an activation, or write to
a running pipeline. The approval queue *shows* pending approvals; approving one
goes through your approvals topic, as it always did.

**Not an APM.** `otlp://` still exists and still reaches Jaeger, Tempo, and
Datadog, and for cross-service tracing that is the right tool. The console is the
*runtime-shaped* view: an activation list keyed by `(entity_key, seq)`, errors
grouped by the runtime's closed `reason` vocabulary, and a suspend → effector →
resume cycle rendered as one trace with two attempts. A generic viewer cannot
show any of those, because none of them are generic.

**Not long-horizon storage.** One SQLite file with a retention window. A
production-rate pipeline pointed at `console://` will outrun a single-writer
SQLite file, and the sink will drop and count rather than backpressure the
pipeline — visibly, in the UI, but it will drop. The documented answer for
production volume is path 2 or 3 above: keep exporting to Kafka or BigQuery and
let the console read a window. A deployment that needs months of history has
BigQuery, and the console can read that too.

**Not multi-tenant.** One store, one process, one machine.

## What lands where

| Thing | Path |
|---|---|
| Image | `docker/console.Dockerfile` — Node builds the UI, a Python slim stage installs the wheel and copies the bundle in |
| Stack | `docker/compose.console.yaml` — standalone; shares nothing with the integration stack in `docker/compose.yaml` |
| Database | the `console-db` named volume, at `/var/lib/beam-agents-console/console.db` in the container |
| UI bundle | built from `frontend/` into `src/beam_agents/console/static/`; force-included in a wheel when present |
| Public API | `ConsoleStore`, `ConsoleSinkResolver`, `WriteToConsole`, `create_app`, `serve` — see [API reference](api.md) |

The container runs as a non-root user (uid 1001) and carries a `HEALTHCHECK`
against `/healthz`, which reports healthy on an empty store: the console being up
is not the same question as the console having data.
