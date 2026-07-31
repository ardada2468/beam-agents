# Quickstart

Every other example in this repo runs offline on purpose — scripted `FakeLLM`, no
credentials, no network — because an example that needs an API key is one most
people never run. This page is the other end: it exists to answer *does this
actually work against the real thing*, so it calls a real model, runs a real
tool, suspends for a real approval, and streams what it records into a console
you can watch it arrive in.

```sh
export ANTHROPIC_API_KEY=sk-ant-...
make quickstart
```

That starts the console and runs the pipeline. Open
[http://localhost:8787](http://localhost:8787) when it finishes.

`OPENAI_API_KEY` works too. **There is no silent downgrade**: with no credential
set the command fails and tells you what to export, because a quickstart that
quietly ran a scripted model while you believed you were testing a real one is
worse than one that refuses. To run it offline anyway, ask for that by name:

```sh
make quickstart PROVIDER=fake
```

## What it exercises

An incident-triage agent over three services — the shortest path that touches
every guarantee the runtime makes:

| Service | What happens | What it proves |
|---|---|---|
| `svc-checkout` | Model says PAGE, approval arrives, activation resumes | Suspend → effector → resume is **one** activation with two attempts under the same `seq` |
| `svc-imagecache` | Model says IGNORE | The model's decision actually routes the pipeline |
| `svc-payments` | Model says PAGE, nobody ever answers | The deadline elapses and the **fail-closed** timeout route runs, rather than the activation hanging |

Along the way it calls a real tool through the tool registry (`service_tier`),
budgets tokens, and delivers traces, errors, and snapshots over `console://`.

With `PROVIDER=fake` the scripted model answers PAGE to everything, so you get
one approved resume and two timeouts instead — the same machinery, none of the
model's judgement.

## The ladder

Four rungs, in increasing order of what you have to provision. The **same
module** runs on all of them; only Beam's runner flags change.

### 1. Local, in process

What `make quickstart` does. Real model, real tools, real HITL, real console —
`DirectRunner` in your terminal. This is the one to start with, and the one to
re-run when you change your agent.

### 2. Real distributed execution, on your laptop

A real Beam-on-Flink cluster in Docker: a JobManager, a TaskManager, and an
external SDK harness. Same pipeline, submitted as a portable job.

```sh
make compose-up        # Redpanda, Redis, Flink, SDK harness
make console-up        # the console, on :8787
make quickstart-flink
```

This is the rung that proves the runtime's state and timers work under a real
distributed runner rather than in one process.

Two constraints are worth knowing before you extend it, both from
[`docker/README.md`](https://github.com/ardada2468/beam-agents/blob/main/docker/README.md):

- **Cross-language Kafka IO does not work on this stack.** `ReadFromKafka` and
  `WriteToKafka` need a Java SDK harness whose environment defaults to `DOCKER`,
  and the Flink image has no docker CLI. Pipelines here use Python-native
  sources — which is why the quickstart scripts its input with `TestStream`
  rather than reading a topic.
- **The SDK harness shares the TaskManager's network namespace**, so restarting
  one means restarting the other, and the console is reached at
  `host.docker.internal:8787` rather than `localhost`.

### 3. Real Dataflow

Needs a GCP project with billing, the Dataflow and Artifact Registry APIs
enabled, and a staging bucket. **This rung costs money** — the others do not.

```sh
uv run python -m examples.quickstart \
  --provider anthropic \
  --console console://YOUR-CONSOLE-HOST:8787 \
  --runner DataflowRunner \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --region us-central1 \
  --temp_location gs://YOUR-BUCKET/tmp \
  --sdk_container_image YOUR-REGION-docker.pkg.dev/YOUR-PROJECT/YOUR-REPO/beam-agents:latest
```

Two things differ from the local rungs and are easy to miss:

- **The console must be reachable from Dataflow workers.** `localhost` is the
  worker's own loopback. Either run the console somewhere the workers can reach
  or — better for anything beyond a trial — export to Kafka or BigQuery and
  point a local console at *that*; see
  [the console's ingest paths](console.md#getting-records-in).
- **Your API key has to reach the workers.** It is read from the environment
  inside `provider_factory`, so it must be present on the worker, not just on
  the machine you submitted from.

For a packaged, deployable version of a pipeline on Dataflow, the fraud-triage
Flex Template is the worked example:
[Fraud triage on Dataflow](examples/fraud-triage-dataflow.md).

### 4. The full test tiers

Once you want the guarantees checked rather than demonstrated:

```sh
make test-unit           # offline, no docker
make compose-up
make test-semantics      # effectively-once, on real Flink
make test-conformance-flink
```

See [the CI workflow map](ci.md) for what runs where.

## Costs and safety

- Rungs 1 and 2 cost only model tokens. The quickstart uses Haiku (or
  `gpt-4o-mini`) capped at 64 output tokens across three activations — a
  fraction of a cent.
- Rung 3 provisions Dataflow workers and bills for them until the job is
  drained or cancelled. Nothing in this repo starts a Dataflow job for you.
- The console has **no authentication** and binds `0.0.0.0` inside its
  container. Publishing port 8787 to a shared network publishes your traces to
  it — see [the console's caveats](console.md#what-this-is-not).

## Where it lands

| Thing | Path |
|---|---|
| Pipeline | `examples/quickstart/pipeline.py` |
| Targets | `make quickstart`, `make quickstart-flink` |
| Console | [The console](console.md) |
