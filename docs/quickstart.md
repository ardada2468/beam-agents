# Quickstart

Every other example in this repo runs offline on purpose — scripted `FakeLLM`, no
credentials, no network — because an example that needs an API key is one most
people never run. This page is the other end: it exists to answer *does this
actually work against the real thing*, so it calls a real model, runs a real
tool, suspends for a real approval, and streams what it records into a console
you can watch it arrive in.

```sh
export ANTHROPIC_API_KEY=sk-ant-...
make quickstart-docker      # docker only, nothing installed
make quickstart             # or from a checkout, with uv
```

Either one starts the console and runs the pipeline against it. Open
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

Five rungs, in increasing order of what you have to provision. Rungs 0 to 2 are
the **same module** and differ only in where it runs; rung 3 is a different
pipeline, for the reason given there.

### 0. Docker only — no checkout, no Python

If you are evaluating rather than developing, this is the whole thing: Docker
and a key. Nothing is installed on your machine, and the pipeline runs in a
container beside the console rather than on your host.

```sh
export ANTHROPIC_API_KEY=sk-ant-...
make quickstart-docker
```

or without a checkout at all:

```sh
ANTHROPIC_API_KEY=sk-ant-... docker compose \
  -f docker/compose.console.yaml --profile quickstart up --build quickstart
```

The console comes up first, the quickstart runs against it once and exits, and
the records stay in the database volume. Open
[http://localhost:8787](http://localhost:8787).

It runs `examples.quickstart` — the same module rung 1 runs on the host — so the
evaluation path and the development path cannot drift into testing different
things. The credential is passed through from your environment and is never
baked into the image.

The service sits behind a `quickstart` profile because it is the only thing in
this repo that needs a credential and reaches the internet. Plain
`make console-up` stays the path that works with nothing configured.

### 1. Local, in process

What `make quickstart` does. Same pipeline, run from your checkout with `uv`
instead of in a container — the form to use while you are changing the agent,
because there is no image to rebuild between runs.

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

### 3. Real Dataflow — a different pipeline, not this one

**This module does not go to Dataflow, and the ladder stops being one module
here.** Its source is a `TestStream`, which scripts both clocks so the approval
and the elapsed deadline happen in seconds rather than in real minutes. That is
what makes rungs 1 and 2 self-contained, and it is exactly what Dataflow does
not run: a streaming job there reads a real source. Pointing
`--runner DataflowRunner` at this module does not produce a slower quickstart,
it produces a submission failure.

The Dataflow-shaped version of the same story is the fraud-triage **Flex
Template**, which is the same agent with its source and sinks parameterised as
Pub/Sub topics instead of scripted in:

- [Fraud triage on Dataflow](examples/fraud-triage-dataflow.md) — running it
- [Wiring the image](deploying.md) — what the container must contain, and how
  the model credential reaches a worker without ever being a template parameter

**That rung costs money**, and the others do not: it provisions Dataflow workers
and bills for them until the job is drained or cancelled. It also needs more
than an API key — a project with billing, the Dataflow, Artifact Registry and
Secret Manager APIs enabled, a staging bucket, an Artifact Registry repository,
the image built and pushed, and the Pub/Sub topics created.

Two things about it differ from the local rungs and are easy to miss:

- **The console must be reachable from the workers.** `console://localhost:8787`
  is the *worker's* own loopback, which is nothing. Export to Kafka or BigQuery
  and point a local console at that instead; see
  [the console's ingest paths](console.md#getting-records-in).
- **The credential must reach the worker, and must not reach the job
  description.** `provider_factory` runs in the worker process, so an
  environment variable set on your laptop is not there. Pass a Secret Manager
  *version reference* as the parameter and fetch the value on the worker —
  [deploying.md](deploying.md#the-model-credential) has the grants.

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

- Rungs 0 to 2 cost only model tokens. The quickstart uses Haiku (or
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
