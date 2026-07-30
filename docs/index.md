# beam-agents

`beam-agents` makes AI agents first-class citizens of Apache Beam streaming
pipelines. An agent becomes a keyed, stateful, fault-tolerant transform —
`events | RunAgent(my_agent)` — for **system-triggered** workloads: fraud
triage, anomaly response, personalization, IoT reaction, ops automation.
Events invoke the agent; its decisions are durable, replayable, and
horizontally scalable. It is not built for sub-second interactive chat.

## A runtime, not a framework

beam-agents deliberately owns only what agent frameworks lack: durable keyed
memory, event/processing-time semantics, effectively-once side effects,
backpressure-aware scale-out, and runner portability (DirectRunner, Dataflow,
Flink). Agent *authoring* belongs to LangGraph, Google ADK, Pydantic AI, or a
plain async function — integrated via adapters. There is no prompt templating,
no orchestration DSL, and no agent-authoring abstraction here, on purpose.

## The shape of a pipeline

```
Kafka/PubSub events ──┐
tool-results topic ───┼─► WithKeys(entity_id) ─► Flatten ─► RunAgent
approvals topic ──────┘                                       │
outputs: .output (main) · .intents ─► outbox ─► effector ─► results (re-injected)
         .traces ─► OTLP/BigQuery · .errors ─► dead-letter sink
```

Side effects never execute inside the pipeline: the agent stages declarative
`ToolIntent`s, an external [effector](effector.md) executes them exactly once
per deterministic `intent_id`, and results re-enter on the same key.

## Guarantees, and the gates that enforce them

Every guarantee below is a machine-verified release gate, not an aspiration.
A claim on this site that nothing enforces is a defect.

| Guarantee | Enforced by |
|---|---|
| Side effects execute effectively once under real worker kills, duplicate sink writes, and full pipeline replay | the [effectively-once e2e gate](ci.md#the-effectively-once-end-to-end-gate) ([`tests/semantics/test_effectively_once_e2e.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_effectively_once_e2e.py)) |
| A retried bundle adds zero provider calls and commits byte-identical intents | the retry-determinism gate ([`tests/semantics/test_retry_determinism.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_retry_determinism.py)) |
| Every adapter exhibits identical lifecycle semantics on DirectRunner and Flink | the adapter conformance matrix ([`tests/conformance/`](https://github.com/ardada2468/beam-agents/tree/main/tests/conformance)) |
| Human-in-the-loop timeouts fail closed at both layers | the HITL semantics gates ([`tests/semantics/test_hitl_fail_closed.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_hitl_fail_closed.py)) |
| A failed activation commits nothing, and coverage/mutation scores never regress | the [`ci` and `quality` workflows](ci.md) |

## Install

The package is unreleased; the install story is source-based, exactly as the
[repository README](https://github.com/ardada2468/beam-agents#readme)
describes:

```sh
git clone https://github.com/ardada2468/beam-agents
cd beam-agents
uv sync --all-groups
```

Requires Python `>=3.11,<3.13` and [`uv`](https://docs.astral.sh/uv/).

## Start here

The three examples are real, runnable Beam pipelines on the real runtime,
driven by a scripted `FakeLLM` so they run offline with no API keys and no
docker. The code each page shows is included verbatim from `examples/` — the
same file CI executes in [`tests/examples/`](https://github.com/ardada2468/beam-agents/tree/main/tests/examples).

- [Hello, world](examples/hello-world.md) — the minimal fast path: one event,
  one model call, one output.
- [Fraud triage](examples/fraud-triage.md) — suspension, human approval, and
  the fail-closed timeout.
- [IoT reaction](examples/iot-reaction.md) — keyed rolling memory on a stream,
  with no model calls for quiet readings.

Then the operator pages: [CI workflow map](ci.md), [running the
effector](effector.md), [the errors output](errors.md), [runtime
metrics](metrics.md), and [trace delivery](traces.md).
