# Quickstart: a real model, in Docker

Every other example here is hermetic on purpose — a scripted [`FakeLLM`], no
credentials, no network — because an example that needs an API key is one most
people never run. This one is the other end. It exists to answer *does this
actually work against the real thing*, so it calls a real provider over the
network and streams what it records into a running [console](../console.md).

[The Quickstart guide](../quickstart.md) is the task-oriented version: how to
run it, what each rung of the ladder costs, and what it proves. This page is
the source.

```sh
export ANTHROPIC_API_KEY=sk-ant-...
make quickstart-docker      # docker only, nothing installed
make quickstart             # or from a checkout, with uv
```

## What it does

An incident-triage agent over three services, one per outcome, so a real model
exercises every leg rather than one:

| Service | Outcome | What it demonstrates |
| --- | --- | --- |
| `svc-checkout` | Model pages, approval arrives, activation resumes | A suspend and its resume are **one** activation with two attempts under the same `seq` |
| `svc-imagecache` | Model declines to page | The model's own decision routes the pipeline — the scripted provider cannot produce this |
| `svc-payments` | Model pages, nobody answers | The deadline elapses and the **fail-closed** timeout route runs, rather than the activation hanging |

Along the way it calls a real tool through the tool registry, budgets tokens,
and delivers traces, errors and snapshots over `console://`.

## No silent downgrade

With no credential the module names the variable to export and exits non-zero.
The offline provider is reachable only by asking for it (`--provider fake`). A
quickstart that quietly ran a scripted model while you believed you were
testing a real one would be worse than one that refuses.

## Not a Dataflow example

Its source is a `TestStream`, which scripts both clocks so the approval and the
elapsed deadline resolve in seconds instead of real minutes. That is what makes
it self-contained, and it is exactly what Dataflow does not run — a streaming
job there reads a real source. The Dataflow-shaped version of the same story is
[the fraud-triage Flex Template](fraud-triage-dataflow.md), with
[deploying.md](../deploying.md) for the image and the credential.

## The pipeline

```python
--8<-- "examples/quickstart/pipeline.py"
```

[`FakeLLM`]: https://github.com/ardada2468/beam-agents/blob/main/src/beam_agents/model/fake.py
