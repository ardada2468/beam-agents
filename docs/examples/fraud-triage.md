# Fraud triage

The runtime's differentiating flow — suspension, human-in-the-loop approval,
and the fail-closed timeout — on the workload the project names first. Two
accounts each send one suspicious transaction through a single streaming
pipeline:

- **Account A**: the agent triages the transaction via a scripted model call,
  stages an approval request, and suspends. An analyst's approval re-enters
  the pipeline on the same key before the deadline, and the resumed activation
  emits the freeze decision: `b"freeze:acct-a"`.
- **Account B**: the same suspension — but nobody answers. When the scripted
  clock passes the 30-second deadline, the HITL timer fires the default deny
  route, which emits its deterministic fallback output `b"__hitl_timeout__"`.
  A timeout is an explicit outcome on the main output, never a silent drop.

## The agent

`triage` is the entire agent. On the first activation it calls the model,
stages an approval intent with `ctx.request_approval(...)`, and returns
`Suspend(timeout_ms=...)`; the runtime persists a continuation and arms the
fail-closed timer. On resume the human's decision is on `ctx.resume_approval`,
and the agent turns it into a freeze or a release. Nothing external has
happened at suspension time — the approval request leaves the pipeline as an
`APPROVAL`-kind `ToolIntent` on `.intents`.

## The harness

Everything below `scripted_stream()` is scripted clockwork, not agent logic. A
`TestStream` plays both the events topic and the approvals topic, and drives
both clocks explicitly: the watermark carries event time, and the
`advance_processing_time(60)` step is what fires account B's real-time HITL
timer. No `sleep()`, no wall clock — the same discipline every timer test in
the repository follows.

!!! note "Where the `intent_id` really comes from"

    The approval branch computes the pending intent's id with the runtime's
    `intent_id_for(entity_key, seq, step_index)` — a pure function, which is
    why a replayed activation mints byte-identical intents and the effector
    can deduplicate them. That line exists only because this example has no
    effector: in production the [effector](../effector.md) consumes the
    approval intent, routes it to a human, and publishes the decision onto the
    approvals topic **already carrying** the `intent_id`. You never compute it
    yourself.

The fail-closed behavior demonstrated here — the timer fires the fallback
route, the continuation is cleared, and a late approval is refused as
`orphaned_result` — is pinned for every pipeline by the HITL semantics gates
([`tests/semantics/test_hitl_fail_closed.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_hitl_fail_closed.py));
see also [the errors output](../errors.md) for the `orphaned_result` and
`hitl_timeout` records those paths produce.

## Run it

```sh
uv run python -m examples.fraud_triage
```

```text
b'freeze:acct-a'
b'__hitl_timeout__'
```

## Running it on Dataflow

The pipeline below is scripted: a `TestStream` plays the transaction and
approval topics so the example runs offline with no credentials. Swapping that
harness for real Pub/Sub topics — the *same* `triage` agent, with topics, the
provider and the approval deadline as launch parameters — is what
[Fraud triage on Dataflow](fraud-triage-dataflow.md) packages as a Flex
Template.

## The whole program

The code below is included verbatim from
[`examples/fraud_triage.py`](https://github.com/ardada2468/beam-agents/blob/main/examples/fraud_triage.py)
— the same file
[`tests/examples/test_fraud_triage.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/examples/test_fraud_triage.py)
executes in CI, asserting the freeze output, the single approval intent for
account A, and the deny fallback for account B.

```python title="examples/fraud_triage.py"
--8<-- "examples/fraud_triage.py"
```
