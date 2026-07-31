## Why

`promote-spark-runner` landed the spark conformance leg with four **provisional** `Run()` declarations — `single_shot`, `multi_tool_inline`, `suspension_resume`, `approval_timeout_fallback` — and said so in as many words: "the first `workflow_dispatch` or scheduled run IS the spike" ([design.md F1](../promote-spark-runner/design.md)). That run has now happened. This change records its outcome.

**Evidence.** The spark leg was executed against a real Beam Spark job server for the first time on **2026-07-31** (26m26s wall clock). **All 16 runnable cells failed identically** — 4 adapters (`reference`, `langgraph`, `pydantic_ai`, `adk`) × 4 scenarios. The other 12 cells were the three pre-existing structural skips, reported as declared.

The surface error was the harness's submission-stall classifier:

```
InfraFailure: spark conformance job for adapter '<a>' never started processing
in 2 submissions (job-server or executor submission stall)
```

That symptom is misleading, and the `spark-jobserver` container log gives the real verdict:

```
ERROR JobInvocation: Error during job invocation sconf-f7aab4f434b1-reference-2
org.apache.beam.sdk.Pipeline$PipelineExecutionException:
  java.lang.UnsupportedOperationException: The ActiveBundle does not have a
  registered bundle checkpoint handler.
```

The job server is **healthy**: it starts a SparkContext, builds the DStream graph, processes batches and writes checkpoints, and then fails the invocation on the exception above.

**Root cause.** The leg's ingest source is a Splittable DoFn: [`SpoolSourceDoFn`](../../../tests/semantics/_e2e/spool.py:160) is decorated `@beam.DoFn.unbounded_per_element()`, and its tail-poll path calls [`tracker.defer_remainder(...)`](../../../tests/semantics/_e2e/spool.py:214) to self-checkpoint while waiting for the next sealed segment. The residual that produces requires the runner to have registered a bundle checkpoint handler; Beam's Spark portable runner does not implement one. Every spark cell routes through this source, which is exactly why the failure is uniform, immediate, and adapter-independent.

This is a **portable-runner capability gap** — triage category (b). It is not infrastructure (the stack came up and the job server ran), and it is not a harness defect (the SDF is the same source the Flink leg uses successfully). Nothing in `src/` is implicated: no agent code, no adapter shim, and no `core/` primitive is ever reached.

## What Changes

- **The four provisional `Run()` declarations become `Skip(reason=...)`** in [tests/conformance/_spec.py](../../../tests/conformance/_spec.py). Each reason names the capability gap (no registered bundle checkpoint handler), the specific mechanism that needs it (the `unbounded_per_element` spool SDF's `defer_remainder` tail poll), that the gap is runner-level and adapter-independent, and the 2026-07-31 first-real-run observation — the specificity the existing spark skips are held to by `test_every_spark_skip_names_a_specific_constraint`.
- **This REDUCES the spark leg to zero executing cells.** All seven scenarios are now declared skips on the spark leg; all 28 spark matrix cells are still collected and counted, but no pipeline is ever submitted and nothing about the Spark portable runner is verified by a green run. The `_spec.py` comment above `SPARK_SCENARIOS` (now an empty tuple) states this and its consequence in place, so a reader meets the fact where the emptiness is produced.
- **Promotion becomes unreachable, correctly.** [`scripts/spark_weekly_status.py`](../../../scripts/spark_weekly_status.py) counts a spark `Skip` added inside the promotion window as coverage shrinkage and resets the streak, so these four additions restart the clock and keep restarting it for the trailing 28 days. Beyond that, an all-skip leg produces green weekly runs that assert nothing — this change makes it explicit that a leg with zero executing cells is never promotion-ready, so no accumulation of vacuous green can flip the support statement.
- **The two ways out are named, and neither is claimed to exist today:**
  1. a Beam Spark portable runner that registers a bundle checkpoint handler (upstream fix — the finding should be filed upstream), or
  2. a non-SDF ingest path for the spark leg (e.g. a bounded/pre-sealed spool read, or Kafka ingest via the runner's own unbounded source), which trades the SDF dependency for a different fixture and must not quietly become a batch-mode conversion — `promote-spark-runner`'s design risk 1 already rejects manufacturing green that way.

This change **documents a limitation; it does not fix Spark.** Spark's status in [project.md](../../project.md) stays best-effort — the support statement is untouched, as it already is, and this change lowers rather than raises what is known to work.

## Capabilities

### Modified Capabilities

- `spark-runner-support`: gains the recorded SDF bundle-checkpoint finding as a first-class constraint on the leg — the declared-skip requirement for scenarios blocked only by that gap, and the rule that a spark leg with zero executing cells can never satisfy the promotion gate no matter how many green weekly runs accumulate. The four-week gate, the demotion path, the weekly cadence, and the reporting requirements from `promote-spark-runner` are unchanged.

## Impact

- **Depends on:** `promote-spark-runner` (implemented, pending archive) — this change edits its leg declarations and extends its capability. It is the stage-1 outcome that change's F1 explicitly deferred to the first real run.
- **Modified code:** [tests/conformance/_spec.py](../../../tests/conformance/_spec.py) only — four leg declarations plus the consequence comment above `SPARK_SCENARIOS`. No `src/`, no `docker/`, no `.github/`, no Flink-leg and no mutation-gate changes.
- **Known follow-up, deliberately not bundled:** [tests/conformance/test_spark.py](../../../tests/conformance/test_spark.py) still carries four *executing* cell tests for the newly-skipped scenarios (its module docstring also still says three declared skips). They must be converted to declared-skip reporters via the file's existing `_declared_skip(...)` helper, exactly like the three structural skips, or the next weekly run submits a job with no scenarios in it and reports a confusing red instead of clean skips. Tracked as task 3.1 below.
- **Reporting:** the next weekly summary prints a seven-entry spark skip inventory and a not-ready verdict citing skip drift. That is the intended, legible outcome; the weekly leg keeps running so the day a bundle checkpoint handler exists, flipping declarations back to `Run()` is the whole change.
- **Gates:** `uv run pytest tests/conformance -m "not integration"` (matrix accounting and the spark-skip specificity meta-test), `uv run pytest tests/scripts/test_spark_weekly_status.py`, `uv run ruff check .`, `uv run ruff format --check .`, `openspec validate record-spark-sdf-checkpoint-gap --strict`. The spark leg itself is **not** re-run by this change — its verdict is the recorded evidence above.
