## Context

`project.md` line 113 declares Spark best-effort, and the repository backs that with nothing: no Spark container, no Spark pipeline submission, no Spark test selection. The adapter conformance matrix (`tests/conformance/`) is the repository's existing instrument for exactly this question — it proves, per adapter and per scenario, that the runtime's keyed state + timer primitives behave identically on a runner ([_spec.py:34](../../../tests/conformance/_spec.py:34) `LEGS`, [test_matrix.py:21](../../../tests/conformance/test_matrix.py:21) meta-accounting). The Flink leg shows the full shape a portable-runner leg needs: a job-server container ([compose.yaml:121](../../../docker/compose.yaml:121)), an external SDK-harness worker pool namespace-bound to the execution container ([compose.yaml:152](../../../docker/compose.yaml:152)–171), a multiplexed one-job-per-adapter pipeline ([_flink/pipeline.py:146](../../../tests/conformance/_flink/pipeline.py:146)), a host-side responder/drainer harness ([_flink/harness.py](../../../tests/conformance/_flink/harness.py)), declared per-scenario skips with reasons ([_spec.py:277](../../../tests/conformance/_spec.py:277), [_spec.py:297](../../../tests/conformance/_spec.py:297)), and `InfraFailure` separation so stack problems never read as verdicts ([_flink_stack.py:46](../../../tests/semantics/_flink_stack.py:46)).

Two facts shape everything below:

1. **The Spark portable runner's streaming state/timer support cannot be verified from this repository offline.** Beam's Spark runner has historically had weaker portable *streaming* support than Flink (batch is the mature path), and the conformance pipeline is a streaming pipeline with checkpointing, an unbounded spool SDF source, keyed user state, and `REAL_TIME` timers. Stage 1 may legitimately end with several scenarios in `Skip` — that is the honest outcome the `Skip(reason)` vocabulary exists for, and the promotion gate is designed so that outcome parks promotion rather than faking it.
2. **Promotion is a claim about sustained behavior, not a single green run.** One green run proves the stack came up once. The differentiator claim ("runner portability") needs a trend, which is why the gate is defined over consecutive scheduled weekly runs, tracked mechanically.

## Goals / Non-Goals

**Goals:**
- Give Spark the same evidence instrument Flink has: a conformance leg with per-scenario Run/Skip declarations, documented reasons, and meta-test accounting that cannot silently shrink.
- Keep the per-PR contributor experience byte-identical: no new required checks, no added wall-clock, no spark cells in any per-PR selection.
- Make the promotion decision mechanical to *assess* (streak + skip-drift reported by the workflow itself) while keeping the flip a reviewed OpenSpec change, per the spec-driven workflow.
- Define demotion with the same rigor as promotion, before promotion ever happens.

**Non-Goals:**
- Claiming Spark works. This change lands the process; the initial leg may be mostly skips.
- Per-PR Spark CI, now or after promotion. Supported status is expressed as a *required weekly* leg (red = release blocker), not a merge gate.
- Dataflow/Flink support-statement changes, adapter changes, or any `src/` runtime changes.
- A Spark-specific effectively-once e2e gate (the 10k-event chaos gate stays Flink-only; conformance-level evidence is what promotion is defined over — revisit only after promotion).

## Decisions

### D1 — The spark leg extends the existing leg vocabulary; nothing parallel is invented

`LEGS` becomes `("direct", "flink", "spark")` and every `ScenarioSpec.legs` dict declares all three. This is deliberately the *only* way to add the leg: the meta-test computes expected cells from `ADAPTERS × SCENARIOS × LEGS` ([test_matrix.py:21](../../../tests/conformance/test_matrix.py:21)–27), so extending `LEGS` forces exactly `len(ADAPTERS) × 7` new cells to exist (as `Run` cells in `test_spark.py` or declared `Skip` cells, which are still collected and counted) — a missing declaration fails with a `KeyError` at cell build and a missing cell fails the meta-test with the exact difference. A unit test additionally asserts every scenario declares every leg, so the failure is named rather than incidental.

The per-leg deadline override generalizes rather than duplicates: `flink_variant()` ([_spec.py:143](../../../tests/conformance/_spec.py:143)) becomes a leg-parameterized `variant_for(leg)` (the flink and spark legs share the same real-time-HITL concern for the same reason — wall clocks under CI load), keeping `flink_hitl_timeout_ms` as the single real-time override rather than minting a spark-specific field. If the spike (D6) shows Spark needs different budgets, a spark override field is added then, with measurements.

### D2 — Spark stack: a dedicated compose overlay, not services in the base compose

Spark services live in `docker/compose.spark.yaml` (started as `docker compose -f docker/compose.yaml -f docker/compose.spark.yaml up`), never in the base file. Rationale: `make compose-up` runs on every per-PR `integration` job with `--wait --build` ([Makefile](../../../Makefile) `compose-up`), so anything in the base file is paid for on every PR — the weekly cadence decision (D3) would be undone at the infrastructure layer. The overlay holds:

- **The Beam Spark job server** (`apache/beam_spark3_job_server`, tag matching the repo's Beam pin — `2.72.0`, digest-pinned like every other image in [compose.yaml](../../../docker/compose.yaml)), initially running its embedded local master (`--spark-master-url=local[4]`) — no separate Spark master/worker containers until a scenario needs a real worker restart (see D6 / Open Questions on `restart_mid_suspension`).
- **A spark-scoped SDK-harness worker pool** reusing the existing `beam-agents-sdk-harness` image, `network_mode`-bound to the container that executes work — the same load-bearing pattern documented at [compose.yaml:152](../../../docker/compose.yaml:152)–171. With an embedded master, executors run inside the job-server container, so the pool binds to *it*, not to the Flink TaskManager (which is why the existing harness service cannot be shared).
- The shared `beam-artifact-staging` volume, for the same staged-artifact-path reason as the Flink trio ([compose.yaml:188](../../../docker/compose.yaml:188)–190).

`make compose-up-spark` / `compose-down-spark` wrap the two-file invocation; the weekly workflow uses them.

### D3 — Weekly, not per-PR

The spark leg runs on a weekly `schedule` (plus `workflow_dispatch` for iteration), never on `pull_request`. Three reasons, in order:

1. **A best-effort runner must not gate contributors.** Until the promotion gate passes, a red spark run says something about Spark, not about the PR under review. Putting it in a per-PR workflow — even non-required — trains people to ignore red, which poisons the very signal promotion needs.
2. **The promotion evidence is defined over scheduled runs.** "Four consecutive weeks of green" is a statement about sustained health under a fixed cadence. Per-PR runs fire at arbitrary rates (dozens on a busy week, zero on a quiet one) and would make "consecutive" meaningless; scheduled runs give the window an unambiguous clock. This is also why `workflow_dispatch` runs are *excluded* from the streak (D5).
3. **Cost and flake maturity.** The Flink leg budgets ~20 minutes per adapter ([test_flink.py:44](../../../tests/conformance/test_flink.py:44)) on a stack whose failure modes took real effort to tame (submission stalls, metaspace exhaustion, worker-pool decay — see [docs/ci.md](../../../docs/ci.md)). The spark stack starts with zero of that hardening; its early flakes belong in a weekly lane where they produce findings, not PR friction.

After promotion the cadence stays weekly — "supported" upgrades the *consequence* of red (release blocker, demotion clock starts), not the frequency. Per-PR spark would double every PR's integration wall-clock for a runner no CI-blocking artifact deploys to.

### D4 — Marker taxonomy: spark cells are `integration + spark`, not `semantics` (while best-effort)

A new `spark` marker joins the closed registry in [pyproject.toml:396](../../../pyproject.toml:396). Spark leg cells carry `integration + spark` (plus the `conformance_cell(adapter, scenario, "spark")` marker the inventory hook counts). They deliberately do **not** carry `semantics` yet: `project.md` defines the semantics tier as correctness gates that "gate every release and never get skipped or marked flaky" ([project.md:75](../../project.md)) — a best-effort leg cannot make that promise, and pretending otherwise would either dilute the tier or force spark red to block releases before any evidence exists.

Selection consequences, each explicit:
- `make test-conformance-spark` = `-m "integration and spark" tests/conformance` — the weekly workflow's selection (same no-exit-5 stance as the flink target at [Makefile:54](../../../Makefile:54): an empty selection is a deselected leg, not a pending one).
- `make test-integration` ([Makefile](../../../Makefile)) gains `and not spark` so per-PR integration jobs cannot accidentally run the leg.
- `make test-semantics`, `make test-semantics-offline`, `make test-conformance-flink`, and `scripts/check_semantics_partition.py` are untouched — spark cells are invisible to the semantics partition by construction.
- A collection-level unit test pins this: the per-PR selections collect zero spark-leg cells, and the spark selection collects exactly the declared ones. Marker taxonomy drift is otherwise silent.

On promotion, the cells stay `integration + spark` — they still must not enter the per-PR partition (D3) — and required-ness is expressed by the weekly workflow's status handling, not by markers.

### D5 — The promotion window is tracked mechanically by the workflow; the flip is a reviewed change

The weekly workflow ends with a `status` job (`if: always()`, `needs` the test job) running `scripts/spark_weekly_status.py`, which:

1. Queries the GitHub API for this workflow's recent runs (`/repos/{repo}/actions/workflows/spark-weekly.yml/runs?event=schedule&status=completed`), **scheduled runs only** — `workflow_dispatch` iterations neither extend nor break the streak.
2. Computes the current **consecutive-green streak** walking backward from the most recent scheduled run (including the in-flight run's own test-job conclusion, passed in from `needs`), where green = final conclusion `success`. Reruns count at their final conclusion: re-running an `InfraFailure` week to green is legitimate — the harness's infra/verdict separation exists precisely so stack breakage is not a Spark verdict — but the rerun must land before the next scheduled run to count.
3. Enforces **cadence**: consecutive means weekly. Two adjacent streak entries more than 8 days apart break the streak — this converts the known GitHub failure mode where scheduled workflows are silently disabled after 60 days of repository inactivity (and ordinary scheduler outages) into an explicit broken window instead of a phantom streak.
4. Scans for **skip drift**: `git log --since='28 days ago' -p -- tests/conformance/_spec.py`, flagging any added spark-leg `Skip` declaration in the window. It also prints the current spark skip inventory (scenario → reason) so week-over-week summaries make drift visible even if the diff scan misses a refactor-shaped edit.
5. Writes streak length, cadence verdict, skip inventory/drift, and a final `PROMOTION READY` / `NOT READY (reason)` line to `$GITHUB_STEP_SUMMARY`.

**Honest limits, stated rather than papered over:** the script reports; it does not and must not merge anything. The flip itself is a stage-2 OpenSpec change whose review re-verifies the evidence (run links for the 4 weeks, the skip-drift scan) — consistent with the spec-driven rule that `project.md`/README claims change only through a reviewed change. The diff-based skip scan is a heuristic (a rename-and-re-add could evade it); the promotion review is the backstop, and the weekly inventory print makes evasion visible in the summaries. The API window also cannot distinguish "schedule disabled by GitHub" from "repo went quiet" — both correctly read as a broken window.

The script is pure-Python over JSON it is handed (API pages, git log text, the current `_spec.py` declarations), so its logic — streak, cadence gaps, dispatch exclusion, drift detection — is unit-testable offline with fixtures, and is written test-first like everything else.

### D6 — Initial Skip policy: skips are findings, frozen by the window, and never a promotion blocker by themselves

The stage-1 spike drives each scenario against the spark leg and records the outcome as its declaration: `Run()` where it passes, `Skip("<the concrete missing runner feature or harness constraint>")` where it cannot — the same discipline as the two existing Flink skips ([_spec.py:277](../../../tests/conformance/_spec.py:277), [_spec.py:297](../../../tests/conformance/_spec.py:297)). Rules:

- Every spark `Skip` reason names a *specific* constraint (e.g. "the Spark portable runner does not deliver REAL_TIME timer firings in streaming mode as of Beam 2.72" or "in-process chaos monkeypatch cannot reach the sdk-harness container" — the latter already skips `bundle_retry_cache` on Flink and applies to Spark identically). "Doesn't work" is not a reason.
- The promotion criterion is **zero skips *added* during the 4-week window**, not zero skips. A scenario that is leg-inexpressible forever (in-process chaos) stays skipped without parking promotion. But the promotion change must *enumerate* the surviving skips, because they bound the supported claim: if `suspension_resume` or `approval_timeout_fallback` is still skipped, the leg is not exercising suspension or fail-closed timers, and "supported" would be hollow — the promotion review makes that call with the inventory in front of it, and the spec requires the promotion change to state what the surviving skips exclude from the claim.
- Adding a skip mid-window is *allowed* (it is the honest response to a discovered gap) — it resets the promotion clock via D5's drift scan, it does not fail CI.

### D7 — Demotion: two consecutive red scheduled weeks, announced

After promotion, the same status script watches the inverse condition: **two consecutive red scheduled weekly runs** (final conclusions, cadence rules identical to D5) trigger a demotion change that flips [project.md:113](../../project.md) and the README back to best-effort, with an announcement in the release notes/README so downstream users of the supported claim hear about it rather than discover it. One red week alone opens an investigation issue but demotes nothing — a single red is too often infra (image drift, runner outage, upstream Beam regression) and the promotion evidence was itself trend-based, so the demotion trigger is a trend too. Asymmetry (4 green to enter, 2 red to leave) is deliberate: the cost of wrongly claiming support exceeds the cost of wrongly withdrawing it. The weekly leg keeps running after demotion; re-promotion uses the unchanged 4-week gate.

### D8 — Spec-delta sequencing with `add-adapter-conformance-matrix`

This change modifies the `adapter-conformance-matrix` capability, whose spec currently exists only as the pending `add-adapter-conformance-matrix` change's `ADDED` delta (implementation complete, archive pending — `openspec/specs/adapter-conformance-matrix/` does not exist yet). This change's `MODIFIED`/`RENAMED` delta is written against that delta's exact requirement headers and **must archive after it**. The pre-archive check in `tasks.md` re-reads the main spec at archive time and confirms the base requirement text is present (the `drop-macos-ci-matrix-leg` D4 rule, applied to an ADD→MODIFY chain rather than a MODIFY/MODIFY collision).

## Risks / Trade-offs

- **The Spark portable runner may not run this shape of pipeline at all.** Streaming + unbounded SDF source + keyed user state + REAL_TIME timers is the hardest profile a portable runner can be asked for, and Spark's portable streaming support has historically trailed Flink's. Worst case, the spike ends with `single_shot` itself skipped and the leg is a scaffold around findings. That outcome is *accepted and designed for*: the leg still lands (declarations, harness, workflow), the summary reports it, promotion stays parked, and the findings feed upstream-Beam issues. What is explicitly rejected is bending the leg to batch mode to manufacture green — batch would not exercise timers, checkpoint recovery, or suspension across bundles, which are the substance of the supported claim.
- **Weekly cadence means slow feedback both ways**: a Spark-breaking runtime change surfaces up to 7 days later, and promotion takes a minimum of 4 weeks + a review. Accepted — this is the cost of a trend-based claim, and `workflow_dispatch` exists for on-demand investigation without polluting the streak.
- **Streak tracking depends on GitHub Actions history retention and API behavior.** Run history retention (90 days default) comfortably covers a 4-week window, but the cadence check (D5.3) is what protects against the silent-schedule-disable failure mode; if the API shape changes, the script's fixtures-first unit tests localize the breakage.
- **Marker/selection drift**: a future `-m` expression edit could leak spark cells into a per-PR lane or orphan them entirely. Mitigated by the collection-level selection test in D4 — the drift fails a unit test, not a code review.
- **Compose overlay drift**: the spark job-server tag must track the repo's Beam version pin (currently `2.72.0` everywhere); a version skew between job server, SDK harness image, and `apache-beam` produces confusing portable-protocol errors. The overlay carries a comment tying the tag to the pin, and the harness's `InfraFailure` classification keeps skew from reading as a Spark verdict.
- **Two pending deltas to one capability** (D8): archiving this change before `add-adapter-conformance-matrix` would target a nonexistent spec. Mitigation is the ordered pre-archive check in `tasks.md`.

## Migration Plan

1. **Stage 1 lands** (this change's tasks): leg vocabulary, spike findings recorded as declarations, spark harness + overlay, weekly workflow + status script, Makefile/marker/docs wiring. No support-statement change; per-PR CI byte-identical.
2. **Observation window**: the workflow runs weekly; summaries accumulate streak/skip evidence. Red weeks and skip additions are handled per D5/D6 (rerun infra failures, file findings, clock resets are automatic).
3. **Stage 2 — promotion change** (separate OpenSpec change, authored when the summary says `PROMOTION READY`): flips [project.md:113](../../project.md), README, `docs/ci.md`; marks the weekly leg required (red = release blocker, demotion clock armed); cites the 4 run links and the skip inventory with what it excludes from the claim.
4. **Steady state / demotion**: status script watches the red-streak condition; two consecutive red scheduled weeks produce the demotion change per D7.

**Rollback (stage 1):** delete the workflow, overlay, `_spark/` package, `test_spark.py`, and the spark entries in `LEGS`/declarations/Makefile/markers — the meta-test and declaration-completeness test shrink back mechanically; no state, schema, or `src/` surface is involved. **Rollback (stage 2)** is the demotion path itself.

## Open Questions

- **Which state/timer primitives does the Spark portable runner actually support in streaming mode on Beam 2.72?** Specifically: Python `ReadModifyWriteStateSpec`/`BagStateSpec`/`CombiningValueStateSpec`, `WATERMARK` timer firings on watermark advance from an unbounded SDF source, and `REAL_TIME` timer firings under wall clock. Cannot be verified offline from this repository; the stage-1 spike answers it per-scenario and the answers become the initial declarations.
- **Does the spool SDF source behave on Spark?** The Flink leg's two empirical findings (worker-pool decay after worker exits; idle residuals not re-fired after restore — [docs/ci.md](../../../docs/ci.md)) are runner-specific; Spark will have its own list, and the freshness machinery Spark needs may differ from `FlinkStackControl`'s.
- **What is `restart_mid_suspension` on an embedded-master Spark?** A real executor restart may require dedicated master/worker containers (rejected initially in D2 for cost). If the embedded topology cannot express the restart, the scenario starts as a documented `Skip` and the promotion review weighs it per D6 — or the overlay grows a worker container in a follow-up.
- **Does benchmark evidence gate promotion or accompany it?** The supported claim inherits the latency budget ([project.md:111](../../project.md)), and `add-benchmark-harness` (C33) provides the instrument — but whether a spark benchmark run must be green *before* promotion, or is attached as evidence with the budget enforced from promotion onward, is left to the promotion change's review. This proposal requires only that the promotion change state which position it takes.
- **Weekly cron slot**: proposed `0 6 * * 1` (Mondays 06:00 UTC, an hour before nightly's `0 7 * * *` to avoid runner contention on self-hosted futures); any weekly slot works — the streak logic keys on `event=schedule`, not the hour.
