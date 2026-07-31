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

## Findings (stage 1 implementation)

Recorded per the `add-adapter-conformance-matrix` precedent. **The D6 spike was
not performed: no docker is available in the implementation environment, so no
Spark job server was ever started and no pipeline was submitted.** What follows
separates what was *determined* from what remains *open* — nothing below claims
Spark works.

### F1 — The spike is deferred to the first weekly runs, by design, not by omission

D6 assumed the initial declarations would be spike outcomes. They are instead
**a priori** declarations of two kinds, and the distinction is load-bearing for
the promotion gate:

* **Structural skips** — three scenarios are inexpressible on this leg for
  reasons that are properties of the harness and the overlay topology, not of
  the Spark runner, so no spike could have changed them (F2).
* **Provisional `Run()`** — the remaining four (`single_shot`,
  `multi_tool_inline`, `suspension_resume`, `approval_timeout_fallback`) are
  declared runnable *pending evidence*. The first `workflow_dispatch` or
  scheduled run IS the spike. If the Spark portable runner cannot execute the
  streaming profile, those cells go red, the weekly summary says so publicly,
  the promotion clock never starts, and the red cells are converted to `Skip`
  declarations naming the concrete gap — which, per D6, resets the window
  rather than failing CI.

This is the honest ordering: declaring them `Skip("unknown")` would have been a
worse lie than declaring them `Run()` and letting the weekly lane produce the
answer, because `Skip` reasons are required to name a *specific* constraint and
"we could not test it" is not one. What is explicitly NOT done is bending the
leg to batch mode to manufacture green (design risk 1).

### F2 — Three scenarios are structurally skipped on spark, with reasons that no runner feature can lift

| Scenario | Why it cannot run on the spark leg |
|---|---|
| `bundle_retry_cache` | The chaos commit-failure monkeypatch is in-process and cannot reach the spark-scoped `beam-sdk-harness` container. Identical to the Flink skip — and *worse* than Flink's, because on Flink the replay evidence is carried by `restart_mid_suspension`, which spark also skips (below). The spark leg therefore exercises **no** replay-after-failure path. |
| `restart_mid_suspension` | D2 chose the job server's embedded `local[4]` master, so the driver and its executors all live in the `spark-jobserver` container. Restarting it tears down the driver, which is a job resubmission, not a mid-suspension executor restart. Answers the D2/open-question "what is `restart_mid_suspension` on an embedded-master Spark?": **not expressible without dedicated master/worker containers.** |
| `ttl_expiry` | The same missing idle-partition watermark control that skips it on Flink: the spool SDF's watermark cannot be advanced past the TTL from the host side. |

Consequence for a future promotion change, stated now so the review cannot
miss it: with these three skips surviving, a "supported" claim would cover
fast-path activation, inline tools, suspension/resume, and fail-closed HITL
timers — and would **not** cover checkpoint-recovery replay, executor restart
mid-suspension, or watermark-driven TTL GC on Spark. Per the spec's *Surviving
skips bound the supported claim* scenario, the promotion change must say so.
Whether that residue is acceptable is a review decision; if it is not, the
overlay grows a real master/worker pair first (D2 follow-up).

### F3 — The Spark job server needs no new Beam options seam, but its options differ from Flink's

The pipeline is submitted through the same `PortableRunner` +
`environment_type=EXTERNAL` path as the Flink leg. Two differences, both in
`tests/conformance/_spark/pipeline.py`:

* `--checkpointing_interval` is a Flink option. The Spark side uses
  `--checkpoint_dir`, pointed at the shared `beam-artifact-staging` volume so
  the checkpoint survives for the run's duration.
* Endpoints are published on `280xx` rather than `180xx`, so both job servers
  can be up at once and a mis-pointed endpoint fails loudly instead of
  silently submitting the spark leg to Flink.

Whether the Spark runner honors `checkpoint_dir` for this pipeline shape is
part of what the first weekly runs will show.

### F4 — Health checking has no REST equivalent; the diagnosis surface is thinner

`FlinkStackControl` leans on Flink's REST API for slot counts, running jobs,
and per-vertex read/write counters — the last of which is what makes a Flink
submission stall self-diagnosing. The Beam Spark job server exposes no such
surface. `SparkStackControl` therefore health-checks by connecting to the job
and artifact ports from the host, and attaches the job server's recent log tail
to every stall and unmet-deadline message instead of vertex counters. Expect
first-run stalls to be harder to attribute than the Flink leg's were; the
`InfraFailure` classification still keeps them out of the verdict.

### F5 — `flink_variant()` generalized without a spark-specific budget

`variant_for(leg)` replaces `flink_variant()` (D1) and applies the single
real-time HITL override to both portable legs, since both wait on a wall clock
under CI load. `flink_hitl_timeout_ms` keeps its name — it is the field the
Flink measurement produced — and a spark-specific field is added only if
measurements demand it. `RULE_BUILDERS` in the Flink leg's pipeline module was
made public so both legs script their adapters' rules from one mapping; a fifth
adapter therefore joins both portable legs at once.

### F6 — Wall-clock per adapter: not measured

Task 3.5 asked for per-adapter wall-clock. Unmeasurable without docker. The
leg's per-cell timeout is budgeted at the Flink leg's 1200 s and its phase
deadline at 420 s (above Flink's, because this stack has none of Flink's
hardening); the weekly workflow is budgeted at 60 minutes for the whole job.
Both should be re-derived from the first green weekly runs.

### F7 — What the offline gates DO prove about this leg

The parts of the change that are not "does Spark work" are fully verified
offline and are what the required lanes protect:

* the three-leg matrix accounting (`test_matrix.py`, derived from
  `len(ADAPTERS) x len(SCENARIOS) x len(LEGS)`) — 4 adapters x 7 scenarios x 3
  legs = 84 cells collected, 28 of them spark;
* every scenario declares every leg, and every spark `Skip` names a specific
  constraint (`test_harness_unit.py`);
* no per-PR selection collects a spark cell, the spark selection collects
  exactly the declared 28, and no spark cell carries the `semantics` marker
  (`test_spark_selection.py`, collection-level);
* the semantics partition is byte-identical in behavior to before
  (`scripts/check_semantics_partition.py`: 65 offline + 29 docker = 94);
* the promotion-window logic — streak, cadence gaps, dispatch exclusion,
  final-conclusion reruns, skip drift, both verdicts, summary rendering — over
  offline fixtures (`tests/scripts/test_spark_weekly_status.py`, 34 tests).

### F8 — The dry run changed the summary

Task 5.3's dry run of both checklists against a synthetic 4-green window and a
synthetic 2-red window found one gap: the demotion checklist has to confirm
both red runs are scheduled runs at their final conclusion, but the summary
printed only the red streak's *length*. `render_summary` now lists the red
runs' links too. Both checklists are answerable from the summary text alone.

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
