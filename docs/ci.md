# CI workflow map

Six workflows under `.github/workflows/` — one per testing tier in
[`openspec/project.md`](https://github.com/ardada2468/beam-agents/blob/main/openspec/project.md),
plus the docs build:

| Workflow / job      | Trigger                          | Tier                          | Required for merge |
|---------------------|-----------------------------------|--------------------------------|---------------------|
| `ci.yml`            | push to `main`, pull request      | lint, type, unit (3.11–3.12 × ubuntu) | yes |
| `integration.yml` → `integration` job | push to `main`, pull request | integration minus semantics gates (core services only: Redpanda, Redis, GCP emulators via `make compose-up-core`) | yes |
| `integration.yml` → `flink-minicluster` job | push to `main`, pull request | docker-backed semantics gates on the Flink mini-cluster (`make test-semantics` + `make test-conformance-flink`, Flink services only via `make compose-up-flink`) | yes (add to required contexts at merge) |
| `quality.yml`       | push to `main`, pull request      | mutation (when `core/` source or tests change) + coverage ratchet | yes |
| `nightly.yml`       | schedule `0 7 * * *` UTC, manual  | mutation and the [benchmark suite](benchmarks.md) unconditionally; the [Dataflow `--update` compatibility gate](#the-dataflow-update-compatibility-gate), the [fraud-triage Flex Template](#the-fraud-triage-flex-template) build and launch gate, and provider smoke tests when credentials exist | no (release-blocking) |
| `spark-weekly.yml`  | schedule `0 6 * * 1` UTC, manual  | the adapter conformance matrix's weekly Spark leg (`make test-conformance-spark`, base stack + `docker/compose.spark.yaml`) plus the promotion-window report | no (never per-PR — see [the weekly Spark leg](#the-weekly-spark-leg)) |
| `docs.yml`          | push to `main`, pull request      | docs (strict `mkdocs` build; Pages deploy from `main`) | no (see the docs-workflow note) |

The two `integration.yml` jobs run in parallel and re-run independently: a
red conformance leg never blocks or re-runs the Kafka/Redis integration
tests, and vice versa. The `flink-minicluster` job pre-builds the SDK-harness
image through a buildx GHA layer cache (a `src/`-only change reuses the
third-party dependency layer) and starts compose against that
just-built image (`COMPOSE_UP_FLAGS=--wait`).

Every workflow step maps 1:1 to a `Makefile` target — see the
[`Makefile`](https://github.com/ardada2468/beam-agents/blob/main/Makefile)
for the exact commands `ci-lint`, `ci-unit`, etc. run locally.

## The docs workflow

`docs.yml` runs `make docs` (`mkdocs build --strict`) on every pull request
and push to `main`: a broken internal link or an unresolvable example-snippet
inclusion fails the build. On pushes to `main` only, it additionally publishes
the built site to GitHub Pages via the official Pages actions
(`upload-pages-artifact` + `deploy-pages`) with a permissions-scoped
`GITHUB_TOKEN` — no `gh-pages` branch and no long-lived credential, the same
no-key posture as `nightly`.

First deployment requires a one-time repository setting outside version
control: **Settings → Pages → Source = GitHub Actions**. The check is
deliberately not merge-required initially; revisit alongside the branch
protection rules below once it has run quietly for a while.

## Triggering `nightly` manually

From the Actions tab, select the `nightly` workflow and use
**Run workflow**. It no-ops (via the `skip-notice` job) until the repository
variables `GCP_PROJECT_ID`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, and
`GCP_SERVICE_ACCOUNT` are configured — no long-lived service-account key is
ever used. The `--update` gate needs two more, `GCP_REGION` and
`GCP_DATAFLOW_TEMP_BUCKET`; without either it skips visibly rather than being
deselected, so a partially configured project produces a reported skip, not a
silent pass. The Flex Template gate needs one further variable,
`GCP_ARTIFACT_REGISTRY_REPO`, and skips the same way without it.

## The fraud-triage Flex Template

The nightly `dataflow` job builds
[`examples/fraud_triage_dataflow/Dockerfile`](https://github.com/ardada2468/beam-agents/blob/main/examples/fraud_triage_dataflow/README.md),
pushes it to Artifact Registry as `fraud-flex:<git-sha>`, and runs `gcloud
dataflow flex-template build` to publish a spec at
`gs://$GCP_DATAFLOW_TEMP_BUCKET/templates/fraud-flex-<git-sha>.json`. Both
artifacts carry the commit SHA that produced them and **no `latest` alias is
published**: nothing downstream should depend on a moving nightly artifact.
`tests/dataflow/test_flex_template_launch.py` then launches that spec — the
run's own, handed to it as `BEAM_AGENTS_FLEX_TEMPLATE_SPEC` — waits for
`JOB_STATE_RUNNING`, and cancels. It validates *packaging*: red means the
template, its parameters, or the worker image broke, not that the runtime did.

The Artifact Registry repository is **pre-provisioned by an admin**, once, not
created by the workflow — CI's service account gets writer, not admin:

```sh
gcloud artifacts repositories create beam-agents \
  --project=MY_PROJECT --location=MY_REGION --repository-format=docker \
  --description="beam-agents Dataflow Flex Template images"

gcloud artifacts repositories add-iam-policy-binding beam-agents \
  --project=MY_PROJECT --location=MY_REGION \
  --member="serviceAccount:$GCP_SERVICE_ACCOUNT" \
  --role=roles/artifactregistry.writer
```

Then set `GCP_ARTIFACT_REGISTRY_REPO` to the repository name (`beam-agents`
above). Retention is a repository-level cleanup policy rather than anything CI
does — SHA-tagged nightlies accumulate one image per green night, so a policy
deleting versions older than ~30 days keeps the last month launchable while
bounding storage. GCS template specs are small JSON objects; a matching
lifecycle rule on the `templates/` prefix does the same job.

<!-- Heading text without the `--` on purpose: this file is rendered twice,
     by mkdocs and by the documentation site, and the two slugifiers disagree
     about a run of hyphens (mkdocs collapses it, github-slugger keeps it). The
     anchor `#the-dataflow-update-compatibility-gate` is linked from this table
     and from docs/state-compat.md, and this phrasing is the one that resolves
     under both. -->
## The Dataflow update-compatibility gate

`tests/dataflow/test_update_compat.py` is the only `dataflow`-marked test and
the executable half of [`docs/state-compat.md`](state-compat.md). It launches a
streaming job at the **previous released version** of `beam-agents` (installed
from PyPI into its own venv), drives it to hold live keyed state — a key
suspended mid-activation with a persisted `Continuation` and a pending
`APPROVAL` intent, plus a key with populated working memory — then replaces the
job in place with `--update` at current head (a wheel built from the checkout),
and asserts from the output topic that the suspension resumes with its
pre-update snapshot, the memory key echoes its pre-update marker, and a fresh
key completes. Both job graphs come from one launcher module
(`tests/dataflow/_update/pipeline.py`) run by two interpreters.

It is **release-blocking, not merge-blocking**: cutting a release requires the
most recent nightly `dataflow` run to be green (see the release procedure in
[`docs/state-compat.md`](state-compat.md#release-procedure)). Cost is bounded to
one Streaming Engine worker per job inside a 35-minute test budget, teardown
force-cancels both jobs and deletes every provisioned resource on pass, fail
and timeout alike, and a sweeper cancels labelled jobs
(`beam-agents-test=update-compat`) a crashed run left behind. `make
test-dataflow` deliberately does **not** tolerate an empty selection: an empty
`dataflow` collection means the gate was deselected, not that the tier is
pending.

### Triaging a red `--update` night

Every failure is classified before it is reported, and the class is the first
thing to read:

- **`UpdateCompatibilityFailure`** — Dataflow refused the replacement graph
  (the new job fails while the old one keeps running, or the service names a
  coder/step mismatch). This is the defect the gate exists to catch: something
  in the release broke state or graph compatibility. Consult the compatibility
  table in [`docs/state-compat.md`](state-compat.md) to find which change class
  it was; do not retry.
- **`StateLossFailure`** — the update took effect but the state did not survive:
  the suspension restarted instead of resuming, the memory echo came back
  `MEMORY-LOST`, or the fresh key never completed. Equally red, equally
  non-retryable.
- **`InfraFailure`** — quota, worker-pool startup, PyPI, or credentials. The
  run says nothing about compatibility; fix the environment and rerun.

Both resolved version strings, both job ids and the service's stated reason are
embedded in every failure message, and the run's banner (printed first, and
repeated in every failure) names the mode. A banner reading
`[SELF-UPDATE (BOOTSTRAP)]` means no release existed on PyPI to update *from*,
so the run compared head with itself — real evidence about the harness and
about graph-name stability, but **not** cross-version evidence. Resource names
and the job name all embed the run id (`ba-update-compat-<date>-<suffix>`),
which is how you find the run's jobs and topics in the console.

## Making checks required

Once this repository has a GitHub remote, mark `ci`, `integration`,
`flink-minicluster`, and `quality` as required status checks on `main` under
**Settings → Branches → Branch protection rules**. `nightly` is intentionally
not required. Note the asymmetry inherited from the job split: the base job
deliberately kept the `integration` context name (renaming a required context
strands branch protection), while `flink-minicluster` is a new context that
must be *added* — until it is, the Flink gates run but are not merge-blocking.

## The benchmark lane

The nightly `bench` job runs `make bench` then `make bench-gate` and uploads
`bench-results/*.json` + `bench-report.md` as the stably named
`benchmark-report` artifact, which the release process attaches to each
release. It is **release-blocking, not merge-blocking**: `project.md` says
benchmark regressions are release blockers, and pyperf's methodology assumes a
quieter machine than a shared PR runner. The one-iteration smoke tests in
`tests/benchmarks/` ride the required `ci` lane instead, so a runtime refactor
that breaks a benchmark fails at PR time. See
[`docs/benchmarks.md`](benchmarks.md) for what each dimension measures, how to
read the report, and the baseline-update procedure.

## The weekly Spark leg

`openspec/project.md` supports DirectRunner, Dataflow, and Flink, and calls
Spark **best-effort**. `spark-weekly.yml` is what turns "best-effort" from
*unexercised* into *measured*: it runs the adapter conformance matrix's third
leg (`spark`) against a Beam Spark job server, once a week.

**It never runs on a pull request and is never a required check.** The spark
cells carry `integration + spark` and deliberately not `semantics`, so all
four per-PR selections (`make test-integration`, `make test-semantics-offline`,
`make test-semantics`, `make test-conformance-flink`) exclude them by
construction; `tests/conformance/test_spark_selection.py` fails the required
`ci` lane if that ever stops being true. The Spark services live in
`docker/compose.spark.yaml`, an overlay the base `make compose-up` never
loads, so a pull request pays nothing for Spark.

Locally:

```sh
make compose-up-spark        # base stack + the Spark overlay
make test-conformance-spark  # the leg
make compose-down-spark      # tears the overlay down too (compose-down does not)
```

Scenarios not expressible on this leg are declared skips in
`tests/conformance/_spec.py` with a reason naming the concrete missing runner
feature or harness constraint. They are still matrix cells: the meta-test
counts adapters x scenarios x three legs, so the spark leg cannot silently
shrink.

### The promotion window

Every weekly run ends with a `status` job running
`scripts/spark_weekly_status.py`, which reports (and only reports):

- the **consecutive green streak** over **scheduled** runs — `workflow_dispatch`
  runs neither extend nor break it, so investigating a red week is free;
- **cadence**: adjacent scheduled runs more than 8 days apart break the streak
  rather than bridging it, which is what turns a silently disabled schedule
  into a visibly broken window;
- **skip drift**: spark `Skip` declarations added in the trailing 28 days,
  plus the full current skip inventory with reasons;
- a `PROMOTION READY` / `NOT READY (reason)` line and a demotion-watch line.

Reruns count at their final conclusion: re-running an infrastructure failure
to green is legitimate (the harness classifies stack breakage as
`InfraFailure`, never as a Spark verdict), but the rerun must land before the
next scheduled run.

### Promotion checklist (author of the stage-2 change)

Spark flips from best-effort to supported only through a reviewed OpenSpec
change. Before opening it, confirm from the weekly job summaries alone:

1. **Four qualifying runs.** The latest summary reports a streak of at least
   `4/4` and `PROMOTION READY`. Copy the four most recent run links it lists
   into the change's proposal — no change may flip the support statement
   without citing them.
2. **Zero skip drift.** The same summary's "Skip drift in the last 28 days"
   section reads `none`. (A skip *added* in the window resets the clock; a
   long-standing skip does not.)
3. **Cadence intact.** No cadence-gap note on the streak; the four runs are
   consecutive Mondays.
4. **Surviving skips enumerated.** Copy the summary's skip inventory into the
   change and state, per skip, what the supported claim consequently does
   **not** cover on Spark. If `suspension_resume` or
   `approval_timeout_fallback` is still skipped, the leg is not exercising
   suspension or fail-closed timers and "supported" would be hollow — the
   review makes that call with the inventory in front of it.
5. **Benchmark position stated.** The supported claim inherits the latency
   budget in `openspec/project.md`. The promotion change must state whether a
   green Spark benchmark run *gates* promotion or merely accompanies it; it
   may not leave the question open.
6. **Files the change flips:** the runner-support statement in
   `openspec/project.md`, the runner-verification note in `README.md`, this
   document, and the weekly leg's required-ness (a red weekly run becomes a
   release blocker — cadence stays weekly, never per-PR).

### Demotion checklist

After promotion, two consecutive red **scheduled** weekly runs demote Spark
back to best-effort. A single red week opens an investigation and demotes
nothing — one red is too often infra, and the promotion evidence was itself
trend-based.

1. **Two consecutive reds confirmed.** The latest summary's demotion-watch
   line reads `DEMOTION TRIGGERED (2 consecutive red scheduled weeks)` and
   lists both runs. Open them and confirm each is at its final conclusion (a
   rerun to green before the next scheduled run clears that week) and that
   neither is an `InfraFailure` left un-rerun — the harness classifies stack
   breakage separately precisely so it is not counted as a Spark verdict.
2. **Files the change flips back:** the support statement in
   `openspec/project.md`, the README note, and this document; the weekly leg
   stops being required.
3. **Announce it.** The demotion goes in the release notes as well as the
   README — downstream users of the supported claim should hear about it
   rather than discover it.
4. **The leg keeps running.** Re-promotion uses the same four-week gate with
   no shortcuts and no partial credit carried across the demotion.

## Mutation-tested surface

`make mutation` generates 911 mutants across `src/beam_agents/core/`, but 452
mutants in `dofn.py` (263) and `transform.py` (189) are reported as `no tests`
and are outside the effective mutation-tested surface. Their Beam DirectRunner
tests cannot run under mutmut because mutmut's `os.wait()` child reaping
intercepts DirectRunner worker subprocesses. The per-module ceilings in
`mutation-baseline.toml` prevent this uncovered surface from growing or being
masked by improvements in another module.

A green mutation gate therefore means every executed core mutant was killed or
documented as behaviorally equivalent, and the uncovered `dofn.py` and
`transform.py` counts did not regress. It does not claim mutation coverage for
those two modules.

## The effectively-once end-to-end gate

`tests/semantics/test_effectively_once_e2e.py` is the most expensive check in
the repository: 10,000 events through real Kafka (Redpanda), `RunAgent` on the
Flink mini-cluster via the Beam job server, real `beam-agents-effector`
processes with Redis dedup, SIGKILLed effector workers, a killed TaskManager,
and a full cancel-and-resubmit replay from the ingest spool. It runs in the
`integration` workflow's `flink-minicluster` job via `make test-semantics` —
the only selection that runs it (`make test-integration` excludes semantics
gates so the gate is not paid for twice; removing the `test-semantics` step
would therefore silently drop the release gate) — and is budgeted
≤ 15 minutes. `BEAM_AGENTS_E2E_EVENTS` tunes the volume down for local
iteration; CI never sets it.

### Debugging a red `flink-minicluster` run

When any test step of a docker-backed job fails, the job runs
`make compose-logs` *before* teardown and uploads the result as a workflow
artifact (`flink-minicluster-diagnostics-attempt-<n>`, or
`integration-diagnostics-attempt-<n>` for the base job; 14-day retention),
downloadable from the run's summary page. It contains per-service
`docker compose logs` files, any TaskManager thread dumps the harness wrote
(`*-tm-threads.txt` — spool segment files are excluded), and best-effort
snapshots of the Flink REST `/jobs/overview` and `/taskmanagers` endpoints.
Green runs upload nothing. The local equivalent after a red
`make test-semantics`, while the stack is still up:

```sh
make compose-logs LOGS_DIR=compose-diagnostics
```

### Replaying a failure from its seed

The kill schedule and duplicate-publication schedule are derived from one
seed, logged at the start of every run
(`run seed=<n> … rerun with BEAM_AGENTS_E2E_SEED=<n>`). To reproduce a red
run exactly:

```sh
make compose-up
BEAM_AGENTS_E2E_SEED=<n> uv run pytest tests/semantics/test_effectively_once_e2e.py
```

### Infrastructure failure vs. invariant failure

The harness classifies every failure. `InfraFailure` (or a `RuntimeError`
naming dead workers) means the environment broke — Flink lost its slots, the
SDK worker pool died, every effector exited — and the run says nothing about
correctness: fix the stack (usually `docker compose restart flink-taskmanager
flink-jobserver beam-sdk-harness`) and rerun. A plain `AssertionError` from
the gate with healthy infrastructure IS the verdict: a duplicate or lost
execution, a lost approval, a drifting intent_id. Do not retry those away —
the zero-flake policy for this tier means every such failure gets root-caused.

Two operational facts worth knowing before debugging (established
empirically; see the change's design F8/F10): the stock Beam SDK worker pool
fails permanently after a handful of worker exits, so the gate restarts the
Flink services per run — a stack that has run many ad-hoc jobs will look
"stalled" until restarted; and an idle-deferred splittable-DoFn residual is
not re-fired after a checkpoint restore on this runner, which is why the
pipeline-kill scenario is cancel-and-resubmit-with-replay rather than
restore-and-continue.
