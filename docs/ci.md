# CI workflow map

Four workflows under `.github/workflows/`, one per testing tier in
[`openspec/project.md`](../openspec/project.md):

| Workflow / job      | Trigger                          | Tier                          | Required for merge |
|---------------------|-----------------------------------|--------------------------------|---------------------|
| `ci.yml`            | push to `main`, pull request      | lint, type, unit (3.11–3.12 × ubuntu) | yes |
| `integration.yml` → `integration` job | push to `main`, pull request | integration minus semantics gates (core services only: Redpanda, Redis, GCP emulators via `make compose-up-core`) | yes |
| `integration.yml` → `flink-minicluster` job | push to `main`, pull request | docker-backed semantics gates on the Flink mini-cluster (`make test-semantics` + `make test-conformance-flink`, full compose stack) | yes (add to required contexts at merge) |
| `quality.yml`       | push to `main`, pull request      | mutation (when `core/` source or tests change) + coverage ratchet | yes |
| `nightly.yml`       | schedule `0 7 * * *` UTC, manual  | mutation unconditionally; dataflow and provider smoke tests when credentials exist | no |

The two `integration.yml` jobs run in parallel and re-run independently: a
red conformance leg never blocks or re-runs the Kafka/Redis integration
tests, and vice versa. The `flink-minicluster` job pre-builds the SDK-harness
image through a buildx GHA layer cache (a `src/`-only change reuses the
third-party dependency layer) and starts compose against that
just-built image (`COMPOSE_UP_FLAGS=--wait`).

Every workflow step maps 1:1 to a `Makefile` target — see the
[`Makefile`](../Makefile) for the exact commands `ci-lint`, `ci-unit`, etc.
run locally.

## Triggering `nightly` manually

From the Actions tab, select the `nightly` workflow and use
**Run workflow**. It no-ops (via the `skip-notice` job) until the repository
variables `GCP_PROJECT_ID`, `GCP_WORKLOAD_IDENTITY_PROVIDER`, and
`GCP_SERVICE_ACCOUNT` are configured — no long-lived service-account key is
ever used.

## Making checks required

Once this repository has a GitHub remote, mark `ci`, `integration`,
`flink-minicluster`, and `quality` as required status checks on `main` under
**Settings → Branches → Branch protection rules**. `nightly` is intentionally
not required. Note the asymmetry inherited from the job split: the base job
deliberately kept the `integration` context name (renaming a required context
strands branch protection), while `flink-minicluster` is a new context that
must be *added* — until it is, the Flink gates run but are not merge-blocking.

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
