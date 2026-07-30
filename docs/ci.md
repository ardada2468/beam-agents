# CI workflow map

Five workflows under `.github/workflows/` — one per testing tier in
[`openspec/project.md`](https://github.com/ardada2468/beam-agents/blob/main/openspec/project.md),
plus the docs build:

| Workflow           | Trigger                          | Tier                          | Required for merge |
|---------------------|-----------------------------------|--------------------------------|---------------------|
| `ci.yml`            | push to `main`, pull request      | lint, type, unit (3.11–3.12 × ubuntu) | yes |
| `integration.yml`   | push to `main`, pull request      | integration + semantics (docker compose) | yes |
| `quality.yml`       | push to `main`, pull request      | mutation (when `core/` source or tests change) + coverage ratchet | yes |
| `nightly.yml`       | schedule `0 7 * * *` UTC, manual  | mutation unconditionally; dataflow and provider smoke tests when credentials exist | no |
| `docs.yml`          | push to `main`, pull request      | docs (strict `mkdocs` build; Pages deploy from `main`) | no (see the docs-workflow note) |

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
ever used.

## Making checks required

Once this repository has a GitHub remote, mark `ci`, `integration`, and
`quality` as required status checks on `main` under
**Settings → Branches → Branch protection rules**. `nightly` is intentionally
not required.

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
`integration` workflow via `make test-semantics` — the only selection that
runs it (`make test-integration` excludes semantics gates so the gate is
not paid for twice per job; removing the `test-semantics` step would
therefore silently drop the release gate) — and is budgeted ≤ 15 minutes.
`BEAM_AGENTS_E2E_EVENTS` tunes the volume down for local iteration; CI never
sets it.

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
