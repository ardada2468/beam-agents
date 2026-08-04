## Why

Two CI gaps, one per surface:

1. **The release process does not attach the benchmark report it is required to attach.** The benchmark-harness spec ([add-benchmark-harness spec.md](../add-benchmark-harness/specs/benchmark-harness/spec.md)) says "Each release SHALL attach the most recent green benchmark report; the attach step belongs to the release process, which consumes this artifact by its stable name", and [docs/benchmarks.md](../../../docs/benchmarks.md) repeats it. The producing half exists — nightly's `bench` job uploads `bench-results/*.json` plus `bench-report.md` as the stably named `benchmark-report` artifact ([nightly.yml](../../../.github/workflows/nightly.yml)) — but nothing consumes it: [release.yml](../../../.github/workflows/release.yml)'s `gh release create` step attaches release notes and nothing else. The latency budget is a release blocker per `project.md`, and today that blocker has no machine teeth at release time; the "latest nightly is green" line in the release checklist is a purely human check.
2. **The console UI has zero CI.** `frontend/` (the Vite + React SPA for the agent console, npm + `package-lock.json`) defines `lint`, `typecheck`, and `build` scripts that no workflow runs. A PR can merge an ESLint violation, a type error, or a change that breaks `vite build` outright, and nothing goes red. `website.yml` is the closest sibling but covers only the pnpm-managed docs site under `website/` — its path filters never see `frontend/**`.

## What Changes

- **`release.yml` attaches the most recent green benchmark report, and fails closed without one.** Two steps added to the `publish` job, deliberately *before* the PyPI publish step (PyPI filenames are immutable — a missing report must abort the release while nothing irreversible has happened):
  1. *Locate*: walk recent `nightly.yml` runs on `main` (`gh run list`) and take the newest whose **`bench` job** concluded `success`, read via the jobs API (`gh api .../runs/<id>/jobs`). The bench job's own conclusion is the filter because both cheaper proxies are wrong: nightly's `dataflow`/`smoke` tiers can be red for reasons that say nothing about latency (so the *run* conclusion under-selects), and the artifact uploads `if: always()` (so artifact *existence* over-selects a red bench's report). If no green bench job exists in the window, the step errors with remediation (dispatch `nightly.yml`, get a green bench, re-run) and the release never reaches PyPI.
  2. *Download and attach*: `gh run download <id> --name benchmark-report`, assert `bench-report.md` is present and non-empty, zip the pyperf JSONs, and pass both as assets to the existing `gh release create` invocation.
  The `publish` job's permissions gain `actions: read` (needed by `gh run list`/`gh run download`; `GITHUB_TOKEN` in the same repository carries it once granted).
- **New `.github/workflows/frontend.yml`.** One `frontend` job on `ubuntu-latest`, path-filtered to `frontend/**` plus the workflow file itself, on `pull_request` and push-to-`main`: `setup-node` (Node 22 — the same line `website/.nvmrc` pins; `frontend/` has no `.nvmrc` and declares no `engines`) with `cache: npm` keyed on `frontend/package-lock.json`, then `npm ci`, `npm run lint`, `npm run typecheck`, `npm run build` as separate steps for step-level failure attribution. Unlike `website.yml` it does not trigger on `src/` or `docs/` — nothing outside `frontend/` changes what these steps see.
- **Not changing:** `nightly.yml` (the `benchmark-report` artifact name and contents are consumed exactly as published), the release verification roster, `docs/` (the releasing checklist's mention of the attach step is owned elsewhere), branch protection (adding the `frontend` context to required checks is an operational step, called out in tasks), and no test or `src/` file of any kind.

## Capabilities

### New Capabilities

- `frontend-ci`: the CI contract for the console UI — every change to `frontend/` is gated by a lockfile-faithful install and the package's full verification surface (lint, typecheck, production build) as distinguishable steps, and changes elsewhere in the repository do not pay for it.

### Modified Capabilities

None. The release-attach work *implements* the benchmark-harness requirement ("Each release SHALL attach the most recent green benchmark report … by its stable name") exactly as written; the requirement itself is not touched, so no delta is stacked on `add-benchmark-harness` (pending archive). The one judgment call the spec text leaves open — what "green" means when the artifact-producing job and its sibling jobs can disagree — is resolved in the workflow comment on the locate step: green is the `bench` job's conclusion, which is the strictly faithful reading (the spec's scenario has "the nightly workflow reports failure and the release process refuses to tag until a green run exists" for the *bench gate*, and only the bench job runs that gate).
<!-- Rationale: one genuinely new capability (frontend CI); the release half is additive wiring that discharges an existing, unmodified requirement — same stance add-flink-minicluster-ci took toward the gates whose wiring it restructured. -->

## Impact

- **Depends on** `add-benchmark-harness` (implemented, pending archive) — the `bench` nightly job and the `benchmark-report` artifact it uploads are what the release step locates and downloads.
- **Modified code:** [.github/workflows/release.yml](../../../.github/workflows/release.yml) only — `publish` job gains `actions: read` and the locate/download steps; `gh release create` gains two asset arguments. No `src/` changes, so no changelog fragment is required (`changelog-fragment-required` gates `src/` edits).
- **New code:** [.github/workflows/frontend.yml](../../../.github/workflows/frontend.yml).
- **Operational follow-ups (user actions, not in this change):** add the `frontend` status-check context to `main`'s required checks once the first run has reported; the attach path is first exercised for real by the next `v*` tag (or a TestPyPI rehearsal will *not* exercise it — the `publish` job runs only on tag push, by design).
- **Gates:** no gate weakens; the release gains a new fail-closed condition (no green bench report ⇒ no release), which is the spec's stated intent. Local verification of the frontend scripts (all four green on the current tree) is recorded in tasks.
