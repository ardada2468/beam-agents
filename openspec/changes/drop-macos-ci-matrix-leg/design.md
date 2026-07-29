## Context

`ci.yml` is the only workflow in the repository with a non-Linux leg. Its matrix is `["3.11", "3.12"] × [ubuntu-latest, macos-latest]`, and each of the four legs runs `make lint`, `make type`, `make test-unit`, `make test-semantics-offline` after a `setup-uv` install. `integration.yml`, `quality.yml`, and all four jobs in `nightly.yml` are `ubuntu-latest`.

Two facts set the cost/benefit:

1. **Billing asymmetry.** GitHub's hosted-runner price list applies a 10× per-minute multiplier to `macos-latest` relative to `ubuntu-latest`. With four legs of roughly equal wall time, macOS accounts for ~10/12 ≈ 83% of `ci.yml`'s billed minutes. `ci` is required, so this is charged per PR, per push to `main`, and per re-run.
2. **No platform surface to cover.** The repository contains no platform-conditional code. `grep -rn "sys.platform\|platform.system\|darwin\|uname"` over `src/` and `tests/` returns nothing; `src/beam_agents/core/*.py` has no `fork`, `spawn`, `multiprocessing`, or `signal` usage. The async bridge is a plain `threading` + `asyncio` loop with `httpx` pools — portable by construction. The macOS legs therefore execute the same code paths as the Linux legs, over pure-Python and Beam `TestPipeline` tests, on a platform we never deploy to (Dataflow, Flink, and Spark workers are Linux).

The 83% figure is derived from the published multiplier and an assumption of comparable per-leg wall time, not from the account's invoice. It is directionally certain — macOS is by far the dominant line item — but the exact share should be read off GitHub's billing page during implementation rather than trusted from this document.

**Constraint that dominates the rollout:** `ci` is a required status check. GitHub matrix jobs report one status context per leg (`ci (3.11, macos-latest)`), and a branch-protection rule that names a context which no longer runs leaves that context pending forever, blocking every PR with no way to merge the fix through the normal path. This is the single failure mode capable of taking the repository down, and it lands *after* merge, not in CI.

## Goals / Non-Goals

**Goals:**
- Eliminate macOS runner minutes from GitHub Actions entirely.
- Keep lint, type, unit, and offline-semantics coverage identical on both supported Python versions.
- Leave `main` mergeable throughout — no window where a required check cannot report.
- Keep the `repo-scaffolding` spec, `docs/ci.md`, and `ci.yml` mutually consistent, including across the concurrently-pending `enforce-mutation-gate-on-core` change.

**Non-Goals:**
- Reducing CI cost anywhere else (cache tuning, `fail-fast`, concurrency limits, job consolidation). Those are independent and separately justified.
- Changing the Python version matrix, the `make` targets, the testing tiers, or which checks are required.
- Supporting or de-supporting macOS as a *development* platform. Contributors keep running the full Makefile locally on macOS; only automated verification of it ends.
- Adding arm64 CI coverage. Discussed below and deliberately deferred.

## Decisions

### D1: Delete the `os` matrix dimension rather than exclude macOS legs

`ci.yml` becomes `os: [ubuntu-latest]` — or equivalently, drops `os` and hardcodes `runs-on: ubuntu-latest`.

Chosen: **keep `os: [ubuntu-latest]` as a one-element matrix.** Retaining the dimension keeps `runs-on: ${{ matrix.os }}` and, critically, keeps the *shape* of the generated status-check context names (`ci (3.11, ubuntu-latest)`) identical to today's Linux legs. Collapsing to a bare `runs-on: ubuntu-latest` would rename those contexts to `ci (3.11)`, invalidating branch-protection entries for the legs we are *keeping* — turning a two-context cleanup into a four-context one, for a cosmetic gain.

Rejected: `matrix.exclude` on the macOS entries. It leaves the expensive label in the file where a future edit can resurrect it, and it reads as "temporarily disabled" rather than "removed", which is not what this change decides.

### D2: No macOS coverage anywhere, including nightly

The obvious compromise — one macOS leg in `nightly.yml`, non-required — is rejected.

A nightly canary still pays the 10× multiplier every day, forever, and produces a signal with no owner: `nightly` is explicitly not a required check, and nothing in the repository's process obliges anyone to read a red non-required workflow. The realistic outcome is a recurring charge for a job whose failures get ignored, which is worse than either alternative. Given zero platform-conditional code, the expected number of genuine macOS-only defects it would catch is near zero.

The reversal condition is stated rather than pre-paid: **if a macOS-specific defect is ever reported by a contributor, reintroduce a macOS leg with that defect as its justification.** Evidence first.

### D3: Accept the arm64 gap; do not close it in this change

`macos-latest` is arm64 and `ubuntu-latest` is x86_64, so this change also removes the repository's only ARM leg. That is the most substantive coverage loss here, and it is worth naming precisely: what we lose is *architecture* coverage, not *macOS* coverage.

It is not closed here because (a) no dependency in the stack is known to be arch-sensitive in a way unit tests would catch — the arch-sensitive artifacts are wheels, and `uv.lock` resolution is verified on every leg regardless; (b) deployment targets are x86_64 Linux; and (c) `ubuntu-24.04-arm` is available as a hosted label at a fraction of macOS pricing, so closing the gap later is a one-line change costing nothing to defer.

Recorded here so that a future "we lost ARM coverage" observation lands on a decision rather than an oversight.

### D4: Spec delta merge rule with `enforce-mutation-gate-on-core`

Both this change and the pending `enforce-mutation-gate-on-core` carry a `MODIFIED` delta for the same requirement — *GitHub Actions workflows mirror the testing tiers* — and OpenSpec `MODIFIED` blocks replace the entire requirement body. Archiving them in either order naively means the second archive silently reverts the first.

The edits are disjoint in content: this change rewrites only the `ci.yml` OS clause; the other rewrites only the `quality.yml` and `nightly.yml` mutation clauses. So the rule is: **whichever change archives second must first re-read `openspec/specs/repo-scaffolding/spec.md`, confirm the other change's clause is present, and include it verbatim in its own delta before archiving.** `tasks.md` carries this as an explicit pre-archive check. Deferring one change until the other archives is unnecessary — the conflict is textual, not semantic.

### D5: Leave the `make lint type test-unit` wording drift alone

The spec requirement says `ci.yml` executes `make lint type test-unit`, but `ci.yml` also runs `make test-semantics-offline` (a required offline check per `project.md`). This drift predates the change and is present identically in the other pending delta.

Not fixed here. Correcting it would widen this change's delta into the exact clause `enforce-mutation-gate-on-core` is rewriting, creating a genuine content conflict where D4 currently has only a mechanical one. It should be fixed in a follow-up once both changes have archived, and is noted in Open Questions so it does not get lost.

## Risks / Trade-offs

- **Branch protection names per-leg contexts → every PR blocks indefinitely after merge.** This is the one way the change can break the repository, and D1 already halves the exposure by preserving the surviving legs' context names. Mitigation: inspect `main`'s required-checks list *before* merging. If it names `ci (3.11, macos-latest)` / `ci (3.12, macos-latest)`, remove those two entries in the same maintenance window as the merge. `docs/ci.md` suggests protection may be configured at the workflow level (`ci`, `integration`, `quality`), in which case nothing needs doing — but this must be verified, not assumed. Rollback if hit: revert the `ci.yml` commit; the contexts start reporting again on the next push.
- **A macOS-only regression reaches `main` unnoticed.** Accepted, and small: no platform-conditional code, pure-Python and TestPipeline tests, Linux-only deployment. The plausible vector is a transitive dependency behaving differently on Darwin — which surfaces on a contributor's machine (where the full Makefile still runs) before it can affect anything deployed. Reversal condition is D2.
- **Loss of the only arm64 leg.** See D3. Mitigation on demand: add `ubuntu-24.04-arm` to the `os` matrix, which the one-element-matrix shape from D1 makes a single-line edit.
- **Silent spec revert between the two pending changes.** See D4. Mitigation is the pre-archive check in `tasks.md`; the failure is detectable by diffing the archived spec against both deltas.
- **Losing a genuine cost datapoint.** After this merges we can no longer measure what macOS CI *would* have cost. Irrelevant in practice — the multiplier is published and the decision is reversible.

## Migration Plan

1. Read `main`'s branch-protection required-check list and record the exact contexts (blocking prerequisite — everything else depends on what this shows).
2. Edit `ci.yml`, `docs/ci.md`.
3. Open the PR. Confirm exactly two `ci` legs run and all four workflows are green.
4. If step 1 found per-leg macOS contexts, remove them from branch protection *before* merging.
5. Merge. Verify the next PR against `main` is mergeable.
6. Read GitHub's billing page after one week to confirm the macOS line item is gone.

Rollback: revert the single commit. No state, schema, or dependency change is involved, so revert is complete and immediate.

## Open Questions

- **What is the actual measured saving?** The 83% share is derived, not invoiced. Step 6 of the migration plan settles it. Nothing in the change depends on the answer.
- **Is branch protection configured per-leg or per-workflow?** Unknown from inside the repository; `docs/ci.md` implies per-workflow but describes an aspiration ("Once this repository has a GitHub remote…"). Resolved by step 1, and it is a blocking prerequisite precisely because it cannot be answered from the code.
- **When is the `test-semantics-offline` wording drift (D5) fixed?** Follow-up change after both pending `repo-scaffolding` deltas archive.
