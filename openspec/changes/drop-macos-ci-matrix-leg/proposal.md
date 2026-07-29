## Why

`ci.yml` runs a 2×2 matrix — Python `3.11`/`3.12` × `ubuntu-latest`/`macos-latest` — on every push to `main` and every pull request. GitHub bills its hosted macOS runners at a **10× per-minute multiplier** versus Linux (`macos-latest` arm64 vs `ubuntu-latest`), so the two macOS legs are **~83% of this workflow's billed minutes while being 50% of its legs**. `ci` is a required check, so that cost is incurred on every PR and every re-run.

The coverage those legs buy is close to zero. There is **no platform-conditional code in the repository**: `grep` for `sys.platform`, `platform.system`, `darwin`, or `uname` across `src/` and `tests/` returns nothing, and `core/` contains no `fork`/`spawn`/`signal` handling. The macOS legs execute the same `make lint type test-unit test-semantics-offline` over the same pure-Python and TestPipeline code paths as the Linux legs, differing only in the OS underneath a `setup-uv` install. Every tier that could plausibly be OS-sensitive — `integration` (docker compose), `quality`, `nightly` (Dataflow) — already runs on `ubuntu-latest` only, and production targets (Dataflow, Flink, Spark workers) are Linux. We are paying a 10× premium to re-run Linux-equivalent tests on a platform we do not deploy to.

## What Changes

- **Remove `macos-latest` from the `ci.yml` matrix.** `os` collapses from `[ubuntu-latest, macos-latest]` to `[ubuntu-latest]`, taking `ci` from four legs to two. Lint, type, unit, and offline-semantics coverage is unchanged — the same four `make` targets still run on both supported Python versions.
- **Do not reintroduce macOS anywhere else.** No nightly macOS canary, no scheduled macOS leg. A canary that nobody is required to act on would keep paying the 10× multiplier for a signal with no owner; if a macOS-specific defect ever appears, this decision gets revisited with evidence rather than pre-paid against.
- **Accept the loss of the repo's only arm64 leg, and record it.** `macos-latest` is arm64; `ubuntu-latest` is x86_64. Dropping it means CI stops exercising ARM. That is an architecture gap, not a macOS gap, and `ubuntu-24.04-arm` is the cheap way to close it if it ever bites — evaluated in `design.md` and deliberately not adopted here (see below).
- **Update `docs/ci.md`** so the workflow map states `3.11–3.12 × ubuntu` instead of `3.11–3.12 × ubuntu/macos`.
- **Verify branch-protection required checks still resolve.** If `main` requires per-leg check names (`ci (3.11, macos-latest)`), those contexts stop reporting after this change and every PR blocks indefinitely. This is an operational step, not a code change, and it is the only way this change can break the repository.
- Not changing: the Python version matrix, the `make` targets each leg runs, the `integration`/`quality`/`nightly` workflows, or which checks are required for merge.

Local macOS development is unaffected — `uv`, the Makefile targets, and the docker-compose stack continue to work on developer machines. What ends is *automated verification* of macOS on every PR.

## Capabilities

### New Capabilities

None. This change removes a matrix dimension; it introduces no new behavior.

### Modified Capabilities

- `repo-scaffolding`: The GitHub-Actions-workflows requirement currently mandates that `ci.yml` run its matrix on **`ubuntu-latest` and `macos-latest`**. It must now mandate `ubuntu-latest` only. No other clause of that requirement changes.

## Impact

**Code / config**
- [.github/workflows/ci.yml:22](.github/workflows/ci.yml:22) — the single line carrying `macos-latest`.
- [docs/ci.md:8](docs/ci.md:8) — workflow-map table cell.
- [openspec/specs/repo-scaffolding/spec.md:128](openspec/specs/repo-scaffolding/spec.md:128) — updated via this change's delta spec at archive time.

**Coordination — this matters**
The pending change `enforce-mutation-gate-on-core` carries its own delta to the *same* `repo-scaffolding` requirement (rewriting the `quality.yml` and `nightly.yml` clauses in the same paragraph). Both deltas replace the full requirement text, so whichever archives second must carry the other's edits forward or it will silently revert them. Sequencing and the merge rule are specified in `design.md`.

**Not affected**
- No `src/` or `tests/` changes; no dependency, API, or wire-schema changes.
- `integration`, `quality`, and `nightly` workflows are untouched — all already `ubuntu-latest`.
- Runner support (DirectRunner, Dataflow, Flink, Spark) and the supported-Python contract are unchanged; macOS was never a deployment target.

**Risk**
- **Low, single-vector**: a macOS-only regression could now reach `main` unnoticed. With zero platform-conditional code and pure-Python + Beam TestPipeline unit tests, the realistic exposure is a transitive dependency behaving differently on Darwin — which would surface on a developer's machine before it could affect a Linux deployment.
- **Medium, one-time**: the branch-protection trap above. Mitigated by verifying required contexts before merging.
