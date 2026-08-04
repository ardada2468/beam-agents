## 1. Pre-flight: branch protection (blocking)

- [x] 1.1 Read `main`'s branch-protection required status checks (`gh api repos/:owner/:repo/branches/main/protection --jq '.required_status_checks.contexts'`, or Settings → Branches) and record the exact context strings in the PR description
- [x] 1.2 Classify the result: **per-workflow** (`ci`, `integration`, `quality`) → nothing to undo later; **per-leg** (`ci (3.11, macos-latest)` etc.) → tasks 4.1–4.2 become mandatory before merge
- [x] 1.3 If the repository has no remote or no protection rule configured yet, record that explicitly so §4 can be skipped for a stated reason rather than forgotten

**Finding (2026-07-28), `ardada2468/beam-agents`, for the PR description:**

Protection on `main` exists and names checks **per-leg** — §4 is **mandatory**, not optional:

```
required_status_checks.contexts = [
  "ci (3.11, ubuntu-latest)",  "ci (3.12, ubuntu-latest)",
  "ci (3.11, macos-latest)",   "ci (3.12, macos-latest)",
  "integration",               "quality"
]
strict = true          # branches must be up to date before merging
enforce_admins = true  # admins CANNOT bypass a pending required context
```

Two consequences beyond what `design.md` assumed:

1. `enforce_admins: true` upgrades the §4 risk from "inconvenient" to "total merge lockout" — with the two macOS contexts pending and no admin override, the repository cannot merge anything, including the revert that would fix it. Task 5.3's rollback would have to be pushed by editing protection first regardless, so **§4 must complete before merge, not after**.
2. The surviving Linux legs are *also* named per-leg, which confirms design D1: collapsing `os` to a bare `runs-on: ubuntu-latest` would have renamed `ci (3.11, ubuntu-latest)` → `ci (3.11)` and broken four contexts instead of two.

`docs/ci.md`'s implication that protection is per-workflow is wrong; see task 7.3.

## 2. Implementation

- [x] 2.1 In [.github/workflows/ci.yml:22](.github/workflows/ci.yml:22), change `os: [ubuntu-latest, macos-latest]` to `os: [ubuntu-latest]`, keeping the `os` matrix key and `runs-on: ${{ matrix.os }}` intact (design D1 — preserves the surviving legs' status-check context names)
- [x] 2.2 In [docs/ci.md:8](docs/ci.md:8), change the `ci.yml` row's tier cell from `lint, type, unit (3.11–3.12 × ubuntu/macos)` to `lint, type, unit (3.11–3.12 × ubuntu)`
- [x] 2.3 Confirm `grep -rn "macos" .github/ docs/` returns no hits

Matrix expansion verified locally by parsing `ci.yml`: 2 legs, named `ci (3.11, ubuntu-latest)`
and `ci (3.12, ubuntu-latest)` — byte-identical to the two surviving required contexts recorded
in §1, so task 3.3's assertion is already established independent of the PR run.

## 3. Verification on the PR

- [ ] 3.1 Open the PR and confirm the `ci` workflow expands to exactly two legs — `(3.11, ubuntu-latest)` and `(3.12, ubuntu-latest)` — and no macOS leg appears
- [ ] 3.2 Confirm both legs pass all four steps (`make lint`, `make type`, `make test-unit`, `make test-semantics-offline`) and that `integration` and `quality` are green
- [ ] 3.3 Confirm the surviving legs' status-check context names are byte-identical to the pre-change Linux legs (verifies D1's rationale held)
- [ ] 3.4 Confirm no required status check sits permanently pending on the PR

## 4. Branch protection cleanup (only if 1.2 found per-leg contexts)

- [ ] 4.1 Remove `ci (3.11, macos-latest)` and `ci (3.12, macos-latest)` from `main`'s required contexts **before** merging the PR
- [ ] 4.2 Re-confirm the remaining required contexts exactly match the checks the PR reported

## 5. Merge and post-merge

- [ ] 5.1 Squash-merge with a commit message referencing `openspec/changes/drop-macos-ci-matrix-leg`
- [ ] 5.2 Open (or check) a subsequent PR against `main` and confirm it reaches a mergeable state — this is the real test that §4 was handled correctly
- [ ] 5.3 If PRs are blocked by a pending required context, revert the `ci.yml` commit immediately (restores the contexts on the next push), then redo §4 before re-landing

## 6. Spec archive coordination

- [x] 6.1 Before archiving, re-read `openspec/specs/repo-scaffolding/spec.md` and check whether `enforce-mutation-gate-on-core` has already archived its edit to the *GitHub Actions workflows mirror the testing tiers* requirement
  - *Checked (2026-08-03):* it has — `enforce-mutation-gate-on-core` archived as `archive/2026-08-04-enforce-mutation-gate-on-core` and its mutation-gate clauses are in the main spec. `add-docs-website` has also archived since (as `archive/2026-08-04-add-docs-website`), so the same requirement additionally carries `website.yml` and its two scenarios.
- [x] 6.2 If it has, copy its `quality.yml`/`nightly.yml` mutation-gate clauses verbatim into this change's `MODIFIED` block before archiving, so the full-body replacement does not revert them (design D4)
  - *Done (2026-08-03):* the `quality.yml`/`nightly.yml` mutation-gate clauses and their three scenarios copied verbatim into this change's `MODIFIED` block, and — same D4 reasoning, discovered at copy time — `add-docs-website`'s landed `website.yml` sentence and its two scenarios folded in as well ("four workflows" → "five", and the no-macOS scenario's "all four workflow files" → "all five"). The block is now the current main-spec text plus exactly this change's macOS edits, so archiving it cannot revert either landed change. §6.3–6.4 remain for actual archive time, which waits on §7.
- [ ] 6.3 Run `openspec validate drop-macos-ci-matrix-leg` and archive
- [ ] 6.4 After archiving, diff the resulting main spec against both changes' deltas to confirm neither change's clauses were lost

## 7. Follow-ups (not part of this change)

- [ ] 7.1 One week post-merge, check GitHub billing to confirm the macOS line item is gone and record the actual saving against the ~83% estimate in `design.md`
- [ ] 7.2 File a follow-up change to fix the `make lint type test-unit` wording drift in the `repo-scaffolding` requirement — `ci.yml` also runs `make test-semantics-offline` (design D5) — once both pending `repo-scaffolding` deltas have archived
- [ ] 7.3 Fix `docs/ci.md`'s "Making checks required" section: it says to mark `ci`, `integration`, and `quality` required as if protection were per-workflow, but §1 found `main` actually pins the four individual `ci` matrix legs. The doc's advice is what made this change's central risk invisible until the API was queried — it should state that `ci` is pinned per-leg and that changing the matrix requires updating protection in the same window
