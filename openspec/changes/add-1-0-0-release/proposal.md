## Why

This is a milestone-gate change with no propose-command in the roadmap: roadmap item C48 is defined purely by its dependencies, and this proposal exists to state the gate, not to build anything. It closes Phase M4 by shipping `beam-agents 1.0.0` — the first release whose version number is a promise rather than a snapshot.

The promise is stability, and it is only credible if the M4 hardening batch lands first. `1.0.0` is the point after which the deprecation policy (`add-1-0-api-freeze`) governs every public-surface change and the documented state-migration guarantees (`add-state-guarantees`) govern every wire/state change. Cutting the tag before those regimes — plus effector intent signing (`add-effector-security`) and a recorded Spark decision (`promote-spark-runner`) — are in force would ship the version number without the thing it advertises. So the substance of this change is a release gate: an explicit, checkable list of conditions under which the tag may be cut, and a refusal to cut it otherwise.

The mechanics, by contrast, are deliberately not new. The release process — version bump, changelog, tag, publish, and their automation — was established by `add-0-1-0-release` (C25) and last exercised by `add-0-5-0-release` (C43). This change reuses that machinery unchanged and adds only the 1.0-specific gate on top.

## What Changes

- **Version bump to `1.0.0`** in `pyproject.toml` (`version = "0.0.0"` today at [pyproject.toml:3](../../../pyproject.toml:3); the intermediate milestone bumps land with their own release changes).
- **Changelog entry for 1.0.0**, produced via the changelog process established by `add-0-1-0-release`, headlined by the stability promise: the API freeze and deprecation policy (C45), the state compatibility guarantees (C46), effector intent signing (C44), and the Spark runner status (C47, whichever way its decision went).
- **Tag `v1.0.0` and publish** through the release workflow established by `add-0-1-0-release` — no new release mechanics, no workflow changes.
- **An explicit 1.0 release-gate checklist** (the spec delta below is its contract): the release SHALL NOT ship until the four M4 hardening changes are implemented and *archived*, their gate signals are green (API-freeze snapshot test; documented state guarantees plus the nightly `--update` compat test; effector security shipped with its rollout complete), and Spark's promotion decision is recorded either way — promoted, or explicitly deferred with the roadmap noting why (see design D2 for how the gate treats a legitimately lagging time-gated promotion).
- Not changing: any runtime code, any test, any CI workflow, any spec other than the new `release-1-0` capability. A release change that needs code changes has found a gate failure, not a task.

## Capabilities

### New Capabilities

- `release-1-0`: the contract for shipping 1.0.0 — the gate conditions that must hold before the tag is cut, and what the 1.0.0 version number promises afterward. Written to stand alone so it validates independently of the sibling M4 proposals it gates on.

### Modified Capabilities

None. The release machinery's own spec (established by `add-0-1-0-release`) is reused as-is; this change adds a gate in front of it without altering how releasing works. The four M4 changes modify their own capabilities in their own proposals — this change only requires that those proposals be archived, it does not restate their requirements.

## Impact

**Depends on:** `add-effector-security` (C44), `add-1-0-api-freeze` (C45), `add-state-guarantees` (C46), `promote-spark-runner` (C47) — all four must be implemented and archived before the gate can pass. Builds on `add-0-1-0-release` (C25) for the release machinery (workflow, changelog automation, versioning policy) and on `add-0-5-0-release` (C43) as the preceding milestone release.

**New code:** none. The only artifacts are the version bump, the changelog entry, and the tag.

**Modified code:** [pyproject.toml:3](../../../pyproject.toml:3) (`version = "0.0.0"` → `1.0.0` at release time, on top of whatever the preceding milestone set) and the changelog file maintained by the C25 process.

**CI/build:** no workflow changes. The existing release workflow from `add-0-1-0-release` runs as-is for `v1.0.0`; the gate consumes signals CI already produces (the C45 snapshot test and C46 compat test run in the suites their own changes placed them in).

**Gates:** this change *is* a gate. Beyond the standard change gates (`make lint`, `make type`, `make test-unit`, `openspec validate --strict` — all trivially green for a docs-plus-version change), the release itself is blocked on the checklist in `tasks.md` §2, which is the executable form of the spec delta. The standing release blockers from `openspec/project.md` (semantics gates never skipped; latency-budget benchmark regressions block release) apply to 1.0.0 as they do to every release and are re-verified in the checklist rather than restated as new requirements.
