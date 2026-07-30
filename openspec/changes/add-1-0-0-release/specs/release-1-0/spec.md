## ADDED Requirements

### Requirement: The 1.0.0 release ships only after the M4 hardening gate is satisfied

The `v1.0.0` tag SHALL NOT be cut, and the 1.0.0 package SHALL NOT be published, until every one of the following holds:

- The changes `add-effector-security`, `add-1-0-api-freeze`, `add-state-guarantees`, and `promote-spark-runner` are implemented and archived.
- The public-API freeze snapshot test introduced by `add-1-0-api-freeze` is green.
- The state compatibility guarantees documented by `add-state-guarantees` are in place and its nightly pipeline-`--update` compat test is green on the latest scheduled run (re-triggered manually if the last run predates a state-touching merge).
- Effector intent signing from `add-effector-security` has shipped and its rollout is complete: signature verification is enforced by the effector, not merely available.
- The Spark promotion decision from `promote-spark-runner` is recorded either way — Spark promoted to supported, or promotion explicitly deferred with the roadmap noting why.

A release attempt while any condition fails SHALL be treated as a gate failure that blocks the release; the failing condition MUST be resolved in its owning change, not waived in this one.

#### Scenario: All gate conditions hold

- **WHEN** the four M4 changes are archived, the API-freeze snapshot test and the nightly `--update` compat test are green, intent-signing enforcement is rolled out, and the Spark decision is recorded
- **THEN** the release proceeds via the release process established by `add-0-1-0-release`: version `1.0.0` in `pyproject.toml`, a 1.0.0 changelog entry, tag `v1.0.0`, and publish

#### Scenario: A hardening change is unarchived at release time

- **WHEN** a 1.0.0 release is attempted while any of the four M4 changes is not yet archived — even if its code is merged and green
- **THEN** the release is blocked, and no version bump, tag, or publish for 1.0.0 occurs until that change archives

#### Scenario: Spark promotion is still inside its four-green-week window

- **WHEN** every other gate condition holds but `promote-spark-runner`'s time-gated promotion window has not yet completed, and the deferral is recorded with the roadmap noting why Spark remains best-effort
- **THEN** the gate is satisfied and 1.0.0 ships with Spark explicitly deferred; the changelog states Spark's best-effort status

#### Scenario: The Spark decision is unrecorded

- **WHEN** every other gate condition holds but no `promote-spark-runner` decision has been recorded in either direction
- **THEN** the release is blocked until the decision — promote or defer-with-rationale — is written down

### Requirement: From 1.0.0 the stability policies govern all public-surface and state changes

From the moment `v1.0.0` is published, the project SHALL treat 1.0.0 as its API-stability promise: every change to the public API surface SHALL be governed by the deprecation policy established by `add-1-0-api-freeze`, and every change to wire or state schemas SHALL be governed by the state-migration guarantees established by `add-state-guarantees`. A post-1.0 proposal that breaks either policy without following it SHALL be rejected or re-scoped.

#### Scenario: A post-1.0 proposal removes a public symbol without deprecation

- **WHEN** a change proposed after `v1.0.0` removes or incompatibly alters a frozen public-API symbol without following the deprecation policy
- **THEN** the proposal is rejected or re-scoped to comply before any implementation lands

#### Scenario: A post-1.0 proposal changes a state schema

- **WHEN** a change proposed after `v1.0.0` alters a wire or state proto
- **THEN** it complies with the documented state-migration guarantees — additive, or version-bumped with migration and compat coverage — before any implementation lands
