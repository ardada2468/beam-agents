## Purpose

The changelog contract: every user-facing change carries a reviewed towncrier fragment named after its OpenSpec change folder, fragment types form a closed registry mapped to the versioning policy, fragment presence is enforced whenever `src/` changes, and `CHANGELOG.md` is assembled mechanically from fragments at release time.

## ADDED Requirements

### Requirement: Changes touching src/ carry a changelog fragment named after their OpenSpec change

Every change that modifies `src/` SHALL include a changelog fragment under `changelog.d/`, named `<openspec-change-name>.<type>.md` after the OpenSpec change folder that backs the commit, written as a user-facing sentence. A local pre-commit hook SHALL block commits touching `src/` when no fragment is present in the working tree, with the escape hatch `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1` bypassing this hook only; the same check SHALL run in CI via the existing repo-wide pre-commit step. Changes with no user-observable effect SHALL satisfy the requirement with an `internal`-type fragment, which is not rendered into the published changelog.

#### Scenario: src/ commit without a fragment is blocked

- **WHEN** a contributor stages a change under `src/beam_agents/` with no file under `changelog.d/`
- **THEN** the commit is rejected by the pre-commit hook with a message pointing to the fragment-authoring documentation

#### Scenario: Internal-only change passes with an unrendered fragment

- **WHEN** a refactor with no user-observable effect adds `changelog.d/<change-name>.internal.md` and is later released
- **THEN** the hook and CI check pass, and the released `CHANGELOG.md` section contains no entry for that fragment

#### Scenario: Escape hatch bypasses only the fragment hook

- **WHEN** a contributor commits with `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1` set
- **THEN** the fragment hook passes without a fragment while every other pre-commit hook still runs

### Requirement: Fragment types form a closed registry mapped to the versioning policy

The changelog configuration SHALL register exactly the fragment types `breaking`, `added`, `changed`, `fixed`, `docs`, and `internal`. A fragment whose filename uses an unregistered type MUST fail changelog assembly rather than being silently dropped. The type registry SHALL be the versioning policy's input: `breaking`, `added`, and `changed` mark a change as MINOR-requiring; `fixed` and `docs` are PATCH-compatible; `internal` is never rendered.

#### Scenario: Unregistered fragment type fails assembly

- **WHEN** a fragment named `<change-name>.feature.md` (an unregistered type) is present and changelog assembly runs
- **THEN** the assembly command exits non-zero identifying the unrecognized fragment instead of omitting it silently

#### Scenario: Types render under their own headings

- **WHEN** a release's pending fragments include `breaking`, `added`, and `fixed` entries and the changelog is assembled
- **THEN** the generated section groups entries under distinct headings per type, with breaking changes listed first

### Requirement: CHANGELOG.md is assembled from fragments at release time

The repository SHALL provide a `make changelog` target that assembles all pending fragments into a new dated version section prepended to `CHANGELOG.md` and consumes (deletes) every pending fragment in the same operation — including `internal` fragments, which are consumed without being rendered — so a fragment is published in exactly one release and `changelog.d/` never accumulates. A draft mode SHALL render the pending section without modifying `CHANGELOG.md` or deleting fragments. The `CHANGELOG.md` SHALL begin with a hand-curated `0.1.0` section summarizing the pre-changelog history; mechanical assembly applies from the first release after fragments exist.

#### Scenario: Assembly consumes fragments exactly once

- **WHEN** `make changelog` runs for version `X.Y.Z` with pending fragments
- **THEN** `CHANGELOG.md` gains a dated `X.Y.Z` section containing every non-internal fragment's text, `changelog.d/` retains no fragment of any type, and running assembly again for the next version does not re-render them

#### Scenario: Draft mode is side-effect free

- **WHEN** the draft changelog command runs with pending fragments
- **THEN** the pending section is printed for review while `CHANGELOG.md` and `changelog.d/` are left byte-identical
