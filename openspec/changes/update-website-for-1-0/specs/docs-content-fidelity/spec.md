## MODIFIED Requirements

### Requirement: Status semantics are enforced, in both directions

The verifier SHALL enforce the meaning of each status:

- `stable` — every assertion resolves, and the page declares at least one `spec:` and at least one `test:` assertion.
- `experimental` — every assertion resolves, and the page declares at least one `test:` assertion.
- `partial` — every assertion resolves, and the page body contains a section explicitly stating what is not implemented.
- `planned` — the page MUST declare at least one `symbol:` or `module:` assertion naming what it is waiting on; a `planned` page with none is a hard failure, because an empty declaration makes the planned/shipped inversion unenforceable. The declared `symbol:`/`module:` entries MUST NOT resolve, and the verifier MUST NOT fault them for not resolving; if the code the page describes exists, the page fails until it is reclassified.

#### Scenario: Stable page without a test assertion fails

- **WHEN** a page declares `status: stable` with only `symbol:` assertions
- **THEN** verification fails stating that `stable` requires a `spec:` and a `test:` assertion

#### Scenario: Partial page must state what is missing

- **WHEN** a page declares `status: partial` and its body has no section stating what is not implemented
- **THEN** verification fails naming the page and the required section

#### Scenario: Planned page fails once the feature ships

- **WHEN** a page declaring `status: planned` names a module that now exists in `src/`
- **THEN** verification fails instructing the author to reclassify the page

#### Scenario: Planned page with an empty verifies list fails

- **WHEN** a page declares `status: planned` with `verifies: []` or with no `symbol:`/`module:` entry
- **THEN** verification fails stating that a planned page must declare what it is waiting on

#### Scenario: Planned page declaring an absent module passes assertion resolution

- **WHEN** a page declares `status: planned` with `module: src/beam_agents/model/vertex.py` and that module does not exist
- **THEN** assertion resolution reports no finding for that entry, and the page passes while the module remains absent

### Requirement: Distribution and install instructions match the package's real state

Install instructions SHALL reflect the package's actual distribution state. Release state SHALL be determined by the existence of the `v{project.version}` git tag — the event the release workflow publishes on — and NOT by the version string, because the version bumps in the release pull request before the tag is pushed and the guard must hold across that window. While the tag does not exist, the primary install path MUST be source installation at a git ref, and any registry-install instruction MUST be presented under an explicit "when released" label. The verifier MUST fail closed — treating the package as unreleased — when the tag lookup cannot be performed.

#### Scenario: Unqualified registry install fails while unreleased

- **WHEN** a page presents `pip install beam-agents` as a currently working command while no `v{project.version}` tag exists
- **THEN** verification fails naming the page and the line

#### Scenario: A declared version alone does not retire the guard

- **WHEN** `project.version` is `1.0.0`, no `v1.0.0` tag exists, and a page presents a registry install as currently working
- **THEN** verification fails naming the page and the line

#### Scenario: Install page leads with the source path

- **WHEN** a reader loads the install page in the current state
- **THEN** the first install instruction is a source installation at a git ref, and the registry path appears under a "when released" heading

## ADDED Requirements

### Requirement: The site's rendered version constants match the repository

`website/lib/site.ts` SHALL declare `PACKAGE_VERSION` equal to `pyproject.toml`'s `project.version`, and SHALL declare `IS_RELEASED` as an explicit boolean literal — never as an expression derived from the version string — equal to the release-tag state defined above. The verifier MUST fail when either constant disagrees with the repository, so the footer and the landing hero, which render from these constants, cannot claim a release before the tag exists nor a stale version after a bump.

#### Scenario: Version drift fails verification

- **WHEN** `pyproject.toml` declares `1.0.0` and `site.ts` declares `PACKAGE_VERSION = '0.0.0'`
- **THEN** verification fails naming `website/lib/site.ts` and both values

#### Scenario: A premature release flag fails verification

- **WHEN** `site.ts` declares `IS_RELEASED = true` while no `v{project.version}` tag exists
- **THEN** verification fails naming the flag and the missing tag

#### Scenario: A derived release flag fails verification

- **WHEN** `site.ts` computes `IS_RELEASED` from the version string rather than declaring a literal
- **THEN** verification fails stating that the flag must be an explicit literal the check can hold to the tag state
