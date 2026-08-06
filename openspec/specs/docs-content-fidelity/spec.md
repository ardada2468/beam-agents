# docs-content-fidelity Specification

## Purpose
TBD - created by archiving change add-docs-website. Update Purpose after archive.
## Requirements
### Requirement: Every page declares a maturity status from a closed set

Every content page SHALL declare `status` as exactly one of `stable`, `experimental`, `partial`, or `planned`. The status MUST render as a visible badge in the page header and beside the page's navigation entry. No page may omit the field or use a value outside the set.

#### Scenario: Status renders in the page and in navigation

- **WHEN** a reader loads any content page
- **THEN** the page header shows its status badge and the navigation entry for that page shows the same status

#### Scenario: Value outside the closed set fails the build

- **WHEN** a page declares `status: beta`
- **THEN** `make site-build` exits non-zero naming the file and listing the four permitted values

### Requirement: Claims are declared as machine-checkable assertions

Each page SHALL declare a `verifies` list whose entries are typed assertions: `symbol:` (a dotted name importable from `beam_agents`), `module:` (a repository-relative source path), `spec:` (a path under `openspec/specs/`), `test:` (a pytest node id), or `example:` (a file under `website/examples/`). `scripts/verify_docs_claims.py` MUST resolve every assertion against the repository and the installed package, and MUST fail on any that does not resolve.

#### Scenario: Symbol assertions resolve by import, not by grep

- **WHEN** a page asserts `symbol: beam_agents.RunAgent`
- **THEN** the verifier resolves it by importing the package and accessing the attribute, and a name that only appears in source text but is not importable fails

#### Scenario: Unresolvable module assertion fails verification

- **WHEN** a page asserts `module: src/beam_agents/memory/stores.py`, which does not exist
- **THEN** `make site-check` exits non-zero naming the page and the missing path

#### Scenario: Test assertions must be collectable

- **WHEN** a page asserts a `test:` node id that `pytest --collect-only` does not report
- **THEN** verification fails naming the page and the uncollectable node id

#### Scenario: Unknown assertion type fails verification

- **WHEN** a page declares an entry whose key is not one of the five permitted assertion types
- **THEN** verification fails naming the page and the unrecognized key

### Requirement: Status semantics are enforced, in both directions

The verifier SHALL enforce the meaning of each status:

- `stable` — every assertion resolves, and the page declares at least one `spec:` and at least one `test:` assertion.
- `experimental` — every assertion resolves, and the page declares at least one `test:` assertion.
- `partial` — every assertion resolves, and the page body contains a section explicitly stating what is not implemented.
- `planned` — the page MUST NOT declare a `symbol:` or `module:` assertion that resolves; if the code it describes exists, the page fails until it is reclassified.

#### Scenario: Stable page without a test assertion fails

- **WHEN** a page declares `status: stable` with only `symbol:` assertions
- **THEN** verification fails stating that `stable` requires a `spec:` and a `test:` assertion

#### Scenario: Partial page must state what is missing

- **WHEN** a page declares `status: partial` and its body has no section stating what is not implemented
- **THEN** verification fails naming the page and the required section

#### Scenario: Planned page fails once the feature ships

- **WHEN** a page declaring `status: planned` names a module that now exists in `src/`
- **THEN** verification fails instructing the author to reclassify the page

### Requirement: Planned material is published as roadmap, never as documentation

Pages with `status: planned` SHALL be excluded from primary navigation and from `sitemap.xml`, MUST carry `noindex`, MUST be reachable only from a single roadmap index, and MUST open with a standard callout stating that the described capability does not exist in the current code. Their code samples MUST be labelled as illustrative and MUST NOT be embedded from `website/examples/`.

#### Scenario: Planned pages are not indexed or primary-linked

- **WHEN** `make site-check` inspects navigation, the sitemap, and page metadata
- **THEN** no `planned` page appears in primary navigation or the sitemap, and each carries `noindex`

#### Scenario: Planned page opens with the not-implemented callout

- **WHEN** a reader loads any `planned` page
- **THEN** the first content element is a callout stating the capability is not implemented, above any prose or code

### Requirement: Claims about other projects require a dated citation

Any statement about a project other than `beam-agents` on a page in the **comparison section** SHALL be backed by a `sources` entry carrying the claim text, a URL, and a `retrieved` date, rendered on the page as a visible footnote with that date. Outside that section, naming another project is a statement about this repository (for example, that an adapter for it exists) and MUST instead be backed by a `module:` or `symbol:` assertion. Every citation reference used in a comparison-table cell MUST resolve to a `sources` entry on the same page, wherever the page lives. Comparison-table cells describing `beam-agents` MUST be backed by a `spec:` or `test:` assertion. A cell with neither MUST render the literal value "Not established".

#### Scenario: Uncited comparative claim fails verification

- **WHEN** a comparison page names another project in a claim with no matching `sources` entry
- **THEN** verification fails naming the page and the uncited claim

#### Scenario: Naming an adapter's framework outside the comparison section is not a citation-bearing claim

- **WHEN** a Learn page states that the LangGraph adapter is implemented, backed by a `module:` assertion, and carries no `sources` entry
- **THEN** verification passes, because the statement is about this repository rather than about LangGraph

#### Scenario: A citation marker with no matching source fails anywhere

- **WHEN** any page renders a comparison cell citing a URL that does not appear in that page's `sources`
- **THEN** verification fails naming the page and the unresolved URL

#### Scenario: Citations render with their retrieval date

- **WHEN** a reader loads a comparison page
- **THEN** each sourced claim shows a footnote with the source URL and the date it was retrieved

#### Scenario: Unbacked comparison cell renders as not established

- **WHEN** a comparison row has no `spec:` or `test:` backing for the `beam-agents` column
- **THEN** the rendered cell reads "Not established" rather than a claim or an empty cell

### Requirement: Quantitative claims are sourced or absent

The site SHALL NOT publish a performance number, benchmark result, throughput figure, latency measurement, or scale claim that is not produced by a benchmark or test in this repository, or carried by a dated citation. The latency budget stated in `openspec/project.md` MUST be presented as a design budget with no published measurement, and MUST NOT be presented as a measured result. No numeric performance comparison against another project is permitted.

#### Scenario: Unsourced numeric performance claim fails the prose check

- **WHEN** a page states a throughput, latency, or speedup figure with no in-repo source and no citation
- **THEN** `make site-check` exits non-zero naming the file and the line

#### Scenario: Latency budget is labelled as a budget

- **WHEN** a reader encounters the p50/p99 overhead figures
- **THEN** the surrounding text identifies them as a design budget and states that no benchmark measurement is published

### Requirement: Prohibited content is rejected by a lexical check

`scripts/check_docs_prose.py` SHALL scan content and fail on: adopter, customer, or testimonial language; unsourced superlatives; unsourced numerics matching a performance-claim pattern outside code fences; and phrasing implying Apache Software Foundation governance. A per-line escape comment MAY exempt a line, and MUST record a reason so its use is auditable.

#### Scenario: Fabricated social proof fails the check

- **WHEN** a page contains "trusted by teams in production" or a named adopter
- **THEN** `make site-check` exits non-zero naming the file and the line

#### Scenario: ASF-governance phrasing fails the check

- **WHEN** a page describes the project as "an Apache project" or "incubating at the ASF"
- **THEN** the check fails naming the file and the line

#### Scenario: Escape comments require a reason

- **WHEN** a line carries an escape comment with no stated reason
- **THEN** the check fails, and every escape in the tree is greppable by a single fixed token

### Requirement: The site states its relationship to the Apache Software Foundation

Every page SHALL carry a footer stating that `beam-agents` is licensed under Apache-2.0 and built on Apache Beam, that it is not an Apache Software Foundation project, and that Apache, Apache Beam, and Apache Flink are trademarks of the ASF. The site MUST NOT use ASF or Apache project logos or branding.

#### Scenario: Footer disclaimer is present on every page

- **WHEN** `make site-check` inspects the rendered HTML of every route
- **THEN** each response contains the non-affiliation statement and the trademark attribution

#### Scenario: No ASF branding is used

- **WHEN** the site's static assets are inspected
- **THEN** no ASF or Apache project logo file is present

### Requirement: Distribution and install instructions match the package's real state

Install instructions SHALL reflect the package's actual distribution state. While `project.version` is `0.0.0` and no release is published, the primary install path MUST be source installation at a git ref, and any registry-install instruction MUST be presented under an explicit "when released" label. The verifier MUST read `project.version` from `pyproject.toml` and fail any page presenting a registry install as currently available while no release exists.

#### Scenario: Unqualified registry install fails while unreleased

- **WHEN** a page presents `pip install beam-agents` as a currently working command while `project.version` is `0.0.0`
- **THEN** verification fails naming the page and the line

#### Scenario: Install page leads with the source path

- **WHEN** a reader loads the install page in the current state
- **THEN** the first install instruction is a source installation at a git ref, and the registry path appears under a "when released" heading

### Requirement: Verification runs in CI on any change that can invalidate it

The claim verifier, prose check, API drift check, link check, and SSR assertion SHALL run under `make site-check`, and a GitHub Actions workflow MUST run it on changes to `website/**`, `src/**`, `docs/**`, `openspec/specs/**`, and the workflow file itself. Every failure MUST name the offending file and line and state the command that reproduces it locally.

#### Scenario: Runtime change that invalidates a claim fails CI

- **WHEN** a pull request deletes a module that a published page asserts via `module:`
- **THEN** the website workflow runs and fails naming the page and the missing module

#### Scenario: Failures are locally reproducible

- **WHEN** any fidelity check fails in CI
- **THEN** its output names the file, the line, and the `make` target that reproduces the failure locally
