## Purpose

A buildable, deployable documentation site over the existing `docs/` tree: strict-mode static build as a CI gate, a landing page that ties each documented guarantee to the gate enforcing it, example pages rendered by verbatim inclusion of the runnable source, and GitHub Pages deployment from `main`.

## ADDED Requirements

### Requirement: The site builds strictly, offline, from the existing docs tree

The repository SHALL build a static documentation site with MkDocs + Material, configured by a repo-root `mkdocs.yml` whose content root is the existing `docs/` directory. The build SHALL run offline via `make docs` (`mkdocs build --strict`) using only the `docs` dependency group, SHALL NOT import `beam_agents` or `apache_beam`, and SHALL treat any broken internal link or missing navigation target as a build failure. The five existing operator pages (`ci.md`, `effector.md`, `errors.md`, `metrics.md`, `traces.md`) SHALL appear in the site navigation from their current locations, and a new `docs/index.md` landing page SHALL state the runtime-not-framework principle and link the effectively-once and adapter-conformance claims to their respective release gates.

#### Scenario: One-command strict build with only the docs group installed

- **WHEN** an environment is synced with only the `docs` dependency group and `make docs` is run with no network access
- **THEN** the site builds successfully with zero warnings, without importing the `beam_agents` package, and the output contains the landing page plus all five existing operator pages

#### Scenario: A broken cross-reference fails the build

- **WHEN** a docs page contains a relative link to a page or file the site cannot resolve
- **THEN** `make docs` exits non-zero naming the offending link, rather than publishing a page with a dead reference

### Requirement: Example pages render the runnable source by inclusion

Each example's documentation page under `docs/examples/` SHALL render the example module's source via snippet inclusion (`pymdownx.snippets` with path checking enabled) rather than a copied code block, so the bytes shown on the site are the bytes of the file the tests execute. A snippet path that does not resolve SHALL fail the strict build. An offline unit-tier test SHALL additionally assert that every `docs/examples/*.md` page includes its example module by path and that the path exists, so a broken inclusion is caught in `make test-unit` without running the docs build.

#### Scenario: Editing an example module updates its page with no second copy

- **WHEN** an example module under `examples/` is edited and the site is rebuilt
- **THEN** the corresponding docs page renders the new source without any docs-side edit, and no copy of the example code exists inside `docs/`

#### Scenario: A moved example file cannot publish silently

- **WHEN** an example module is renamed or moved without updating its docs page's snippet path
- **THEN** the strict build fails on the unresolved snippet, and the unit-tier snippet-integrity test fails offline for the same reason

### Requirement: The docs build gates pull requests and the site deploys from main

A dedicated `docs.yml` workflow SHALL run the strict build on every pull request and push to `main`. On pushes to `main` only, the workflow SHALL publish the built site to GitHub Pages using the official Pages actions with a permissions-scoped `GITHUB_TOKEN` — no long-lived credentials and no committed `gh-pages` branch. The docs workflow SHALL NOT install or run any runtime test tier, and the existing `ci` unit lane's locked dependency sync SHALL remain unchanged by the filled `docs` group.

#### Scenario: A pull request with a docs regression fails the docs workflow

- **WHEN** a pull request introduces a broken link or unresolvable snippet in the docs tree
- **THEN** the `docs` workflow fails on the strict build, and no deployment step runs for the pull request

#### Scenario: Merge to main publishes the current site

- **WHEN** a commit lands on `main` with a passing strict build
- **THEN** the workflow uploads the built site as a Pages artifact and deploys it, and the published site reflects that commit's `docs/` and `examples/` content
