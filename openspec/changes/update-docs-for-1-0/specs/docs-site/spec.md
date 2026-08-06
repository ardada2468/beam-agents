## ADDED Requirements

### Requirement: Documentation states the versioning regime of the version it ships with

The documentation SHALL describe the versioning and API-stability policy of the release it ships with, with `docs/releasing.md` as the single full statement of that policy (the page `CONTRIBUTING.md`, `CHANGELOG.md`, and the website refer readers to). From `1.0.0` that statement SHALL be: semver over the frozen surface recorded in `public-surface.toml`, with removals gated by the deprecation window `CONTRIBUTING.md` defines. Superseded regimes MAY be described only when explicitly scoped as history. Facts coupled to the version string — install instructions, package pins quoted in docs and docstrings, transcript output showing a CLI's version — SHALL be true at the commit that carries them, phrased conditionally where they flip at tag time, and the release checklist SHALL enumerate every such version-coupled reference so a version bump sweeps them deliberately.

#### Scenario: The policy page matches the shipped version's regime

- **WHEN** a reader opens `docs/releasing.md` at a commit whose `pyproject.toml` version is `1.0.0` or later
- **THEN** the page states the semver-over-frozen-surface policy and the deprecation window, names `public-surface.toml` as the frozen surface, and confines the 0.x rules to a section scoped as history

#### Scenario: A version bump has a checklist step for coupled references

- **WHEN** a release PR bumps `[project].version`
- **THEN** the release checklist in `docs/releasing.md` names each version-coupled reference to update — the `docs/yaml.md` pins, the `src/beam_agents/yaml/` provider-listing pins, the `docs/replay.md` transcript version, the website's `PACKAGE_VERSION`, and `uv.lock` — so none is rediscovered stale after tagging

### Requirement: Every finished docs page is reachable from the site navigation

Every non-draft page under `docs/` SHALL appear in the `mkdocs.yml` nav; a finished page reachable only by URL is a defect. Pages under `docs/design/` are drafts and SHALL stay out of the nav deliberately, but factual claims inside them (version markers, shipped/unshipped statements) SHALL NOT contradict the repository state.

#### Scenario: A page outside the nav is a deliberate draft, not an orphan

- **WHEN** `mkdocs build --strict` reports pages that exist in the docs directory but are absent from the nav
- **THEN** every reported page is under `docs/design/`, and any other absence is treated as a defect to fix rather than an accepted warning
