## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/release/test_check_wheel.py` — drive `scripts/check_wheel.py` against synthetic wheel/sdist archives built in-test: passing case with `py.typed` + `_protos/*_pb2.py` + entry point + correct metadata; one failing case per check (missing typing marker, missing proto bindings, `tests/` leakage, missing console script, drifted `Requires-Python`, missing/extra extras), each asserting the specific error message — implements `release-process` scenarios "Wheel missing the typing marker fails verification", "Wheel missing generated proto bindings fails verification", "Metadata drift fails verification", "Specifier reordering is not drift", "Verification logic is unit-tested offline" (fails first because the script does not exist)
- [x] 1.2 `tests/release/test_check_release.py` — drive `scripts/check_release.py` with injected tag/`pyproject.toml`/`uv.lock`/fragment-dir inputs: tag≠version, lock≠version, non-ancestor-of-main, patch-tag-with-`breaking`/`added`/`changed`-fragment all fail with named reasons; consistent minor tag with breaking+added fragments passes — implements `release-process` scenarios "Tag and version disagree", "Lockfile lags the version bump", "Tag on a commit not on main", "Patch tag with a breaking fragment is rejected", "Minor tag accepts feature and breaking fragments"
- [x] 1.3 `tests/release/test_changelog_fragments.py` — fragment-contract tests: the `changelog-fragment-required` check script blocks a staged `src/` diff with an empty `changelog.d/`, passes with an `internal` fragment, and honors `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`; an unregistered fragment type (`.feature.md`) makes assembly fail; draft mode leaves the tree byte-identical — implements `changelog-automation` scenarios "src/ commit without a fragment is blocked", "Internal-only change passes with an unrendered fragment", "Escape hatch bypasses only the fragment hook", "Unregistered fragment type fails assembly", "Types render under their own headings", "Assembly consumes fragments exactly once", "Draft mode is side-effect free"
- [x] 1.4 Verify all of the above fail for the right reason (missing scripts/config, not import or fixture errors) before any implementation lands — confirmed: `ModuleNotFoundError: No module named 'scripts.check_release'` / `'scripts.check_wheel'` at collection, no fixture errors

## 2. Changelog automation

- [x] 2.1 Add the `release` dependency group (towncrier) to `pyproject.toml` and refresh `uv.lock`
- [x] 2.2 Add `[tool.towncrier]` configuration: `changelog.d/` directory, the closed type registry (`breaking`, `added`, `changed`, `fixed`, `docs`, `internal` with `showcontent`/rendering per spec, breaking listed first), Keep-a-Changelog-style section template targeting `CHANGELOG.md`
- [x] 2.3 Create `changelog.d/` (with a README stub explaining fragment naming `<openspec-change-name>.<type>.md`) and this change's own fragment `add-0-1-0-release.added.md`
- [x] 2.4 Add `scripts/check_changelog_fragment.sh` (shape of `scripts/check_openspec_change.sh`, escape hatch `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`) and wire the `changelog-fragment-required` local hook into `.pre-commit-config.yaml`; confirm it runs in the `quality` workflow's existing `pre-commit run --all-files` step with no workflow edit
- [x] 2.5 Add `make changelog` (towncrier build for an explicit version) plus its draft variant; bring the section-1 fragment/assembly tests green
- [x] 2.6 Seed `CHANGELOG.md` with the hand-curated `0.1.0` section summarizing the archived changes (`openspec/changes/archive/`) and shipped capability set — implements the "assembly consumes fragments exactly once" flow end-to-end on top of the curated base

## 3. Packaging and release verification

- [x] 3.1 Add `make build`: clean `dist/`, `uv build` producing sdist + wheel
- [x] 3.2 Implement `scripts/check_wheel.py` (zip/tarball inspection, no install, no network) and bring the 1.1 tests green; run it against a real `make build` output locally to confirm the current packaging passes
- [x] 3.3 Implement `scripts/check_release.py` (tag == `[project].version` == `uv.lock` `beam-agents` version; tagged commit is an ancestor of `main`; fragment-type-vs-bump policy) and bring the 1.2 tests green
- [x] 3.4 Decide sdist proto-source inclusion (design Open Question) from `uv build`'s actual collection and pin the decision in `check_wheel.py`'s sdist checks — **resolved yes**: `uv build`'s default sdist collection already ships `protos/beam_agents.proto`, so `REQUIRED_SDIST_MEMBERS` now asserts it (a downstream regenerator gets the source, and stripping it becomes a verification failure rather than a silent loss)

## 4. Release workflow and policy docs

- [x] 4.1 Add `.github/workflows/release.yml`: `push: tags: ["v*"]`; job `build-and-verify` (uv sync `--locked`, `make build`, `check_release.py`, `check_wheel.py`, `make lint type test-unit test-semantics-offline`, upload `dist/` artifact) and job `publish` (`needs`, `environment: pypi`, `permissions: id-token: write`, `pypa/gh-action-pypi-publish` trusted publishing, GitHub Release created with the version's changelog section as body); plus a `workflow_dispatch` rehearsal input targeting TestPyPI — implements `release-process` scenarios "Green tag publishes", "Failing gate blocks publish", "No static PyPI credential exists"
- [x] 4.2 Write `docs/releasing.md`: the pre-1.0 versioning policy with the enumerated compatibility surface (public API re-exports, protos/`state_schema_version`, effector CLI, extras/console-script names, Python/runner matrix), the fragment-type → version-component mapping, the release checklist (nightly green, no open latency-budget regression, required checks green on the release commit), the release-PR + tag procedure, and the recorded trusted-publisher binding tuples — implements `release-process` scenario "State schema bump rides a MINOR release" as documented policy
- [x] 4.3 Update `CONTRIBUTING.md` (fragment authoring section; pointer to `docs/releasing.md`) and `openspec/project.md`'s conventions if reviewers deem the fragment rule constitution-worthy — `CONTRIBUTING.md` gains "Changelog fragments" and "Releasing" sections and `docs/releasing.md` joins the mkdocs nav; `openspec/project.md` left untouched, deferred to reviewers per the task's own conditional

## 5. 0.1.0 execution

- [ ] 5.1 One-time PyPI/TestPyPI project registration and trusted-publisher bindings to `release.yml` + the `pypi`/`testpypi` environments; record the binding tuples in `docs/releasing.md` **(blocked: needs release infra)** — the binding table is written in `docs/releasing.md`; creating the bindings on the PyPI side is a human action outside this repo
- [ ] 5.2 TestPyPI rehearsal via `workflow_dispatch` with a `0.1.0rc1` build: verify OIDC exchange, artifact upload, metadata rendering, and a clean-environment `pip install` (base + each extra) with working `beam-agents-effector` entry point **(blocked: needs release infra)** — the `workflow_dispatch` path and the `publish-testpypi` job exist; the run requires 5.1
- [x] 5.3 Release PR: bump `version` to `0.1.0`, refresh `uv.lock`, finalize the curated changelog section folding in fragments pending since 2.4; squash-merge — the bump, the lock refresh, and the curated `0.1.0` section land in this change's commit; the squash-merge is this change's own merge
- [ ] 5.4 Push annotated `v0.1.0` on the merged commit; confirm `release.yml` publishes to PyPI and creates the GitHub Release with the 0.1.0 notes; smoke-install from PyPI **(blocked: needs release infra)** — requires a real tag push and a registered PyPI project

## 6. Gates

- [x] 6.1 `make lint`
- [x] 6.2 `make type`
- [x] 6.3 `make test-unit` (new `tests/release/` suite included; coverage ratchet respected)
- [x] 6.4 `make coverage-ratchet`
- [x] 6.5 `uv run pre-commit run --all-files` (including the new `changelog-fragment-required` hook)
- [x] 6.6 `openspec validate add-0-1-0-release --strict`
- [x] 6.7 `make test-semantics-offline` (the offline half of the release gate roster `release.yml` re-runs)

## Revisions

Numbered corrections to the planning artifacts, made because implementation
proved them wrong.

### Revision 1 — the published extras are four, not three, and the expected metadata is derived rather than restated

`proposal.md`, `design.md` (D3, D5), and the `release-process` requirement
"Distribution contents are verified before publishing" all enumerated three
extras (`effector`, `langgraph`, `otlp`). `add-vllm-provider` (C26) landed a
fourth, `vllm`, before this change was implemented, so a check hardcoding three
would have failed the very first real release — and would have gone stale again
on the next extra.

Building the real wheel surfaced a second, related defect: hatchling emits
`Requires-Python: <3.13,>=3.11` for a `pyproject.toml` declaring
`>=3.11,<3.13`. A literal string comparison, which the design's "metadata drift
on `Requires-Python` (`>=3.11,<3.13`)" wording implies, would have reported
drift on every release.

The `release-process` spec requirement is amended: expected `Requires-Python`
and extra names are **derived from `pyproject.toml`** (so the check compares the
built artifact against the source declaration and cannot go stale), and
comparison is over specifier *sets*, not strings. A new scenario, "Specifier
reordering is not drift", pins the second half. `scripts/check_wheel.py`
implements both via `expected_from_pyproject()` and `_specifier_set()`; both are
unit-tested, including a test asserting the repo's real `pyproject.toml` still
matches what the test fixtures encode.

### Revision 2 — `internal` fragments must be consumed explicitly; towncrier will not do it

D4 says `internal` fragments "satisfy enforcement, not rendered", and left the
mechanism to `[tool.towncrier]` configuration. Neither available configuration
achieves it: `showcontent = false` on a registered type still emits an
"Internal" heading listing the fragment names, and leaving `internal`
unregistered makes towncrier *skip* the fragment — which does render nowhere,
but towncrier then never deletes what it skipped. Verified against
towncrier 25.8.0: after `towncrier build --yes`, `refactor.internal.md` is
still sitting in `changelog.d/`.

Left as designed, every `internal` fragment ever written would accumulate
forever and the `changelog-automation` requirement "a fragment is rendered in
exactly one release" would be false for that type.

Resolution: `internal` stays unregistered with towncrier (that is what makes it
render nowhere) and `make changelog` gains a third step,
`scripts/check_release.py --consume-internal`, run *after* a successful
`towncrier build` so a failed assembly never destroys fragments. The
`changelog-automation` requirement is amended to say assembly consumes every
pending fragment including `internal`, and its scenario now asserts
`changelog.d/` retains no fragment *of any type*. Covered by
`TestConsumeInternal` and
`TestRealAssembly::test_towncrier_leaves_internal_fragments_for_the_consume_step`.

### Revision 3 — `uv.lock` needed a full refresh, not only the version line

D1 anticipated that a version bump requires a lockfile refresh because
`uv.lock` records the project's own version. In practice `uv sync --locked` was
*already* failing on the base branch: `add-docs-site` (C24) added the
`mkdocs-material` docs group and `add-vllm-provider` (C26) added the `vllm`
extra without relocking. `uv lock` therefore did three things at once — bumped
the recorded project version to `0.1.0`, added the `release` group's towncrier,
and materialized the mkdocs/vllm trees C24 and C26 left unlocked. The resulting
lock diff is large but is the minimum that makes `uv sync --locked` succeed at
all; `tests/release/test_check_release.py::TestRepositoryStateIsSelfConsistent`
now pins pyproject/lock agreement in the unit lane so this failure mode is
legible next time.

## 6. Revision: extras pin refreshed for the M2 batch (integration)

- [x] 6.1 `tests/release/test_check_wheel.py` pins the published extras set in three places (the
  `EXPECTED` constant, the synthetic wheel `METADATA`, and the synthetic pyproject fed to
  `expected_from_pyproject`). That literal pin is the point — adding a distribution extra must be
  a deliberate act, not a silent one — so the correct response to the three extras that landed in
  the same merge window is to update it, not to loosen the assertion. Extras went from four
  (`effector`, `langgraph`, `otlp`, `vllm`) to seven, adding `memory-stores` (C29), `adk` (C31),
  and `pydantic-ai` (C39). Verified: `pytest tests/release` 96 passed, 5 skipped.
- [x] 6.2 `uv.lock` regenerated once against the fully merged `pyproject.toml` rather than taking
  either side of the merge, which is what this change's own Revision 3 anticipated: the lock now
  carries every M2 extra and dependency-group mirror at once. `uv sync --locked` succeeds, so the
  `ci` lane's locked sync — broken on the intermediate branch states — is green again.
