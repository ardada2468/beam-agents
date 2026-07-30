## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/release/test_check_wheel.py` — drive `scripts/check_wheel.py` against synthetic wheel/sdist archives built in-test: passing case with `py.typed` + `_protos/*_pb2.py` + entry point + correct metadata; one failing case per check (missing typing marker, missing proto bindings, `tests/` leakage, missing console script, drifted `Requires-Python`, missing/extra extras), each asserting the specific error message — implements `release-process` scenarios "Wheel missing the typing marker fails verification", "Wheel missing generated proto bindings fails verification", "Metadata drift fails verification", "Verification logic is unit-tested offline" (fails first because the script does not exist)
- [ ] 1.2 `tests/release/test_check_release.py` — drive `scripts/check_release.py` with injected tag/`pyproject.toml`/`uv.lock`/fragment-dir inputs: tag≠version, lock≠version, non-ancestor-of-main, patch-tag-with-`breaking`/`added`/`changed`-fragment all fail with named reasons; consistent minor tag with breaking+added fragments passes — implements `release-process` scenarios "Tag and version disagree", "Lockfile lags the version bump", "Tag on a commit not on main", "Patch tag with a breaking fragment is rejected", "Minor tag accepts feature and breaking fragments"
- [ ] 1.3 `tests/release/test_changelog_fragments.py` — fragment-contract tests: the `changelog-fragment-required` check script blocks a staged `src/` diff with an empty `changelog.d/`, passes with an `internal` fragment, and honors `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`; an unregistered fragment type (`.feature.md`) makes assembly fail; draft mode leaves the tree byte-identical — implements `changelog-automation` scenarios "src/ commit without a fragment is blocked", "Internal-only change passes with an unrendered fragment", "Escape hatch bypasses only the fragment hook", "Unregistered fragment type fails assembly", "Draft mode is side-effect free"
- [ ] 1.4 Verify all of the above fail for the right reason (missing scripts/config, not import or fixture errors) before any implementation lands

## 2. Changelog automation

- [ ] 2.1 Add the `release` dependency group (towncrier) to `pyproject.toml` and refresh `uv.lock`
- [ ] 2.2 Add `[tool.towncrier]` configuration: `changelog.d/` directory, the closed type registry (`breaking`, `added`, `changed`, `fixed`, `docs`, `internal` with `showcontent`/rendering per spec, breaking listed first), Keep-a-Changelog-style section template targeting `CHANGELOG.md`
- [ ] 2.3 Create `changelog.d/` (with a README stub explaining fragment naming `<openspec-change-name>.<type>.md`) and this change's own fragment `add-0-1-0-release.added.md`
- [ ] 2.4 Add `scripts/check_changelog_fragment.sh` (shape of `scripts/check_openspec_change.sh`, escape hatch `BEAM_AGENTS_ALLOW_NO_FRAGMENT=1`) and wire the `changelog-fragment-required` local hook into `.pre-commit-config.yaml`; confirm it runs in the `quality` workflow's existing `pre-commit run --all-files` step with no workflow edit
- [ ] 2.5 Add `make changelog` (towncrier build for an explicit version) plus its draft variant; bring the section-1 fragment/assembly tests green
- [ ] 2.6 Seed `CHANGELOG.md` with the hand-curated `0.1.0` section summarizing the archived changes (`openspec/changes/archive/`) and shipped capability set — implements the "assembly consumes fragments exactly once" flow end-to-end on top of the curated base

## 3. Packaging and release verification

- [ ] 3.1 Add `make build`: clean `dist/`, `uv build` producing sdist + wheel
- [ ] 3.2 Implement `scripts/check_wheel.py` (zip/tarball inspection, no install, no network) and bring the 1.1 tests green; run it against a real `make build` output locally to confirm the current packaging passes
- [ ] 3.3 Implement `scripts/check_release.py` (tag == `[project].version` == `uv.lock` `beam-agents` version; tagged commit is an ancestor of `main`; fragment-type-vs-bump policy) and bring the 1.2 tests green
- [ ] 3.4 Decide sdist proto-source inclusion (design Open Question) from `uv build`'s actual collection and pin the decision in `check_wheel.py`'s sdist checks

## 4. Release workflow and policy docs

- [ ] 4.1 Add `.github/workflows/release.yml`: `push: tags: ["v*"]`; job `build-and-verify` (uv sync `--locked`, `make build`, `check_release.py`, `check_wheel.py`, `make lint type test-unit test-semantics-offline`, upload `dist/` artifact) and job `publish` (`needs`, `environment: pypi`, `permissions: id-token: write`, `pypa/gh-action-pypi-publish` trusted publishing, GitHub Release created with the version's changelog section as body); plus a `workflow_dispatch` rehearsal input targeting TestPyPI — implements `release-process` scenarios "Green tag publishes", "Failing gate blocks publish", "No static PyPI credential exists"
- [ ] 4.2 Write `docs/releasing.md`: the pre-1.0 versioning policy with the enumerated compatibility surface (public API re-exports, protos/`state_schema_version`, effector CLI, extras/console-script names, Python/runner matrix), the fragment-type → version-component mapping, the release checklist (nightly green, no open latency-budget regression, required checks green on the release commit), the release-PR + tag procedure, and the recorded trusted-publisher binding tuples — implements `release-process` scenario "State schema bump rides a MINOR release" as documented policy
- [ ] 4.3 Update `CONTRIBUTING.md` (fragment authoring section; pointer to `docs/releasing.md`) and `openspec/project.md`'s conventions if reviewers deem the fragment rule constitution-worthy

## 5. 0.1.0 execution

- [ ] 5.1 One-time PyPI/TestPyPI project registration and trusted-publisher bindings to `release.yml` + the `pypi`/`testpypi` environments; record the binding tuples in `docs/releasing.md`
- [ ] 5.2 TestPyPI rehearsal via `workflow_dispatch` with a `0.1.0rc1` build: verify OIDC exchange, artifact upload, metadata rendering, and a clean-environment `pip install` (base + each extra) with working `beam-agents-effector` entry point
- [ ] 5.3 Release PR: bump `version` to `0.1.0`, refresh `uv.lock`, finalize the curated changelog section folding in fragments pending since 2.4; squash-merge
- [ ] 5.4 Push annotated `v0.1.0` on the merged commit; confirm `release.yml` publishes to PyPI and creates the GitHub Release with the 0.1.0 notes; smoke-install from PyPI

## 6. Gates

- [ ] 6.1 `make lint`
- [ ] 6.2 `make type`
- [ ] 6.3 `make test-unit` (new `tests/release/` suite included; coverage ratchet respected)
- [ ] 6.4 `make coverage-ratchet`
- [ ] 6.5 `uv run pre-commit run --all-files` (including the new `changelog-fragment-required` hook)
- [ ] 6.6 `openspec validate add-0-1-0-release --strict`
