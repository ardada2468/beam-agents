## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/test_public_surface.py`: AST-derive, for every non-generated module under `src/beam_agents`, the sorted public top-level names and any declared `__all__`; compare both against `public-surface.toml` by exact equality; on mismatch, name the module, the name, and the fix path. Fails initially because no snapshot file exists. Derived from "Every public name is frozen by a committed surface snapshot" (all four scenarios: unreviewed addition, unreviewed removal, underscore exemption, `__all__` drift).
- [ ] 1.2 Extend `tests/test_import.py`'s frozen root set from nine to eleven names (`+ StreamAgent`, `+ tool`) and assert both new names are importable eagerly. Fails until the re-exports land. Derived from "The root namespace re-exports a fixed, curated surface" (scenarios: frozen list, project.md-promised names importable, lazy adapter contract unchanged — the existing `ImportError`-names-the-extra assertions must keep passing untouched).
- [ ] 1.3 `tests/test_deprecation.py`: drive the `_deprecation` helper through a fixture module's `__getattr__` and assert the access succeeds, exactly one `DeprecationWarning` is emitted, and its message names the replacement and the first removal release; assert a non-deprecated attribute passes through unwarned. Derived from "Removing a frozen public name requires a deprecation window" (scenario: a deprecated name warns and still works).
- [ ] 1.4 Documentation drift test (in `tests/test_public_surface.py` or a sibling): every name recorded in `public-surface.toml` appears in `docs/api.md`; failure names the missing name. Fails until the page exists. Derived from "The frozen surface is documented on an API reference page" (both scenarios).
- [ ] 1.5 Note the mutmut boundary: these tests introspect source (same constraint recorded for `tests/test_import.py` in the `[tool.mutmut]` comment) and live at the `tests/` root, outside the `tests/core` mutation selection — verify no `[tool.mutmut]` edit is needed.

## 2. Deprecation policy

- [ ] 2.1 Create `src/beam_agents/_deprecation.py`: one helper that emits a `DeprecationWarning` (correct `stacklevel`, message naming old name, replacement or "no replacement", and first removal release), documented and `mypy --strict`-clean.
- [ ] 2.2 Add a "Deprecation policy" section to `CONTRIBUTING.md`: the one-minor-release window, the `DeprecationWarning` requirement, the `__getattr__` shim pattern, the snapshot-diff review rule for both edges of the window, and the explicit pre-1.0 bulk-privatization exemption used by this change.

## 3. Public-surface audit

- [ ] 3.1 Commit the initial `public-surface.toml` capturing the current surface verbatim (245 names, 50 modules) with a header comment documenting the format and the update rule; confirm 1.1 now passes against the unaudited tree.
- [ ] 3.2 Classify every name into root tier, module tier, or internal, recording the classification as the diff of the following tasks (design D2's table is the starting point; the effector takes the conservative config+CLI-public split).
- [ ] 3.3 Underscore-rename the internal tier — including the LangGraph transport helpers (`find_async_client`, `install_transport`, `warn_fallback`), the provider `decode` functions, and audit-identified effector wiring — updating in-repo importers and their tests without weakening any assertion.
- [ ] 3.4 For any renamed `core/` function (e.g. `error_records.serialize_error_envelope` if privatized): re-key the four `mutation-exclusions.toml` entries whose mutant names embed it, re-run `make mutation`, and record any `mutation-baseline.toml` movement in that file's comment, per the renumbering precedent.
- [ ] 3.5 Add `__all__` to every module-tier public module, naming exactly its contract names.
- [ ] 3.6 Re-export `tool` and `StreamAgent` from `beam_agents/__init__.py` (eager imports; `__all__` 9 → 11) while preserving the import-side-effect audit in `tests/test_import.py` (both are `ast.ImportFrom`-only additions).
- [ ] 3.7 Amend `openspec/project.md`'s code-style bullet to enumerate the actual eleven root names, replacing the stale five-item list.
- [ ] 3.8 Regenerate `public-surface.toml` to the post-audit surface; confirm the diff shows exactly the intended renames, removals, and additions, and that 1.1 and 1.2 pass.

## 4. Docstring completeness gate

- [ ] 4.1 In `[tool.ruff.lint]`, add `"D1"` to `select`; add `D105` and `D107` to `ignore` with the design's rationale in a comment; add `"tests/*" += ["D1"]` to per-file-ignores.
- [ ] 4.2 Fix the three `D103` findings in `src/beam_agents/effector/__main__.py` (`build_parser`, `config_from_args`, `main`).
- [ ] 4.3 Write docstrings for every `D102` finding surviving the audit's privatization (baseline: 129 across effector 54, core 22, model 15, observability 13, adapters 10, memory 9, tools 5, actions 1), stating contract — inputs' meaning, staged-vs-applied effects, raise conditions — not restating signatures.
- [ ] 4.4 Confirm `make lint` passes with zero `D` findings and that `@overload` stubs (the `tool` decorator) produce none.
- [ ] 4.5 Observe the gate failing: strip one public docstring, confirm `make lint` exits non-zero naming it, revert. Do the same for the surface test with a throwaway public name. A gate never observed failing is not known to work.

## 5. API reference page

- [ ] 5.1 Write `docs/api.md`: the eleven root names first, then each module-tier module with its `__all__` and one line of contract per name, cross-linking `docs/metrics.md`/`docs/errors.md`/`docs/effector.md` where deeper pages exist.
- [ ] 5.2 Confirm the drift test (1.4) passes, and fails when a snapshot name is removed from the page.

## 6. Gates

- [ ] 6.1 `make lint` (now including `D1`) and `make type` (`mypy --strict`) clean.
- [ ] 6.2 `make test-unit` passes offline, including the new surface, deprecation, and docs-drift tests.
- [ ] 6.3 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new tests improve it.
- [ ] 6.4 `make mutation` passes if `core/` was touched by renames (3.4), with exclusion keys and baseline comments updated in the same commit.
- [ ] 6.5 `uv run pre-commit run --all-files` clean.
- [ ] 6.6 `openspec validate add-1-0-api-freeze --strict` passes.
