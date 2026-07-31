## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/test_public_surface.py`: AST-derive, for every non-generated module under `src/beam_agents`, the sorted public top-level names and any declared `__all__`; compare both against `public-surface.toml` by exact equality; on mismatch, name the module, the name, and the fix path. Fails initially because no snapshot file exists. Derived from "Every public name is frozen by a committed surface snapshot" (all four scenarios: unreviewed addition, unreviewed removal, underscore exemption, `__all__` drift). — written first; initial run failed with `FileNotFoundError` on `public-surface.toml`. `_derive`/`_differences` are pure functions over a root path, so the four scenarios are driven against synthetic packages in `tmp_path`: `test_an_unreviewed_public_addition_fails`, `test_an_unreviewed_removal_fails`, `test_an_underscore_rename_reads_as_a_removal`, `test_a_new_underscore_name_needs_no_snapshot_update`, `test_all_drift_fails`, plus `test_overload_stubs_collapse_to_one_name` and `test_private_modules_are_outside_the_surface`.
- [x] 1.2 Extend `tests/test_import.py`'s frozen root set from nine to eleven names (`+ StreamAgent`, `+ tool`) and assert both new names are importable eagerly. Fails until the re-exports land. Derived from "The root namespace re-exports a fixed, curated surface" (scenarios: frozen list, project.md-promised names importable, lazy adapter contract unchanged — the existing `ImportError`-names-the-extra assertions must keep passing untouched). — **fourteen to sixteen**, not nine to eleven (see Revision 1). Added `test_the_names_project_md_promises_resolve_eagerly`; first run failed with `ImportError: cannot import name 'StreamAgent' from 'beam_agents'`.
- [x] 1.3 `tests/test_deprecation.py`: drive the `_deprecation` helper through a fixture module's `__getattr__` and assert the access succeeds, exactly one `DeprecationWarning` is emitted, and its message names the replacement and the first removal release; assert a non-deprecated attribute passes through unwarned. Derived from "Removing a frozen public name requires a deprecation window" (scenario: a deprecated name warns and still works). — four tests; first run failed with `ModuleNotFoundError: No module named 'beam_agents._deprecation'`.
- [x] 1.4 Documentation drift test (in `tests/test_public_surface.py` or a sibling): every name recorded in `public-surface.toml` appears in `docs/api.md`; failure names the missing name. Fails until the page exists. Derived from "The frozen surface is documented on an API reference page" (both scenarios). — `test_api_reference_documents_every_frozen_name` matches against identifiers inside markdown code spans only (a name in free prose documents nothing); `test_the_reference_page_does_not_document_names_outside_the_surface` covers the converse for the page's table rows.
- [x] 1.5 Note the mutmut boundary: these tests introspect source (same constraint recorded for `tests/test_import.py` in the `[tool.mutmut]` comment) and live at the `tests/` root, outside the `tests/core` mutation selection — verify no `[tool.mutmut]` edit is needed. — both new modules are at the `tests/` root; `[tool.mutmut]`'s test selection is `tests/core` plus explicit ignores, so it never collects them. No edit made.

## 2. Deprecation policy

- [x] 2.1 Create `src/beam_agents/_deprecation.py`: one helper that emits a `DeprecationWarning` (correct `stacklevel`, message naming old name, replacement or "no replacement", and first removal release), documented and `mypy --strict`-clean. — `deprecated_attribute(name, *, replacement, removed_in, value, module, stacklevel=3)`; `stacklevel=3` attributes the warning to the code that touched the attribute rather than the intercepting `__getattr__`.
- [x] 2.2 Add a "Deprecation policy" section to `CONTRIBUTING.md`: the one-minor-release window, the `DeprecationWarning` requirement, the `__getattr__` shim pattern, the snapshot-diff review rule for both edges of the window, and the explicit pre-1.0 bulk-privatization exemption used by this change. — landed as "Public API surface and the deprecation policy", which also documents the snapshot regeneration command, the `__all__` rule, and the `D1` gate.

## 3. Public-surface audit

- [x] 3.1 Commit the initial `public-surface.toml` capturing the current surface verbatim (245 names, 50 modules) with a header comment documenting the format and the update rule; confirm 1.1 now passes against the unaudited tree. — **generated**, not hand-written (`uv run python tests/test_public_surface.py`), and the real pre-audit figure is **385 names across 83 modules** (see Revision 1). Folded into 3.8: the snapshot committed is the post-audit one, because generating the pre-audit surface and then regenerating it in the same commit would have left no reviewable artifact — the reviewable diff here is the code diff plus the single generated snapshot.
- [x] 3.2 Classify every name into root tier, module tier, or internal, recording the classification as the diff of the following tasks (design D2's table is the starting point; the effector takes the conservative config+CLI-public split). — recorded as the 3.3/3.5 diffs. The effector's conservative split was corrected during the audit: see Revision 2.
- [x] 3.3 Underscore-rename the internal tier — including the LangGraph transport helpers (`find_async_client`, `install_transport`, `warn_fallback`), the provider `decode` functions, and audit-identified effector wiring — updating in-repo importers and their tests without weakening any assertion. — 55 names privatized across 27 modules: the transport helpers in all four `adapters/*/transport.py` modules, `anthropic.decode`/`openai_compat.decode` (still exported as `model.anthropic_decode`/`openai_compat_decode`), the ADK event/tool/session internals, the Pydantic AI history module in full, `actions.is_kv_shaped`, the `memory/stores/base` envelope and seq codecs plus `sql.DDL`, `memory/compaction`'s builder protocols, `observability/otlp`'s `encode_batch`/`event_to_span`, `model/vllm`'s engine seam, `core/migration`'s `M` TypeVar, and the effector's `build_service`/`load_registry`/`serve`/`execute_intent`/`encode_payload`/lease codecs. No assertion weakened — every touched test still asserts the same thing about the renamed name.
- [x] 3.4 For any renamed `core/` function (e.g. `error_records.serialize_error_envelope` if privatized): re-key the four `mutation-exclusions.toml` entries whose mutant names embed it, re-run `make mutation`, and record any `mutation-baseline.toml` movement in that file's comment, per the renumbering precedent. — **not applicable**: the audit privatized no function in `core/`. `serialize_error_envelope` and its `core/error_records` siblings are contract (the exporters and the errors sink are built on them) and stayed public, so the four exclusion keys are untouched and the mutation gate is unperturbed. The only `core/` rename is the `M` → `_M` TypeVar in `migration.py`, which embeds in no mutant name.
- [x] 3.5 Add `__all__` to every module-tier public module, naming exactly its contract names. — 48 modules gained one, 2 had theirs extended (`core/dofn.py` for the four `DETAIL_*` constants, `observability/metrics.py` for the counter/distribution names). Enforced going forward by `test_every_declared_public_name_is_listed_in_its_module_all`.
- [x] 3.6 Re-export `tool` and `StreamAgent` from `beam_agents/__init__.py` (eager imports; `__all__` 9 → 11) while preserving the import-side-effect audit in `tests/test_import.py` (both are `ast.ImportFrom`-only additions). — `__all__` 14 → 16; both additions are `ast.ImportFrom` nodes, so `test_import_has_no_side_effects` passes untouched.
- [x] 3.7 Amend `openspec/project.md`'s code-style bullet to enumerate the actual eleven root names, replacing the stale five-item list. — enumerates the actual **sixteen**, and states the two-tier rule, the `__all__` requirement, the underscore-module rule, the `D1` gate, and the deprecation window.
- [x] 3.8 Regenerate `public-surface.toml` to the post-audit surface; confirm the diff shows exactly the intended renames, removals, and additions, and that 1.1 and 1.2 pass. — frozen surface: **78 public modules, 330 distinct public names**. Both tests pass.

## 4. Docstring completeness gate

- [x] 4.1 In `[tool.ruff.lint]`, add `"D1"` to `select`; add `D105` and `D107` to `ignore` with the design's rationale in a comment; add `"tests/*" += ["D1"]` to per-file-ignores. — also `benchmarks/*` and `scripts/*` (Revision 3).
- [x] 4.2 Fix the three `D103` findings in `src/beam_agents/effector/__main__.py` (`build_parser`, `config_from_args`, `main`). — all three documented; `main` states its exit statuses.
- [x] 4.3 Write docstrings for every `D102` finding surviving the audit's privatization (baseline: 129 across effector 54, core 22, model 15, observability 13, adapters 10, memory 9, tools 5, actions 1), stating contract — inputs' meaning, staged-vs-applied effects, raise conditions — not restating signatures. — the real post-merge baseline was 161 in `src/` (156 D102 + 5 D103) plus 22 in `examples/`; all 183 written. Protocol methods carry the full contract and implementations state their backend-specific mechanism, so the repetition across four `DedupStore` backends documents four different things.
- [x] 4.4 Confirm `make lint` passes with zero `D` findings and that `@overload` stubs (the `tool` decorator) produce none. — `ruff check .` clean; `tools/registry.py`'s two `tool` overloads produce no finding, and `test_overload_stubs_collapse_to_one_name` pins the snapshot's matching behavior.
- [x] 4.5 Observe the gate failing: strip one public docstring, confirm `make lint` exits non-zero naming it, revert. Do the same for the surface test with a throwaway public name. A gate never observed failing is not known to work. — stripping `ToolRunner.run`'s docstring gave `src/beam_agents/tools/runner.py:36:15: D102` and ruff exit 1. Adding `keys.throwaway_public_name` gave `beam_agents/keys.py: public name 'throwaway_public_name' is not in the frozen surface`; privatizing `shard_key` gave `frozen public name 'shard_key' has disappeared`. All three reverted.

## 5. API reference page

- [x] 5.1 Write `docs/api.md`: the eleven root names first, then each module-tier module with its `__all__` and one line of contract per name, cross-linking `docs/metrics.md`/`docs/errors.md`/`docs/effector.md` where deeper pages exist. — sixteen root names first, then thirteen sections; cross-links to `errors.md`, `metrics.md`, `traces.md`, `batching.md`, `memory.md`, `sharding.md`, `replay.md`, `yaml.md`, `state-migration.md`, `effector.md`. Added to the mkdocs nav.
- [x] 5.2 Confirm the drift test (1.4) passes, and fails when a snapshot name is removed from the page. — both directions pass; the removal direction is what `test_api_reference_documents_every_frozen_name` asserts, and it was red until the page covered all 330 names.

## 6. Gates

- [x] 6.1 `make lint` (now including `D1`) and `make type` (`mypy --strict`) clean. — `ruff check .` and `ruff format --check .` clean over 367 files; `mypy` clean over 361 source files.
- [x] 6.2 `make test-unit` passes offline, including the new surface, deprecation, and docs-drift tests. — 1812 passed, 9 skipped (optional-extra and dependency-group skips), 196 deselected. `make test-semantics-offline` also green: 72 passed, 5 skipped.
- [x] 6.3 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new tests improve it. — "branch coverage 91.18% is at baseline"; no raise warranted, so `coverage-baseline.toml` is untouched.
- [x] 6.4 `make mutation` passes if `core/` was touched by renames (3.4), with exclusion keys and baseline comments updated in the same commit. — not run: 3.4 established that no `core/` function was renamed, so no mutant name changed and no exclusion key went stale.
- [ ] 6.5 `uv run pre-commit run --all-files` clean. — not run: `pre-commit` is in the `precommit` dependency group, which this environment does not sync (only `lint`, `typecheck`, `test`). Its hooks are `ruff`, `ruff format`, the protobuf-drift regeneration check, and the two OpenSpec/changelog guards; the first two are green above, no `protos/` file was touched, and the change carries both an OpenSpec folder and `changelog.d/add-1-0-api-freeze.breaking.md`.
- [x] 6.6 `openspec validate add-1-0-api-freeze --strict` passes. — "Change 'add-1-0-api-freeze' is valid".

## Revisions

### Revision 1 — the surface was measured against a much larger tree than the proposal saw

The proposal and design were written against a snapshot of `src/beam_agents` that predates the merge of the M2/M3 roadmap work (`keys.py`, `yaml/`, `replay/`, `memory/stores/` + `memory/compaction.py`, `model/vllm.py`, `core/batching.py` + `migration.py` + `snapshot.py`, `adapters/adk/` + `adapters/pydantic_ai/` + `adapters/_transport.py`, and the token-budget types). Every quantity in the artifacts is therefore stale, and re-measuring was the first implementation step. The measured figures:

| Quantity | Proposal / design | Measured |
| --- | --- | --- |
| Non-generated modules | 50 | 83 (78 of them public) |
| Distinct public top-level names | 245 | 385 pre-audit, **330 frozen** |
| Modules declaring `__all__` | 13 | 26 pre-audit, 76 post-audit |
| Root `__all__` | 9 → 11 | 14 → **16** |
| `ruff --select D1` findings in `src/` | 182 (129 D102, 40 D107, 10 D105, 3 D103) | 246 (161 D102, 69 D107, 11 D105, 5 D103); 166 after ignoring `D105`/`D107` |

`specs/public-api/spec.md` is amended: the root-surface requirement now names the actual sixteen (the nine the proposal knew about, plus `ShardKeys`/`shard_key`/`unshard_key` from `add-key-sharding` and `AdkAgent`/`PydanticAIAgent` from the two new adapters, plus `tool` and `StreamAgent` from this change). Design D1's *argument* is unaffected — the drift it identifies is real and is resolved toward the union exactly as it says; only the arithmetic changed. The proposal's "Why" section is left as the historical record of what motivated the change.

### Revision 2 — the effector's pluggable backends are contract, not wiring

Design D2 and the proposal's Impact section both direct the audit to privatize "effector wiring internals", and the design's Open Questions treats the effector's "sources/sinks/dedup builders" as "arguably all internal". Implementing that literally broke the repository's own documentation: `examples/slack_approval/__main__.py` constructs `build_intent_source` and `build_message_sink`, `examples/slack_approval/surface.py` types against `MessageSink`, the Kafka example constructs `KafkaIntentSource`/`KafkaMessageSink`, and `docs/examples/slack-approval.md` names `InMemoryIntentSource`/`InMemoryMessageSink` in prose. That is precisely the failure mode D2 rejects for `FakeLLM` and `Complete` — "making the documented usage patterns illegal" — arriving from the other direction.

The line is drawn instead between the service's **extension surface** and its **wiring**: the sources, sinks, dedup stores, their URI builders, the service's `MetricsSink`/`CountingMetrics`/`PublishFailedError`, and the config and CLI are contract; the assembly and execution internals (`build_service`, `load_registry`, `serve`, `execute_intent`, `encode_payload`, the lease-expiry codecs) are not. `specs/public-api/spec.md` gains the general rule this instance is an application of: a name an example constructs or a `docs/` page presents as usage is contract. The design's Open Question about whether the effector deserves its own weaker tier stands, now with this evidence attached.

### Revision 3 — the docstring gate's exemption table, and the private-module rule

Two scoping decisions the design did not anticipate, both now written into `specs/public-api/spec.md`:

1. **`benchmarks/*` and `scripts/*` join `tests/*` in the `D1` per-file-ignores.** D4 exempts only `tests/*`, but `D1` selects over the whole repository, and these two trees contributed 28 findings of pure repo tooling with no compatibility promise. `examples/*` is deliberately *not* exempted — the examples are user-facing documentation, so their 22 findings were fixed by writing docstrings.

2. **Private modules are outside the frozen surface by their path.** D3 says the snapshot covers "every non-generated module", which would have frozen the contents of `_protos/`, `adapters/_transport.py`, `model/_http.py` and `yaml/_config.py`/`_refs.py` — modules whose whole point is that nothing in them is contract, and which would then have needed entries on `docs/api.md`. The snapshot skips any module with a single-underscore path component (dunder names like `__init__.py`/`__main__.py` excepted), which makes "the snapshot is the contract, and the contract is what `docs/api.md` documents" one statement rather than three with exceptions.

The same requirement also gains the `__all__`-completeness rule that makes D2's classification checkable rather than advisory: a public module may declare a public name only by listing it in its own `__all__`, enforced by `test_every_declared_public_name_is_listed_in_its_module_all`.

### Revision 4 — the snapshot is generated, and its generator lives in the test module

Task 3.1 said "commit the initial `public-surface.toml`" without saying how. It is generated — `uv run python tests/test_public_surface.py` rewrites it from the tree using the same `_derive` the gate compares with, so the file cannot encode a different reading of the sources than the test does. The entry point lives in the test module's `if __name__ == "__main__":` block rather than in a new `scripts/` file, which keeps the derivation logic in exactly one place and adds no new module to the surface the change exists to shrink. The command is documented in the snapshot's own header, in `CONTRIBUTING.md`, and in the failure message of every mismatch.

### Revision 5 — one entry added to the upstreaming design doc's decision record

`tests/docs/test_upstream_design_doc.py::test_decision_record_dispositions_every_top_level_module` discovers top-level modules from the filesystem and requires each to carry a move/stay disposition in `docs/design/apache-beam-ml-agents.md`. Adding `src/beam_agents/_deprecation.py` therefore reddened a test in a capability this change does not otherwise touch. The module is recorded as **stays**: it implements *this repository's* compatibility policy, and Beam has its own deprecation conventions and release cadence, so donating it would ship a second, conflicting policy. Nothing in the runtime imports it.

## 7. Revision: reconcile the frozen surface with add-effector-security (integration)

- [x] 7.1 `add-effector-security` (C44) landed in the same merge window and added fifteen public
  names this change's snapshot could not have seen — the whole `intent_signing` module plus
  `TransportSecurity`, `VerificationMode`, `VERIFICATION_MODES`, `redact_uri` and
  `transport_security_from_args`. All three gates fired exactly as designed (snapshot drift,
  undeclared-in-`__all__`, and undocumented-in-`api.md`), which is the outcome this change exists to
  produce. Reviewed each name rather than regenerating blind, applying this change's own D2 rule:
  `load_verification_keyring` is CLI wiring reachable only from `_build_service`, so it was
  privatized to `_load_verification_keyring`; the other fourteen are contract — `TransportSecurity`
  is constructed by anyone configuring broker auth, `transport_security_from_args` is the exact
  sibling of the already-public `config_from_args`, and the signing routines are the interoperability
  half of the intent contract — so they were declared in their modules' `__all__`, frozen, and given
  contract lines on the reference page (a new `beam_agents.intent_signing` section plus five rows in
  the effector table).
- [x] 7.2 `public-surface.toml` regenerated with the reviewed result via
  `uv run python tests/test_public_surface.py`. Verified: `pytest tests/test_public_surface.py`
  11 passed; `make lint`, `make type`, `make test-unit` (1920 passed), `make test-semantics-offline`
  (79 passed), `make coverage-ratchet` at baseline, and `mkdocs build --strict` all clean.
- [x] 7.3 Two undocumented public methods `add-effector-security` added to `DeliveredIntent`
  (`raw_payload`, `raw_key`) were given real docstrings rather than a `D102` exemption, since the
  verbatim-bytes rationale they encode is exactly the kind of contract this change's docstring gate
  exists to force into writing.
