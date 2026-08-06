## Why

After 1.0, every public name in `beam_agents` is a compatibility promise. Today the surface is not in a promisable state, and the gap is measurable:

1. **The public surface is mostly accidental.** An AST scan of the 50 non-generated modules under `src/beam_agents` finds **245 distinct public (non-underscore) top-level names**, and only 13 modules declare `__all__`. The set includes names that could never be one coherent contract: two unrelated `Matcher` aliases ([fake.py](../../../src/beam_agents/model/fake.py) vs [chaos.py](../../../src/beam_agents/testing/chaos.py)), two unrelated `MetricsSink` protocols ([metrics.py](../../../src/beam_agents/observability/metrics.py) vs [service.py](../../../src/beam_agents/effector/service.py)), and plainly-internal helpers like [`find_async_client`](../../../src/beam_agents/adapters/langgraph/transport.py:85) that are public only because nobody prefixed an underscore.

2. **The constitution and the code disagree about the surface.** [project.md:86](../../project.md:86) says the public API is "only what `beam_agents/__init__.py` re-exports: `RunAgent`, `AgentConfig`, `tool`, `StreamAgent`, adapter classes" — but [`tool`](../../../src/beam_agents/tools/registry.py:178) and [`StreamAgent`](../../../src/beam_agents/core/agent.py:56) are **not** re-exported today, while the actual [`__all__`](../../../src/beam_agents/__init__.py:24) carries nine names including the whole HITL vocabulary (`HitlPolicy`, `Deny`, `Drop`, `Escalate`, `FallbackContext`) that project.md's list omits. A user following the constitution imports `beam_agents.tool` and gets an `AttributeError`.

3. **The scaffolding spec is stale in the other direction.** The active requirement "Public API surface starts empty and typed" ([repo-scaffolding/spec.md:164](../../specs/repo-scaffolding/spec.md:164)) still carries scenarios asserting `dir(beam_agents)` is empty and that a fresh import loads only `beam_agents` itself — both false since `add-runagent-transform` landed.

4. **Only the root `__all__` is frozen.** [test_import.py:19](../../../tests/test_import.py:19) pins the root re-export set, which is the right precedent — but nothing stops a new public name from appearing in any of the other 42 modules unreviewed. Surface growth is exactly the kind of silent drift the repo already ratchets for coverage and mutation.

5. **There is no deprecation policy.** [CONTRIBUTING.md](../../../CONTRIBUTING.md) documents quality gates and baselines but says nothing about how a public name may be renamed or removed, and no machinery exists to emit a `DeprecationWarning`.

6. **Docstring completeness is unenforced and incomplete.** Measured with ruff's pydocstyle completeness rules (`ruff check --select D1`): **182 findings** in `src/` — 129 undocumented public methods (D102), 40 undocumented `__init__`s (D107), 10 magic methods (D105), and 3 undocumented public functions (D103, all in [effector/`__main__.py`](../../../src/beam_agents/effector/__main__.py:60)). Zero D100/D101/D104: every module, package, and class already has a docstring, so the gap is closable.

The one 1.0-ready part of the contract is typing: `mypy --strict` gates `src/` and `py.typed` ships in the wheel. This change brings the name surface, its documentation, and its evolution rules up to the same standard, and is the last window to prune names cheaply — C43 (`add-0-5-0-release`) establishes the 0.5.0 release, after which removals start costing deprecation windows.

## What Changes

- **Deprecation policy.** A new CONTRIBUTING section defines the rule: removing or renaming a frozen public name requires at least one minor release in which the old name still works and emits a `DeprecationWarning` naming the replacement and the removal release. A small private helper (`beam_agents/_deprecation.py`) makes the policy executable — module `__getattr__` shims call it, following the lazy-resolution pattern [`beam_agents/__init__.py:42`](../../../src/beam_agents/__init__.py:42) already uses. The one-time privatization performed by this change's audit is exempt: it lands pre-1.0, in the 0.x line, before the surface is declared frozen.

- **Public-surface audit.** Every one of the 245 public top-level names is classified as *root surface*, *module-tier public contract*, or *internal*. Internals get underscore-prefixed; every public module gains an explicit `__all__`. The root surface gains the two names project.md promises and the code lacks — `tool` and `StreamAgent` — and [project.md:86](../../project.md:86) is amended to enumerate the actual eleven-name root surface, resolving the drift in both directions at once.

- **Surface snapshot test.** A committed snapshot file (`public-surface.toml`, sibling to `coverage-baseline.toml` and `mutation-baseline.toml`) records, per module, the sorted public top-level names and each declared `__all__`. A new AST-based test (`tests/test_public_surface.py`, extending the introspection technique of [test_import.py](../../../tests/test_import.py)) fails on any unreviewed difference — growth *or* removal — so accidental surface change fails CI in `make test-unit` and every intentional change is a reviewable diff.

- **Docstring completeness gate.** `[tool.ruff.lint]` `select` ([pyproject.toml:127](../../../pyproject.toml:127)) gains `D1` (the pydocstyle *completeness* rules only), with `D105`/`D107` ignored and `tests/*` exempted via the existing per-file-ignores table. The remaining findings — the 3 D103s plus whatever fraction of the 129 D102s survives privatization — are fixed by writing the docstrings. The gate rides `make lint` unchanged; no new tool, target, or workflow step.

- **API reference page.** A hand-written `docs/api.md` (sibling to the existing `docs/metrics.md`, `docs/errors.md`) enumerates the frozen surface: the eleven root names and every documented public module with its `__all__`, one line of contract each. A drift test asserts every snapshot-frozen name appears on the page.

- **Scaffolding spec reconciliation.** The stale "Public API surface starts empty and typed" requirement is modified: import hygiene (no I/O, no threads, no optional frameworks at import time) is retained, and the empty-surface scenarios are replaced by snapshot conformance.

Not changing: any runtime behavior; the lazy adapter-class resolution and its `ImportError`-naming-the-extra contract; `mypy --strict`; the `py.typed` marker; the ruff style rules (no `D2xx`/`D4xx`).

## Capabilities

### New Capabilities

- `public-api`: the package's frozen public surface and its evolution rules — what the root namespace re-exports, how every public name is enumerated and frozen by the committed snapshot, the underscore-privacy rule for internals, the docstring-completeness gate, the deprecation window, and the API reference page.

### Modified Capabilities

- `repo-scaffolding`: the "Public API surface starts empty and typed" requirement's scenarios assert an empty `dir(beam_agents)` and a submodule-free import — both false since the transform landed. The requirement keeps its import-hygiene and typing clauses and delegates surface enumeration to the `public-api` snapshot. The ruff requirement is *not* modified: its selector list is a MUST-include floor, which adding `D1` satisfies without a contract change.

## Impact

- **Depends on:** C43 (`add-0-5-0-release`, a sibling proposal in this batch, not yet merged). The deprecation window is counted in minor releases, so the policy needs the release cadence C43 establishes; and the audit's one-time privatization must land in a 0.x minor released under C43's process before the surviving surface is declared frozen for 1.0.
- **New code:** `src/beam_agents/_deprecation.py` (private warning helper), `tests/test_public_surface.py` (snapshot test), `tests/test_deprecation.py`, `public-surface.toml` (committed snapshot), `docs/api.md` plus its drift test.
- **Modified code:** `src/beam_agents/__init__.py` (re-export `tool` and `StreamAgent`; `__all__` grows 9 → 11); [tests/test_import.py:19](../../../tests/test_import.py:19) (frozen root set widens to match); underscore renames and `__all__` declarations across the audited modules — candidates include [adapters/langgraph/transport.py](../../../src/beam_agents/adapters/langgraph/transport.py:85)'s helpers, the provider `decode` functions, and effector internals, with the final list produced by the audit itself; docstrings added across `src/` for every surviving D1 finding; [openspec/project.md:86](../../project.md:86) amended to the enumerated root surface; CONTRIBUTING.md gains the deprecation-policy section.
- **CI/build:** none. No new workflow, job, or make target: the docstring gate is a ruff `select` addition inside the existing `make lint`, and the snapshot test runs in the existing `make test-unit`.
- **Gates:** `make lint` gains the `D1` rules; `make test-unit` gains the exact-equality surface test. The mutation gate is affected only if the audit renames names inside `core/`: renaming [`serialize_error_envelope`](../../../src/beam_agents/core/error_records.py) would change the mutant names behind the four `error_records` entries in [mutation-exclusions.toml:38](../../../mutation-exclusions.toml:38), and `scripts/mutation_gate.py` fails on stale entries — so any `core/` rename must re-run `make mutation` and update the exclusion keys in the same commit. Coverage ratchet: docstrings and renames add no uncovered lines; the new test files only raise coverage.
