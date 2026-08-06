## Context

Three sources of truth about the public API disagree today:

| Source | What it says the surface is |
|---|---|
| [project.md:86](../../project.md:86) | `RunAgent`, `AgentConfig`, `tool`, `StreamAgent`, adapter classes; "everything else is private" |
| [`__init__.py:24`](../../../src/beam_agents/__init__.py:24) | `AgentConfig`, `Deny`, `Drop`, `Escalate`, `FallbackContext`, `HitlPolicy`, `LangGraphAgent` (lazy), `RunAgent`, `RunAgentOutputs` — no `tool`, no `StreamAgent` |
| [repo-scaffolding/spec.md](../../specs/repo-scaffolding/spec.md) | empty — "MUST NOT expose any public names until subsequent OpenSpec changes add them", with scenarios asserting `dir(beam_agents)` prints `[]` |

Meanwhile the *de facto* surface — what users must import by dotted path to use the runtime at all — is much wider than any of the three: `FakeLLM` is the documented default test model, agent authors return `Complete`/`Suspend` ([agent.py:83](../../../src/beam_agents/core/agent.py:83)) and receive an `AgentContext`, tool authors need `Tool`/`ToolRegistry` and the `tools.errors` taxonomy, model configuration needs `RetryPolicy`/`CircuitBreaker`, memory needs `Memory`/`Compactor`/`MemoryOverflow`, and operators import `WriteIntents`, the effector config, and the observability constants. The AST scan puts the total at **245 distinct public top-level names in 50 modules, 13 of which declare `__all__`**.

Existing machinery this change builds on rather than inventing:

- [tests/test_import.py](../../../tests/test_import.py) already freezes the root `__all__` by set equality and audits `__init__.py` by AST — the snapshot test generalizes both techniques to the whole tree.
- The repo's drift-control idiom is a committed file plus a failing check: `coverage-baseline.toml`, `mutation-baseline.toml`, `mutation-exclusions.toml`, all documented in [CONTRIBUTING.md](../../../CONTRIBUTING.md). `public-surface.toml` is the same trust model applied to names.
- ruff already runs in `make lint` with a per-file-ignores table ([pyproject.toml:132](../../../pyproject.toml:132)); the docstring gate is a `select` addition, not a new tool.
- The lazy `__getattr__` pattern in [`__init__.py:42`](../../../src/beam_agents/__init__.py:42) is the exact shape a deprecation shim needs.

Two mechanical couplings constrain the work:

- **mutmut names mutants by qualified function name.** Renaming a `core/` function renames its mutants, and `scripts/mutation_gate.py` fails on exclusion entries naming mutants that no longer exist — the four `beam_agents.core.error_records.x_serialize_error_envelope__mutmut_*` entries ([mutation-exclusions.toml:38](../../../mutation-exclusions.toml:38)) go stale if the audit privatizes that function.
- **mutmut instrumentation breaks source-introspecting tests.** The `[tool.mutmut]` comment in [pyproject.toml](../../../pyproject.toml:442) records that `tests/test_import.py` "cannot pass while mutmut has instrumented it". The surface test uses the same AST introspection, so it must live outside the `tests/core` mutation selection — at the `tests/` root, where mutmut's test selection never reaches.

Docstring baseline, measured with `ruff check --select D1` on `src/`: 182 findings — D102 129, D107 40, D105 10, D103 3; D100/D101/D104 all zero.

## Goals / Non-Goals

**Goals:**

- One authoritative, machine-checked enumeration of every public name, frozen so unreviewed surface change fails CI.
- Resolve the project.md ↔ `__init__.py` ↔ repo-scaffolding three-way drift in a single change, in a reviewable direction for each name.
- A written, executable deprecation policy in place *before* 1.0 makes it binding.
- Every frozen public name documented, enforced by the existing lint gate.
- A single API reference page a user can read instead of the source.

**Non-Goals:**

- No runtime behavior change. Renames and docstrings only; every test that passed before must pass after (modulo updated import paths in tests exercising renamed internals).
- Not the release process itself — version numbers, tags, changelog automation are C43's (`add-0-5-0-release`).
- No signature freezing. The snapshot pins *names*, not parameters or types; `mypy --strict` plus review covers signatures until a dedicated tool earns its keep (see Open Questions).
- No docs toolchain (Sphinx/mkdocs). `docs/` is hand-written markdown today and stays that way; the `docs` dependency group is empty and remains so.
- No docstring *style* enforcement (`D2xx`/`D4xx`) — completeness only.
- Not documenting or freezing the effector's internal adapter modules beyond what the audit classifies (see D2 and Open Questions).

## Decisions

### D1. Resolve the root-surface drift by re-exporting `tool` and `StreamAgent` and amending project.md to the real list

Both halves of the drift get fixed toward the union, not the intersection. `tool` becomes a root re-export because it is the authoring decorator every inline-tool pipeline needs the moment it populates `AgentConfig.tool_registry` — making users reach into `beam_agents.tools.registry` for the single most-typed symbol contradicts "the public API is what the root exports". `StreamAgent` becomes a root re-export because it is the protocol adapter authors implement and the type `RunAgent` accepts; it belongs next to `RunAgent` the way `FallbackContext` already sits next to `HitlPolicy`. Both resolve eagerly (neither drags in an optional extra), so `__all__` grows 9 → 11 with `LangGraphAgent` remaining the only lazy name.

[project.md:86](../../project.md:86) is then amended to enumerate the actual eleven names, because its current prose list is wrong in both directions — it omits the five HITL names and `RunAgentOutputs` that are deliberately exported, and promises two names that are not.

*Rejected — amend project.md down to the current nine:* it would leave the constitution documenting that configuring an inline tool requires a deep import, and adapter authors implementing an undocumented protocol. The constitution's list was the design intent; the code is what drifted.

*Rejected — promote more (e.g. `Complete`, `Suspend`, `FakeLLM`) to root:* each promotion is a forever-name. The module tier (D2) makes them public contract without flattening namespaces; promotion remains possible later at zero cost, demotion after 1.0 costs a deprecation cycle. Kept as an Open Question for the 1.0 release review.

### D2. A two-tier surface: curated root plus documented public modules — everything else underscore-private

The audit classifies all 245 names into:

1. **Root tier** — the eleven re-exports. The walled, constitution-listed surface.
2. **Module tier** — names that are public contract at their dotted path: the agent-authoring vocabulary (`core/agent.py`, `core/context.py`), the tool system (`tools/`), the model configuration and error taxonomy (`model/client.py`, `model/facade.py`, `model/fake.py`), memory (`memory/facade.py`), HITL constants (`hitl.py`), transforms (`actions/write_intents.py`, observability exporters), adapter extension points (`BeamCheckpointSaver`, `BeamToolNode`), the chaos helper, and the effector's user-facing config/CLI. Each such module declares `__all__` naming exactly its contract.
3. **Internal** — everything else, renamed with a leading underscore: e.g. the LangGraph transport helpers ([transport.py:85](../../../src/beam_agents/adapters/langgraph/transport.py:85)), the provider `decode` functions, effector wiring internals. The frozen snapshot then contains only intended names.

A single flat root tier is untenable on the evidence: the scan finds `Matcher` defined twice (model fakes vs chaos), `MetricsSink` twice (runtime metrics vs effector), and `NAMESPACE`/`COUNTERS` twice within observability alone — these can only coexist behind module paths. Declaring everything outside root private is equally untenable: it would privatize `FakeLLM`, `Complete`, and the exception taxonomy every `except` clause names, making the documented usage patterns illegal. Two tiers is not a compromise; it is what the package already is, made explicit and frozen.

*Rejected — leave classification implicit (freeze all 245 as-is):* freezing accidental names converts today's sloppiness into tomorrow's compatibility promises; the pre-1.0 window is the one time pruning is free.

### D3. Freeze via a committed AST-derived snapshot with exact-equality comparison

`public-surface.toml` records, per module: the sorted public top-level names (functions, classes, assignments) and the declared `__all__` if any. `tests/test_public_surface.py` re-derives both by parsing every non-generated module under `src/beam_agents` with `ast` and compares for exact equality — any addition, removal, or `__all__` mismatch fails with a message naming the module, the name, and the review path (update the snapshot in the same PR, which makes the change a visible diff).

AST derivation over runtime introspection (`dir()`/`importlib`) for three reasons already learned in this repo: `dir()` picks up imported submodules and instrumentation artifacts (the [test_import.py](../../../tests/test_import.py) comment records mutmut's injected `MutantDict`); importing every module couples the test to optional-extra availability and import order; and AST sees the *declared* surface including names hidden behind `TYPE_CHECKING` or lazy `__getattr__` — the root's lazy `LangGraphAgent` is snapshotted from `__all__`, exactly as the existing test freezes it.

Exact equality, not a ratchet: unlike coverage, where less is unambiguously worse, both growth (accidental API) and shrinkage (breaking removal) of the surface are review-worthy events. The test lives at the `tests/` root, outside the `tests/core` mutmut selection, for the instrumentation reason in Context.

*Rejected — freezing only `__all__` declarations:* a module-level `def helper():` outside `__all__` is still importable and still gets depended on (Hyrum's law); the snapshot must see what the AST sees.

### D4. Docstring gate: ruff `D1` selectors inside the existing lint target, ignoring `D105`/`D107`, tests exempt

The gate is `select += ["D1"]` in [pyproject.toml:127](../../../pyproject.toml:127) — the pydocstyle *completeness* subset (D100–D107) only, so no docstring-style opinions enter the build. Two ignores, with reasons: `D107` (`__init__` docstrings) because constructor contracts belong in the class docstring — every class already has one (D101 = 0) — and a separate `__init__` docstring is duplication; `D105` (magic methods) because dunder semantics are defined by the language protocol, not the implementation. `tests/*` joins the per-file-ignores table for `D1` (test names are the documentation; the tests-as-scenario-names convention already carries this). Enforced rules therefore: D100–D104 at zero already or fixed here, D102/D103 fixed by writing the missing docstrings that survive the audit's privatization (an underscore rename removes the finding; a name kept public gets documented — the two workstreams compose).

*Rejected — `interrogate`-style coverage:* it gates a percentage, so a new undocumented public name passes as long as the ratio holds — precisely the drift this change exists to stop; it is also a new dev dependency with its own config and CI step, where ruff is already wired into `make lint`, pre-commit, and editors.

*Rejected — full `D` with a `convention` setting:* pulls in the D2xx/D4xx formatting rules, which are style churn orthogonal to the 1.0 contract and would bury the completeness signal in whitespace-of-docstring diffs.

ruff's D rules skip `@overload`-decorated stubs, so the two `tool` overloads ([registry.py:169](../../../src/beam_agents/tools/registry.py:169)) are correctly not findings.

### D5. Deprecation policy: one-minor-release window, executable via a `_deprecation` helper, reviewed via the snapshot diff

The policy, written into CONTRIBUTING.md: a frozen public name may be removed or renamed only after at least one minor release in which it still works and importing or accessing it emits a `DeprecationWarning` naming the replacement and the first release that may remove it. `src/beam_agents/_deprecation.py` provides the single helper that formats and emits the warning; a module deprecating a name keeps it out of `__all__`'s replacement entry and serves the old name through module `__getattr__` — the same PEP 562 pattern the root already uses for lazy adapters, so the idiom is proven in-tree. The snapshot makes the policy reviewable: a deprecation is a snapshot edit (name moves to the module's deprecated list), a removal is a second snapshot edit one minor later, and both are diffs a reviewer sees.

Enforcement is convention plus review, not CI: no check can know whether a removal had its window without release metadata CI does not have. What CI *does* enforce is that no removal is silent (snapshot equality) and that the warning path works (a unit test drives the helper through a fixture module).

The audit's own bulk privatization is explicitly exempt: it happens in 0.x, before the freeze takes effect, and is the reason this change depends on C43 — the renames ship in a 0.5.x minor so that what 1.0 freezes is the post-audit surface.

*Rejected — deprecation shims for the audited renames:* shimming ~dozens of internals nobody should have imported would enshrine the accident this change removes; pre-1.0 semantics exist precisely for this.

### D6. Modify repo-scaffolding's stale public-surface requirement rather than leaving it contradicted

The "Public API surface starts empty and typed" requirement's two scenarios are both false today (`dir(beam_agents)` is not empty; a fresh import loads `beam_agents.core` and friends). Leaving an active spec asserting falsehoods while adding a new capability that contradicts it would make the spec corpus internally inconsistent. The modification keeps everything still true and valuable — `mypy --strict` on the module, import performs no I/O, spawns no threads, imports no optional frameworks — and replaces surface enumeration with a delegation to the `public-api` snapshot. The ruff requirement is untouched: its "MUST include E, F, I, B, UP, SIM, ASYNC, PL, RUF" is a floor that `D1` extends without contradiction, and the docstring gate's contract lives in `public-api` where it belongs.

## Risks / Trade-offs

- **The privatization breaks 0.x users' deep imports.** Accepted deliberately: 0.x is the sanctioned window, and the alternative — freezing accidental names — is strictly worse. Mitigation: the renames land in a C43-cadenced minor with a changelog section listing every moved name and its replacement (or its non-replacement).
- **This batch of parallel changes will conflict with the snapshot.** Twenty-five sibling proposals are adding public names concurrently; whichever lands after this change must update `public-surface.toml`. That is the feature, not a bug — but it front-loads merge friction. Mitigation: the snapshot is per-module and sorted, so conflicts are local and mechanical; the C43 dependency already sequences this change late in the batch.
- **~130 docstrings written under gate pressure invite boilerplate** ("Returns the result.") that satisfies D102 while documenting nothing. Mitigation: review standard written into the tasks — a docstring must state the contract (inputs' meaning, staged-vs-applied effects, raise conditions), not restate the signature; the audit shrinks the count first so effort concentrates on names that are actually contract.
- **`core/` renames perturb the mutation gate.** Mutant names embed function names, so privatizing e.g. `serialize_error_envelope` invalidates four exclusion entries and shifts `mutation-baseline.toml` accounting. Mitigation: any `core/` rename re-runs `make mutation` in the same commit and updates the exclusion keys, following the renumbering precedent recorded in `add-runtime-metrics` task 6.4.
- **A snapshot test that inspects source can rot into a maintenance tax** if it grows opinions beyond name equality. Mitigation: the test asserts exactly two things per module (public-name set, `__all__` consistency); anything more belongs in a new requirement.
- **The deprecation window is unenforceable by CI** and relies on reviewers honoring CONTRIBUTING. Accepted: the snapshot guarantees visibility, which is the enforceable part; the window itself is a release-review judgment, the same trust model as "never weaken a test".

## Migration Plan

1. Land the snapshot test, the initial `public-surface.toml` capturing the *current* 245-name surface verbatim, and the `_deprecation.py` helper with its unit test. Nothing breaks; drift detection starts immediately.
2. Run the audit: classify, underscore-rename internals, add per-module `__all__`, re-export `tool`/`StreamAgent`, update `tests/test_import.py`'s frozen set, amend project.md, update the snapshot to the post-audit surface, and fix the four mutation-exclusion keys if `core/` names moved. One reviewable commit per package area to keep diffs legible.
3. Enable `D1` in ruff, write the surviving missing docstrings, add the `tests/*` per-file ignore. `make lint` goes green and stays gating.
4. Write `docs/api.md` and its drift test; add the CONTRIBUTING deprecation section.
5. Update the repo-scaffolding spec delta; run the full gate suite; confirm the gate fails when it should by adding a throwaway public name (snapshot test reddens) and stripping one docstring (`make lint` reddens), then reverting both — a gate never observed failing is not known to work.

Rollback: each step is independently revertible; reverting step 2's renames restores the old import paths with no state, wire, or behavior implications. The freeze itself (steps 1, 3) has no runtime footprint at all.

## Open Questions

- Should `Complete`, `Suspend`, and `AgentContext` be promoted to the root at 1.0? Every protocol-agent author types them; the counterargument is root-namespace minimalism and that agent *authoring* is nominally the frameworks' job. Deferred to the 1.0 release review with the module tier as the safe default (promotion later is free; demotion is not).
- Does the effector deserve a separate, weaker tier? It is a reference *service* — its CLI flags and config are its real contract, and its Python module surface (sources/sinks/dedup builders) is arguably all internal. The audit takes the conservative path (config + CLI public, wiring internal) but the service's own 1.0 story may want its own change.
- Should the freeze eventually cover *signatures* (e.g. a griffe-based API diff in CI) rather than names only? Out of scope here; the snapshot's TOML shape leaves room to grow per-name metadata without a format break.
