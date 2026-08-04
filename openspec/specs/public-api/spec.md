# public-api Specification

## Purpose
TBD - created by archiving change add-1-0-api-freeze. Update Purpose after archive.
## Requirements
### Requirement: The root namespace re-exports a fixed, curated surface

`beam_agents/__init__.py` SHALL re-export exactly sixteen names: `AdkAgent`, `AgentConfig`, `Deny`, `Drop`, `Escalate`, `FallbackContext`, `HitlPolicy`, `LangGraphAgent`, `PydanticAIAgent`, `RunAgent`, `RunAgentOutputs`, `ShardKeys`, `StreamAgent`, `shard_key`, `tool`, and `unshard_key`, and `__all__` SHALL list exactly these. Every name except the three adapter classes MUST resolve eagerly; `LangGraphAgent`, `AdkAgent` and `PydanticAIAgent` MUST continue to resolve lazily via module `__getattr__`, each raising an `ImportError` that names the extra to install when its adapter's dependencies are absent. Importing the package MUST remain free of side effects.

The code-style section of `openspec/project.md` SHALL enumerate the same sixteen names, so the constitution and the code state one surface.

#### Scenario: The root surface matches the frozen list

- **WHEN** `beam_agents` is imported and its `__all__` compared against the sixteen frozen names
- **THEN** the sets are equal, and every eagerly-resolved name is importable directly from the root

#### Scenario: The names project.md promises are importable

- **WHEN** a user follows the constitution and runs `from beam_agents import tool, StreamAgent`
- **THEN** the import succeeds, yielding the tool decorator and the streaming agent protocol

#### Scenario: The lazy adapter contract is unchanged

- **WHEN** `beam_agents.LangGraphAgent` is accessed in an environment without the `langgraph` extra installed
- **THEN** an `ImportError` is raised naming `beam-agents[langgraph]` as the extra to install, and no other root name is affected

### Requirement: Every public name is frozen by a committed surface snapshot

The repository SHALL commit a snapshot file enumerating, for every non-generated *public* module under `src/beam_agents`, the sorted public (non-underscore) top-level names and the module's declared `__all__` where one exists. A module is public when no component of its path relative to `src/` is a single-underscore private name — so `_protos/`, `adapters/_transport.py`, `model/_http.py` and the `yaml/_*.py` modules are outside the frozen surface by their path alone, while `__init__.py` and `__main__.py` (dunder names, not private ones) stay inside it. The snapshot file SHALL be generated from the tree rather than hand-maintained. A unit test in the offline `ci` lane SHALL re-derive both from the module sources by AST inspection and fail on any difference from the snapshot — an added name, a removed name, or an `__all__` that disagrees with the snapshot — naming the module and the offending name.

The comparison MUST be exact equality in both directions: unreviewed surface growth and unreviewed removal are both failures. Changing the surface intentionally SHALL require updating the snapshot in the same change, so every surface change is a reviewable diff. The snapshot derivation MUST NOT rely on runtime `dir()` introspection, which observes imported submodules and instrumentation artifacts rather than the declared surface.

#### Scenario: An unreviewed public addition fails the suite

- **WHEN** a change adds a public top-level function to any `beam_agents` module without updating the snapshot
- **THEN** the surface test fails, naming the module and the new name

#### Scenario: An unreviewed removal fails the suite

- **WHEN** a change deletes or underscore-renames a snapshot-listed public name without updating the snapshot
- **THEN** the surface test fails, naming the module and the missing name

#### Scenario: Underscore-prefixed names are outside the frozen surface

- **WHEN** a change adds a top-level name with a leading underscore
- **THEN** the surface test passes without a snapshot update, because private names are not part of the contract

#### Scenario: A declared `__all__` cannot drift from the snapshot

- **WHEN** a module's `__all__` is edited so it no longer matches the snapshot's record for that module
- **THEN** the surface test fails for that module

### Requirement: Public names outside the audited contract are underscore-private

Every top-level name in `src/beam_agents` SHALL be either part of the audited public contract — the root tier or a module-tier name listed in its module's `__all__` — or prefixed with a leading underscore. Every public module that declares a public top-level name SHALL therefore declare an `__all__`, and a unit test SHALL fail on any public module that declares a public name absent from its own `__all__`. Modules that exist only as internal machinery SHALL expose no public contract names. The audit SHALL resolve every name that is public today but internal in intent — including the adapter transport helpers, the provider `decode` functions, and effector wiring internals — by underscore-renaming rather than by documenting them into the contract.

A name whose only in-repo consumers are the runtime's own modules and their tests is internal; a name an example under `examples/` constructs, or a `docs/` page presents as usage, is contract and SHALL stay public. This is what keeps the audit from privatizing a name the repository itself documents as the way to do something.

Where a rename changes a qualified function name inside `src/beam_agents/core/`, the corresponding mutant names change, and any affected `mutation-exclusions.toml` entries MUST be updated in the same change so the mutation gate's stale-entry detection does not fire.

#### Scenario: Internal helpers are not part of the public surface

- **WHEN** the post-audit snapshot is inspected
- **THEN** internal machinery such as the LangGraph transport helpers appears under underscore-prefixed names or not at all, and every remaining public name is contract

#### Scenario: A core rename keeps the mutation gate green

- **WHEN** the audit renames a public function in `src/beam_agents/core/` that has exclusion entries keyed by its mutant names
- **THEN** the exclusion entries are re-keyed in the same change and `make mutation` passes with no stale-entry failure

### Requirement: Every public name has a docstring, enforced by the lint gate

Every public module, class, function, and method in `src/beam_agents` SHALL have a docstring, as SHALL every public name under `examples/` — the examples are user-facing documentation, not tooling. Enforcement SHALL be ruff's pydocstyle completeness rules (the `D1` selector) added to the existing `[tool.ruff.lint]` `select`, running inside the existing `make lint` target with no new tool or CI step. `D105` (magic methods) and `D107` (`__init__`) SHALL be ignored — dunder semantics are defined by the language protocol, and constructor contracts belong in the class docstring — and `tests/*`, `benchmarks/*` and `scripts/*` SHALL be exempted from `D1` via the per-file-ignores table: test names are the documentation under the scenario-named-test convention, and benchmark and script entry points are repo tooling with no compatibility promise. The gate MUST be a per-name check, not a percentage threshold: a single new undocumented public name fails the build regardless of overall coverage.

Docstrings written to satisfy the gate MUST state the name's contract — meaning of inputs, effects and their staging, raise conditions — rather than restating the signature.

#### Scenario: A missing public docstring fails lint

- **WHEN** a change adds a public function without a docstring to any `src/beam_agents` module
- **THEN** `make lint` exits non-zero with a `D103` finding naming the function

#### Scenario: Overload stubs are not flagged

- **WHEN** ruff checks a function with `@overload`-decorated stubs followed by a documented implementation, such as the `tool` decorator
- **THEN** the stubs produce no docstring finding

#### Scenario: Privatized names are not gated

- **WHEN** a top-level function is underscore-renamed by the audit
- **THEN** it produces no `D1` finding, because the completeness rules apply to public names only

### Requirement: Removing a frozen public name requires a deprecation window

After the freeze, a frozen public name SHALL NOT be removed or renamed without a deprecation window of at least one minor release. During the window the old name MUST keep working and MUST emit a `DeprecationWarning` that names the replacement (or states that there is none) and the first release that may remove it. The package SHALL provide a single private helper that emits these warnings, usable from a module-level `__getattr__` so the deprecated name stays out of the module's living namespace. The policy, including the bulk-privatization exemption for pre-1.0 changes, SHALL be documented in `CONTRIBUTING.md`.

Both edges of the window SHALL be visible in review through the surface snapshot: the deprecation and the eventual removal each require a snapshot update, so neither can happen silently.

#### Scenario: A deprecated name warns and still works

- **WHEN** code accesses a public name that is inside its deprecation window
- **THEN** the access succeeds and a `DeprecationWarning` is emitted naming the replacement and the first release that may remove the name

#### Scenario: A silent removal cannot pass CI

- **WHEN** a change removes a frozen public name without touching the snapshot
- **THEN** the surface test fails, forcing the removal into a reviewable diff where the deprecation window is checked

### Requirement: The frozen surface is documented on an API reference page

The repository SHALL ship an API reference page under `docs/` enumerating the frozen public surface: every root re-export and every module-tier public name grouped by its module, each with a one-line statement of its contract. A unit test SHALL assert that every public name recorded in the surface snapshot appears on the page inside a code span — a name mentioned only in free prose has not been documented — so the reference cannot silently fall behind the frozen surface. A second test SHALL assert the converse for the page's entry rows, so a privatized name cannot linger there as stale documentation.

#### Scenario: The reference page covers the frozen surface

- **WHEN** the documentation drift test runs against the committed reference page and surface snapshot
- **THEN** every snapshot-frozen public name is found on the page

#### Scenario: A surface change without a documentation update fails

- **WHEN** a change adds a public name to the snapshot but not to the reference page
- **THEN** the documentation drift test fails, naming the undocumented name
