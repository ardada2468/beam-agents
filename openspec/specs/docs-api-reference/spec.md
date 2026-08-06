# docs-api-reference Specification

## Purpose
TBD - created by archiving change add-docs-website. Update Purpose after archive.
## Requirements
### Requirement: The API reference is generated from the installed package by introspection

`scripts/gen_api_reference.py` SHALL import `beam_agents` and emit `website/generated/api.json` describing the public surface. For each entry it MUST record the qualified name, kind (class, function, dataclass, constant), signature with resolved type annotations, the docstring verbatim, the defining source path and line number, and the optional extra required to import it (if any). The generator MUST NOT accept hand-written descriptions or override docstrings.

#### Scenario: Generated reference matches the package's declared surface

- **WHEN** `scripts/gen_api_reference.py` runs against the installed package
- **THEN** the top-level entries in `api.json` are exactly the names in `beam_agents.__all__`

#### Scenario: Signatures come from introspection

- **WHEN** a contributor adds a parameter to a public function's signature and regenerates
- **THEN** the new parameter, its annotation, and its default appear in `api.json` with no other edit

#### Scenario: Source locations are recorded

- **WHEN** any entry in `api.json` is inspected
- **THEN** it carries a repository-relative source path and line number that resolve to the defining statement

### Requirement: Lazily-exported symbols are documented with their required extra

Symbols exported through the package's module-level `__getattr__` (optional-dependency adapters) SHALL appear in the reference, annotated with the extra required to import them. The generator MUST NOT silently omit a symbol because its optional dependency is absent, and MUST fail loudly if it cannot resolve a name listed in `__all__` for any other reason.

#### Scenario: Adapter symbol is documented with its extra

- **WHEN** the generator runs in an environment with the `langgraph` extra installed
- **THEN** `LangGraphAgent` appears in `api.json` with `requires_extra: "langgraph"`

#### Scenario: Missing optional dependency is reported, not swallowed

- **WHEN** the generator runs without the `langgraph` extra installed
- **THEN** it exits non-zero naming the symbol and the extra required to generate the reference

### Requirement: The committed reference is drift-checked against the package

`website/generated/api.json` SHALL be committed. `make site-check` MUST regenerate it and fail if the result differs from the committed file, reporting the exact regeneration command.

#### Scenario: Changing the public API without regenerating fails the check

- **WHEN** a contributor changes a public signature under `src/beam_agents/` and does not regenerate
- **THEN** `make site-check` exits non-zero showing the diff and naming the regeneration command

#### Scenario: Regenerating a clean tree produces no diff

- **WHEN** a contributor runs the generator on an unmodified checkout
- **THEN** `git status --porcelain website/generated/api.json` reports no change

### Requirement: The reference renders as server-rendered, linkable pages

The site SHALL render `api.json` as pre-rendered pages — one per top-level symbol — each with a stable anchor per member, its signature, its docstring rendered as prose, its required extra when applicable, and a link to the defining source line in the repository. The API index MUST list every documented symbol.

#### Scenario: Symbol page is pre-rendered and linkable

- **WHEN** a client issues `GET /api/RunAgent` without executing JavaScript
- **THEN** the response is `200` and contains the symbol's signature and docstring text

#### Scenario: Source links resolve

- **WHEN** `make site-check` validates the reference's source links
- **THEN** every link targets an existing repository path, and a link to a missing path fails the check

#### Scenario: Adding a public symbol adds a page

- **WHEN** a symbol is added to `beam_agents.__all__` and the reference is regenerated
- **THEN** a page for it exists after rebuild with no route wiring, and it appears in the API index and the sitemap

### Requirement: The reference states its own coverage honestly

The API section SHALL state that it documents only the package's declared public surface, and MUST NOT present private or underscore-prefixed modules as public API. Where a docstring is absent, the page MUST render an explicit "No docstring" marker rather than inventing a description.

#### Scenario: Private modules are excluded

- **WHEN** the API index is inspected
- **THEN** no underscore-prefixed module or member appears in it

#### Scenario: Missing docstring is marked, not filled

- **WHEN** a public symbol has no docstring
- **THEN** its page renders an explicit "No docstring" marker and contains no generated prose describing it
