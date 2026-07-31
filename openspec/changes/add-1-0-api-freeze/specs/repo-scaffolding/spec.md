## MODIFIED Requirements

### Requirement: Public API surface starts empty and typed

`src/beam_agents/__init__.py` SHALL exist and `mypy --strict` MUST pass on the module. The surface started empty at scaffolding time and SHALL grow only through OpenSpec changes; as of the 1.0 API freeze, the names it exposes are governed by the `public-api` capability and its committed surface snapshot rather than by this requirement.

Importing the package MUST NOT perform I/O, spawn threads, mutate global state, or import optional dependencies — adapter frameworks, effector transport clients, and exporter protos MUST stay out of the import graph until their feature is used. The single sanctioned piece of import-time indirection is the module-level `__getattr__` that lazily resolves optional-extra adapter classes.

#### Scenario: Fresh import is side-effect free

- **WHEN** a contributor imports `beam_agents` in a clean interpreter with no optional extras installed
- **THEN** the import succeeds without network or filesystem access, no threads are spawned, and no optional-extra module (LangGraph, effector transports, opentelemetry-proto) appears in `sys.modules`

#### Scenario: Public surface matches the committed snapshot

- **WHEN** the surface test derives the root module's `__all__` and public names from source
- **THEN** they equal the `public-api` snapshot's record for `beam_agents/__init__.py`, and any deviation fails the offline `ci` lane
