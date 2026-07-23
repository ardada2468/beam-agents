## Why

`beam-agents` has no code, no tooling, and no CI. Before any capability can be built spec-first + TDD-style, the repository needs deterministic environments, enforced quality gates, reproducible local infrastructure, and CI that maps 1:1 to the testing tiers defined in `openspec/project.md`. Landing this scaffolding first prevents every subsequent change from having to relitigate toolchain, layout, and gate decisions.

## What Changes

- Add `uv`-managed project layout (`pyproject.toml` + `uv.lock` committed, `src/beam_agents/` package, `tests/` root, `protos/` placeholder, `docker/` for compose assets).
- Declare Python `>=3.10,<3.13` and `apache-beam[gcp]>=2.60`; split runtime vs. dev deps into `pyproject.toml` dependency groups (`dev`, `test`, `lint`, `typecheck`, `integration`, `docs`, `bench`).
- Configure `ruff` (lint + format, including `ASYNC` rules) and `mypy --strict` for `src/`, with the project-mandated `ignore_missing_imports` scoped to Beam modules only.
- Configure `pytest` with markers `integration`, `semantics`, `dataflow`, `slow` (registered in `pyproject.toml`, unregistered marker use is an error); default run stays offline and docker-free.
- Add `pre-commit` config wiring ruff, mypy, protobuf-generation drift check, and a guard that blocks `src/` commits without a referenced OpenSpec change.
- Add `docker/compose.yaml` with Redpanda (Kafka API), Redis, and Flink 1.19 services for `-m integration` runs; unit tests MUST still pass with compose down.
- Add GitHub Actions workflows: `ci.yml` (lint + type + unit matrix 3.10/3.11/3.12), `integration.yml` (compose-backed integration + semantics tiers), `quality.yml` (mutation on touched `core/` + coverage ratchet), `nightly.yml` (Dataflow via Workload Identity Federation, FakeLLM-over-HTTP).
- Add `Makefile`/`justfile` shortcuts (`fmt`, `lint`, `type`, `test`, `integ`, `compose-up/down`, `proto`) so contributor commands match CI verbatim.

No runtime code is added — this change delivers scaffolding only. Every subsequent change lands atop these gates.

## Capabilities

### New Capabilities

- `repo-scaffolding`: reproducible developer environment, dependency management, code-quality gates, local service topology, and CI workflow contracts that every future change relies on.

### Modified Capabilities

<!-- None: this is the first change; no existing specs to modify. -->

## Impact

- **Files created**: `pyproject.toml`, `uv.lock`, `.python-version`, `src/beam_agents/__init__.py`, `tests/conftest.py`, `docker/compose.yaml`, `.pre-commit-config.yaml`, `.github/workflows/{ci,integration,quality,nightly}.yml`, `Makefile`, `.gitignore`, `.editorconfig`, `ruff.toml`-equivalent config block, `mypy.ini`-equivalent config block.
- **APIs**: none yet; `beam_agents/__init__.py` is an empty public surface placeholder.
- **Dependencies introduced**: `apache-beam[gcp]`, `httpx[http2]`, `pydantic>=2`, `protobuf`, `ruff`, `mypy`, `pytest`, `pytest-asyncio`, `pytest-timeout`, `hypothesis`, `mutmut`, `pre-commit`, `testcontainers` (integration group only).
- **CI**: four new required-check workflows; `ci` becomes the merge gate for all future PRs.
- **Contributor workflow**: `uv sync --all-groups` becomes the canonical bootstrap; `docker compose up` provisions integration deps locally.
- **Downstream**: unblocks every subsequent capability (core DoFn, model client, tool registry, adapters) by giving them a place to land with gates already green.
