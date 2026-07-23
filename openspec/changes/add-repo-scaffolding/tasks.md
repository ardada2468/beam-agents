## 1. Repository layout and Python floor

- [x] 1.1 Create `.python-version` pinned to `3.11`
- [x] 1.2 Create `src/beam_agents/__init__.py` (empty, no public names) and `src/beam_agents/py.typed`
- [x] 1.3 Create empty `tests/` with `tests/__init__.py` and `tests/conftest.py` (registers async mode, no fixtures yet)
- [x] 1.4 Create empty `protos/.gitkeep` and `docker/.gitkeep` placeholders
- [x] 1.5 Add `.gitignore` (Python, `.venv/`, `__pycache__/`, `.mypy_cache/`, `.ruff_cache/`, `dist/`, `.coverage*`, `htmlcov/`, generated `*_pb2.py` NOT ignored)
- [x] 1.6 Add `.editorconfig` (utf-8, LF, 4-space Python, 2-space YAML/TOML)

## 2. `pyproject.toml` and `uv` lockfile

- [x] 2.1 Draft `[project]` table: `name = "beam-agents"`, `version = "0.0.0"`, `requires-python = ">=3.10,<3.13"`, `description`, `readme`, `license`
- [x] 2.2 Add `[project.dependencies]`: `apache-beam[gcp]>=2.60`, `httpx[http2]`, `pydantic>=2`, `protobuf`
- [x] 2.3 Add `[dependency-groups]`: `dev` (aggregator of the rest), `test` (`pytest`, `pytest-asyncio`, `pytest-timeout`, `pytest-cov`, `hypothesis`), `lint` (`ruff`), `typecheck` (`mypy`), `integration` (`testcontainers[kafka,redis]`), `bench` (`pyperf`), `docs` (placeholder empty for now)
- [x] 2.4 Add `[tool.hatch.build.targets.wheel]` (or equivalent) so `uv build` produces a wheel; set `packages = ["src/beam_agents"]`
- [x] 2.5 Run `uv lock` and commit `uv.lock`
- [x] 2.6 Verify `uv sync --all-groups` succeeds and `python -c "import beam_agents"` exits 0

## 3. `ruff` configuration

- [x] 3.1 Add `[tool.ruff]` with `line-length = 100`, `target-version = "py310"`, `src = ["src", "tests"]`
- [x] 3.2 Add `[tool.ruff.lint]` `select = ["E", "F", "I", "B", "UP", "SIM", "ASYNC", "PL", "RUF"]`, sensible `ignore` for `PLR0913` etc. as needed
- [x] 3.3 Add `[tool.ruff.lint.per-file-ignores]` for `tests/*` (allow `PLR2004`, `S101`)
- [x] 3.4 Add `[tool.ruff.format]` with defaults (double quotes, spaces)
- [x] 3.5 Verify `uv run ruff check .` and `uv run ruff format --check .` both pass on the empty scaffold

## 4. `mypy --strict` configuration

- [x] 4.1 Add `[tool.mypy]` `strict = true`, `python_version = "3.10"`, `files = ["src", "tests"]`, `enable_error_code = ["ignore-without-code"]`, `warn_unreachable = true`
- [x] 4.2 Add per-module override for `apache_beam.*` with `ignore_missing_imports = true`
- [x] 4.3 Verify `uv run mypy` exits 0 on the empty scaffold

## 5. `pytest` configuration

- [x] 5.1 Add `[tool.pytest.ini_options]` with `addopts = "-ra --strict-markers --strict-config --cov=beam_agents --cov-report=term-missing"`, `asyncio_mode = "auto"`, `timeout = 30`, `testpaths = ["tests"]`
- [x] 5.2 Register markers: `integration`, `semantics`, `dataflow`, `slow` (with descriptions)
- [x] 5.3 Add a smoke test `tests/test_import.py` that asserts `import beam_agents` succeeds and public surface is empty
- [x] 5.4 Verify `uv run pytest` passes offline

## 6. Pre-commit hooks

- [x] 6.1 Add `.pre-commit-config.yaml` with `ruff` (check + format), `mypy` (local hook using project env), `check-yaml`, `check-toml`, `end-of-file-fixer`, `trailing-whitespace`
- [x] 6.2 Add local hook `protobuf-drift`: regenerates from `protos/*.proto` and runs `git diff --exit-code` on `*_pb2.py`
- [x] 6.3 Add local hook `openspec-change-required`: bash script that fails if staged paths touch `src/` while no `openspec/changes/*/proposal.md` exists in the tree, honouring `BEAM_AGENTS_ALLOW_NO_CHANGE=1`
- [x] 6.4 Install and run `pre-commit run --all-files` locally to verify clean pass

## 7. Docker compose stack

- [x] 7.1 Author `docker/compose.yaml` with `redpanda` (Kafka on `19092`), `redis` (on `16379`), `flink-jobmanager` + `flink-taskmanager` (JobManager UI on `18081`); pin every image by `@sha256:` digest
- [x] 7.2 Add healthchecks: Redpanda via `rpk cluster health`, Redis via `redis-cli ping`, Flink via `curl -f http://localhost:8081/overview`
- [x] 7.3 Bring stack up with `docker compose -f docker/compose.yaml up -d` and verify `docker compose ps` shows all healthy within 60 s
- [x] 7.4 Add `docker/README.md` documenting non-default ports and rationale

## 8. `Makefile` targets

- [x] 8.1 Add `Makefile` targets: `bootstrap` (`uv sync --all-groups && pre-commit install`), `fmt`, `lint`, `type`, `test-unit`, `test-integration`, `test-semantics`, `mutation`, `compose-up`, `compose-down`, `proto`, `help` (default)
- [x] 8.2 Ensure each target uses `uv run` so the pinned env is authoritative
- [x] 8.3 Verify `make lint`, `make type`, `make test-unit` all succeed on the empty scaffold

## 9. GitHub Actions workflows

- [x] 9.1 Add `.github/workflows/ci.yml`: matrix `python-version: [3.10, 3.11, 3.12]` × `os: [ubuntu-latest, macos-latest]`, steps = checkout → setup-uv → `uv sync --frozen --group lint --group typecheck --group test` → `make lint type test-unit`; cache `~/.cache/uv` keyed on `uv.lock` hash
- [x] 9.2 Add `.github/workflows/integration.yml`: `ubuntu-latest`, steps = checkout → setup-uv → `uv sync --frozen --group test --group integration` → `make compose-up` → `make test-integration test-semantics` → `make compose-down` (always run)
- [x] 9.3 Add `.github/workflows/quality.yml`: `ubuntu-latest`, steps = checkout with fetch-depth 0 → setup-uv → `uv sync --frozen --group test --group lint --group typecheck` → `make mutation` (guarded to only run when `core/` files changed vs. `main`) → coverage ratchet script comparing `coverage.xml` vs. `main` baseline
- [x] 9.4 Add `.github/workflows/nightly.yml`: `schedule: "0 7 * * *"` + `workflow_dispatch`, `permissions: { id-token: write, contents: read }`, `google-github-actions/auth@v2` with `workload_identity_provider`, guard body on `if: vars.GCP_PROJECT_ID != ''`, then `uv sync --frozen --group test --group integration --group bench` → `uv run pytest -m dataflow`
- [x] 9.5 Add `docs/ci.md` or README section listing which workflows are required for merge and how to trigger `nightly` manually
- [ ] 9.6 In repo settings (documented in the tasks list, applied out-of-band): mark `ci`, `integration`, `quality` as required checks on `main` — **blocked: no GitHub remote configured yet; documented in `docs/ci.md` for the user to apply once the repo is pushed**

## 10. README and contributor docs

- [x] 10.1 Add top-level `README.md` covering: what beam-agents is (1 paragraph pointing to `openspec/project.md`), bootstrap (`uv sync --all-groups && pre-commit install`), running tests (four tiers), running compose, CI workflow map
- [x] 10.2 Add `CONTRIBUTING.md` covering OpenSpec workflow requirement, marker registry, `Makefile` as CI parity contract, `BEAM_AGENTS_ALLOW_NO_CHANGE` bypass caveat

## 11. Verification

- [ ] 11.1 Fresh clone → `uv sync --all-groups` → `make bootstrap lint type test-unit` all green
- [ ] 11.2 `make compose-up` brings stack healthy within 60 s; `make test-integration` passes; `make compose-down` cleans up
- [ ] 11.3 Push a throwaway branch with an intentional `ASYNC` violation; confirm `ci-lint` fails on that job
- [ ] 11.4 Push a throwaway branch with an unregistered pytest marker; confirm `ci-unit` fails with `--strict-markers`
- [ ] 11.5 Attempt to commit an edit under `src/beam_agents/` without an active OpenSpec change; confirm pre-commit blocks with a pointer to the workflow
- [ ] 11.6 Run `openspec validate add-repo-scaffolding --strict` and confirm clean

## 12. Archive readiness

- [ ] 12.1 All tasks above checked and verified in a clean environment
- [ ] 12.2 PR opened linking each new scenario in `specs/repo-scaffolding/spec.md` to the verification step (11.x) that exercises it
- [ ] 12.3 On merge, run `/opsx:archive add-repo-scaffolding` to promote `specs/repo-scaffolding/spec.md` into `openspec/specs/repo-scaffolding/spec.md`
