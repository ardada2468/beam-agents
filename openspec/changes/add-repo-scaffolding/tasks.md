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
- [x] 9.6 In repo settings (documented in the tasks list, applied out-of-band): mark `ci`, `integration`, `quality` as required checks on `main` — applied via `gh api PUT .../branches/main/protection` against [ardada2468/beam-agents](https://github.com/ardada2468/beam-agents) with `strict=true` and all 8 exact check-run contexts (`ci` matrix ×6, `integration`, `quality`); `enforce_admins=false` so the maintainer isn't locked out solo

## 10. README and contributor docs

- [x] 10.1 Add top-level `README.md` covering: what beam-agents is (1 paragraph pointing to `openspec/project.md`), bootstrap (`uv sync --all-groups && pre-commit install`), running tests (four tiers), running compose, CI workflow map
- [x] 10.2 Add `CONTRIBUTING.md` covering OpenSpec workflow requirement, marker registry, `Makefile` as CI parity contract, `BEAM_AGENTS_ALLOW_NO_CHANGE` bypass caveat

## 11. Verification

- [x] 11.1 Fresh clone → `uv sync --all-groups` → `make bootstrap lint type test-unit` all green — verified by removing `.venv` and re-running `make bootstrap lint type test-unit` from scratch (no GitHub remote yet to do a literal clone)
- [x] 11.2 `make compose-up` brings stack healthy within 60 s; `make test-integration` passes; `make compose-down` cleans up — stack healthy in ~7s; found and fixed a real gap where pytest exit code 5 ("no tests collected") failed the integration/semantics/dataflow make targets on this pre-`core/` repo, now tolerated
- [x] 11.3 Push a throwaway branch with an intentional `ASYNC` violation; confirm `ci-lint` fails on that job — verified locally via `make lint` (no remote to push to yet); `ASYNC251` caught a `time.sleep` inside an `async def`
- [x] 11.4 Push a throwaway branch with an unregistered pytest marker; confirm `ci-unit` fails with `--strict-markers` — verified locally via `uv run pytest` with a typo'd marker, correctly rejected
- [x] 11.5 Attempt to commit an edit under `src/beam_agents/` without an active OpenSpec change; confirm pre-commit blocks with a pointer to the workflow — verified in an isolated sandbox repo: blocks with no change present, passes with a change present, passes with `BEAM_AGENTS_ALLOW_NO_CHANGE=1`
- [x] 11.6 Run `openspec validate add-repo-scaffolding --strict` and confirm clean

## 12. Archive readiness

- [x] 12.1 All tasks above checked and verified in a clean environment
- [x] 12.2 Scenario-to-verification traceability documented (no literal PR: this change was pushed directly to `main` as an initial-commit trunk push before a remote existed, so there is no branch with a diff to open a PR against; the traceability a PR description would carry is captured below instead)
- [x] 12.3 On merge, run `/opsx:archive add-repo-scaffolding` to promote `specs/repo-scaffolding/spec.md` into `openspec/specs/repo-scaffolding/spec.md`

### 12.2 Scenario traceability

Every scenario in `specs/repo-scaffolding/spec.md`, and how it was actually exercised (not just asserted):

| Scenario | Verified by |
|---|---|
| Bootstrap on a clean checkout | 11.1 — removed `.venv`, ran `make bootstrap lint type test-unit` from scratch |
| Required top-level directories exist | 11.1 (implicit: bootstrap requires every referenced path to exist) |
| Install rejects Python 3.13 | Ad hoc: `uv sync --python 3.13` → resolver error citing `requires-python` |
| Runtime dependencies present after minimal install | 2.6, reconfirmed after the default-groups fix (below) |
| Base install omits integration tooling | Ad hoc, isolated venv: bare `uv sync` → `testcontainers`/`mutmut` absent. **Found and fixed a real bug**: `dev` aggregated every group and uv syncs `dev` by default, so a bare `uv sync` was silently installing everything. Fixed via `[tool.uv] default-groups = []` |
| Lint job installs only lint group | Same ad hoc test, `--group lint` alone → `ruff` present, `pytest` absent, after the same default-groups fix (explicit groups add to defaults rather than replacing them, so this scenario had the identical bug) |
| Blocking call in async function fails lint | 11.3 — `time.sleep` inside `async def` → `ASYNC251` |
| Formatter drift fails CI | Ad hoc: intentionally malformed temp file → `ruff format --check` exit 1 |
| Missing type hint fails typecheck | Ad hoc: temp function with no return annotation → `mypy` `no-untyped-def` error |
| Beam stub gaps do not fail typecheck | Ad hoc: temp module importing and calling `apache_beam` → `mypy` clean |
| Unregistered marker is an error | 11.4 — typo'd marker → `strict-markers` rejection |
| Default run excludes integration and dataflow tiers | 11.1/5.4 — `test-unit` collects only unmarked tests; marker-filter direction cross-checked in 11.2 |
| Committing to src/ without a change fails | 11.5 — isolated sandbox repo, all three branches (block / active change / bypass env var) |
| Protobuf drift blocks commit | Ad hoc: added a temp `.proto`, generated bindings, then edited the `.proto` without regenerating → `check_proto_drift.sh` correctly caught the stale `_pb2.py`/`.pyi`. **Found and fixed a real bug**: `grpcio-tools` was never declared in any dependency group, so this hook would have failed with `ModuleNotFoundError` the first time anyone added a `.proto` file. Added to the `dev` group |
| Compose stack starts healthy | 11.2/7.3 — healthy in ~7–12s, well under the 60s budget |
| Unit tests pass with compose down | 11.1 — `test-unit` run before compose was ever started |
| CI workflow runs on pull request | Push-triggered runs on `main` observed directly via `gh run list` (all three of `ci`, `integration`, `quality` fire and must pass); the `pull_request` trigger is configured identically in each workflow's `on:` block |
| Nightly workflow uses Workload Identity Federation | Not runtime-tested — no live GCP project exists yet. `nightly.yml` is wired to `google-github-actions/auth@v2` with `workload_identity_provider`/`service_account` vars and no static key is stored anywhere; this is the explicit open question in `design.md` (Q3), deferred until GCP infra is procured |
| Nightly workflow no-ops without configured GCP project | Ad hoc: `gh workflow run nightly.yml` with `GCP_PROJECT_ID` unset → `skip-notice` job ran and succeeded, `dataflow` job was skipped |
| CI step invokes a make target | Static: every step in all four workflow files is `make <target>` (setup steps for `uv sync`/auth excepted) |
| Local lint matches CI lint | `make lint` passed locally, then the identical command passed in the actual `ci` GitHub Actions run for the same commit |
| Fresh import is side-effect free | 2.6 / `test_import.py::test_import_succeeds` |
| Public surface is empty | `test_import.py::test_public_surface_is_empty`, run in every `test-unit` invocation |

Two bugs were found and fixed during this pass that the original task checklist didn't anticipate — both are now covered by the scenarios above and committed (`df63675`).
