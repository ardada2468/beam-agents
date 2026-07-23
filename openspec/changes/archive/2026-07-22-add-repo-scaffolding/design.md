## Context

`beam-agents` is a greenfield repo. The project's governing principle — "runtime, not framework" — puts an unusually heavy load on the toolchain: we must enforce async correctness, protobuf-only state, per-key determinism, and mutation-tested `core/`, all before the first line of `core/` exists. The scaffolding needs to make the correctness invariants in `openspec/project.md` mechanically checkable rather than culturally aspired to.

Constraints that shape this design:

- Python `>=3.10,<3.13` — Beam Python SDK does not yet support 3.13 stably.
- `apache-beam[gcp]>=2.60` pulls a large native dep tree; lockfile churn dominates without careful group scoping.
- Unit tests MUST pass offline with no docker (project.md testing tiers).
- CI mirrors the four testing tiers 1:1 (`ci` ↔ unit, `integration` ↔ integration+semantics, `quality` ↔ mutation+coverage ratchet, `nightly` ↔ dataflow).
- Nightly Dataflow uses Workload Identity Federation; no long-lived JSON keys land in secrets.
- OpenSpec workflow requires that no `src/` commit lands without a referenced change; the pre-commit hook must enforce this locally so CI is not the first line of defense.

Stakeholders: solo maintainer today; design must survive contributor onboarding without a private runbook.

## Goals / Non-Goals

**Goals:**

- One-command bootstrap (`uv sync --all-groups`) reproduces every CI job locally.
- CI/pre-commit/local `make` commands are the same commands with the same flags — no CI-only invocations that a contributor cannot reproduce.
- Dependency groups let `ci` install only what it needs (fast unit matrix) while `integration`/`nightly` pull heavier deps.
- `ruff` and `mypy --strict` gates fail loudly on day one, so future changes cannot silently regress.
- `pytest` marker registry is closed: unknown markers are errors, so `-m integration` cannot silently miss a mis-typed marker.
- Docker compose spins up Redpanda + Redis + Flink 1.19 in under 60 s with fixed image digests.
- CI workflow definitions are short and delegate to `Makefile` targets, so behaviour is unified and testable locally.

**Non-Goals:**

- No `core/`, `model/`, `tools/`, `adapters/`, or protobuf schemas land in this change — those get their own OpenSpec changes.
- No published package (no PyPI release job). Only build validation.
- No documentation site scaffolding (mkdocs etc.) — deferred until there is API to document.
- No coverage floor number; the ratchet is "no decrease from previous main" with the initial baseline set to whatever the first `core/` change ships.
- No Windows CI; developers may work on Windows via WSL but supported CI runners are `ubuntu-latest` + `macos-latest`.

## Decisions

### D1: `uv` as the sole environment/dependency manager

**Choice:** `uv` with `pyproject.toml` + committed `uv.lock`; `uv sync --group <name>` drives every install path.

**Alternatives considered:**
- `poetry`: fine, but slower and lockfile format is Poetry-specific; the project.md already commits to `uv`.
- `pip-tools`: no groups, requires more custom orchestration.
- `hatch`: environments are more opinionated; adds a second layer over pip.

**Rationale:** project.md fixes `uv`; `uv`'s resolver is fast enough that CI can `uv sync --frozen` on cache miss without dominating runtime, and it produces the same environment as local.

### D2: Dependency group topology

**Choice:** groups `dev`, `test`, `lint`, `typecheck`, `integration`, `bench`, `docs`. CI jobs install only what they need:

| Job          | Groups                                     |
|--------------|--------------------------------------------|
| ci-lint      | `lint`                                     |
| ci-type      | `typecheck`                                |
| ci-unit      | `test`                                     |
| integration  | `test`, `integration`                      |
| quality      | `test`, `lint`, `typecheck` (mutmut needs source-runnable env) |
| nightly      | `test`, `integration`, `bench`             |
| dev-local    | `--all-groups`                             |

**Rationale:** heavy deps (`testcontainers`, `mutmut`, `hypothesis`, benchmark rigs) stay out of the fast unit matrix. Runtime deps (`apache-beam[gcp]`, `httpx[http2]`, `pydantic`, `protobuf`) go in the base `[project.dependencies]` so they are always installed — they are load-bearing for `beam_agents.__init__` even in lint-only jobs.

### D3: `ruff` config in `pyproject.toml`, `ruff.toml` not used

**Choice:** all `ruff` config lives in `[tool.ruff]` in `pyproject.toml`. Enabled rule groups: `E,F,I,B,UP,SIM,ASYNC,PL,RUF`, plus explicit `ASYNC` selections since the project bans blocking the async bridge. Formatter enabled (`ruff format`). Line length 100.

**Alternatives:** separate `ruff.toml` (simpler but splits config surface); `black` + `isort` (older, slower, two tools).

**Rationale:** one config file to review. Enabling `ASYNC` at the linter layer means violations of "never block the bridge event loop" fail `ci-lint` even if a reviewer misses them.

### D4: `mypy --strict` on `src/`, targeted opt-outs for Beam

**Choice:** `[tool.mypy]` sets `strict = true`, `python_version = "3.10"`. Per-module overrides use `ignore_missing_imports = true` for `apache_beam.*` only. Tests default to strict but allow `# type: ignore[...]` with error code required (`disable_error_code = []`, `enable_error_code = ["ignore-without-code"]`).

**Rationale:** project.md fixes this exact shape. Restricting the Beam escape hatch by module (not globally) means missing stubs in *our* code still fail.

### D5: `pytest` marker registry + strict markers

**Choice:** `[tool.pytest.ini_options]` sets `addopts = "-ra --strict-markers --strict-config"`, declares `markers = ["integration: requires docker compose", "semantics: correctness gates", "dataflow: nightly-only real Dataflow", "slow: >5s"]`, `asyncio_mode = "auto"`, `timeout = 30`. Default run collects everything but `-m "not integration and not semantics and not dataflow"` in `ci-unit`; integration adds `-m "integration or semantics"`.

**Rationale:** strict markers make typos (`-m integratoin`) an error instead of a silent zero-test pass. `asyncio_mode = "auto"` matches project.md.

### D6: Pre-commit hooks (local first line of defense)

**Choice:** `.pre-commit-config.yaml` runs:
1. `ruff check --fix` and `ruff format`
2. `mypy` on staged files (fast path; CI runs the full pass)
3. Protobuf generation drift check: regen from `protos/`, `git diff --exit-code` on `_pb2.py`
4. Custom local hook: block `src/` commits without an active OpenSpec change referenced in the commit message or staged files

**Rationale:** the OpenSpec workflow rule is unenforceable if it depends on reviewer memory. A tiny bash hook reading `git diff --cached --name-only` and checking for `openspec/changes/<name>/` presence suffices.

### D7: Docker compose topology

**Choice:** `docker/compose.yaml` with three services pinned by digest:
- `redpandadata/redpanda:v24.x@sha256:...` (single-node, `--overprovisioned --smp 1 --memory 1G`)
- `redis:7-alpine@sha256:...`
- `apache/flink:1.19-scala_2.12-java17@sha256:...` (jobmanager + taskmanager)

Ports fixed and namespaced: Kafka 19092, Redis 16379, Flink JobManager 18081 — non-default to avoid clashing with developer-local services. Healthchecks required; `testcontainers` in tests attaches to running compose or spins its own.

**Rationale:** digest-pinning avoids "works on my laptop" from a floating tag. Alt ports avoid the standard-port collision that bites every contributor once.

### D8: CI workflow decomposition

**Choice:** four workflows, all triggered by `push` and `pull_request` except `nightly` (schedule + `workflow_dispatch`):

- `ci.yml` — matrix `[3.10, 3.11, 3.12]` × `[ubuntu-latest, macos-latest]` running `make lint type test-unit`. Required check.
- `integration.yml` — `ubuntu-latest` only, `docker compose up -d`, run `make test-integration test-semantics`. Required check.
- `quality.yml` — `ubuntu-latest`, `make mutation` (mutmut on files touched vs. `main`) + coverage ratchet vs. `main`. Required check.
- `nightly.yml` — schedule `0 7 * * *` UTC, Dataflow via WIF (`google-github-actions/auth@v2` with `workload_identity_provider`), runs `-m dataflow`. Not required.

Each workflow: `uv sync --frozen --group <needed>` → `make <target>`. No inline scripts longer than three lines.

**Rationale:** required checks match testing tiers. WIF avoids service account keys.

### D9: `Makefile` as the CI/local contract

**Choice:** `Makefile` targets: `bootstrap`, `fmt`, `lint`, `type`, `test-unit`, `test-integration`, `test-semantics`, `mutation`, `compose-up`, `compose-down`, `proto`. CI calls `make <target>` and nothing else; contributors do the same. `justfile` is optional and mirrors targets.

**Rationale:** eliminates the "CI runs a different command than I run locally" failure mode.

## Risks / Trade-offs

- **[Risk] Beam pulls heavy native deps → slow `uv sync`.** → Mitigation: cache `~/.cache/uv` keyed on `uv.lock` hash in every workflow; expected cache hit reduces install to seconds.
- **[Risk] `mypy --strict` friction blocks the first PRs.** → Mitigation: scaffolding change lands with an empty `beam_agents/__init__.py` so `mypy` passes trivially; strictness surface grows as `src/` grows, giving contributors incremental exposure.
- **[Risk] `--strict-markers` breaks contributors who forget to register a new marker.** → Mitigation: documented in `README.md` bootstrap section; the error message from pytest is self-explanatory.
- **[Risk] Docker digest pinning goes stale (CVEs, EOL images).** → Mitigation: `nightly.yml` includes a "compose pull with tag, warn on digest drift" step that opens an issue rather than silently updating.
- **[Risk] Pre-commit "no src/ without OpenSpec change" hook has false positives on refactors.** → Mitigation: bypass via env var `BEAM_AGENTS_ALLOW_NO_CHANGE=1` (documented, discouraged); reviewers still enforce culturally.
- **[Risk] Coverage ratchet + mutation on greenfield repo is meaningless until `core/` exists.** → Mitigation: `quality.yml` runs but is not marked required until the first `core/` change lands; documented in the workflow's `if:` guard and README.
- **[Trade-off] macOS in CI matrix doubles runner minutes.** Accepted: several contributors will develop on macOS, and Beam's native deps behave differently there; catching mac-only breakage in `ci` beats catching it during a release.
- **[Trade-off] Non-standard compose ports add mental overhead.** Accepted: cost < collision-debugging cost.

## Migration Plan

Not applicable — greenfield. Rollback is `git revert` on the scaffolding commit; no persistent state or deployed system to unwind.

## Open Questions

- **Q1:** Do we want a `renovate.json` / dependabot config in this change, or as a follow-up? *Working assumption:* follow-up, keeps this change reviewable.
- **Q2:** `mutmut` vs. `cosmic-ray` — project.md says `mutmut`; keep as-is unless benchmarking shows it can't finish `quality.yml` within 15 min once `core/` exists.
- **Q3:** Should `nightly.yml` gate on a Dataflow *staging* project we don't yet have? *Working assumption:* land the workflow skeleton with `if: vars.GCP_PROJECT_ID` guard so it no-ops until secrets are wired; do not block scaffolding on infra procurement.
