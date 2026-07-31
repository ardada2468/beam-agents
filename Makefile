.DEFAULT_GOAL := help
COMPOSE := docker compose -f docker/compose.yaml
# Lazily expanded (`=`, not `:=`): every target, including `help`, would
# otherwise pay a `uv run` subprocess just to evaluate this.
MUTATION_CHILDREN = $(shell uv run python -c 'import os; print(os.cpu_count() or 1)')

.PHONY: help bootstrap fmt lint type test-unit test-integration test-semantics test-semantics-offline test-conformance-flink test-dataflow test-smoke mutation coverage-ratchet compose-up compose-up-core compose-down compose-logs harness-build proto docs docs-serve build changelog changelog-draft

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "%-18s %s\n", $$1, $$2}'

bootstrap: ## Install all dependency groups and git hooks
	uv sync --all-groups
	uv run pre-commit install

fmt: ## Auto-format and fix lint issues
	uv run ruff check --fix .
	uv run ruff format .

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .

type: ## Run mypy --strict
	uv run mypy

test-unit: ## Run the unit test tier (offline, no docker), with coverage
	uv run pytest -m "not integration and not semantics and not dataflow and not smoke" \
		--cov=beam_agents --cov-report=term-missing --cov-report=xml

# `not semantics`: the docker semantics gate carries both markers and runs in
# its own dedicated step (test-semantics, below) — the ONLY place it runs.
# Without the exclusion the ~10-minute gate executes twice per integration
# job. The trade-off is deliberate: deleting the test-semantics step from the
# workflow now silently removes the release gate, so treat any edit to that
# step as review-sensitive.
test-integration: ## Run integration-marked tests except semantics gates (requires compose-up)
	uv run pytest -m "integration and not semantics"

# Docker-backed semantics gates only: the offline gates run (required) in ci
# via test-semantics-offline, and re-running them here would let a new gate
# hide in the overlap. scripts/check_semantics_partition.py enforces that the
# two selections exactly partition the tier. No exit-5 tolerance: an empty
# selection is a deselected gate, not a pending one.
# Scoped to tests/semantics: the adapter conformance matrix's Flink leg
# carries the same markers but runs as its own integration.yml step
# (test-conformance-flink, below) so an e2e-gate timeout and a conformance
# failure stay distinguishable in CI.
test-semantics: ## Run docker-backed semantics gates (requires compose-up)
	uv run pytest -m "semantics and integration" tests/semantics

# Same no-exit-5 stance: an empty conformance selection means the matrix was
# deselected, not that it is pending.
test-conformance-flink: ## Run the adapter conformance matrix's Flink leg (requires compose-up)
	uv run pytest -m "semantics and integration" tests/conformance

# No exit-5 tolerance: this selection is required to be non-empty. An empty
# collection here means the gate was accidentally deselected, not that it's
# still pending — it must fail the build, not pass silently.
test-semantics-offline: ## Run offline (no-docker) semantics gates; required in ci
	uv run pytest -m "semantics and not integration"

test-dataflow: ## Run dataflow-marked tests (nightly only, requires real GCP)
	uv run pytest -m dataflow; test $$? -eq 0 -o $$? -eq 5

test-smoke: ## Run smoke-marked tests against live providers (nightly only, requires credentials)
	uv run pytest -m smoke; test $$? -eq 0 -o $$? -eq 5

mutation: ## Run and enforce the core/ mutation gate
	uv run mutmut run --max-children $(MUTATION_CHILDREN)
	uv run python scripts/mutation_gate.py

coverage-ratchet: ## Fail if coverage.xml regressed vs. coverage-baseline.toml
	uv run python scripts/coverage_ratchet.py

# `--build`: the SDK harness image bakes in the current `src/beam_agents`, so a
# stale image would run the gate against yesterday's runtime and pass.
# COMPOSE_UP_FLAGS is overridable for CI ONLY where the flink-minicluster job
# just built the image from the same checkout (harness-build/buildx) and a
# compose `--build` in the daemon's builder could not see that cache anyway:
# `make compose-up COMPOSE_UP_FLAGS=--wait`. Locally, keep the default.
COMPOSE_UP_FLAGS ?= --wait --build
compose-up: ## Start the local Redpanda/Redis/Flink stack
	$(COMPOSE) up -d $(COMPOSE_UP_FLAGS)

# The base integration lane's services only — no Flink JobManager/TaskManager,
# no jobserver, no SDK-harness build. Service-list audit (2026-07-30): the
# `integration and not semantics` tests reach exactly Redpanda
# (localhost:19092 — tests/actions/test_write_intents_integration.py,
# tests/effector/test_service_integration.py), Redis (localhost:16379 —
# tests/effector/test_dedup_redis.py, test_service_integration.py), the
# Pub/Sub emulator (localhost:8085 — test_write_intents_integration.py,
# test_service_integration.py), and the Bigtable emulator (localhost:8086 —
# tests/effector/test_dedup_bigtable.py). Nothing else in that selection
# touches Flink (docker/compose.yaml: only the Beam-on-Flink gates submit
# jobs). If a new test needs another service, grow this list — loudly.
compose-up-core: ## Start only the non-Flink services (base integration lane)
	$(COMPOSE) up -d --wait redpanda redis pubsub-emulator bigtable-emulator

# Local-parity equivalent of the flink-minicluster job's cached buildx build
# (the CI step uses docker/build-push-action with the same tag and file).
# HARNESS_CACHE_ARGS is empty locally; CI passes the type=gha cache arguments.
HARNESS_CACHE_ARGS ?=
harness-build: ## Build the SDK-harness image via buildx (cache args overridable)
	docker buildx build --load -t beam-agents-sdk-harness:2.72.0 \
		-f docker/sdk-harness.Dockerfile $(HARNESS_CACHE_ARGS) .

compose-down: ## Tear down the local stack
	$(COMPOSE) down

# Failure diagnostics for the docker lanes, run by CI `if: failure()` strictly
# BEFORE `compose-down` removes the containers — and usable locally after a
# red `make test-semantics`. Everything is best-effort (`|| true` / error
# notes): a missing service or an unreachable Flink REST API must never fail
# the capture, and teardown must still run. Spool *segment* files are
# deliberately excluded (large, content-free for debugging); the harness's
# `*-tm-threads.txt` thread dumps are the diagnostics worth keeping.
LOGS_DIR ?= compose-diagnostics
compose-logs: ## Collect compose service logs + Flink diagnostics into LOGS_DIR
	mkdir -p $(LOGS_DIR)
	for svc in redpanda redis pubsub-emulator bigtable-emulator \
			flink-jobmanager flink-taskmanager flink-jobserver beam-sdk-harness; do \
		$(COMPOSE) logs --no-color --timestamps $$svc > $(LOGS_DIR)/$$svc.log 2>&1 || true; \
	done
	cp docker/e2e-spool/*-tm-threads.txt $(LOGS_DIR)/ 2>/dev/null || true
	curl -fsS --max-time 10 http://localhost:18081/jobs/overview \
		> $(LOGS_DIR)/flink-jobs-overview.json 2>/dev/null \
		|| echo "flink REST /jobs/overview unreachable at capture time" \
			> $(LOGS_DIR)/flink-jobs-overview.json
	curl -fsS --max-time 10 http://localhost:18081/taskmanagers \
		> $(LOGS_DIR)/flink-taskmanagers.json 2>/dev/null \
		|| echo "flink REST /taskmanagers unreachable at capture time" \
			> $(LOGS_DIR)/flink-taskmanagers.json

proto: ## Regenerate protobuf Python bindings from protos/*.proto
	scripts/gen_proto.sh

# Strict: a broken internal link or an unresolvable example snippet fails the
# build instead of publishing a dead reference. Run from the repo root — the
# snippet base_path resolves examples/*.py inclusions against the cwd.
docs: ## Build the docs site strictly (broken links/snippets fail)
	uv run mkdocs build --strict

docs-serve: ## Serve the docs site locally with live reload
	uv run mkdocs serve

# `rm -rf dist` first: a stale artifact from an earlier version would otherwise
# be picked up by `scripts/check_wheel.py dist/` and — worse — uploaded by the
# publish job. Releases are built ONLY by .github/workflows/release.yml from a
# clean tag checkout; a local `make build` is for inspection and never uploads
# (there is no `uv publish` path anywhere in this repo).
build: ## Build the sdist and wheel into a clean dist/
	rm -rf dist
	uv build

# Assembly is gated on the closed fragment-type registry BEFORE towncrier runs:
# towncrier ignores a fragment whose type it does not know, so a typo'd
# `.feature.md` would be silently dropped from the release notes. The gate lives
# in check_release.py (not in towncrier config) so the unit lane can exercise it
# without the `release` group installed.
# VERSION is required and explicit — never inferred from the package — so the
# assembled section can never disagree with the tag that publishes it.
changelog: ## Assemble changelog.d/ fragments into CHANGELOG.md (VERSION=X.Y.Z)
	@test -n "$(VERSION)" || { echo "usage: make changelog VERSION=X.Y.Z" >&2; exit 1; }
	uv run --group release python scripts/check_release.py --fragments-only
	uv run --group release towncrier build --version "$(VERSION)" --yes
	uv run --group release python scripts/check_release.py --consume-internal

# Side-effect free by construction: `--draft` prints the rendered section to
# stdout and touches neither CHANGELOG.md nor changelog.d/.
changelog-draft: ## Print the pending changelog section without writing anything
	uv run --group release python scripts/check_release.py --fragments-only
	uv run --group release towncrier build --version "$(or $(VERSION),UNRELEASED)" --draft
