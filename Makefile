.DEFAULT_GOAL := help
COMPOSE := docker compose -f docker/compose.yaml
# Lazily expanded (`=`, not `:=`): every target, including `help`, would
# otherwise pay a `uv run` subprocess just to evaluate this.
MUTATION_CHILDREN = $(shell uv run python -c 'import os; print(os.cpu_count() or 1)')

.PHONY: help bootstrap fmt lint type test-unit test-integration test-semantics test-semantics-offline test-dataflow test-smoke mutation coverage-ratchet compose-up compose-down proto

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
test-semantics: ## Run docker-backed semantics gates (requires compose-up)
	uv run pytest -m "semantics and integration"

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
compose-up: ## Start the local Redpanda/Redis/Flink stack
	$(COMPOSE) up -d --wait --build

compose-down: ## Tear down the local stack
	$(COMPOSE) down

proto: ## Regenerate protobuf Python bindings from protos/*.proto
	scripts/gen_proto.sh
