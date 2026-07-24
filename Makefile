.DEFAULT_GOAL := help
COMPOSE := docker compose -f docker/compose.yaml

.PHONY: help bootstrap fmt lint type test-unit test-integration test-semantics test-semantics-offline test-dataflow mutation coverage-ratchet compose-up compose-down proto

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

test-unit: ## Run the unit test tier (offline, no docker)
	uv run pytest -m "not integration and not semantics and not dataflow"

# Exit code 5 means "no tests collected", which is expected until core/ and
# its adapters exist; treat it as success rather than failing empty CI runs.
test-integration: ## Run integration-marked tests (requires compose-up)
	uv run pytest -m integration; test $$? -eq 0 -o $$? -eq 5

test-semantics: ## Run semantics/correctness-marked tests (requires compose-up)
	uv run pytest -m semantics; test $$? -eq 0 -o $$? -eq 5

# No exit-5 tolerance: this selection is required to be non-empty. An empty
# collection here means the gate was accidentally deselected, not that it's
# still pending — it must fail the build, not pass silently.
test-semantics-offline: ## Run offline (no-docker) semantics gates; required in ci
	uv run pytest -m "semantics and not integration"

test-dataflow: ## Run dataflow-marked tests (nightly only, requires real GCP)
	uv run pytest -m dataflow; test $$? -eq 0 -o $$? -eq 5

mutation: ## Run mutmut against core/
	uv run mutmut run

coverage-ratchet: ## Fail if coverage.xml regressed vs. origin/main
	uv run python scripts/coverage_ratchet.py

compose-up: ## Start the local Redpanda/Redis/Flink stack
	$(COMPOSE) up -d --wait

compose-down: ## Tear down the local stack
	$(COMPOSE) down

proto: ## Regenerate protobuf Python bindings from protos/*.proto
	scripts/gen_proto.sh
