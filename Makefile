.DEFAULT_GOAL := help
COMPOSE := docker compose -f docker/compose.yaml
# The spark overlay is NEVER part of $(COMPOSE): `make compose-up` runs on
# every per-PR integration job, and the weekly-cadence decision for the Spark
# leg (promote-spark-runner, design D2/D3) would be undone at the
# infrastructure layer if a PR paid for Spark containers.
COMPOSE_SPARK := docker compose -f docker/compose.yaml -f docker/compose.spark.yaml
# The console stack is standalone, never an overlay: it shares no service with
# $(COMPOSE) and no test tier requires it. `compose.console.yaml` sets its own
# `name:` so the two projects cannot tear each other down.
COMPOSE_CONSOLE := docker compose -f docker/compose.console.yaml
# Lazily expanded (`=`, not `:=`): every target, including `help`, would
# otherwise pay a `uv run` subprocess just to evaluate this.
MUTATION_CHILDREN = $(shell uv run python -c 'import os; print(os.cpu_count() or 1)')

.PHONY: help bootstrap fmt lint type test-unit test-integration test-semantics test-semantics-offline test-conformance-flink test-conformance-spark test-dataflow test-smoke mutation coverage-ratchet bench bench-gate compose-up compose-up-core compose-up-spark compose-down compose-down-spark compose-logs compose-logs-spark harness-build console-build console-up console-down console-logs console-frontend proto docs docs-serve build changelog changelog-draft

BENCH_RESULTS := bench-results
# Local-iteration knob only. CI pins the modules' own sampling constants by
# passing nothing here (the e2e gate's discipline: env knobs tune local runs,
# never the gated one).
BENCH_ARGS ?=

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
#
# `not spark`: the Spark conformance leg carries `integration + spark` (never
# `semantics` while Spark is best-effort — promote-spark-runner design D4), so
# without this exclusion a per-PR integration job would run a best-effort
# runner's leg against a stack it never started. The spark leg runs ONLY in the
# weekly spark-weekly workflow, via test-conformance-spark below.
test-integration: ## Run integration-marked tests except semantics gates and the spark leg (requires compose-up)
	uv run pytest -m "integration and not semantics and not spark"

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

# The weekly workflow's ONLY test selection, and the only selection anywhere
# that reaches a spark cell. Same no-exit-5 stance as the Flink target: an
# empty selection is a deselected leg, not a pending one. Requires
# compose-up-spark (base stack + spark overlay).
test-conformance-spark: ## Run the adapter conformance matrix's Spark leg (weekly; requires compose-up-spark)
	uv run pytest -m "integration and spark" tests/conformance

# No exit-5 tolerance: this selection is required to be non-empty. An empty
# collection here means the gate was accidentally deselected, not that it's
# still pending — it must fail the build, not pass silently.
test-semantics-offline: ## Run offline (no-docker) semantics gates; required in ci
	uv run pytest -m "semantics and not integration"

# No exit-5 tolerance, same stance as test-semantics/test-conformance-flink
# above: the `--update` compatibility gate (tests/dataflow/test_update_compat.py)
# is release-blocking, so an empty `dataflow` collection means it was
# deselected — renamed module, dropped marker — not that the tier is still
# pending. Without GCP configuration the gate *skips* (a collected, reported
# skip; exit 0), which is visible in the report; a deselection is not.
test-dataflow: ## Run dataflow-marked tests (nightly only, requires real GCP)
	uv run pytest -m dataflow

test-smoke: ## Run smoke-marked tests against live providers (nightly only, requires credentials)
	uv run pytest -m smoke; test $$? -eq 0 -o $$? -eq 5

mutation: ## Run and enforce the core/ mutation gate
	uv run mutmut run --max-children $(MUTATION_CHILDREN)
	uv run python scripts/mutation_gate.py

coverage-ratchet: ## Fail if coverage.xml regressed vs. coverage-baseline.toml
	uv run python scripts/coverage_ratchet.py

# Offline (no docker, no network, FakeLLM only). One JSON per benchmark under
# bench-results/, which bench-gate is the single reader of. `--fast` (via
# BENCH_ARGS) is for local iteration; CI runs the pinned defaults.
bench: ## Run the offline pyperf benchmark suite into bench-results/
	mkdir -p $(BENCH_RESULTS)
	uv run python -m benchmarks.bench_noop_throughput -o $(BENCH_RESULTS)/bench_noop_throughput.json $(BENCH_ARGS)
	uv run python -m benchmarks.bench_overhead_tiers -o $(BENCH_RESULTS)/bench_overhead_tiers.json $(BENCH_ARGS)
	uv run python -m benchmarks.bench_suspension_roundtrip -o $(BENCH_RESULTS)/bench_suspension_roundtrip.json $(BENCH_ARGS)
	uv run python -m benchmarks.bench_state_commit -o $(BENCH_RESULTS)/bench_state_commit.json $(BENCH_ARGS)
	uv run python -m benchmarks.bench_runinference_compare -o $(BENCH_RESULTS)/bench_runinference_compare.json $(BENCH_ARGS)

bench-gate: ## Enforce the latency budget + benchmark-baseline.toml, render bench-report.md
	uv run python scripts/bench_gate.py

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

# Base stack + the Spark job server and its spark-scoped worker pool. Used by
# the weekly spark-weekly workflow and for local iteration on the spark leg;
# nothing per-PR calls it.
compose-up-spark: ## Start the local stack plus the Spark job-server overlay
	$(COMPOSE_SPARK) up -d $(COMPOSE_UP_FLAGS)

compose-down: ## Tear down the local stack
	$(COMPOSE) down

# Tears down the overlay's services too: `compose-down` alone leaves the
# Spark containers running, because they are not in the base file's project view.
compose-down-spark: ## Tear down the local stack including the Spark overlay
	$(COMPOSE_SPARK) down

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

# The spark overlay's half of compose-logs, for the weekly workflow's
# capture-before-teardown step. Separate target (not extra services in
# compose-logs) because $(COMPOSE) does not know the overlay's services: asking
# it for them writes a compose error into the log file instead of logs.
compose-logs-spark: ## Collect the Spark overlay's service logs into LOGS_DIR
	mkdir -p $(LOGS_DIR)
	for svc in spark-jobserver beam-sdk-harness-spark; do \
		$(COMPOSE_SPARK) logs --no-color --timestamps $$svc > $(LOGS_DIR)/$$svc.log 2>&1 || true; \
	done

# The console stack (docs/console.md). Separate from the compose-* targets on
# purpose: nothing in the test suite needs it, and no CI lane starts it.
console-build: ## Build the console image (UI bundle included)
	$(COMPOSE_CONSOLE) build

# `--build` by default, matching compose-up's reasoning: the image bakes in the
# current `src/beam_agents` AND the current `frontend/`, so a stale image shows
# yesterday's console. Both are cheap to rebuild — the Dockerfile's layer order
# keeps a source-only change off the dependency-install layers.
#
# No `--wait`, deliberately, and it is not needed. `console-demo` disables the
# healthcheck it would otherwise inherit from the shared image (it is a
# producer, and the inherited probe would fail forever) — but compose refuses to
# wait on a service with no healthcheck and fails the whole command with
# "container ... has no healthcheck configured", so `--wait` here exits 1 on a
# stack that came up perfectly. The guarantee `--wait` was carrying is already
# in `compose.console.yaml`: `console-demo` declares
# `depends_on: console: condition: service_healthy`, so `up -d` blocks until the
# console reports healthy before it starts the demo, and returns 0.
CONSOLE_UP_FLAGS ?= --build
console-up: ## Start the console at http://localhost:8787 with the demo pipeline
	$(COMPOSE_CONSOLE) up -d $(CONSOLE_UP_FLAGS)

# No `-v`: the database volume is the point (design D6). `docker compose
# -f docker/compose.console.yaml down -v` is the deliberate way to discard it.
console-down: ## Tear down the console stack, keeping the database volume
	$(COMPOSE_CONSOLE) down

console-logs: ## Follow the console and demo-pipeline logs
	$(COMPOSE_CONSOLE) logs -f console console-demo

# The UI bundle without docker, for frontend iteration and for building a wheel
# that ships the UI: hatchling force-includes `console/static/**` when it
# exists, so running this before `make build` is what makes a local wheel
# self-contained (design D9). Needs Node; nothing else in this Makefile does.
console-frontend: ## Build the console UI bundle into the package (needs Node)
	cd frontend && npm ci && npm run build

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
