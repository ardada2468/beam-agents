## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/scripts/test_semantics_partition.py`: unit tests for the pure partition/coverage function extracted from `scripts/check_semantics_partition.py` — a docker-backed semantics nodeid outside both path-scoped selections is reported by name (from "An escaped docker-semantics test fails the required check"); the current-shape inputs (offline ∪ docker partition holds, path-scoped selections disjoint and covering) return no problems (from "The current layout passes"); an empty `tests/semantics` or `tests/conformance` selection is reported as an emptied lane (from "An emptied path-scoped selection fails"); the pre-existing partition problems (overlap, uncovered, phantom, empty lane) still surface unchanged. Fails initially because the pure function does not exist yet. *(Written first; initial run failed with `ImportError: cannot import name 'partition_problems'` — the right reason. 13 tests, all pass post-refactor.)*
- [x] 1.2 Extend the same module with a collection-shape test asserting, against the real repo layout via `collect()`-equivalent inputs recorded from `--collect-only`, that today's docker-semantics population is fully covered by the two path-scoped selections — pinning the property the workflow split depends on before any workflow edit (from "The current layout passes"). *(`test_repo_docker_semantics_population_is_covered_by_the_path_scoped_selections`, marked `slow` with a raised timeout; passes: 15 docker = 1 tests/semantics + 14 tests/conformance, disjoint.)*

## 2. Partition-check coverage guarantee

- [x] 2.1 Refactor `scripts/check_semantics_partition.py`: extract `partition_problems(...)` as a pure function over the five collected sets; `collect()` gains an optional `paths` argument mirroring the make targets' invocations (`tests/semantics`, `tests/conformance`); `main()` collects and reports only
- [x] 2.2 Add the new assertions (path-scoped selections non-empty, disjoint, union equals the repo-wide docker selection) and update the module docstring to state both properties: marker partition across lanes, path coverage within the docker lane *(Escaped tests are named by nodeid; success output reports all selection sizes.)*
- [x] 2.3 Verify the required `ci` step needs no workflow edit (it already runs the script) and that the script still completes offline with no docker services *(`ci.yml` step "Semantics tier partition check" unchanged; `uv run python scripts/check_semantics_partition.py` passes offline: "36 offline + 15 docker = 51 total; docker lane covered by 1 tests/semantics + 14 tests/conformance".)*

## 3. Makefile targets

- [x] 3.1 Add `compose-up-core`: `$(COMPOSE) up -d --wait redpanda redis pubsub-emulator bigtable-emulator` — audit the `integration and not semantics` tests' fixtures to confirm this service list is exactly sufficient, and record the audit result in the target's comment *(Audit: `tests/actions/test_write_intents_integration.py` → Redpanda:19092 + Pub/Sub:8085; `tests/effector/test_dedup_redis.py` → Redis:16379; `tests/effector/test_service_integration.py` → Redpanda + Redis + Pub/Sub; `tests/effector/test_dedup_bigtable.py` → Bigtable:8086. No other service reached; recorded in the Makefile comment.)*
- [x] 3.2 Parameterize `compose-up` with `COMPOSE_UP_FLAGS ?= --wait --build` so its default behavior is byte-identical to today while CI can suppress the rebuild after `harness-build` *(`make -n compose-up` prints the identical `docker compose ... up -d --wait --build`; `make -n compose-up COMPOSE_UP_FLAGS=--wait` drops only `--build`.)*
- [x] 3.3 Add `harness-build`: the buildx invocation for `docker/sdk-harness.Dockerfile` tagging `beam-agents-sdk-harness:2.72.0`, with overridable cache arguments (empty locally, `type=gha` in CI) *(`HARNESS_CACHE_ARGS ?=` empty by default; `make -n harness-build` verified.)*
- [x] 3.4 Add `compose-logs`: write per-service `docker compose logs --no-color --timestamps` files, copy `docker/e2e-spool/*-tm-threads.txt` (segments excluded), and snapshot the Flink REST `/jobs/overview` and `/taskmanagers` endpoints best-effort into `LOGS_DIR`, never failing on a missing service or unreachable REST API *(Every step `|| true` / error-note guarded; `make -n compose-logs LOGS_DIR=diag` verified.)*

## 4. SDK-harness Dockerfile layer reorder

- [x] 4.1 Reorder `docker/sdk-harness.Dockerfile`: third-party `RUN pip install` (protobuf pin, httpx, pydantic, aiokafka, langgraph/langchain-core) before `COPY pyproject.toml README.md src`, then `RUN pip install --no-deps /src` with the existing import self-check, then `COPY tests`; keep every pin and the explanatory comments intact *(All pins byte-identical; self-check line unchanged; comments carried into the split layers.)*
- [ ] 4.2 Verify locally: `docker compose -f docker/compose.yaml build beam-sdk-harness` passes the self-check; a `src/`-only edit rebuilds without re-downloading third-party wheels; `make test-semantics` and `make test-conformance-flink` pass against the reordered image (blocked: needs docker)

## 5. Workflow restructure

- [x] 5.1 Split `.github/workflows/integration.yml` into two parallel jobs: `integration` (checkout, uv sync, `make compose-up-core`, `make test-integration`, `timeout-minutes: 20`) and `flink-minicluster` (checkout, uv sync, buildx setup + cached harness build, `make compose-up COMPOSE_UP_FLAGS=--wait`, `make test-semantics`, `make test-conformance-flink` as separate steps, `timeout-minutes: 45`); keep the workflow-level triggers, permissions, and concurrency group unchanged *(`yaml.safe_load` clean; jobs have no `needs`, so they parallelize; triggers/permissions/concurrency byte-identical.)*
- [x] 5.2 Wire the harness-image cache: `docker/setup-buildx-action` (docker-container driver) and `docker/build-push-action` with `load: true`, `cache-from: type=gha`, `cache-to: type=gha,mode=max`, building from the checkout before compose bring-up
- [x] 5.3 Add failure diagnostics to both jobs: `make compose-logs` step `if: failure()`, then `actions/upload-artifact` `if: failure()` (artifact name embedding job and run attempt, `retention-days: 14`), then the existing `make compose-down` `if: always()` — in that order *(Names: `integration-diagnostics-attempt-<n>` / `flink-minicluster-diagnostics-attempt-<n>`.)*
- [x] 5.4 Carry the existing step comments forward (e2e-gate budget note, the test-semantics-step-is-the-release-gate warning, the conformance-distinguishability note) so the review-sensitivity documented in the Makefile survives the restructure *(Budget note now heads the `flink-minicluster` job; release-gate warning sits on the `test-semantics` step; distinguishability note on the conformance step.)*

## 6. Verification on the PR

- [ ] 6.1 Confirm the workflow expands to exactly two jobs, running in parallel, with `integration` reporting under its unchanged context name (blocked: needs CI run)
- [ ] 6.2 Confirm the base job's runner has no `flink-*` or `beam-sdk-harness` container during `make test-integration` (assert via a `docker ps` step output on the PR run) (blocked: needs CI run)
- [ ] 6.3 Push a scratch commit with a deliberately failing docker-semantics assertion; confirm the `flink-minicluster` job uploads the diagnostics artifact containing all six service logs, then revert the scratch commit and confirm the green run uploads nothing (blocked: needs CI run)
- [ ] 6.4 Push a `src/`-only touch and confirm from the build-step log that the third-party layer was a cache hit (second run onward) (blocked: needs CI run)
- [ ] 6.5 Record both jobs' measured wall-clock in the PR description and adjust the 20/45 `timeout-minutes` split if the measurements demand it (blocked: needs CI run)

## 7. Branch protection and docs

- [ ] 7.1 Read `main`'s required status checks (`gh api repos/:owner/:repo/branches/main/protection --jq '.required_status_checks.contexts'`); confirm `integration` still resolves after the split, and add `flink-minicluster` to the required contexts in the merge window (additive — no lockout risk if delayed, but the Flink gates are not merge-blocking until it is done) (blocked: needs CI/merge window — operational step at PR time)
- [x] 7.2 Update `docs/ci.md`: split the `integration.yml` workflow-map row into the two jobs, name the `flink-minicluster` job in the effectively-once-gate section, and document the failure-artifact location and the `make compose-logs` local equivalent *(New "Debugging a red flink-minicluster run" subsection; required-checks section notes the add-at-merge asymmetry.)*
- [x] 7.3 Update `docker/README.md`: correct the "one test" claim for `flink-jobserver`/`beam-sdk-harness` to name both Flink-backed suites (e2e gate and conformance Flink leg), and note `compose-up-core` alongside `compose-up` *(Usage block now lists `compose-up-core` and `compose-logs` too.)*

## 8. Gates

- [x] 8.1 `make lint` and `make type` clean (the script refactor and new tests are typed; no `Any` in signatures) *(Both pass; mypy: "no issues found in 200 source files". One mechanical ripple: adding `tests/scripts/` made ruff-isort classify `scripts` first-party, requiring a one-line import re-sort in `tests/core/test_mutation_gate.py`.)*
- [x] 8.2 `make test-unit` passes offline; the new `tests/scripts/` tests run in the unit tier with no docker *(959 passed, 1 pre-existing env skip; the 13 new tests are in the selection.)*
- [x] 8.3 `make coverage-ratchet` at or above baseline (scripts are outside `--cov=beam_agents`; the ratchet must not regress) *("branch coverage 94.84% is at baseline".)*
- [x] 8.4 `uv run python scripts/check_semantics_partition.py` passes against the current layout with the new assertions active *("semantics tier partition OK: 36 offline + 15 docker = 51 total; docker lane covered by 1 tests/semantics + 14 tests/conformance".)*
- [ ] 8.5 `uv run pre-commit run --all-files` clean (blocked: pre-commit is not in this environment's synced dependency groups; lint/type/format checks it wraps were run directly and are clean)
- [x] 8.6 `openspec validate add-flink-minicluster-ci --strict` passes *(`npx @fission-ai/openspec@1.7.0 validate add-flink-minicluster-ci --strict` → "Change 'add-flink-minicluster-ci' is valid".)*
