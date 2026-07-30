## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/scripts/test_semantics_partition.py`: unit tests for the pure partition/coverage function extracted from `scripts/check_semantics_partition.py` — a docker-backed semantics nodeid outside both path-scoped selections is reported by name (from "An escaped docker-semantics test fails the required check"); the current-shape inputs (offline ∪ docker partition holds, path-scoped selections disjoint and covering) return no problems (from "The current layout passes"); an empty `tests/semantics` or `tests/conformance` selection is reported as an emptied lane (from "An emptied path-scoped selection fails"); the pre-existing partition problems (overlap, uncovered, phantom, empty lane) still surface unchanged. Fails initially because the pure function does not exist yet.
- [ ] 1.2 Extend the same module with a collection-shape test asserting, against the real repo layout via `collect()`-equivalent inputs recorded from `--collect-only`, that today's docker-semantics population is fully covered by the two path-scoped selections — pinning the property the workflow split depends on before any workflow edit (from "The current layout passes").

## 2. Partition-check coverage guarantee

- [ ] 2.1 Refactor `scripts/check_semantics_partition.py`: extract `partition_problems(...)` as a pure function over the five collected sets; `collect()` gains an optional `paths` argument mirroring the make targets' invocations (`tests/semantics`, `tests/conformance`); `main()` collects and reports only
- [ ] 2.2 Add the new assertions (path-scoped selections non-empty, disjoint, union equals the repo-wide docker selection) and update the module docstring to state both properties: marker partition across lanes, path coverage within the docker lane
- [ ] 2.3 Verify the required `ci` step needs no workflow edit (it already runs the script) and that the script still completes offline with no docker services

## 3. Makefile targets

- [ ] 3.1 Add `compose-up-core`: `$(COMPOSE) up -d --wait redpanda redis pubsub-emulator bigtable-emulator` — audit the `integration and not semantics` tests' fixtures to confirm this service list is exactly sufficient, and record the audit result in the target's comment
- [ ] 3.2 Parameterize `compose-up` with `COMPOSE_UP_FLAGS ?= --wait --build` so its default behavior is byte-identical to today while CI can suppress the rebuild after `harness-build`
- [ ] 3.3 Add `harness-build`: the buildx invocation for `docker/sdk-harness.Dockerfile` tagging `beam-agents-sdk-harness:2.72.0`, with overridable cache arguments (empty locally, `type=gha` in CI)
- [ ] 3.4 Add `compose-logs`: write per-service `docker compose logs --no-color --timestamps` files, copy `docker/e2e-spool/*-tm-threads.txt` (segments excluded), and snapshot the Flink REST `/jobs/overview` and `/taskmanagers` endpoints best-effort into `LOGS_DIR`, never failing on a missing service or unreachable REST API

## 4. SDK-harness Dockerfile layer reorder

- [ ] 4.1 Reorder `docker/sdk-harness.Dockerfile`: third-party `RUN pip install` (protobuf pin, httpx, pydantic, aiokafka, langgraph/langchain-core) before `COPY pyproject.toml README.md src`, then `RUN pip install --no-deps /src` with the existing import self-check, then `COPY tests`; keep every pin and the explanatory comments intact
- [ ] 4.2 Verify locally: `docker compose -f docker/compose.yaml build beam-sdk-harness` passes the self-check; a `src/`-only edit rebuilds without re-downloading third-party wheels; `make test-semantics` and `make test-conformance-flink` pass against the reordered image

## 5. Workflow restructure

- [ ] 5.1 Split `.github/workflows/integration.yml` into two parallel jobs: `integration` (checkout, uv sync, `make compose-up-core`, `make test-integration`, `timeout-minutes: 20`) and `flink-minicluster` (checkout, uv sync, buildx setup + cached harness build, `make compose-up COMPOSE_UP_FLAGS=--wait`, `make test-semantics`, `make test-conformance-flink` as separate steps, `timeout-minutes: 45`); keep the workflow-level triggers, permissions, and concurrency group unchanged
- [ ] 5.2 Wire the harness-image cache: `docker/setup-buildx-action` (docker-container driver) and `docker/build-push-action` with `load: true`, `cache-from: type=gha`, `cache-to: type=gha,mode=max`, building from the checkout before compose bring-up
- [ ] 5.3 Add failure diagnostics to both jobs: `make compose-logs` step `if: failure()`, then `actions/upload-artifact` `if: failure()` (artifact name embedding job and run attempt, `retention-days: 14`), then the existing `make compose-down` `if: always()` — in that order
- [ ] 5.4 Carry the existing step comments forward (e2e-gate budget note, the test-semantics-step-is-the-release-gate warning, the conformance-distinguishability note) so the review-sensitivity documented in the Makefile survives the restructure

## 6. Verification on the PR

- [ ] 6.1 Confirm the workflow expands to exactly two jobs, running in parallel, with `integration` reporting under its unchanged context name
- [ ] 6.2 Confirm the base job's runner has no `flink-*` or `beam-sdk-harness` container during `make test-integration` (assert via a `docker ps` step output on the PR run)
- [ ] 6.3 Push a scratch commit with a deliberately failing docker-semantics assertion; confirm the `flink-minicluster` job uploads the diagnostics artifact containing all six service logs, then revert the scratch commit and confirm the green run uploads nothing
- [ ] 6.4 Push a `src/`-only touch and confirm from the build-step log that the third-party layer was a cache hit (second run onward)
- [ ] 6.5 Record both jobs' measured wall-clock in the PR description and adjust the 20/45 `timeout-minutes` split if the measurements demand it

## 7. Branch protection and docs

- [ ] 7.1 Read `main`'s required status checks (`gh api repos/:owner/:repo/branches/main/protection --jq '.required_status_checks.contexts'`); confirm `integration` still resolves after the split, and add `flink-minicluster` to the required contexts in the merge window (additive — no lockout risk if delayed, but the Flink gates are not merge-blocking until it is done)
- [ ] 7.2 Update `docs/ci.md`: split the `integration.yml` workflow-map row into the two jobs, name the `flink-minicluster` job in the effectively-once-gate section, and document the failure-artifact location and the `make compose-logs` local equivalent
- [ ] 7.3 Update `docker/README.md`: correct the "one test" claim for `flink-jobserver`/`beam-sdk-harness` to name both Flink-backed suites (e2e gate and conformance Flink leg), and note `compose-up-core` alongside `compose-up`

## 8. Gates

- [ ] 8.1 `make lint` and `make type` clean (the script refactor and new tests are typed; no `Any` in signatures)
- [ ] 8.2 `make test-unit` passes offline; the new `tests/scripts/` tests run in the unit tier with no docker
- [ ] 8.3 `make coverage-ratchet` at or above baseline (scripts are outside `--cov=beam_agents`; the ratchet must not regress)
- [ ] 8.4 `uv run python scripts/check_semantics_partition.py` passes against the current layout with the new assertions active
- [ ] 8.5 `uv run pre-commit run --all-files` clean
- [ ] 8.6 `openspec validate add-flink-minicluster-ci --strict` passes
