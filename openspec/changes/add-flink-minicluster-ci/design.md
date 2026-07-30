## Context

See `proposal.md` — Why. Constraints that shape the approach:

- The Flink-backed gates already exist and are release-blocking: the effectively-once e2e gate (C16) and the conformance Flink leg (C22) run today as the last two steps of the single `integration` job, via `make test-semantics` and `make test-conformance-flink`. Their selections, budgets, and zero-flake policy are spec'd in their own capabilities and must not move.
- `make compose-up`'s `--build` is load-bearing ([Makefile:76](../../../Makefile:76) comment: a stale SDK-harness image runs the gate against yesterday's runtime and passes). Any caching scheme must preserve "the image the gates run against was built from this checkout".
- The docker lane's make targets are path-scoped (`tests/semantics`, `tests/conformance`) while `scripts/check_semantics_partition.py` reasons only about markers repo-wide — the coverage gap this change closes.
- Branch protection on `main` names status-check contexts explicitly (`integration`, `quality`, per-leg `ci (…)` — recorded in `drop-macos-ci-matrix-leg` §1's finding, with `enforce_admins: true`). Job names in `integration.yml` are therefore protection-visible identifiers, not free to change casually.
- Two pending changes (`drop-macos-ci-matrix-leg`, `enforce-mutation-gate-on-core`) both carry deltas to `repo-scaffolding`'s "GitHub Actions workflows mirror the testing tiers" requirement and document that whichever archives second must merge the other's text. Adding a third concurrent delta to that requirement multiplies the hazard.
- The stack machinery already produces failure diagnostics on disk: `capture_tm_thread_dump` writes `*-tm-threads.txt` under `docker/e2e-spool/` ([_flink_stack.py:181](../../../tests/semantics/_flink_stack.py:181)), and `job_vertex_summary` renders per-vertex stall reports. CI currently discards all of it: `make compose-down` runs `if: always()` and removes the containers.
- No `integration and not semantics` test touches Flink (grep across `tests/actions/` and `tests/effector/` finds no Flink endpoint, and [compose.yaml:120](../../../docker/compose.yaml:120) documents that only the Beam-on-Flink gates submit jobs), so the base lane can run without the Flink services, the jobserver image, or the harness build.

## Goals / Non-Goals

**Goals:**

- A dedicated `flink-minicluster` job owning exactly the docker-backed `semantics and integration` selections, in parallel with a slimmed base `integration` job, each with its own timeout and re-run unit.
- Failure-time capture of Flink service logs, harness thread dumps, and Flink REST snapshots as a downloadable CI artifact, without ever skipping teardown.
- Layer-cache reuse for the SDK-harness image so an unchanged dependency set costs a cache restore, not a network install — while keeping the freshness invariant (image built from the current checkout) airtight.
- A partition check that proves, offline and on every PR, that the path-scoped docker make targets cover every docker-backed semantics test in the repo.
- Preserve every existing contract textually: step separation (e2e vs. conformance), marker selections, `make compose-up` semantics for local use, and the `integration` status-check context name.

**Non-Goals:**

- No change to the gates themselves, their selections, or their budgets-as-spec'd; no new test scenarios on Flink.
- No compose service or image changes beyond the Dockerfile layer reorder — no profiles, no new services, no digest bumps.
- No caching of pulled base images (see D5) and no self-hosted-runner or registry infrastructure.
- No edit to `repo-scaffolding`'s workflow requirement (see D2) — a follow-up change can consolidate the requirement's wording once the two pending deltas archive.
- Not a general CI-observability change: artifact upload is failure-path diagnostics for the docker lanes only, not a metrics/reporting system.

## Decisions

### D1. Two jobs; the base job keeps the `integration` name

The workflow splits into `integration` (core services + `make test-integration`, `timeout-minutes: 20`) and `flink-minicluster` (full stack + `make test-semantics` + `make test-conformance-flink` as separate steps, `timeout-minutes: 45` — the e2e gate is budgeted ≤ 15 min, the conformance leg measured ~70 s per adapter, and stack bring-up/build dominates the remainder). The base job keeps the name `integration` because that exact context string is a required check on `main` with `enforce_admins: true`; renaming it would leave every PR blocked on a phantom context — the same trap `drop-macos-ci-matrix-leg` documented for the `ci` matrix legs. `flink-minicluster` is a *new* context and must be added to protection at merge time (an additive operation: until it is added, PRs merge without waiting on it, which is safe-but-weaker, never a lockout).

*Why not three jobs (e2e gate and conformance leg each their own)?* Each additional job pays a full stack bring-up (image pulls + harness build + `--wait` on six services) and another 2× Redpanda/Redis startup; the two Flink suites share the same stack and already self-isolate via `freshen_flink` per run ([_flink_stack.py:62](../../../tests/semantics/_flink_stack.py:62)) and run-id topic naming. Step-level separation inside one job preserves failure attribution (the existing requirement) at a fraction of the cost. *Why not keep one job?* Wall-clock: the non-Flink integration tests and the Flink gates are disjoint in both services and code; serializing them puts ~10 minutes of Kafka/Redis tests in front of the release gates on every PR and couples their re-runs.

### D2. Service partition via explicit service lists in the Makefile; the Flink job keeps plain `make compose-up`

`compose-up-core` is `$(COMPOSE) up -d --wait redpanda redis pubsub-emulator bigtable-emulator` — an explicit service list, no `--build` (the harness image is not in the list), no edit to `docker/compose.yaml` (compose resolves `depends_on` from a service list, and none of these four depend on Flink). The `flink-minicluster` job runs the existing `make compose-up` for the full stack.

*Why not compose profiles?* Profiles would edit `docker/compose.yaml`'s contract, and — more importantly — the clean way to express "base lane" would end up rewording `repo-scaffolding`'s workflow requirement ("`integration.yml` MUST run `make compose-up test-integration test-semantics`"), which already has two pending full-body-replacement deltas in flight. With this design the requirement stays satisfied verbatim: `integration.yml` still invokes `make compose-up`, `make test-integration`, and `make test-semantics` on `ubuntu-latest`; the requirement does not say "in one job". The Flink job pulling the (unused) GCP emulator images is the accepted cost — both are the same digest-pinned `google/cloud-sdk` image, so it is one pull, and D4's caching does not interact with it.

### D3. The coverage guarantee lives in `check_semantics_partition.py`, refactored around a pure function

The script gains two more collections — `collect("semantics and integration", paths=("tests/semantics",))` and `collect(..., paths=("tests/conformance",))`, mirroring the make targets' exact invocations — and new assertions: each path-scoped selection is non-empty, the two are disjoint, and their union equals the repo-wide docker selection. A docker-backed semantics test added outside both directories now fails the required `ci` step with its nodeid named, instead of silently never running. The set algebra (current partition checks + new coverage checks) moves into a pure `partition_problems(offline, docker, everything, docker_semantics, docker_conformance) -> list[str]` so it is unit-testable without spawning pytest-in-pytest; `main()` shrinks to collection + reporting.

*Why extend this script rather than add a second one?* Both properties are facets of one claim — "every semantics gate runs in exactly one CI lane" — and the script's docstring already frames it that way; a second script would need the same `collect()` plumbing and a second required step. *Why not assert inside a meta-test like the conformance matrix does?* The conformance meta-test guards *its own* directory's cell count; this property is repo-global and must hold even when someone adds a test to a directory with no meta-test — a collection-level check in a required `ci` step is the only place that sees the whole repo.

### D4. Harness-image caching: Dockerfile layer reorder + buildx `type=gha`, with `--build` suppressed only where the image was just built

Two moves, both required:

1. **Layer order.** Today `COPY pyproject.toml README.md /src/` and `COPY src /src/src` ([sdk-harness.Dockerfile:30](../../../docker/sdk-harness.Dockerfile:30)) precede the single `RUN` that installs both third-party deps and `/src` ([sdk-harness.Dockerfile:36](../../../docker/sdk-harness.Dockerfile:36)) — so any `src/` edit invalidates the network-bound install of protobuf/httpx/pydantic/aiokafka/langgraph. The reorder splits it: `RUN pip install <third-party pins>` first (depends on nothing from the repo), then `COPY pyproject.toml README.md src`, then `RUN pip install --no-deps /src` plus the existing import self-check, then `COPY tests`. Same final image contents, same self-check, but the expensive layer's cache key is the Dockerfile line itself.
2. **Cache transport.** GitHub runners start with an empty daemon, so local layer cache never survives a run. The job uses `docker/setup-buildx-action` (docker-container driver — required for `type=gha`) and `docker/build-push-action` with `load: true`, `tags: beam-agents-sdk-harness:2.72.0`, `cache-from: type=gha`, `cache-to: type=gha,mode=max`, building `docker/sdk-harness.Dockerfile` from the checkout. Because a compose `--build` in the *daemon's* builder cannot see the buildx container driver's cache, the subsequent `make compose-up` must not rebuild: `compose-up` gains `COMPOSE_UP_FLAGS ?= --wait --build`, and the CI step invokes `make compose-up COMPOSE_UP_FLAGS=--wait`. The freshness invariant holds by construction — the image tag compose starts was produced two steps earlier from the same checkout — and locally nothing changes: the default flags keep `--build`, and `make harness-build` exposes the buildx invocation (cache args overridable) for parity.

*Why not `actions/cache` + `docker save/load` of the whole image?* The image is multi-GB (Beam SDK base + langgraph); save/load round-trips regularly cost more than the rebuild they avoid, and a whole-image cache is all-or-nothing — a one-line `src/` change gets zero reuse. Layer-granular `type=gha` caching reuses exactly the unchanged prefix.

### D5. Base images are pulled by digest, not cached

Redpanda, Redis, Flink, the Beam jobserver, and `google/cloud-sdk` are all digest-pinned in `docker/compose.yaml`. They stay ordinary registry pulls: GHA cache storage is 10 GB per repo with LRU eviction, the pinned images sum to several GB, and a `docker save`/`actions/cache`/`docker load` cycle for them is typically *slower* than Docker Hub/GHCR pulls on GitHub's network — while evicting the far more valuable harness layer cache. Digest pins already give the determinism that image caching would otherwise be buying. If pull flakiness ever becomes measurable, a registry mirror is the right fix, not the actions cache.

### D6. Diagnostics are captured by a make target, `if: failure()`, strictly before teardown

`make compose-logs LOGS_DIR=<dir>` writes: `docker compose logs --no-color --timestamps <service>` to one file per service; any `docker/e2e-spool/*-tm-threads.txt` thread dumps (the harness's existing stall diagnostics — spool *segment* files are explicitly excluded, they are large and content-free for debugging); and `curl` snapshots of the Flink REST `/jobs/overview` and `/taskmanagers` endpoints (best-effort — the REST API may be down in exactly the failures that matter, so a failed snapshot writes an error note rather than failing the target). The workflow ordering is load-bearing: capture step `if: failure()` → `actions/upload-artifact` (`if: failure()`, name embedding job + run attempt, `retention-days: 14`) → `make compose-down` (`if: always()`). Putting capture behind a make target keeps the repo-scaffolding "CI invokes make" discipline and gives local runs the same command after a red `make test-semantics`.

*Why not stream logs continuously (e.g. `docker compose logs -f` in a background step)?* Failure-time capture gets the same bytes with zero happy-path cost and no background-process lifecycle in the workflow; the only loss is logs from a container that was *removed* mid-run, which no current chaos does (kills and restarts keep the container around).

## Risks / Trade-offs

- **[Branch protection: the new job is not required until someone flips it]** → mirror-image of the `drop-macos` trap, in the safe direction: forgetting the operational step means the Flink gates can be red while a PR merges. Mitigated by making it an explicit, blocking task with a verification step (`gh api .../branches/main/protection`) and by keeping the base job's context name unchanged so nothing can lock the repo.
- **[Two jobs double the Redpanda/Redis bring-up and the checkout/uv-sync cost]** → accepted: billed-minute delta is small (both services start in seconds; `setup-uv` cache makes the sync cheap), and it buys parallel wall-clock plus independent re-runs. The expensive resources (Flink images, harness build) are paid once, in the job that uses them.
- **[Suppressing `--build` in CI weakens the stale-image guard if steps are reordered later]** → the guard is re-established structurally: the spec requires the image compose starts to be built from the current checkout in the same job, and the build step precedes `compose-up` in the same job with no path between checkout and compose that skips it. A workflow edit that removes the build step fails visibly — the tag doesn't exist and compose (without `--build`, image absent) errors rather than silently building stale.
- **[GHA cache eviction or a `type=gha` outage]** → degradation is a full image build (today's behavior), never a wrong image: `cache-from` misses are non-fatal by design.
- **[Artifact size from verbose Flink logs]** → per-service files, `--no-color`, spool segments excluded, 14-day retention; the e2e gate's 10k-event run produces tens of MB of TaskManager logs at most, well inside artifact limits.
- **[The path-scoped coverage check hard-codes the make targets' directory choices]** → deliberate: the check exists precisely to fail when the Makefile's path scoping and the repo's test layout drift apart. If a third docker-semantics directory is ever legitimate, the change adding it updates the script and the Makefile together — loudly.
- **[Timeout split guesses wrong]** → 20/45 are set from measured components (e2e ≤ 15 min budget, conformance ~70 s/adapter, integration suite minutes) but validated on the first PR run; adjusting `timeout-minutes` is a one-line follow-up and both numbers still sum under today's single 60.

## Migration Plan

1. Land the partition-check refactor + unit tests first (pure function, path-scoped assertions) — it must pass against the *current* layout before any workflow change, proving no docker-semantics test escapes today.
2. Reorder `sdk-harness.Dockerfile` layers; verify locally that `docker compose build beam-sdk-harness` still passes its build-time import self-check and that a `src/`-only edit rebuilds without re-downloading third-party deps.
3. Add the Makefile targets (`compose-up-core`, `harness-build`, `compose-logs`, `COMPOSE_UP_FLAGS`) with `compose-up`'s default behavior byte-identical to today.
4. Restructure `integration.yml` into the two jobs with caching and artifact steps; open the PR and let both jobs run — this run is the verification vehicle for parallelism, cache behavior (second push = warm cache), and a deliberately-broken scratch commit to demonstrate the failure artifact.
5. In the merge window: add `flink-minicluster` to `main`'s required status checks (additive; can be done immediately after merge without lockout risk), and verify the `integration` context still reports.
6. Docs (`docs/ci.md`, `docker/README.md`) ride the same PR. Rollback at any point is a revert of `integration.yml` — the make targets and script changes are backward-compatible with the monolithic workflow.

## Open Questions

- The exact service list for `compose-up-core` is fixed at implementation time by auditing the `integration and not semantics` tests' fixtures (currently: Redpanda, Redis, Pub/Sub emulator, Bigtable emulator); if a test turns out to reach another service, the list grows rather than the design changing.
- Whether `flink-minicluster` should also carry the `slow`-marker note in its job comment for future nightly-only additions — cosmetic, decided in review.
- Whether to bump `retention-days` for artifacts attached to failures on `main` (vs. PRs) — start uniform at 14 and revisit if a post-merge investigation ever wants older logs.
