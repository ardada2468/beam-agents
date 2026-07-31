# Tasks: verify-live-infrastructure

**Read `design.md` first.** Two rules govern every phase:

1. **Never weaken a test, threshold, or gate to get a green run** (D4). If something fails, triage
   it, record it, and file a defect as its own OpenSpec change. Seeding an empty baseline is the one
   permitted edit.
2. **Record one of four verdicts per phase** (D2): `pass`, `fail (defect)`, `fail (infra)`,
   `blocked`. An infra failure is remediated and re-run; it is never recorded as pass or defect.

Work top to bottom. Phases 1–5 need only Docker. Phase 6 needs GCP. Phase 7 is optional.

## 1. Phase 0 — preflight

- [x] 1.1 Record the host in `verification-report.md`: OS, CPU count, total RAM, `docker --version`,
  `docker compose version`, `uv --version`, `python --version`, and `git rev-parse HEAD`.
- [x] 1.2 Confirm Docker has **at least 8 GB** of memory allocated (the full stack runs Flink
  jobmanager + taskmanager + jobserver + SDK harness + 5 service containers). On Docker Desktop this
  is Settings → Resources. Under-allocation is the single most common cause of clustered `infra`
  failures.
- [x] 1.3 `uv sync --locked --all-groups` — this is the only sync that installs `precommit`
  (pre-commit, grpcio-tools), `integration` (testcontainers, aiokafka, pubsub, redis, bigtable),
  `bench` (pyperf) and `docs`. The offline environment could not run several gates purely because
  these were absent.
- [x] 1.4 Re-confirm the offline baseline still passes on your machine before adding any variables:
  `make lint`, `make type`, `make test-unit`, `make test-semantics-offline`. Expect ~1936 unit tests
  passing and 79 offline semantics gates. Record the counts. A discrepancy here is about *your*
  environment, not the code — resolve it before continuing.
- [x] 1.5 `uv run pre-commit run --all-files` — **never executed in the implementing environment**
  (10 hooks: ruff, ruff-format, protobuf drift, OpenSpec change guard, changelog fragment guard, and
  the file hygiene hooks). Record the result. The proto-drift hook is the interesting one: it
  re-runs `scripts/gen_proto.sh` and fails if the committed bindings differ.

## 2. Phase 1 — base services and the integration lane

- [x] 2.1 `make compose-up-core` — starts only Redpanda, Redis, and the Pub/Sub and Bigtable
  emulators (no Flink). Confirm all four report healthy: `docker compose -f docker/compose.yaml ps`.
- [x] 2.2 `make test-integration` — the `integration and not semantics` selection, **119
  `integration`-marked tests of which none have ever run**. Covers: the Kafka and ordered-Pub/Sub
  outbox writers, the effector's Redis and Bigtable dedup stores, the memory tier's Redis and
  Bigtable backends, the effector transport-security path, and the Slack example's Kafka leg.
  Record pass/fail counts and duration.
- [x] 2.3 Triage any failure per D2. Common `infra` signatures here: connection refused (service not
  actually ready despite `--wait`), emulator port already bound by another process, testcontainers
  unable to reach the Docker socket. Common `defect` signatures: an assertion about message
  ordering, dedup outcome, or stored bytes.
- [x] 2.4 The Firestore memory-store leg needs the `firestore-emulator` service (present in
  `docker/compose.yaml`) **and** the `google-cloud-firestore` client. Confirm it is no longer
  skipping after 1.3's `--all-groups` sync; if it still skips, record why.
- [x] 2.5 Record the phase verdict and evidence in the report.

## 3. Phase 2 — Flink: conformance matrix and the effectively-once gate

- [x] 3.1 `make compose-up` — the full stack including Flink and the SDK harness. First run builds
  the harness image; expect several minutes. `make harness-build` builds it explicitly if you want
  the build isolated from the compose step.
- [x] 3.2 `make test-conformance-flink` — **28 cells (4 adapters × 7 scenarios) that have never
  run.** This is the change that matters most for the adapter work: it is the only evidence that
  reference, LangGraph, ADK and Pydantic AI behave identically on a distributed runner. Record which
  cells pass, and the identity of any that fail.
- [x] 3.3 Note the one declared adapter skip: ADK's `bundle_retry_cache` cell is a documented
  `Skip` (ADK always drives a summarization turn after a function response, so a discarded attempt
  legitimately repeats it). A *reported skip* there is correct; a *failure* is not.
- [x] 3.4 `make test-semantics` — **`tests/semantics/test_effectively_once_e2e.py`, the release gate
  for correctness invariant 4, never executed.** 10,000 events through Redpanda, `RunAgent` on the
  Flink mini-cluster, real `beam-agents-effector` processes with Redis dedup, SIGKILLed effector
  workers, a killed TaskManager, and a cancel-and-resubmit replay from the ingest spool. Budget
  ≤ 15 minutes on CI hardware.
- [x] 3.5 If 3.4 is slow or flaky on your machine, you may first smoke it with
  `BEAM_AGENTS_E2E_EVENTS=500 uv run pytest tests/semantics/test_effectively_once_e2e.py`. A
  reduced-volume run does **not** discharge the gate — record it as a smoke pass and then run the
  default volume for the recorded verdict.
- [x] 3.6 On failure, read the seed line the harness logs (`run seed=<n> … rerun with
  BEAM_AGENTS_E2E_SEED=<n>`) and reproduce deterministically before triaging. The kill schedule is
  derived from that seed.
- [x] 3.7 On any red docker phase, capture diagnostics **before** teardown: `make compose-logs
  LOGS_DIR=compose-diagnostics` (per-service logs, TaskManager thread dumps, Flink REST snapshots).
  This is the local equivalent of what CI uploads as an artifact.
- [x] 3.8 Record verdicts and evidence for 3.2 and 3.4 separately — they are distinct gates.

## 4. Phase 3 — Spark leg (first execution ever)

- [x] 4.1 `make compose-up-spark` — brings up the Spark job server overlay
  (`docker/compose.spark.yaml`) alongside the base stack.
- [x] 4.2 `make test-conformance-spark` — **28 cells, never run against a job server.** Per D3 this
  is a *spike*: its purpose is to establish what currently works. Four scenarios declare `Run()`
  provisionally and three declare `Skip()` with stated constraints.
- [x] 4.3 Triage each failure into: (a) infra — job server unreachable, artifact staging failure;
  (b) a genuine Spark portable-runner capability gap (state/timer semantics the runner does not
  provide); or (c) a real defect in the leg harness or the runtime. Record which.
- [x] 4.4 For any (b): **do not edit the declaration inline.** File a change converting that
  scenario's `Run()` to `Skip(reason=...)` with the evidence — that is `promote-spark-runner`'s own
  documented mechanism.
- [x] 4.5 Record the verdict. Note in the report that a successful local run is evidence the leg
  works but does **not** count toward the four consecutive green *scheduled weekly* runs that
  `promote-spark-runner` requires for promotion (Open Question in design.md).
- [x] 4.6 `make compose-down-spark` when finished.

## 5. Phase 4 — quality gates that need the full lane

- [x] 5.1 `make mutation` — **never run on this tree**; five changes deferred it. Generates ~900
  mutants across `src/beam_agents/core/` and enforces `mutation-baseline.toml`'s per-module
  ceilings. Expect this to take a long time; it forks per mutant.
- [x] 5.2 If the gate fails, list the surviving mutants in the report. Raising a ceiling or adding an
  exclusion requires a justification per `mutation-exclusions.toml`'s existing rules and is a **filed
  change**, not an inline edit. Note the known follow-up already recorded by `add-token-budgets`:
  `mutation-exclusions.toml` indexes mutants positionally and that change inserted statements into
  `ActivationContext.call_model`, both `__init__`s and `_stage_llm_trace`, so four entries may need
  renumbering.
- [x] 5.3 Re-measure coverage over the **full** lane (offline + integration) rather than the offline
  lane alone, and raise `coverage-baseline.toml` to the measured value with a comment naming this
  run. Expect it to move **up**: `memory/stores/{redis,bigtable,firestore}.py` currently sit at 0.00
  branch coverage purely because every branch they own is inside a live-client call path, which is
  the documented reason the baseline was lowered to 0.9015 during integration.
- [x] 5.4 Record both verdicts.

## 6. Phase 5 — benchmarks and baseline seeding

- [x] 6.1 Decide whether this host qualifies to seed medians (D3, and the requirement that the host
  be recorded). A laptop running a browser and an IDE does not. If it does not qualify, run the
  suite anyway for the absolute budget verdict, leave `[medians_ms]` empty, and record why.
- [x] 6.2 `make bench` — runs all five dimensions into `bench-results/`: no-op throughput, FakeLLM
  overhead tiers (50/500/2000 ms), suspension round-trip, state-commit cost vs `MemoryBlob` size,
  and the `RunInference` comparison.
- [x] 6.3 `make bench-gate` — enforces the absolute budget (**p50 < 15 ms, p99 < 60 ms** over pooled
  per-activation samples) and renders `bench-report.md`. On an unseeded `[medians_ms]` it will
  report each unseeded entry with the `seed medians_ms.<name> = <value>` line to add. **The budget
  verdict is a real pass/fail on the first run; the ratchet is not, because there is nothing to
  ratchet against yet.**
- [x] 6.4 If the host qualifies: seed `benchmark-baseline.toml`'s `[medians_ms]` from the measured
  values, re-run `make bench-gate` to confirm it is now at baseline, and record the seeding host in
  the report.
- [x] 6.5 Record the budget verdict and the seeding decision.
- [x] 6.6 `make compose-down` — tear the local stack down.

## 7. Phase 6 — Dataflow (when GCP is provisioned)

- [x] 7.1 Configure and record: `GCP_PROJECT_ID`, `GCP_REGION`, `GCP_DATAFLOW_TEMP_BUCKET`,
  `GCP_ARTIFACT_REGISTRY_REPO`, and Workload Identity Federation (or ADC for a local run). The
  nightly workflow gates on these being present.
- [ ] 7.2 `make test-dataflow` — **2 `dataflow`-marked tests, never run.** Note this target has no
  exit-5 tolerance by design: an empty selection means the gate was deselected, not that it is
  pending.
- [ ] 7.3 `tests/dataflow/test_update_compat.py` — launches a streaming pipeline at the previous
  released version with live keyed state (a suspended activation plus memory), `--update`s it to
  head, and asserts the continuation nonce, the memory marker, and a fresh key all survive.
  **Important caveat to record:** no version has been tagged yet (all three release gates are red),
  so this exercises its documented head→head bootstrap leg. That proves the mechanism; it does not
  prove cross-version compatibility. Say so explicitly in the report.
- [ ] 7.4 `tests/dataflow/test_flex_template_launch.py` — builds and pushes the fraud-example flex
  template image to Artifact Registry, launches it, asserts `JOB_STATE_RUNNING`, and cancels.
  Verify the launcher resolves its provider API key from Secret Manager and that **no secret appears
  in the template parameters, the job options, or the logs** — that is a specified requirement of
  `add-dataflow-flex-template`, not an incidental check.
- [ ] 7.5 Confirm teardown: the ledger and sweeper in `tests/dataflow/_update/resources.py` must
  leave no job running, including if the phase failed. Record the sweeper's output as evidence.
- [ ] 7.6 Record verdicts, the GCP project used, and the cost/duration observed.

## 8. Phase 7 — optional tiers

- [x] 8.1 `make test-smoke` with `ANTHROPIC_API_KEY` and/or `OPENAI_API_KEY` set — 3 `smoke` tests
  against live providers. Optional; records real-provider behavior. **(blocked without keys)**
- [x] 8.2 `tests/smoke/test_vllm_sidecar.py` on a GPU host — the only path that exercises
  `VllmSidecarProvider`'s real engine rather than its fake-engine seam. **(blocked without a GPU)**
- [x] 8.3 Record both as `blocked` with the missing prerequisite if unavailable — do not omit them.

## 9. Report, discharge, and findings

- [x] 9.1 Write `verification-report.md`: commit verified, host details, compose image identifiers,
  and one row per phase with command, verdict, evidence, and any defect filed. This is the artifact
  the three release gates cite.
- [x] 9.2 Discharge the blocked task items across the 26 implemented change folders — **105 unchecked
  items**, in five blocker classes (10 release infra, 9 CI run, 5 mutation, 5 docker/cloud, 5 CI
  hardware, plus the remainder). Check off each item the run actually discharges, citing the phase
  that discharged it. Leave genuinely still-blocked items unchecked with their blocker intact.
- [x] 9.3 Commit the task-item discharge separately from the report and baselines, so a reviewer can
  read the wide `tasks.md` diff independently of the substantive artifacts.
- [x] 9.4 File one OpenSpec change per defect found. Do **not** fix defects in this change.
- [x] 9.5 If any release-gate condition is now satisfiable, note it in the report — but do **not**
  edit `add-0-3-0-release`, `add-0-5-0-release` or `add-1-0-0-release`'s gate verdicts here. Those
  gates additionally require their dependencies to be *archived*, which is a separate step.

## 10. Gates

- [x] 10.1 `make lint` and `make type` clean at the end of the run (nothing this change does should
  move them).
- [x] 10.2 `make test-unit` and `make test-semantics-offline` still green — confirming the run did
  not perturb the offline tier.
- [x] 10.3 `make coverage-ratchet` at or above the re-measured baseline.
- [x] 10.4 `uv run pre-commit run --all-files` clean.
- [x] 10.5 `npx --yes @fission-ai/openspec@1.7.0 validate verify-live-infrastructure --strict` passes.
- [x] 10.6 `verification-report.md` committed, with every phase carrying a verdict.
