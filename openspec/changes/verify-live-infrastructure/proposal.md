## Why

The M2–M4 batch (C23–C48, 26 changes) was implemented in an environment with **no Docker, no
GPU, and no cloud credentials**. Every offline gate is green — 1936 unit tests, 79 offline
semantics gates, `mypy --strict` on 374 files, `mkdocs build --strict`, `uv sync --locked` — but
that is only the tier the environment could reach. The tiers it could not reach are substantial
and, in several cases, are the *only* evidence for the guarantees the project advertises:

- **119 `integration`-marked tests** never executed, including every live-broker, live-store, and
  emulator path.
- **The conformance matrix's Flink and Spark legs** — 28 cells each (4 adapters × 7 scenarios) —
  never ran. Only the 28 DirectRunner cells are verified, so "adapters cannot silently diverge"
  is currently proven on one runner out of three.
- **`tests/semantics/test_effectively_once_e2e.py`** — the release gate for correctness invariant
  4 — never ran. It is the most expensive check in the repository (10,000 events, real Kafka,
  Flink mini-cluster, SIGKILLed effectors, a killed TaskManager, cancel-and-resubmit replay), and
  nothing else substitutes for it.
- **The Spark leg has never executed against a job server at all.** `promote-spark-runner`'s four
  `Run()` declarations are explicitly *provisional* (its design Findings F1–F8); the first real run
  is the spike.
- **`make mutation` has never run** on this tree, and 5 changes deferred it.
- **The benchmark baseline is unseeded by design** — `benchmark-baseline.toml`'s `[medians_ms]` is
  empty because `docs/benchmarks.md` forbids seeding from developer hardware, so
  `make bench-gate` currently cannot pass by construction.
- **2 `dataflow`-marked tests** (the `--update` state-compatibility gate and the flex-template
  launch) and **3 `smoke` tests** need real GCP and provider credentials.

That leaves **105 unchecked task items** across the 26 change folders, every one annotated with an
explicit blocker rather than quietly skipped. This change is the instrument that discharges them:
a single, ordered, agent-executable verification run against real infrastructure, with a triage
rubric that distinguishes an environment failure from a defect, and with the specific artifacts
(seeded baselines, recorded verdicts) the release gates are waiting on.

It is deliberately *not* a code change. If it finds defects, each becomes its own OpenSpec change —
this one records findings and seeds baselines, nothing more.

## What Changes

- **A new `live-infrastructure-verification` capability**: the contract for what a live run must
  cover, what counts as a pass, how an infrastructure failure is distinguished from a defect, and
  what must be recorded.
- **`docs/verification.md`**: the operator-facing runbook — prerequisites, the phase order, the
  triage rubric, and the "do not weaken a test to make it pass" rule stated as policy.
- **`verification-report.md`** (generated, committed at the end of a run): one row per phase with
  the command, verdict, evidence, and any defect filed. This is the artifact the three release
  gates cite.
- **Baseline seeding**: `benchmark-baseline.toml`'s `[medians_ms]` seeded from a real quiet-hardware
  run, and `coverage-baseline.toml` re-measured over the *full* lane (offline + integration), which
  is expected to move it **up** — the three memory-store backends currently sitting at 0.00 branch
  coverage are only unreachable in the offline lane.
- **Task-item discharge**: the 105 blocked items across the 26 change folders get checked off with
  the run's evidence, or converted into filed defects.

No `src/` change is proposed. No test is modified. No gate threshold is relaxed.

## Capabilities

### New Capabilities

- `live-infrastructure-verification`: the end-to-end verification contract — phase coverage,
  pass criteria, infra-vs-defect triage, baseline seeding, and the recorded report.

### Modified Capabilities

<!-- Every tier this change runs is already specified by the change that introduced it; running
     them is what those specs always intended. Nothing about their required behavior changes. -->

None. The conformance matrix, the effectively-once gate, the Spark leg, the mutation gate, the
benchmark gate, and the Dataflow `--update` gate each keep their existing requirements verbatim —
this change executes them for the first time rather than altering what they demand. The only
files it edits outside its own folder are the two baseline TOMLs (seeded, as their own comments
instruct) and the 26 `tasks.md` files (checked off with evidence, which is the repo's standing
convention).

## Impact

- **Depends on** all 26 implemented changes C23–C48, which are merged on
  `claude/phase-3-m2-roadmap-tlksqq`. It is the verification counterpart to C35
  (`add-0-3-0-release`), C43 (`add-0-5-0-release`) and C48 (`add-1-0-0-release`), all three of which
  currently record their gate as **not green** and decline to tag — partly because the evidence this
  change produces does not exist yet.
- **New code:** none. New docs: `docs/verification.md`; new generated artifact:
  `verification-report.md`.
- **Modified code:** none under `src/`. `benchmark-baseline.toml` and `coverage-baseline.toml` are
  seeded/re-measured; the 26 `openspec/changes/*/tasks.md` files are checked off.
- **CI/build:** no workflow change. This run is what proves the existing workflows would pass;
  where it finds a workflow defect, that becomes a filed change, not an inline edit.
- **Gates:** the run itself IS the gate. It additionally produces the two seeded baselines that
  `make bench-gate` and `make coverage-ratchet` need in order to be meaningful in CI.
- **Environment required:** Docker (compose stack: Redpanda, Redis, Pub/Sub + Bigtable + Firestore
  emulators, Flink jobmanager/taskmanager/jobserver, SDK harness) for phases 1–5; a GCP project
  with Dataflow, GCS, Artifact Registry and Secret Manager for phase 6; optionally a GPU host and
  provider API keys for phase 7. Phases 1–5 are runnable **today** on a laptop with Docker;
  phase 6 unblocks when Dataflow is provisioned; phase 7 is optional.
