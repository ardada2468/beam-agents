## Context

Twenty-six changes were implemented against an environment that could run roughly four fifths of
the test suite. The remaining fifth is not incidental — it is where the project's load-bearing
claims live. `openspec/project.md` states seven correctness invariants; invariant 4 (effectively-once
side effects) is evidenced by exactly one test, and that test needs Kafka, Flink, and the ability to
SIGKILL a worker. The adapter conformance matrix exists precisely to prove adapters behave
identically *across runners*, and two of its three legs have never executed.

So the risk this change addresses is not "some tests are pending". It is that the offline lane's
greenness is easy to mistake for verification of things it structurally cannot verify. A DirectRunner
cell passing tells you the adapter's logic is right; it tells you nothing about whether the runner
commits state and output atomically under a real bundle retry on a distributed runner.

Three facts shape the design:

1. **Some of this has never run anywhere.** The Spark leg is brand new and its `Run()`/`Skip()`
   declarations are provisional. A red Spark leg on first execution is a *finding*, not necessarily
   a regression, and the run must be able to say which.
2. **Two gates cannot pass until this run seeds them.** `make bench-gate` needs
   `benchmark-baseline.toml`'s `[medians_ms]`, which is deliberately empty. `make coverage-ratchet`
   is calibrated against the offline lane only.
3. **The repository already distinguishes infra failure from invariant failure.** The e2e harness
   raises `InfraFailure` when the stack breaks, and `docs/ci.md` documents the distinction. The
   triage rubric here generalizes that existing concept rather than inventing one.

## Goals / Non-Goals

**Goals:**

- Execute every tier this project defines, in an order where each phase's failures are diagnosable
  before the next phase depends on them.
- Produce a committed `verification-report.md` that the three release gates can cite as evidence.
- Seed the two baselines that are currently unseedable, from hardware entitled to seed them.
- Discharge the 105 blocked task items with real evidence, or convert them into filed defects.
- Make an infrastructure failure loud and distinguishable from a product defect, so a flaky broker
  is never recorded as a passing or failing invariant.

**Non-Goals:**

- **Fixing defects inline.** A defect found here becomes its own OpenSpec change with its own
  proposal, spec and tests. This change records; it does not repair. (The one exception is a typo
  or a path that is unambiguously wrong and has no behavioral surface — and even then it is noted.)
- **Weakening any test, threshold, or gate to obtain a green run.** Explicitly forbidden; see D4.
- **Tagging a release.** The three release gates remain the release authority. This change supplies
  evidence to them; it does not decide.
- **Promoting Spark.** `promote-spark-runner` requires four consecutive green *weekly* runs. One
  successful run here starts that clock; it does not finish it.
- **Running the GPU tier** unless a GPU host happens to be available. It is phase 7 and optional.

## Decisions

### D1. Phase order follows the dependency of *diagnosis*, not of code

Phases run cheapest-and-most-foundational first, so that when something breaks you are debugging one
new variable rather than five. Base services (Redpanda/Redis/emulators) before Flink, because the
Flink legs publish to those same brokers; Flink conformance before the effectively-once gate, because
the gate assumes a working Flink submission path and takes ~15 minutes to tell you otherwise; the
Spark leg after Flink, because Spark is the least-proven and its failures are least interpretable
without a known-good portable-runner reference.

Rejected: running the e2e gate first because it is the most valuable. Its value is exactly why it
should not be the first thing to fail — a red e2e gate with an unproven stack underneath it costs an
hour to attribute.

### D2. Infrastructure failure and invariant failure are recorded as different verdicts

Every phase records one of four verdicts: `pass`, `fail (defect)`, `fail (infra)`, or `blocked`.
The rubric, generalized from the e2e harness's existing `InfraFailure` concept:

- **infra** — the stack did not provide the conditions the test needs: containers unhealthy, Flink
  lost its slots, the SDK worker pool died, a broker refused connections, an emulator never became
  ready, disk or memory exhausted. Signature: failures cluster across unrelated tests, or the error
  names a connection/timeout/resource rather than an assertion.
- **defect** — the stack was healthy and an assertion about *behavior* failed. Signature: a plain
  `AssertionError` about ordering, duplication, byte-identity, state contents, or an exit code.

An `infra` verdict is retried after remediation (the runbook gives the standard remedies) and never
recorded as either pass or defect. A `defect` verdict files a change and does not block the
remaining phases unless the failure invalidates them.

Rejected: a binary pass/fail. It is precisely the ambiguity between "your Docker ran out of memory"
and "the runtime duplicated a side effect" that would make this report untrustworthy.

### D3. First-ever runs are recorded as *baseline*, not as regression

The Spark leg (28 cells), the mutation gate, and the benchmark suite on quiet hardware have no prior
green run on this tree. Their first execution establishes what the current state *is*. Concretely:

- Spark: any cell that fails is triaged, and if it is a genuine portable-runner capability gap it
  converts its `Run()` declaration to a `Skip(reason=...)` **in a filed change**, not inline — that
  is `promote-spark-runner`'s own documented mechanism for a leg that cannot express a scenario.
- Mutation: if `make mutation` fails against `mutation-baseline.toml`'s ceilings, the surviving
  mutants are listed in the report. Raising a ceiling requires justification per
  `mutation-exclusions.toml`'s existing rules and is a filed change.
- Benchmarks: the first run seeds `[medians_ms]`. It does not "pass" or "fail" the ratchet, because
  there is nothing to ratchet against yet. The p50 < 15 ms / p99 < 60 ms *budget* is absolute,
  however, and IS a pass/fail on the first run.

### D4. No test, threshold, or gate may be weakened to obtain a green run

Stated as a requirement, not a convention, because the temptation is real and the whole value of the
run is that its verdict means something. Specifically forbidden: adding `skip`/`xfail` to a failing
test, widening a tolerance, lowering a budget, raising a mutation ceiling, adding a coverage `omit`,
or converting a `Run()` to a `Skip()` inline. Each of those *may* turn out to be the right answer —
via a filed change, with a rationale, reviewed. The runbook says this in the operator's voice too.

The one asymmetry: baselines may be **seeded** (from nothing to a measured value) and coverage may
be **raised**, because both are recording reality rather than relaxing a bar.

### D5. Dataflow is a separate phase with its own teardown discipline

Phase 6 spends real money and leaves real resources. It runs only when `GCP_PROJECT_ID` and friends
are configured, it labels every job it creates with the run id, and it tears down unconditionally —
`tests/dataflow/_update/resources.py` already implements a ledger and sweeper for exactly this, so
the phase uses it rather than inventing cleanup. A phase-6 failure must never leave a streaming job
running: the report records the sweeper's output as evidence.

The `--update` compatibility test additionally needs a *previously released* version to update
*from*. No release has been tagged (all three gates are red), so on first run it exercises its
documented head→head bootstrap leg. That is a real limitation of running this before any release
exists, and the report says so rather than implying cross-version compatibility was proven.

### D6. The report is committed, and it is the deliverable

`verification-report.md` is a real artifact in the tree, not console output. The three release gates
each have a "pending (CI run)" / "pending (CI hardware)" row that this file is designed to satisfy,
and a reviewer must be able to see, months later, what was actually run and on what. It records the
host (OS, CPU count, RAM, Docker version), the commit verified, the compose image digests, and one
row per phase.

## Risks / Trade-offs

- **The e2e gate is expensive and can be flaky under a loaded laptop.** Budgeted at ≤ 15 minutes on
  CI hardware; a developer machine running other things may exceed it. Mitigation:
  `BEAM_AGENTS_E2E_EVENTS` tunes the volume down for a first smoke pass, but a *recorded* pass must
  be at the default volume — a reduced-volume run is recorded as such and does not discharge the gate.
- **Spark may simply not work yet.** It has never run. The plan treats that as an expected outcome
  with a defined path (triage → filed change), rather than as a blocker for the rest of the run.
- **Seeding benchmark medians from a developer laptop would be wrong** — that is the whole reason
  they are empty. The runbook requires the seeding run to come from a quiet machine and to record
  the host; if the only available host is a busy laptop, the correct outcome is to leave
  `[medians_ms]` empty and record the budget verdict alone.
- **Docker resource limits are the most likely source of noise.** The full stack (Flink jobmanager +
  taskmanager + jobserver + SDK harness + 5 service containers) is heavy. The runbook states minimum
  memory and makes "raise Docker's memory allocation" the first remedy on a clustered failure.
- **This change touches 26 other changes' `tasks.md` files.** That is intentional and is how the
  repo records verification evidence, but it makes the diff wide. Mitigation: task-item edits are a
  separate commit from the report and baselines, so a reviewer can read them independently.

## Migration Plan

None — nothing ships to users. The run is executed on a branch, its findings are filed as changes,
and its report and baselines merge. If the run is abandoned partway, the report records which phases
completed; a later run resumes at the first unrecorded phase.

## Open Questions

- **Should a failed phase block later phases?** Currently only when the failure invalidates them
  (e.g. Flink submission broken ⇒ the e2e gate cannot be meaningfully attempted). Whether a red
  Spark leg should block the *release* gates is a question for `promote-spark-runner`'s promotion
  process, not this change.
- **How many consecutive green local runs should count toward Spark's four-week promotion window?**
  `promote-spark-runner` counts *scheduled weekly CI runs*. A local run is evidence the leg works,
  not a tick on that clock. Confirm with the promotion process before counting it.
- **Whether the head→head `--update` bootstrap leg is sufficient evidence for C46's guarantee.**
  It proves the mechanism; it cannot prove cross-version compatibility until a version exists to
  update from. C46's spec should be read carefully by whoever reviews the phase-6 result.
