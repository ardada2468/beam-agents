# Live Infrastructure Verification Report

Produced by the `verify-live-infrastructure` OpenSpec change. Each phase carries exactly one of
four verdicts per design D2: `pass`, `fail (defect)`, `fail (infra)`, `blocked`.

## Run identity

| Field | Value |
| --- | --- |
| Commit verified | `c2e966355c4f021dbe197749686c782dfd976c3f` |
| Branch | `claude/phase-3-m2-roadmap-tlksqq` |
| Run date | 2026-07-30 |
| OS | macOS 26.5.1 (build 25F80), Darwin arm64 |
| CPU count (host) | 10 |
| RAM (host) | 64 GiB |
| Docker | 28.3.3 |
| Docker Compose | v2.39.2-desktop.1 |
| Docker memory allocated | 15.6 GiB (≥ 8 GiB minimum — task 1.2 satisfied) |
| Docker CPUs allocated | 10 |
| uv | 0.9.21 |
| Python (venv) | 3.11 |
| GCP | not configured (`GCP_PROJECT_ID` unset) |
| GPU | none (`nvidia-smi` absent) |
| Provider API keys | none (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY` unset) |

## Phase summary

| Phase | Command | Verdict | Evidence |
| --- | --- | --- | --- |
| 0 — preflight: lint | `make lint` | pass | ruff check + format clean, 380 files |
| 0 — preflight: type | `make type` | **fail (defect)** | 1 error under `--all-groups`; see D-1 |
| 0 — preflight: unit | `make test-unit` | pass | 1955 passed, 219 deselected, 95.68% coverage, 151 s |
| 0 — preflight: offline semantics | `make test-semantics-offline` | pass | 79 passed, 1 skipped (documented ADK skip), 16 s |
| 0 — preflight: pre-commit | `uv run pre-commit run --all-files` | pass | all 10 hooks passed, incl. proto-drift and mypy |
| 1 — base services | `make compose-up-core` | pass | redpanda, redis, pubsub-emulator, bigtable-emulator all healthy; `firestore-emulator` started manually — see D-2 |
| 1 — integration lane | `make test-integration` | **fail (defect)** | 73 passed, 2 failed, 1 skipped, 1 xfailed, 26.5 s. Both failures Firestore — see D-3 |
| 2 — full stack | `make compose-up` | pass | all 9 services healthy (Flink jobmanager/taskmanager/jobserver + SDK harness + 5 services) |
| 2 — Flink conformance (first run) | `make test-conformance-flink` | **fail (defect)** | 15 passed, 5 failed, 8 skipped, 8 m 47 s. All 5 failures `pydantic_ai` — see D-4 |
| 2 — Flink conformance (after D-4 fix, cold stack) | `make test-conformance-flink` | **pass** | **20 passed, 8 skipped, 0 failed**, 6 m 54 s — all 4 adapters green on a distributed runner |
| 2 — effectively-once gate | `make test-semantics` | **pass** | 1 passed, 482.77 s (8 m 02 s), default volume (no `BEAM_AGENTS_E2E_EVENTS` override). Budget ≤ 15 min met |
| 3 — Spark overlay | `make compose-up-spark` | pass | `spark-jobserver` + `beam-sdk-harness-spark` healthy |
| 3 — Spark conformance (first run) | `make test-conformance-spark` | **fail (defect)** | 16 failed, 12 skipped, 26 m 27 s. All 16 runnable cells fail identically — see D-5. Triage category (b): portable-runner capability gap |
| 3 — Spark conformance (after declaration fix) | `make test-conformance-spark` | **pass (0 executing cells)** | 28 skipped, 0.21 s, exit 0. Green because nothing runs — see D-5 |
| 4 — mutation gate | `make mutation` | **fail (defect)** | Aborts in 7 s during baseline stats collection; 0 of ~900 mutants generated — see D-7. Task 5.2 undischargeable |
| 5 — benchmarks | `make bench` | pass | all 5 dimensions written to `bench-results/` |
| 5 — latency budget | `make bench-gate` | **pass (budget)** | p50 **2.1324 ms** (< 15 ms), p99 **9.3208 ms** (< 60 ms). Gate verdict FAIL is solely the 14 unseeded medians |
| 6 — Dataflow `--update` gate | `make test-dataflow` | **pass** | 1 passed, 531.23 s (8 m 51 s), project `beamagents`, region `us-east1`. Predecessor `Updated`, successor `Cancelled`, 0 jobs left running |
| 6 — Dataflow flex template | `tests/dataflow/test_flex_template_launch.py` | **blocked** | `BEAM_AGENTS_FLEX_TEMPLATE_SPEC` unset — needs the nightly template build/push step |
| 5 — median seeding | — | **not seeded (host unqualified)** | Laptop running a 6-container Docker stack; see "Benchmark seeding" below |
| 4 — full-lane coverage | `pytest -m "not semantics and not dataflow and not smoke and not spark" --cov` | pass (measured, **not seeded**) | 2028 passed, 2 failed (D-3), 150 s. Branch-rate **0.9358** vs baseline 0.9164. Baseline deliberately left unchanged — see D-6 |

## Scope change: this run also repaired what it found

This change was specified as record-only (design D4, and "Fixing defects inline" as an explicit
non-goal). **The repository owner directed that the defects be fixed rather than only filed**, so the
findings below carry a remediation status in addition to their verdict. Each `src/` fix is carried by
its own OpenSpec change and changelog fragment, as the repo's pre-commit guards require.

| Defect | Status |
| --- | --- |
| D-1 `make type` fails with `integration` installed | **fixed** — `make type` passes under `--all-groups` and under CI's selection |
| D-2 `compose-up-core` omits `firestore-emulator` | **fixed** — documented sequence now runs the lane clean (75 passed) |
| D-3 Firestore cannot store keys containing `/` | **fixed** — 11/11 emulator conformance cells pass |
| D-4 SDK-harness image lacks `pydantic-ai` | **fixed** — image now imports `pydantic_ai` 2.21.0; build-time assertion added |
| D-5 Spark runner has no bundle checkpoint handler | **not fixable** (Beam runner gap) — **documented**: 4 `Run()` → evidence-bearing `Skip()`; leg now reports 28 skips in 0.2 s instead of 16 failures in 26 min |
| D-9 full-matrix run fails one adapter regardless of which | **under investigation** — pre-existing; see D-9 |
| D-6 coverage baseline vs CI lane mismatch | **resolved as recorded constraint** — baseline correctly left unchanged |
| D-7 mutation gate non-functional | **fixed** — gate now runs 2475 mutants; it then **fails**, exposing pre-existing debt (D-8) |

## Defects filed

### D-1 — `make type` fails when the `integration` group is installed

**Found by:** task 1.4, after task 1.3's mandated `uv sync --locked --all-groups`.

**Signature:**

```
src/beam_agents/memory/stores/firestore.py:102: error: Call to untyped function "close" in typed context  [no-untyped-call]
Found 1 error in 1 file (checked 374 source files)
```

**Root cause:** `google-cloud-firestore` is declared only in the `integration` dependency group.
No CI workflow installs `typecheck` and `integration` together — `ci.yml`, `release.yml` and
`quality.yml` sync `lint typecheck test` (plus `precommit`), while `integration.yml` and
`spark-weekly.yml` sync `test integration` without `typecheck`. The pre-commit mypy hook likewise
runs in an isolated environment without the integration clients. Consequently
`AsyncClient.close()` resolves to `Any` in every environment that runs the typecheck gate, and the
`no-untyped-call` error is invisible to all of them.

**Confirmed, not inferred:** an isolated venv synced with CI's exact selection
(`uv sync --locked --group lint --group typecheck --group test`) reports
`Success: no issues found in 374 source files`. The same tree under `--all-groups` reports the
error above. The gate is green in CI because of the group selection, not because the code
typechecks.

**Not fixed here** per design D4 and the change's non-goals — recorded for a separate filed change.

### D-2 — `compose-up-core` omits `firestore-emulator` from the base integration lane

**Found by:** task 2.4.

`tests/memory/stores/test_firestore_emulator.py:29` declares
`pytestmark = [pytest.mark.integration, pytest.mark.slow]`, so it is inside `make test-integration`'s
`integration and not semantics and not spark` selection. But `compose-up-core` starts only
`redpanda redis pubsub-emulator bigtable-emulator`. Its Makefile comment carries a service-list
audit dated 2026-07-30 that enumerates the services the selection reaches and concludes with "If a
new test needs another service, grow this list — loudly" — the Firestore leg is missing from that
enumeration.

Consequence: running the documented base-lane sequence (`make compose-up-core && make
test-integration`) fails to connect for the two Firestore tests rather than exercising them. The run
worked around it by starting `firestore-emulator` explicitly; the target itself is unchanged here.

**Not fixed here** per D4 — recorded for a separate filed change.

### D-3 — `FirestoreMemoryStore` cannot store a key containing `/`

**Found by:** task 2.2, first-ever live execution of the Firestore memory-store backend.

**Failing tests:**

```
FAILED tests/memory/stores/test_firestore_emulator.py::TestFirestoreMemoryStoreConformance::test_prefix_search_returns_ordered_bounded_entity_scoped_results
FAILED tests/memory/stores/test_firestore_emulator.py::TestFirestoreMemoryStoreConformance::test_search_round_trips_the_full_record
E  ValueError: A document must have an even number of path elements
   path = ('ltm-636857925c27', '656e746974792d61#case', '2')
```

**Root cause:** `src/beam_agents/memory/stores/firestore.py:50` builds the document ID as
`f"{entity_key.hex()}#{key}"`. Firestore treats `/` inside a document ID as a **path separator**, so
a record key of `case/2` yields the three-element path above — an invalid document reference. Any
memory key containing `/` is unstorable in this backend.

**Why it matters:** the shared `MemoryStoreConformance` suite
(`tests/memory/stores/_conformance.py:134`) deliberately uses hierarchical keys — `case/1`, `case/2`,
`case/3`, `note/1` — and its prefix-search requirement is built on them. The Redis and Bigtable
backends pass the identical suite. The three backends are therefore **not interchangeable**, which is
the specific guarantee a shared conformance suite exists to provide.

This is the failure mode design.md predicted the offline lane structurally could not catch:
`memory/stores/firestore.py` sits at 0.00 branch coverage offline because every branch it owns is
inside a live-client call path.

**Verdict:** `fail (defect)` — the emulator was healthy and six sibling conformance tests passed in
the same session, so this is a behavioral assertion failure against a healthy stack, not infra.

**Not fixed here** per D4 — recorded for a separate filed change.

### D-4 — the SDK-harness image is missing `pydantic-ai`, so the entire Pydantic AI Flink leg cannot run

**Found by:** task 3.2, first-ever execution of the conformance matrix's Flink leg.

**Failing cells** (all 5 runnable `pydantic_ai` cells; the other 2 are declared skips):

```
FAILED tests/conformance/test_flink.py::test_flink_single_shot[pydantic_ai]
FAILED tests/conformance/test_flink.py::test_flink_multi_tool_inline[pydantic_ai]
FAILED tests/conformance/test_flink.py::test_flink_suspension_resume[pydantic_ai]
FAILED tests/conformance/test_flink.py::test_flink_approval_timeout_fallback[pydantic_ai]
FAILED tests/conformance/test_flink.py::test_flink_restart_mid_suspension[pydantic_ai]
InfraFailure: conformance job for adapter 'pydantic_ai' never started processing in 2 submissions
```

**Root cause:** `docker/sdk-harness.Dockerfile:40` installs

```
pip install ... "pydantic>=2" "aiokafka" "langgraph>=1.0,<2" "langchain-core>=1.0,<2" "google-adk>=2.6,<3"
```

`pydantic>=2` is the *validation library*, not the Pydantic AI *agent framework* (`pydantic-ai-slim`,
which the host venv installs at 2.21.0). The framework is never installed in the worker image, so
the adapter's import fails inside the SDK worker, the worker dies, no bundle is ever processed, and
the harness times out its submission-stall window.

Confirmed directly in the running container:

```
$ docker compose exec beam-sdk-harness python -c "import pydantic_ai"
langgraph: OK   google.adk: OK   pydantic: OK
pydantic_ai: MISSING -> ModuleNotFoundError: No module named 'pydantic_ai'
```

**Why it stayed invisible:** the image's own build-time smoke check (`sdk-harness.Dockerfile:51`)
asserts `import langgraph; import google.adk` and does **not** assert `import pydantic_ai`, so the
image builds green while missing a framework its own comment says must be importable. That comment
("the adapter conformance matrix's Flink leg runs **both** framework adapters' cells inside this
harness, so each framework must be importable here — their e2e cells would otherwise fail
worker-side instead of skipping host-side") was written when there were two framework adapters; the
Pydantic AI adapter was added later and the image was never updated.

**Verdict: `fail (defect)`, not `fail (infra)`** — despite the harness raising `InfraFailure`.
Justification: the failure reproduced identically on a **completely fresh stack** (`make
compose-down && make compose-up`, then the `pydantic_ai` cells in isolation: 5 failed, 2 skipped in
319 s), it falls perfectly along one adapter while the other three adapters' 15 cells pass in the
same session, and the cause is a deterministic missing dependency rather than a resource or
connectivity condition.

**Secondary finding — the triage rubric has a blind spot.** A missing worker-side dependency presents
to the harness as a *submission stall*, which `_submit_with_stall_retry`
(`tests/conformance/_flink/harness.py:330`) classifies as `InfraFailure`. Under D2's rubric as
written, that verdict would be remediated and retried indefinitely, and a hard packaging defect would
never be recorded as a defect. The rubric's "failures cluster across unrelated tests" signature is
what distinguishes them here — the clustering was along an *adapter*, not across the stack — and it is
worth making that explicit in the runbook.

**Impact on the change's central claim:** "adapters cannot silently diverge" is, after this run,
evidenced on a distributed runner for **3 of 4** adapters. The Pydantic AI adapter has never executed
a single cell on Flink.

**Not fixed here** per D4 — recorded for a separate filed change (add `pydantic-ai-slim` to the image
*and* add `pydantic_ai` to the build-time import assertion, so the next omission fails the build).

### D-5 — the Spark portable runner registers no bundle checkpoint handler, so the whole Spark leg is unrunnable as architected

**Found by:** task 4.2, the first-ever execution of the Spark leg against a job server (the D3 spike).

**Result:** all **16 runnable cells failed** (4 adapters × 4 scenarios); the other 12 are the leg's
declared skips. Every failure is identical and independent of adapter and scenario.

**Surface symptom:** `InfraFailure: spark conformance job for adapter '<a>' never started processing
in 2 submissions (job-server or executor submission stall)`.

**Actual root cause**, from the job-server log:

```
ERROR JobInvocation: Error during job invocation sconf-f7aab4f434b1-reference-2
org.apache.beam.sdk.Pipeline$PipelineExecutionException:
  java.lang.UnsupportedOperationException: The ActiveBundle does not have a registered bundle checkpoint handler.
```

The job server is healthy and functioning — it starts a SparkContext, builds the DStream graph,
processes batches, and writes checkpoints — then fails the invocation on the above. The conformance
pipeline's ingest source (`tests/semantics/_e2e/spool.py:160`) is
`@beam.DoFn.unbounded_per_element()` — a Splittable DoFn — and line 214 calls
`tracker.defer_remainder(...)` to self-checkpoint while tailing the spool. A residual from
`defer_remainder` requires the runner to have registered a **bundle checkpoint handler**; Beam's
Spark portable runner does not. Every Spark cell routes through this source, which is why the
failure is uniform.

**Triage (task 4.3): category (b) — a genuine Spark portable-runner capability gap.** Not (a) infra:
the job server and worker pool were healthy, the pipeline translated and executed, and the failure is
a deterministic `UnsupportedOperationException` from the runner rather than a connectivity, staging
or resource condition. Not (c) either: the harness and runtime behave correctly; the runner lacks the
capability the source requires.

**Consequence for `promote-spark-runner`:** its four provisional `Run()` declarations
(single-shot, multi-tool-inline, suspension-resume, approval-timeout-fallback) are **all**
unachievable while the leg ingests through a self-checkpointing SDF. This is not four independent
scenario gaps — it is one architectural constraint that invalidates the leg's entire runnable set.
Converting the four `Run()`s to `Skip(reason=...)` is the mechanism the change documents, but that
would leave the Spark leg with **zero** executing cells, which is worth an explicit decision rather
than a mechanical edit: the alternative is a non-SDF ingest path for the Spark leg.

**Promotion clock:** per task 4.5, this run is a local run and does not count toward the four
consecutive green *scheduled weekly* runs `promote-spark-runner` requires. The leg is red, so the
clock has not started.

**Not fixed here** per D4 and task 4.4 — the declarations are left untouched; recorded for a
separate filed change.

**Secondary finding (same shape as D-4):** here too a hard, deterministic failure presented to the
harness as an `InfraFailure` submission stall. Two of this run's three most significant findings were
misclassified by the harness's own infra signal. The real diagnosis in both cases came from the
service logs, not the pytest output — worth reflecting in the runbook's triage rubric.

### D-6 — task 5.3's instruction to raise the coverage baseline to the full-lane value would break CI

**Found by:** task 5.3, on attempting the prescribed baseline raise.

**Measured, as instructed:**

| Lane | Selection | Branch rate |
| --- | --- | --- |
| Offline (what CI measures) | `not integration and not semantics and not dataflow and not smoke` | 0.9164 (committed baseline) |
| Full (offline + integration) | `not semantics and not dataflow and not smoke and not spark` | **0.9358** |

Coverage moved **up by ~1.9 points**, exactly as design.md predicted, and for the predicted reason —
the three live-client memory-store backends are reachable only in the integration lane.

**Why the baseline was NOT raised:** `.github/workflows/quality.yml` runs `make test-unit` (line 37)
and then `make coverage-ratchet` (line 40). `make test-unit` produces `coverage.xml` from the
**offline** selection only. `scripts/coverage_ratchet.py` compares that file's `branch-rate` against
`coverage-baseline.toml`. Raising the baseline to the full-lane 0.9358 would therefore make every
subsequent `quality` run fail with `branch coverage dropped from 93.58% to 91.64%` — permanently, and
for no defect.

Task 5.3 assumes the ratcheted `coverage.xml` comes from the full lane. It does not, and this
change's own proposal states "**CI/build:** no workflow change." The two cannot both hold. Seeding
the number as instructed would not weaken a gate (D4's concern) — it would *break* one, which is
worse.

**Recorded, not applied.** `coverage-baseline.toml` is left at 0.9164. The measured full-lane value
is recorded here so a follow-up change can either (a) fold integration coverage into the ratcheted
artifact and then raise the baseline to 0.9358 in the same commit, or (b) keep the ratchet
offline-lane-scoped and note the full-lane number as informational. That decision needs the workflow
edit this change is scoped out of.

Note the measured 0.9358 comes from a lane in which the two D-3 Firestore tests fail; fixing D-3
should raise it further, so 0.9358 is a floor, not a ceiling.

### D-7 — the mutation gate aborts before generating a single mutant

**Found by:** task 5.1, the first-ever invocation of `make mutation` on this tree (five changes had
deferred it).

**Result:** the gate fails in **7 seconds**, during mutmut's baseline stats collection, without
generating any of the ~900 mutants:

```
tests/core/test_state_compat_doc.py:48: AssertionError:
  the published compatibility policy is missing:
  /Users/arnavdadarya/coding/Beam-Agents/mutants/docs/state-compat.md
19 passed, 1 error in 0.10s
failed to collect stats. runner returned 1
make: *** [mutation] Error 1
```

**Root cause:** `tests/core/test_state_compat_doc.py:26-27` resolves
`REPO_ROOT = Path(__file__).resolve().parents[2]` and reads `REPO_ROOT / "docs" / "state-compat.md"`.
mutmut copies only `src/` into `mutants/` — `[tool.mutmut]`'s own comment states this explicitly
("source_paths deliberately stays at its default (src/): it controls both what is mutated AND what is
copied into mutants/") — and runs pytest from inside that tree. `parents[2]` therefore resolves to
`<repo>/mutants`, and `mutants/docs/` does not exist.

`pytest_add_cli_args_test_selection` already carries `--ignore` entries for precisely this class of
problem (`tests/test_import.py`, `tests/core/test_mutation_gate.py`, described as "intentionally
outside mutmut's copied src/ tree"). `test_state_compat_doc.py` reads a repo-root document and
belongs in that list; it was never added.

**Consequence:** the mutation gate has not merely gone un-run — it has been **non-functional** for the
entire period five changes deferred it. Any CI job invoking it would have failed at the same point.

**Not fixed here** per D4 — recorded for a separate filed change (add the `--ignore`, or make the
test resolve its doc path independently of CWD).

**A second, independent blocker of the same class.** To establish whether the missing `--ignore` was
the only obstacle, it was added to `pyproject.toml` **locally and reverted immediately afterwards**
(the "temporarily, not committed" pattern the e2e change's own fault-injection tasks use; the
committed tree is unchanged — `git diff pyproject.toml` is empty). The gate then advanced past the
doc test and failed again, still in baseline stats collection:

```
tests/core/test_schema_compat.py:93: in test_the_corpus_cannot_silently_shrink
    assert list(GOLDEN_DIR.glob("*.bin")) == []
AssertionError: first extra item:
  PosixPath('<repo>/mutants/tests/core/golden/llm_cache_blob.bin')
1 failed, 95 passed in 0.42s
failed to collect stats. runner returned 1
```

`GOLDEN_DIR` is `Path(__file__).parent / "golden"` (`test_schema_compat.py:43`). The test asserts no
fixture sits outside a per-version directory — and in mutmut's copied tree the `.bin` fixtures appear
**flattened directly under `golden/`** rather than under `v1/`, `v2/`. The mutant tree's copy does
not preserve the corpus layout the test asserts.

**No mutation data could be produced, and none is reported.** Escalating past this would mean
stacking further `--ignore`s until the gate ran, but each one changes what the gate measures, so any
resulting survivor list would describe a configuration that does not exist. The honest finding is
that `make mutation` is non-functional for at least two independent reasons and that repairing it is
design work, not a one-line ignore.

**Consequently task 5.2 cannot be discharged**: there is no list of surviving mutants, and the known
`add-token-budgets` follow-up (positional renumbering of four `mutation-exclusions.toml` entries)
remains unverifiable until the gate runs at all.

## Benchmark seeding decision (task 6.1)

**`benchmark-baseline.toml` was NOT seeded, and `[medians_ms]` is left empty.**

The spec requires medians be seeded "only from a run on a quiet machine". This host is a macOS
laptop that spent the run driving a nine-container Docker stack (Flink jobmanager/taskmanager/
jobserver, SDK harness, Redpanda, Redis and three emulators), plus an IDE. It does not qualify, and
the spec's own scenario — "a busy laptop does not seed the medians" — is the governing case.

Per D3 the absolute budget is nonetheless a real pass/fail on a first run, and it **passes** with
substantial margin (p50 2.13 ms against a 15 ms budget; p99 9.32 ms against 60 ms).

The measured medians are recorded here as *evidence of magnitude only* — they are explicitly **not**
proposed as baseline values, and seeding them from this host would be exactly the error
`docs/benchmarks.md` forbids:

| benchmark | measured median (ms) |
| --- | --- |
| noop_throughput | 0.1279 |
| overhead_50ms | 2.1324 |
| overhead_500ms | 2.1045 |
| overhead_2000ms | 1.6578 |
| suspension_roundtrip | 0.3002 |
| state_commit_1kib / 16kib / 64kib / 100kib | 0.1348 / 0.1370 / 0.1248 / 0.1250 |
| encode_1kib / 16kib / 64kib / 100kib | 0.0003 / 0.0009 / 0.0036 / 0.0051 |
| runinference_delta | 0.3599 |

`make bench-gate` therefore still cannot pass on this tree, and that remains correct by construction
until a qualified host runs it.

### D-8 — with the gate running, the mutation suite fails: 154 survivors and two ratchet regressions

**Found by:** task 5.1, once D-7's repair let `make mutation` complete for the first time.

```
2475 core mutants -- killed: 1841, no tests: 479, survived: 154, timeout: 1
```

Three independent gate errors:

1. **Stale exclusions** — two `mutation-exclusions.toml` entries named mutants that are now `[killed]`.
   Diagnosed and repaired; see "Positional drift" below.
2. **146 mutants not killed** (145 survived + 1 indeterminate) after accounting for the 9 still-valid
   exclusions. Concentrated in `core/dofn.py` (102, of which **64 in `_flush` alone**), `core/context.py`
   (17), `core/migration.py` (15, plus the one timeout), `core/loop.py` (7), `core/transform.py` (3),
   `core/batching.py` (2).
3. **"no tests" ratchet regressions** — `transform.py` rose 409 → 474 and `snapshot.py` rose 0 → 2.

The scale (64 survivors in a single function, and a brand-new `snapshot.py` entry) is consistent with
the gate having been dead across several changes rather than with any single regression. Per the
repository owner's direction, the survivors were **recorded, not killed** — writing tests for 145
mutants is its own project. No ceiling was raised and no exclusion was added.

#### Positional drift in `mutation-exclusions.toml` — confirmed, and repaired by renumbering

The `add-token-budgets` follow-up note anticipated this, and it is real. Mutant names are **positional**
(`__mutmut_<n>` is an index), so inserting a statement into a mutated function silently re-points every
later entry at a different mutant while its reason text stays put.

| Entry | Reason text describes | Mutant actually at that index | Repair |
| --- | --- | --- | --- |
| `call_model__mutmut_45` | `cache_hit=None` ≡ `False` | `self._tally.llm_calls += 1` → `+= 2` | renumbered → `__mutmut_51` |
| `AgentContext.__init____mutmut_51` | `_drained = None` ≡ `False` | `_completion_tokens = 0` → `None` | renumbered → `__mutmut_56` |

The first is the dangerous shape: an entry justified by a statement about `cache_hit` had come to guard
a genuine off-by-one in the LLM call tally.

**Repaired by renumbering, deliberately not by deletion.** The gate's message says "Remove or
investigate"; investigation showed removal would have been wrong. Verification against the generated
mutant tree found the originally-described mutants alive at shifted indices —
`call_model__mutmut_51` is the `cache_hit=False → None` mutation (index 24 is the *`True` → None*
mutation, which is **not** equivalent and should stay killable), and `__init____mutmut_56` is
`_drained = None`. Both are still genuinely equivalent and still appear in the survivor list, so
deleting the entries would have converted two unkillable mutants into permanent survivors.

The four other entries `add-token-budgets` could have disturbed (`_stage_intent__mutmut_21/32`,
`AgentContext._stage_intent__mutmut_6/7`) were each verified against the generated tree and still match
their reasons exactly.

A standing warning about positional drift, and the procedure for re-verifying an entry, is now recorded
in `mutation-exclusions.toml`'s header.

### D-9 — a full 28-cell Flink matrix run fails one adapter's cells, whichever adapter that happens to be

**Found by:** re-running the full matrix after D-4's repair, and it materially corrects part of this
report's earlier reading.

**Observations:**

| Run | Stack state at start | Result |
| --- | --- | --- |
| Full matrix #1 | long-lived stack | 15 passed, **5 failed (`pydantic_ai`)**, 8 skipped, 8 m 47 s |
| `pydantic_ai` isolated | `compose-down` + `compose-up` | 5 failed — D-4's missing dependency |
| `pydantic_ai` isolated, after D-4 fix | `compose-down` + `compose-up` | **5 passed**, 72 s |
| Full matrix #2 (after D-4 fix) | stack with ~7 jobs of history | 15 passed, **5 failed (`langgraph`)**, 8 skipped, 8 m 38 s |
| `langgraph` isolated | `compose-down` + `compose-up` | **5 passed**, 61 s |
| **Full matrix #3** | `compose-down` + `compose-up` (cold) | **20 passed, 8 skipped, 0 failed**, 6 m 54 s |

The failing adapter **moved** from `pydantic_ai` to `langgraph`, and `langgraph` passes cleanly in
isolation. So a full-matrix run loses one adapter's cells to a submission stall independently of which
adapter it is — this is `fail (infra)` in D2's terms, and it is **pre-existing**, not introduced by any
fix here.

**Correction to this report's earlier reading.** Full matrix #1 attributed all five `pydantic_ai`
failures to D-4. D-4 is independently proven — `import pydantic_ai` failed inside the running
container, which no ordering effect can explain, and the cells pass in 72 s once the dependency is
present. But run #1 would very likely have lost *some* adapter to D-9 regardless, so D-4 was not the
sole cause of that run's redness. Both defects were present and one was masking the other.

**Probable mechanism (not yet confirmed):** `FlinkStackControl.freshen_flink()`
(`tests/semantics/_flink_stack.py:62`) restarts `flink-taskmanager`, `flink-jobserver` and
`beam-sdk-harness` before each adapter — but never `flink-jobmanager`, which therefore accumulates
every submission across all 28 cells. Every isolated run recorded above began with
`compose-down`/`compose-up`, i.e. a fresh JobManager, which is exactly the variable the full runs lack.
The TaskManager's repeated
`NoClassDefFoundError: org/apache/beam/vendor/grpc/.../IntObjectHashMap$2` is consistent with
classloader exhaustion across repeated job deployments. The stack control's own docstring already
records the related constraint that "the pool fails permanently after a handful of worker exits
(design F8)".

**Resolved by run #3, which narrows the defect substantially.** A full matrix on a **cold** stack passes
every runnable cell — 20 passed, 0 failed. So the matrix does *not* degrade across its own 28 cells;
it degrades when it inherits a stack that has already served jobs. `freshen_flink()`'s per-adapter
restart of the TaskManager, jobserver and SDK harness does not clear that inherited state, and the
JobManager — the one service it never restarts — is the remaining candidate.

**Severity: low, and CI is structurally unaffected.** Every CI workflow job starts from a fresh
compose stack, which is precisely the cold-start condition under which the matrix is green. This is a
*local re-run hygiene* defect: running the matrix twice against one stack fails the second time.

**Not remediated here, deliberately.** The candidate fix (adding `flink-jobmanager` to
`freshen_flink()`'s restart list) touches `tests/semantics/_flink_stack.py`, which the effectively-once
release gate also depends on. The cold/warm correlation is established, but the JobManager has **not**
been proven to be the mechanism, and modifying a release gate's stack setup on an unproven hypothesis
is not warranted. Recommended follow-up: either confirm the mechanism and add the restart, or document
`make compose-down && make compose-up` as a precondition of a full matrix run.

### D-11 — `beam_agents` was unimportable on a stock Dataflow worker (unbounded `protobuf`)

**Found by:** task 7.2, the first-ever Dataflow launch. **Severity: highest of this run — a
shipped-artifact defect.**

Every SDK worker crashed on startup:

```
Error message from worker: generic::aborted: SDK harness sdk-0-0 disconnected.
Could not load main session.
  File ".../site-packages/beam_agents/_protos/beam_agents_pb2.py", line 12, in <module>
google.protobuf.runtime_version.VersionError: Detected mismatched Protobuf Gencode/Runtime
major versions ... gencode 6.33.5 runtime 5.29.5. Same major version is required.
```

**Root cause:** `pyproject.toml` declared a bare, unbounded `"protobuf"`. The committed `_pb2.py`
bindings are 6.x gencode and protobuf requires gencode and runtime to share a **major** version. An
unbounded requirement is satisfied by whatever is already installed, and
`apache/beam_python3.11_sdk:2.72.0` — the base image Dataflow workers run — ships **5.29.5**. pip left
it in place and the package could not be imported.

**The project had already diagnosed this and fixed it in one place only.**
`docker/sdk-harness.Dockerfile`'s header names this exact `VersionError` as one of two reasons that
image is built rather than pulled, and pins `protobuf==6.33.6`. That protected the Flink harness.
Dataflow uses the same base image *without* the pin — which is precisely why every Flink leg passed
and Dataflow failed on first contact.

**Why no existing gate caught it:** offline, unit, integration, semantics and conformance all run in
environments where uv resolves protobuf 6.x, and the Flink harness bakes the pin in. Only a real
Dataflow launch reaches a worker that supplies its own protobuf.

**Fixed** — `protobuf>=6,<7` in the package's own dependencies (composing with Beam's
`protobuf<7.0.0.dev0`), `uv.lock` regenerated. Carried by
`openspec/changes/fix-protobuf-runtime-pin/`. Verified: the re-run shows zero `VersionError` entries,
workers start, and the gate passes.

### D-10 — the `--update` gate died in provisioning under uv (`pip freeze`)

**Found by:** task 7.2, before any job launched. **Release-blocking.**

```
subprocess.CalledProcessError: Command '[.../python, -m, pip, freeze]' returned non-zero exit status 1
No module named pip
```

`tests/dataflow/_update/versions.py:232` shelled out to `<python> -m pip freeze`, but the head leg's
interpreter is the job's own **uv-managed venv**, which has no `pip`. The nightly job provisions with
`uv sync --locked --group test --group integration --group bench`, so **CI would have failed
identically**. The same module already used `uv build` for the head wheel, so it was internally
inconsistent.

**Fixed** — `uv pip freeze --python <path>`, correct for both legs (the cross-version leg's venv comes
from `python -m venv` and does have pip; uv reads the target interpreter either way). 46 harness unit
tests still pass.

**Latent sibling, recorded not fixed:** `download_wheel_command`
(`versions.py:197`) also uses `sys.executable -m pip` and will hit the same wall on the first
*cross-version* run. There is no `uv pip download`, so the fix means reordering provisioning to use
the pip-bearing prev-venv — a path that cannot be exercised until a release exists. Not worth an
untestable edit inside a release gate.

### Infra event — `ZONE_RESOURCE_POOL_EXHAUSTED` in `us-central1`

Recorded per D2 as `fail (infra)`, remediated, not a verdict on the gate:

```
Startup of the worker pool in us-central1 failed to bring up any of the desired 1 workers.
ZONE_RESOURCE_POOL_EXHAUSTED: the zone 'us-central1-f' does not have enough resources
```

Google capacity, unrelated to this code. Remediation: moved the phase to `us-east1` with a matching
temp bucket (`gs://beamagents-dataflow-temp-use1`); workers came up immediately.

### Phase 6 caveat, stated plainly (design D5 / task 7.3)

The `--update` gate passed on its **bootstrap (head → head) leg**, because no version has been tagged.
The harness said so itself:

```
launch version: 1.0.0 / update version: 1.0.0 (head, built from the checkout)
resolution: PyPI resolution failed (404 Not Found for beam-agents)
NOTE: this is a SELF-UPDATE run. It proves the harness, the Dataflow update mechanics, and that
head's job graph is update-compatible with itself. It is NOT cross-version evidence.
```

So this run proves the `--update` **mechanism** — a live suspended activation plus working memory
survives a pipeline update on real Dataflow. It does **not** prove cross-version compatibility, and
C46's guarantee remains unevidenced until a release exists to update *from*.

## Headline result — correctness invariant 4 is now evidenced

`tests/semantics/test_effectively_once_e2e.py` **passed on its first-ever execution**, at default
volume, in 8 m 02 s against the ≤ 15 min budget. This is the single test that evidences correctness
invariant 4 (effectively-once side effects) and nothing else in the repository substitutes for it.
The run exercised 10,000 events through Redpanda, `RunAgent` on the Flink stack, real
`beam-agents-effector` processes with Redis dedup, SIGKILLed effector workers, a killed TaskManager,
and a cancel-and-resubmit replay from the ingest spool.

No reduced-volume smoke pass was needed (task 3.5 was not exercised); the recorded verdict is from a
full-volume run. The Flink job (`e2e-15898687136a-a1`) was observed in `RUNNING` state processing
bundles, in contrast to the `pydantic_ai` conformance jobs which never left zero-progress.

## Task-item discharge (task 9.2)

**31 of the 103 blocked items across the other change folders were discharged**, each with an inline
citation naming the phase and its evidence. The remaining 72 keep their blockers intact.

| Class | Discharged | Evidence |
| --- | --- | --- |
| `pre-commit` not installed / network-blocked | 19 | `uv run pre-commit run --all-files`, all 10 hooks passed on the merged tree |
| docker integration lane (Redis / Bigtable / Firestore stores, Slack Redpanda loop) | 6 | `make test-integration` → 75 passed, 0 failed |
| Flink conformance (ADK and Pydantic AI axes) | 3 | `make test-conformance-flink` → 20 passed, 0 failed, cold stack |
| coverage ratchet | 3 | `make coverage-ratchet` → at baseline |

**Deliberately left blocked**, with the blocker text preserved:

- **Release infra (10)** — tagging, signing and publishing; no release was cut and task 9.5 forbids
  touching the three release gates' verdicts here.
- **CI-specific runs (~9)** — items whose evidence is a PR-run artifact, a `docker ps` assertion on a
  GitHub runner, or a deliberately-failing scratch commit. A local run is not that evidence.
- **`add-effector-security` SASL leg (2)** — the integration lane *skipped* the SASL test
  (`no SASL-enabled broker: set EFFECTOR_SASL_BOOTSTRAP...`), and the compose SASL profile the task
  asks for does not exist. A reported skip is not a pass.
- **GPU / provider-credential tiers (3)** — phase 7, unavailable.
- **Dataflow / cloud (5)** — phase 6, blocked on credentials.
- **Benchmark medians / paired environment (2)** — this host is not qualified to seed.
- **Mutation survivors** — the gate now runs but fails; see D-8.

## Notes

- Task 1.4 expected ~1936 unit tests; the tree currently has **1955**. More tests, not fewer; the
  offline baseline is intact.
- The working tree carried an uncommitted edit to
  `openspec/changes/add-effectively-once-e2e-gate/tasks.md` pre-checking items 7.1, 7.3, 7.4 and
  7.6 — the full-volume e2e run and the two fault-injection proofs. Those are exactly the items
  Phase 2 is meant to discharge, and the spec requires evidence from an executed phase before an
  item is checked off. Reconciled against real evidence in Phase 2 (see that phase's row).
