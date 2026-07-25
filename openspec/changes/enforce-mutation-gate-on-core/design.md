## Context

`quality.yml` runs `make mutation` (= `uv run mutmut run`) on any PR that touched `src/beam_agents/core`. Measured with mutmut 3.6.0, before and after scoping mutation to `core/`:

| | Before | After `only_mutate` |
|---|---|---|
| Files mutated | 27 (all of `src/beam_agents`) | **8** |
| Total mutants | 2,219 | **911** |
| Wall time | >25 min at `--max-children 4`, never finished | **~2.8 min at `--max-children 10`** (5.39 mutations/s) |
| Result entries outside `beam_agents.core.*` | 1,076 | **0** |
| Exit code with survivors | 0 | 0 |

Before scoping, **299 of 418 survivors were in `model/`/`tools/`/`memory/`/`actions/`** — modules `tests/core` cannot kill mutants in. `beam_agents.tools.runner.xǁToolRunnerǁrun__mutmut_1: survived` was a configuration artifact, not a test gap.

After scoping, the 911 mutants break down as **304 killed, 154 survived, 453 `no tests`, 0 timeout, 0 suspicious** — a 66% kill rate on mutants that ran tests. Split by module:

| Module | survived | `no tests` |
|---|---|---|
| `context.py` | **91** | 1 |
| `loop.py` | 33 | — |
| `bridge.py` | 28 | — |
| `coders.py` | 1 | — |
| `agent.py` | 1 | — |
| `dofn.py` | — | **263** |
| `transform.py` | — | **189** |

The split falls exactly on the deselection boundary. `dofn.py` and `transform.py` — 452 mutants, half of `core/` — are entirely outside the mutation surface because `test_dofn_pipeline.py`, `test_dofn_streaming.py`, and `test_transform.py` are `--ignore`d: mutmut reaps children with `os.wait()`, which also reaps Beam DirectRunner worker subprocesses and crashes on the unexpected PID. Everything that *is* mutation-tested has survivors, 91 of them in `context.py` — the module implementing correctness invariant #1 (staged effects, atomic commit).

Two mutmut mechanics constrain the design:

- **`mutmut run` never signals failure.** `_run()` returns `None`; the click command exits 0. Results live in `mutants/<path>.py.meta` as an `exit_code_by_key` mapping, readable via `mutmut.SourceFileMutationData`.
- **Mutant names are per function**: `beam_agents.core.loop.xǁClassNameǁmethod__mutmut_7`, package-relative (never `src.`-prefixed).

Relevant constraint from `openspec/project.md`: tests are derived from spec scenarios, written first, and **never weakened to make something pass**. That directly shapes how surviving mutants get resolved.

## Goals / Non-Goals

**Goals:**

- Make `quality`'s mutation step actually fail on survivors, so the required check means something.
- Reach and hold zero undeclared survivors on the mutation-tested part of `core/`.
- Prevent the un-mutation-tested surface from growing in any `core/` module.
- Keep the per-PR cost affordable (it already is, at ~2.8 min locally).
- Keep `make <target>` the single local/CI contract (`repo-scaffolding` requires it).

**Non-Goals:**

- Extending mutation to `model/`, `tools/`, `memory/`, or `actions/`. Those need their own test selections first; separate change per the one-capability-per-change rule.
- Closing the 453-mutant `dofn.py`/`transform.py` gap. That needs an upstream mutmut fix for `os.wait()` child reaping, or restructuring those tests to be reachable without DirectRunner. Tracked, not attempted here.
- Diff-scoped mutant selection (see D3), mutation-score dashboards, or replacing mutmut.
- Touching the `unit`, `integration`, `semantics`, or `dataflow` tiers, or the coverage ratchet.

## Decisions

### D1: Scope with `only_mutate`, not `source_paths` — applied and verified

`only_mutate = ["src/beam_agents/core/*"]`, leaving `source_paths` at its default `src/`.

`source_paths` controls both what is mutated *and* what is copied into `mutants/`. Narrowing it to `core/` would leave `beam_agents.model`, `beam_agents.tools`, etc. unimportable inside the mutant tree, breaking every core test — which is what the pre-existing config comment warned about. `only_mutate` filters mutation while the copied tree stays whole. mutmut's `should_mutate` uses `fnmatch` on the path string, where `*` spans `/`, so the pattern matches `src/beam_agents/core/loop.py` and excludes `src/beam_agents/model/anthropic.py`. Confirmed by measurement: 8 files mutated, 21 ignored, 0 non-core result entries.

`do_not_mutate = ["*/_protos/*"]` stays — redundant under the current `only_mutate`, but the correct guard if it ever widens.

*Rejected:* enumerating the non-core packages in `do_not_mutate`. An allowlist fails safe when a new package appears; a denylist silently starts mutating it.

### D2: `mutate_only_covered_lines` is unusable; ratchet the `no tests` count instead

Setting `mutate_only_covered_lines = true` makes mutmut call `gather_coverage(PytestRunner(), …)`, which runs pytest **in-process** under `coverage.Coverage.collect()`. apache-beam's numpy import then fails:

```
src/beam_agents/memory/facade.py:17: from apache_beam.metrics.metric import Metrics
  apache_beam/__init__.py:86: from numpy import ...
  ImportError: cannot load module more than once per process
```

numpy's `_multiarray_umath` C extension cannot be initialised twice in one interpreter. This is an upstream mutmut × apache-beam incompatibility with no configuration workaround, so the 453 `no tests` mutants stay in the report. Their runtime cost is negligible (they are skipped without running tests); the problem is purely that they are noise obscuring real survivors.

**The replacement is a per-module ratchet.** `mutation-baseline.toml` commits the current `no tests` counts (`dofn.py`: 263, `transform.py`: 189, `context.py`: 1; total: 453). The gate compares every live module independently, treats an absent module as having a baseline of zero, and fails if any module rises. A reduction in `transform.py` therefore cannot hide newly uncovered code in `dofn.py`, and a new core module cannot start uncovered without an explicit, reviewed baseline increase.

This is a better outcome than the original plan. `mutate_only_covered_lines` would have *deleted* those 453 mutants from the report, silently shrinking the mutation-tested surface to half of `core/` with nothing recording that fact. The ratchet keeps it measured. The failure forced the more honest design.

*Rejected:* failing on any `no tests` mutant. Unsatisfiable today without re-enabling the pipeline suites under mutmut, which the `os.wait()` bug blocks — it would block this change indefinitely.

*Rejected:* reporting the count informationally without gating. Nothing would stop new untested core code from landing silently, which is most of what the ratchet is for.

*Rejected:* a single repository-wide count. It allows a decrease in one module to offset a regression in another, contradicting the claim that the uncovered surface cannot grow.

### D3: One tier — the full `core/` sweep — not diff-scoped selection

The original design mapped changed lines to their enclosing functions and ran only those mutants, justified by a >25 min sweep. At **2.8 min** (911 mutants, 5.39 mutations/s, 10 children; est. ~7–8 min on a 4-vCPU GitHub runner) the full sweep is affordable as a required check, and diff-scoping is dropped.

The full sweep is also *strictly stronger*. A diff-scoped gate cannot see a mutant that stops being killed because its only killing test was weakened in a file the PR did change — the mutant's own line is unchanged, so it falls outside scope. That class of regression is invisible to the cheap tier and is precisely what mutation testing exists to catch.

The complexity argument matters as much as the cost one. Diff-scoping needed `git merge-base` handling, `git diff --unified=0` hunk parsing, `ast` line→function mapping, and reconstruction of mutmut's `ǁ`-mangled names. Every one of those is a place where a bug **silently shrinks the gate's scope** — a green check that tested less than it claimed, which is the exact failure this change exists to remove. A gate simple enough to be obviously correct is worth more here than one that is 4× faster.

*Rejected:* keeping diff-scoping as a fast pre-push tier alongside the full CI sweep. Two selection paths mean two things to keep correct, for a saving on a check that already fits in a coffee break.

### D4: A gate script owns pass/fail, reading `.meta` not stdout

`scripts/mutation_gate.py` reads each `mutants/src/beam_agents/core/*.py.meta` as JSON, validates its `exit_code_by_key` mapping, maps those exit codes through mutmut's authoritative `status_by_exit_code`, and exits non-zero on any failing status. Direct JSON keeps the gate independent of mutmut's internal data-class location while preserving its status semantics. Reports identify the source file, qualified function, and full mutant name. The script follows `scripts/coverage_ratchet.py`'s shape: `main() -> int`, actionable stderr.

*Rejected:* parsing `mutmut results` stdout. It prints only non-killed mutants without `--all`, its format is display-oriented and unversioned, and it carries no source locations.

Status classification:

| Status | Gate | Rationale |
|---|---|---|
| `killed` | pass | |
| `skipped`, `caught by type check` | pass | Legitimately not a test's job. |
| `survived` | **fail** | The defect being gated on. |
| `timeout` | **fail** | Not confidently killed; also a runtime signal worth investigating. Reported distinctly. |
| `suspicious`, `segfault` | **fail** | Indeterminate; must not read as green. |
| `not checked`, `check was interrupted by user` | **fail** | The run did not complete; silence is not success. |
| `no tests` | ratcheted | Cannot be killed by the selected suite (D2). Fails only if the count exceeds the committed baseline. |

One known soft spot: mutmut maps pytest exit code 3 (internal error) to `killed`. A mutant that crashes pytest for an unrelated reason reads as a kill. Not worth working around now; recorded so a suspiciously easy kill is debuggable.

### D5: Exclusions live in a reviewed file with stale-entry detection

`mutation-exclusions.toml` at the repo root, keyed by mutant name with a mandatory `reason`. The gate exempts a listed mutant only while its live status is `survived`; exclusions cannot turn a timeout, suspicious result, crash, or incomplete run green. It **fails if a listed mutant no longer exists or no longer survives**, so obsolete entries are removed rather than accumulating into a blanket suppression.

Equivalent mutants are real (`x + 0`, reordered commutative operands, mutations inside `__repr__`) and unkillable by construction. Without a sanctioned escape hatch, the pressure under a zero-survivor bar lands on deselecting tests — which `project.md` forbids outright. Every entry is a diff-visible, reviewable claim.

*Rejected:* a percentage kill-rate threshold. It admits arbitrary unexplained survivors and records nothing about which ones were accepted or why.

### D6: PR runs on a broadened filter; nightly runs unconditionally

`quality.yml` keeps its "did it change?" filter but broadens it from `src/beam_agents/core` to **`src/beam_agents/core` or `tests/core`** — otherwise a PR that only weakens a core test skips the gate entirely, which is the cheapest way to defeat it.

`nightly.yml` gains an **unconditional** sweep, because even the broadened filter has a hole: `core/context.py` imports `beam_agents.memory.facade`, so a change to `memory/` can make a core mutant survive while touching neither `core/` nor `tests/core/`. The nightly job needs no GCP credentials or provider keys, so unlike the existing `dataflow`/`smoke` jobs it runs without a `vars`/`secrets` guard.

Both invoke the same `make mutation` — one target, one behaviour, no second code path to keep correct.

### D7: `--max-children` from `os.cpu_count()`

Computed via `python -c` rather than `nproc`, which is absent on macOS and `make mutation` must work on a developer Mac. (mutmut already defaults `use_setproctitle=False` on Darwin, so no extra handling there.)

## Risks / Trade-offs

- **Resolving 154 survivors is the bulk of this change**, and it is test-authoring work inside what is otherwise CI plumbing. 91 are in `context.py`. → Ordered by risk: `context.py` first, since it implements the atomic-commit invariant. Exclusions are limited to genuinely equivalent survivors with a written reason; they are not a burn-down list for test gaps.
- **Zero-survivor pressure can produce assertion-shaped noise** — tests written to kill a mutant rather than to express a behaviour. → Every new test must trace to a scenario in the owning capability's spec, per `project.md`. A mutant no scenario justifies killing is a spec gap or an equivalent mutant; both have defined outcomes (amend the spec first, or file an exclusion with a reason).
- **The zero-survivor bar covers only about half of `core/`.** `dofn.py` and `transform.py` (452 mutants) are outside the mutation surface entirely. A reader could take "mutation gate green on core/" to mean much more than it does. → The ratchet bounds it, and `docs/ci.md` must name the excluded modules explicitly (task 5.5). Closing it for real is its own change.
- **A ~8 min required check on a 4-vCPU runner is a real tax on `core/` PRs.** → Accepted as the cost of a gate that works; `only_mutate` already cut it ~9×. If it becomes painful the measured escape hatches are a larger runner or revisiting D3 — but only with fresh timings, not on a hunch.
- **A failing nightly is only useful if someone reads it.** → Out of scope here, but noted: nightly has no notification path, so a cross-module regression could sit for days.
- **The ratchet baseline can be edited upward to silence a failure.** → It is a committed file, so any increase is diff-visible and reviewable — the same trust model as `coverage_ratchet.py`.

## Migration Plan

1. **(Applied)** Config scoping — `only_mutate` + rewritten comment block. No gating yet, so nothing breaks.
2. Land `mutation_gate.py` with its own unit tests, plus `mutation-baseline.toml` and an empty `mutation-exclusions.toml`. Still not wired to CI.
3. Kill the 154 `core/` survivors with scenario-derived tests, `context.py` first; file justified equivalents with reasons.
4. Wire `Makefile` and both workflows; update `docs/ci.md`.
5. Confirm the gate fails when it should: deliberately weaken one assertion, observe a red check, revert. A gate never observed failing is not known to work.

Rollback: revert the `Makefile`/workflow wiring (step 4). The config scoping, the gate script, and the new tests are independently valuable and need not be reverted.

## Open Questions

- Does closing the 453-mutant `dofn.py`/`transform.py` gap warrant its own change? It requires either an upstream mutmut fix for `os.wait()` child reaping or restructuring those tests so the logic is reachable without DirectRunner. It is the difference between "half of `core/` is mutation-tested" and all of it, and `dofn.py` is the stateful runtime.
