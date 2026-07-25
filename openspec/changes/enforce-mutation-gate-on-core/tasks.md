## 1. Scope mutation to `core/` and measure the real baseline

- [x] 1.1 In `pyproject.toml` `[tool.mutmut]`, add `only_mutate = ["src/beam_agents/core/*"]`, keeping `source_paths` at its default `src/` and keeping `do_not_mutate = ["*/_protos/*"]` — verified: **8 files mutated, 21 ignored** (was 27/2)
- [x] 1.2 ~~Add `mutate_only_covered_lines = true`~~ — **NOT VIABLE.** It makes mutmut run pytest in-process under `coverage.collect()`; apache-beam's numpy import then fails with `ImportError: cannot load module more than once per process`. Upstream mutmut × numpy incompatibility, no config workaround. Superseded by the `no tests` ratchet (revised D2).
- [x] 1.3 Rewrite the `[tool.mutmut]` comment block: scope is `core/` only, why `source_paths` stays `src/`, why `mutate_only_covered_lines` is absent, and the `os.wait()`/DirectRunner justification for the `--ignore`/`-k` deselections
- [x] 1.4 Authoritative baseline recorded: **911 mutants** (was 2,219, −59%), 8 files, **5.39 mutations/second → ~169 s (2.8 min) wall at `--max-children 10`**. Statuses: **263 killed, 195 survived, 453 no tests, 0 timeout, 0 suspicious**. Kill rate on mutants that ran tests: **57%**
- [x] 1.5 195 survivors recorded by module: **context 120, loop 43, bridge 30, coders 1, agent 1**
- [x] 1.6 Verified: **0** result entries outside `beam_agents.core.*`
- [x] 1.7 Un-mutation-tested surface recorded — the 453 `no tests` mutants: **dofn 263, transform 189, context 1**, i.e. essentially all of `dofn.py` and `transform.py`, matching the deselected `test_dofn_pipeline`/`test_dofn_streaming`/`test_transform` suites
- [x] 1.8 **Escalation raised: 195 survivors > ~150 threshold.** Reviewer chose to resolve all 195 in this change; diff-scoping dropped (revised D3); `no tests` ratcheted (revised D2)

## 2. Gate script

- [x] 2.1 Create `scripts/mutation_gate.py` (`main() -> int`, matching `scripts/coverage_ratchet.py` conventions) that reads and validates `exit_code_by_key` from `mutants/src/beam_agents/core/*.py.meta` JSON and maps values through mutmut's authoritative `status_by_exit_code`
- [x] 2.2 Fail on `survived`, `timeout`, `suspicious`, `segfault`, `not checked`, `check was interrupted by user`; pass on `killed`, `skipped`, `caught by type check`; report `timeout` distinctly from `survived`
- [x] 2.3 Print each failing mutant's name and source location, grouped by module, with a summary line of per-status counts
- [x] 2.4 Exit non-zero with an actionable message if `mutants/` is missing or contains no core `.meta` files — an absent run must never read as a pass
- [x] 2.5 Create `mutation-baseline.toml` recording per-module `no tests` counts (`dofn.py`: 263, `transform.py`: 189, `context.py`: 0 after lowering the initial baseline of 1); compare modules independently, treat missing entries as zero, and report module-specific baselines that can be lowered
- [x] 2.6 Create `mutation-exclusions.toml` with a documented schema (mutant name → mandatory `reason`), initially empty
- [x] 2.7 Exempt an excluded mutant only when it is currently `survived`; exit non-zero when an exclusion is missing, no longer survives, or attempts to suppress an indeterminate status
- [x] 2.8 Add `tests/core/test_mutation_gate.py` covering: each status classification, survivor exclusion honored, missing/killed/indeterminate exclusion rejected, undeclared survivor fails, per-module `no tests` above/at/below baseline, cross-module offset rejected, new-module implicit zero, malformed metadata, and missing `mutants/`
- [x] 2.9 Confirm `make lint` passes on the new script and its tests

## 3. Resolve the 195 `core/` survivors

- [x] 3.1 `context.py` (120 survivors) — highest risk first, since it implements correctness invariant #1 (staged effects, atomic commit). Map each survivor to its owning spec scenario before writing tests
- [x] 3.2 `loop.py` (43 survivors)
- [x] 3.3 `bridge.py` (30 survivors)
- [x] 3.4 `coders.py` (1) and `agent.py` (1)
- [x] 3.5 For any survivor no spec scenario justifies killing: either raise the spec gap and amend the owning spec first, or record an equivalent-mutant entry in `mutation-exclusions.toml` with its reason. Never weaken or deselect an existing test
- [x] 3.6 Re-run the sweep and confirm `scripts/mutation_gate.py` exits 0 with no undeclared survivor
- [x] 3.7 Confirm `make test-unit` passes (368 passed) and `make coverage-ratchet` accepts 95.07% coverage (no baseline exists on `origin/main` yet)

## 4. Makefile and CI wiring

- [x] 4.1 Update the `mutation` target: `mutmut run --max-children <cpu_count>` followed by `python scripts/mutation_gate.py`, so a survivor fails the target
- [x] 4.2 Compute the child count via `python -c 'import os;print(os.cpu_count())'`, not `nproc` (absent on macOS)
- [x] 4.3 Keep the `## ` help text accurate for the changed `mutation` target; confirm `make help` still lists it
- [x] 4.4 In `.github/workflows/quality.yml`, broaden the `core-changed` filter from `src/beam_agents/core` to `src/beam_agents/core` **or** `tests/core` so weakening a core test cannot skip the gate
- [x] 4.5 In `.github/workflows/nightly.yml`, add a `mutation` job running `make mutation` with no GCP auth and no `vars.GCP_PROJECT_ID` / API-key condition

## 5. Verification and docs

- [x] 5.1 Run `make mutation` end-to-end and confirm it exits 0 on a clean tree
- [x] 5.2 Deliberately weaken one assertion, confirm `make mutation` exits non-zero and names the survivor, then revert — a gate never observed failing is not known to work
- [x] 5.3 Confirm the gate fails when `mutants/` is absent (guards against a skipped-run false pass)
- [x] 5.4 Record final `make mutation` wall time (**6.95 s warm-cache**) and compare against the 2,219-mutant / >25 min starting point
- [x] 5.5 Update the `quality` and `nightly` rows of the `docs/ci.md` workflow table, and add a subsection naming `dofn.py` and `transform.py` as outside the mutation-tested surface (452 of 911 mutants) with the `os.wait()`/DirectRunner reason, so "mutation gate green" is not read as covering all of `core/`
- [x] 5.6 Document the exclusion and baseline files in `CONTRIBUTING.md`: what may go in them, that a `reason` is mandatory, and that weakening tests is not an alternative
- [x] 5.7 Run `openspec validate enforce-mutation-gate-on-core --strict` and confirm `make lint type test-unit` all pass
