## 1. Tests (written first, must fail for the right reason)

This section is deliberately thin: a release gate adds no runtime behavior, so it derives no new test code. Its spec scenarios are enforced by tests that *already exist and are owned by the depended-on changes* — the gate's job is to require them green, not to duplicate them. The one failure mode this change can itself introduce (a checklist that drifts from the spec) is checked by inspection in 1.2.

- [ ] 1.1 Map each spec scenario to its enforcing signal and record the mapping in the PR description: *All gate conditions hold* / *A hardening change is unarchived* → archive-state inspection (§2.1); *Spark inside its window* / *Spark decision unrecorded* → decision-record inspection (§2.5); *post-1.0 public-symbol removal* → C45's API-freeze snapshot test; *post-1.0 state-schema change* → C46's golden-blob and nightly `--update` compat tests
- [ ] 1.2 Confirm the §2 checklist below matches the gate requirement in `specs/release-1-0/spec.md` condition-for-condition, with nothing added and nothing dropped

## 2. 1.0 release-gate checklist (all boxes required before §3 may start)

- [ ] 2.1 `add-effector-security`, `add-1-0-api-freeze`, `add-state-guarantees`, and `promote-spark-runner` are all present under `openspec/changes/archive/` (implemented and archived, not merely merged)
- [ ] 2.2 The API-freeze snapshot test from `add-1-0-api-freeze` is green on `main`
- [ ] 2.3 The state guarantees from `add-state-guarantees` are documented and its nightly `--update` compat test is green on the latest scheduled run; if a state-touching change merged after that run, re-trigger the nightly and wait for green (design risk: stale signal)
- [ ] 2.4 Effector intent signing from `add-effector-security` is shipped with rollout complete: the effector *enforces* signature verification (design D1(d))
- [ ] 2.5 The Spark decision from `promote-spark-runner` is recorded either way: promoted to supported, or explicitly deferred with the roadmap noting why (design D2); note the outcome for the changelog
- [ ] 2.6 Standing release blockers from `openspec/project.md` re-verified: semantics gates green and unskipped; no open latency-budget benchmark regression

## 3. Release execution (via the `add-0-1-0-release` process, unchanged)

- [ ] 3.1 Bump `version` in [pyproject.toml:3](../../../pyproject.toml:3) to `1.0.0`
- [ ] 3.2 Write the 1.0.0 changelog entry per the C25 changelog process: headline the stability promise (deprecation policy now governs the public surface; state-migration guarantees now govern wire/state), state Spark's recorded status from 2.5, and list any in-flight deprecations with removal horizons (design open question)
- [ ] 3.3 Tag `v1.0.0` and publish via the release workflow established by `add-0-1-0-release`, with no workflow modifications

## 4. Post-release verification

- [ ] 4.1 `pip install beam-agents==1.0.0` succeeds in a clean 3.11 and 3.12 environment; `import beam_agents` works, and each extra (`effector`, `langgraph`, `otlp`) resolves
- [ ] 4.2 The tag, the published version, and the changelog entry agree on `1.0.0`
- [ ] 4.3 Archive this change; from archival, the `release-1-0` regime requirement (design D3) is normative for all future proposals

## 5. Gates

- [ ] 5.1 `make lint`
- [ ] 5.2 `make type`
- [ ] 5.3 `make test-unit`
- [ ] 5.4 `uv run pre-commit run --all-files`
- [ ] 5.5 `openspec validate add-1-0-0-release --strict`
