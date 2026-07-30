## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 This change adds no new test code, deliberately: a release gate verifies existing gates rather than introducing behavior to test. Every spec scenario maps to a checklist step below instead of a test file — "A dependency change is still pending" and "All seven dependency changes are archived" to §2.1; "Conformance matrix is green on the release commit" and "Benchmark gate is green on the release commit" to §2.2–2.3; the version/changelog scenarios to §3.1–3.3. Record this mapping in the PR description so the scenario → verification chain is explicit despite there being no test files.

## 2. Gate verification (all on the intended release commit — blocking, in order)

- [ ] 2.1 Confirm all seven M3 dependency changes are **archived** (directories present under `openspec/changes/archive/`): `add-yaml-provider`, `add-dataflow-flex-template`, `add-replay-cli`, `add-pydantic-ai-adapter`, `add-slack-approval-example`, `add-eval-pipeline-example`, `add-upstream-design-doc`. Any missing entry blocks the release (design D1/D2); record the seven archive paths in the PR description.
- [ ] 2.2 Confirm the adapter conformance matrix is green on the release commit: the DirectRunner leg via the required offline `ci` semantics selection, and the Flink leg via `make test-conformance-flink` in the `integration` workflow — with the matrix meta-test passing, which proves every registered adapter (including Pydantic AI per `add-pydantic-ai-adapter`) × all seven scenarios × both legs is accounted for.
- [ ] 2.3 Confirm the benchmark regression gate on the runtime-overhead latency budget (p50 < 15 ms / p99 < 60 ms per activation, excluding LLM/tool time) is green on the release commit.
- [ ] 2.4 Confirm `ci`, `integration`, and `quality` are all green on the release commit.

## 3. Release mechanics (only after §2 is fully checked)

- [ ] 3.1 Set `pyproject.toml` `version` to `0.5.0`.
- [ ] 3.2 Add the `0.5.0` section to the changelog established by `add-0-1-0-release`: one entry per change archived since the previous release tag, verified by diffing `openspec/changes/archive/` against the previous tag's date (design D4) — the seven M3 changes at minimum.
- [ ] 3.3 Merge the release PR, then tag `v0.5.0` on the merged commit and publish via the release process established by `add-0-1-0-release`, unchanged.
- [ ] 3.4 Verify the published artifact: `pip install beam-agents==0.5.0` into a clean environment succeeds and `importlib.metadata.version("beam-agents")` reports `0.5.0`.

## 4. Gates

- [ ] 4.1 `make lint`
- [ ] 4.2 `make type`
- [ ] 4.3 `make test-unit`
- [ ] 4.4 `uv run pre-commit run --all-files`
- [ ] 4.5 `openspec validate add-0-5-0-release --strict`
