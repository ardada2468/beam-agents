## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/dataflow/test_update_compat_harness.py` (offline, default tier — the harness pieces that can lie): the version resolver returns the latest PyPI release and falls back to the loudly-labelled bootstrap self-update mode when none exists (PyPI responses faked, never live in unit tests); the launcher's event/approval builders round-trip through `AgentEnvelope` bytes; the failure classifier maps a refused replacement job (new job `FAILED`, old job healthy) to compatibility-failure, a phase-2 assertion timeout to state-loss, and quota/provisioning errors to infrastructure; teardown invokes cancel-and-delete for every provisioned resource on the success, failure, and timeout paths; the sweeper selects only labelled resources older than the age threshold. Derived from "A refused compatibility check is reported as the defect it is", "A failing run leaves nothing behind", "A crashed run is bounded to one night", and "Before any PyPI release exists, the gate runs a labelled self-update leg".
- [ ] 1.2 `tests/dataflow/test_update_compat.py` skeleton, marked `dataflow` + `slow`: skips visibly when `GCP_PROJECT_ID`/`GCP_REGION`/`GCP_DATAFLOW_TEMP_BUCKET` are unset (the smoke tier's credential-skip pattern); its assertion structure names the spec scenarios — suspension survives, memory survives, fresh key completes, classification on refusal. Must fail for the right reason before the harness exists (missing harness module, not a silent pass).
- [ ] 1.3 Doc-contract test `tests/core/test_state_compat_doc.py` (offline): `docs/state-compat.md` exists and contains a row for every change class the spec enumerates (additive field, enum value, new state spec, remove/renumber/retype, coder encoding, transform/state-spec-id rename, coder-type change, versioned migration) — a cheap tripwire so the table cannot silently lose a row. Derived from "The table classifies a safe change and a breaking change differently" and "A graph-shape change is classified even though no bytes change".

## 2. The guarantees document

- [ ] 2.1 Write `docs/state-compat.md`: the adjacent-release promise (design D1), the explicit non-promises (skip-level best-effort, downgrade unsupported, byte-identity not promised, Flink/cross-runner out of scope), and the wire-format statement (raw `SerializeToString(deterministic=True)`, no framing).
- [ ] 2.2 Add the compatibility table (design D2) with the readable / `--update`-safe / required-action columns for all eight change classes, including the graph-shape rows (transform names, state spec ids) and the `--transform_name_mapping` guidance.
- [ ] 2.3 Cross-review against `add-state-schema-migration`'s spec so the versioned-change rows cite exactly the migration obligations C32 defines (version bump, registered migration, retained golden fixture) — the two documents must not disagree.
- [ ] 2.4 Add the release procedure section: latest nightly `dataflow` leg green before tagging; a red gate is resolved by fix or migration release, never by weakening the gate.

## 3. Harness — `tests/dataflow/_update/` (test-only; no `src/` changes)

- [ ] 3.1 `versions.py`: PyPI latest-release resolution, `pip download --no-deps` of the previous wheel, venv provisioning with the matching interpreter, `uv build` of the head wheel, and the bootstrap fallback (design D7); both legs log `pip freeze` and the resolved version strings.
- [ ] 3.2 `pipeline.py`: the single self-contained launcher run by both interpreters (design D3) — public API + packaged `FakeLLM` only, module-level agent and matchers, `--save_main_session`, `--extra_packages` for its leg's wheel, phases `launch` (fresh job) and `update` (`--update` + same `--job_name`); scripted turns produce the suspension (`APPROVAL` intent, far-future deadline), the memory write/echo, and the canary/fresh-key completions (design D4).
- [ ] 3.3 `resources.py`: per-run-id Pub/Sub topic/subscription provisioning, job labelling, force-cancel teardown guaranteed on every exit, and the label+age sweeper (design D6).
- [ ] 3.4 `poll.py`: deadline-driven pollers for Dataflow job state (launch running, update takeover, refusal detection) and output-subscription reads; the three-way failure classifier (design D5). No `sleep`-based correctness anywhere.

## 4. The gate — `tests/dataflow/test_update_compat.py`

- [ ] 4.1 Wire the run: sweep, provision, resolve versions, launch the previous-release job, drive phase 1 (suspend `K-suspend`, write `K-memory`, confirm `K-canary`), update to head, drive phase 2, tear down — all within the ≤ 35-minute budget with per-phase deadlines.
- [ ] 4.2 Assert the suspension resumed: post-update approval injection yields the terminal output derivable only from the pre-update continuation ("A suspension survives the update").
- [ ] 4.3 Assert memory survived: the post-update echo activation returns the pre-update marker ("Working memory survives the update"), and a fresh key completes normally on the updated job.
- [ ] 4.4 Assert classification: a refused replacement is reported as a compatibility failure naming both versions and the service reason, never as infrastructure noise.
- [ ] 4.5 Verify the gate carries no `xfail`, `skipif`-on-flake, or retry tolerance, and that the bootstrap leg's report is prominently labelled.

## 5. CI wiring and docs

- [ ] 5.1 `.github/workflows/nightly.yml`: raise the `dataflow` job's `timeout-minutes` 30 → 50 and pass `GCP_REGION` and `GCP_DATAFLOW_TEMP_BUCKET` repository variables through to the test env, keeping the `vars.GCP_PROJECT_ID` gate and skip-notice job unchanged.
- [ ] 5.2 `Makefile`: drop `test-dataflow`'s exit-5 tolerance (design D8) with a comment matching the `test-semantics` no-exit-5 stance; confirm the gate skips (exit 0) rather than deselects without GCP env.
- [ ] 5.3 `docs/ci.md`: update the nightly row and add a triage note distinguishing the three failure classifications and where to find the version strings and job ids in a red run.

## 6. Gates

- [ ] 6.1 `make lint` and `make type` clean (`mypy --strict` on the typed harness modules; ruff ASYNC rules where the pollers are async).
- [ ] 6.2 `make test-unit` passes offline with no docker and no GCP: harness unit tests run, the gate module skips visibly.
- [ ] 6.3 `make coverage-ratchet` at or above baseline (no `src/` change; the ratchet must not move down).
- [ ] 6.4 `uv run pre-commit run --all-files` clean.
- [ ] 6.5 `openspec validate add-state-guarantees --strict` passes.
