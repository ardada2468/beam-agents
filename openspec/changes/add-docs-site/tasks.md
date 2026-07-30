## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/examples/test_hello_world.py`: drives `examples.hello_world.build` under `TestPipeline` and asserts exactly one terminal output carrying the FakeLLM-scripted response with `.intents` and `.errors` empty. Derived from "One event in, one output out". Fails first with `ModuleNotFoundError: examples`.
- [ ] 1.2 `tests/examples/test_fraud_triage.py`: drives `examples.fraud_triage.build` under a streaming `TestPipeline` with the module's scripted `TestStream` — one test asserting the approved account's freeze output and its single approval intent ("Approved account resumes to a freeze decision"), one asserting the unanswered account's deterministic deny fallback after the scripted processing-time advance with no freeze output for that key ("Unanswered account fails closed at the deadline").
- [ ] 1.3 `tests/examples/test_iot_reaction.py`: drives `examples.iot_reaction.build` — one test asserting zero FakeLLM calls and per-reading memory growth while readings stay below threshold ("Quiet readings accumulate memory without model calls"), one asserting exactly one model call and the documented reaction output on the breaching device's key ("A threshold breach triggers exactly one reaction").
- [ ] 1.4 `tests/examples/test_docs_snippets.py`: asserts every `docs/examples/*.md` page contains a `--8<--` snippet directive naming its `examples/<name>.py` module and that each named path exists ("A moved example file cannot publish silently", offline half), and asserts no module under `examples/` imports from `tests` (AST-level import scan — "An example importing test helpers fails the unit lane"). Fails first because `docs/examples/` does not exist.
- [ ] 1.5 Confirm each new test fails for the right reason (missing module/page, not a collection or import error in existing code), and that `tests/examples/test_failure_streak_alarm.py` still passes untouched.

## 2. Example modules

- [ ] 2.1 `examples/__init__.py` (empty package marker) and `examples/hello_world.py`: module-level scripted `FakeLLM` factory (`match_any()` + `respond_with`), agent returning `Complete`, `build(pipeline)` wiring `beam.Create` → `WithKeys(entity_key).with_output_types(tuple[bytes, AgentEnvelope])` → `RunAgent`, and a `main()` printing `.output` under `if __name__ == "__main__"`.
- [ ] 2.2 `examples/fraud_triage.py`: transaction envelopes for two accounts on a `TestStream`; agent triages via a payload-matched FakeLLM rule, calls `ctx.request_approval(...)`, returns `Suspend(timeout_ms=...)`; approval branch re-injects account A's decision on the same key using `intent_id_for(entity_key, seq, step_index)` with a comment stating production approvals arrive from the effector carrying this id; processing-time advance elapses account B's deadline onto the default deny route.
- [ ] 2.3 `examples/iot_reaction.py`: per-device `TestStream` readings; agent appends to `ctx.memory.append(...)` with a bounded `max_items`, reads `ctx.memory.ring(...)`, completes with no model call below threshold, and calls the model for the reaction decision on a breach.
- [ ] 2.4 Verify each module runs standalone: `uv run python -m examples.hello_world` (and the other two) exits zero offline with no docker and no credentials; confirm no example imports anything under `tests/`.
- [ ] 2.5 Hold `examples/` to repo standard: add `"examples"` to `[tool.mypy].files` and `[tool.ruff].src` in `pyproject.toml`, plus `examples.*` (and the new `tests.examples.*` modules) per-module mypy overrides mirroring the existing Beam-untyped-API relaxations; confirm the wheel still packages only `src/beam_agents`.

## 3. Site toolchain

- [ ] 3.1 Fill the `docs` dependency group: `mkdocs-material>=9,<10`; regenerate `uv.lock`; confirm the `ci` unit lane's `uv sync --locked --group lint --group typecheck --group test` resolves unchanged.
- [ ] 3.2 `mkdocs.yml` at the repo root: site metadata, Material theme, default `docs_dir`, nav covering `index.md`, the five operator pages, and the three example pages; `pymdownx.snippets` with repo-root `base_path` and `check_paths: true`; `pymdownx.superfences` for the existing pages' fenced blocks.
- [ ] 3.3 `docs/index.md` landing page: what the runtime is and is not (runtime-not-framework), the dataflow shape, install-from-source instructions matching the README (no PyPI claims), and each guarantee linked to its gate (effectively-once e2e gate, adapter conformance matrix, retry determinism, mutation/coverage).
- [ ] 3.4 Rewrite the two out-of-tree relative links in `docs/ci.md` (lines 4 and 14) as repository URLs; audit the other four pages for out-of-tree links (none expected).
- [ ] 3.5 `Makefile`: `docs` target (`uv run mkdocs build --strict`) and `docs-serve` (`uv run mkdocs serve`), both with `## help` annotations; `make docs` passes clean.

## 4. Example docs pages

- [ ] 4.1 `docs/examples/hello-world.md`: what the fast path is, the included source via `--8<-- "examples/hello_world.py"`, how to run it, and what the output means.
- [ ] 4.2 `docs/examples/fraud-triage.md`: the suspend/approve/timeout narrative split into "the agent" and "the harness"; the included source; an admonition that `intent_id_for` is shown to demystify deterministic intent IDs and that production approvals arrive from the effector (link `docs/effector.md`); the fail-closed behavior linked to the HITL semantics gate.
- [ ] 4.3 `docs/examples/iot-reaction.md`: keyed rolling memory, the no-model-call-on-quiet-readings property and the test that pins it, the included source, memory caps and the TTL note pointing at project.md's state bounds.
- [ ] 4.4 `README.md`: add a link to the published site near the top; `docs/ci.md`: add the `docs` workflow row to the workflow table.

## 5. CI and deployment

- [ ] 5.1 `.github/workflows/docs.yml`: on pull request and push-to-main — checkout, `uv sync --locked --group docs`, `make docs`; on push-to-main only — `actions/upload-pages-artifact` + `actions/deploy-pages` with `pages: write` / `id-token: write` permissions and the `github-pages` environment.
- [ ] 5.2 Verify the PR leg fails on an intentionally broken link and an intentionally wrong snippet path (then revert), matching both docs-site failure scenarios.
- [ ] 5.3 Record the repository-settings prerequisite (Pages source = GitHub Actions) in `docs/ci.md`'s docs-workflow note; leave the check not-required per design Open Question 2.

## 6. Gates

- [ ] 6.1 `make lint` clean (ruff now covering `examples/`).
- [ ] 6.2 `make type` clean (`mypy --strict` including `examples/` with only the established per-module relaxations).
- [ ] 6.3 `make test-unit` passes offline — the three example test modules and the snippet-integrity test all execute, none skipped.
- [ ] 6.4 Coverage ratchet at or above baseline; raise `coverage-baseline.toml` if the example pipelines' exercising of `src/` improves it.
- [ ] 6.5 `make docs` clean under `--strict`. (No mutation-gate run: nothing under `core/` is touched.)
- [ ] 6.6 `uv run pre-commit run --all-files` clean.
- [ ] 6.7 `openspec validate add-docs-site --strict` passes.
