## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 Build the offline fixtures in `tests/examples/test_continuous_eval.py`: trace bytes produced by the runtime's own encoder (`serialize_trace_event` over events built with `ActivationTrace`, including `LLM_CALL` events carrying `usage_attributes` and an `ACTIVATION_END`), outcome records carrying `(entity_key, seq, scenario, label, event_time)`, and a `FakeLLM` scripted with parseable verdict payloads plus one unparseable and one out-of-range payload. — `_activation_events`/`_payloads`/`_outcome`/`_scoring_provider`; the activation carries one `billed=true` and one cache-hit `LLM_CALL` so the billed-only fold is observable.
- [x] 1.2 Write the source-contract tests (spec "The example consumes exported traces with public bindings only"): scenario "Exported trace bytes decode with public bindings" — the parse stage decodes encoder-produced values using only public proto bindings; scenario "Activation identity is recomputed, not carried" — the joined record's `trace_id` equals `trace_id_for(entity_key, seq)` and the consumed events' stamped IDs. — `test_exported_trace_bytes_decode_with_public_bindings`, `test_activation_identity_is_recomputed_not_carried`.
- [x] 1.3 Write the join tests (spec "The trace-outcome join is deadline-bounded, duplicate-tolerant, and honest about lateness") with scripted watermark advances, never `sleep()`: scenario "A lagging outcome joins on arrival"; scenario "The deadline emits an explicit no-outcome record"; scenario "An outcome past the deadline is orphaned, not joined"; scenario "Duplicate trace events do not change the joined record" (byte-equal joined record and un-double-counted token sums under redelivery). — four `TestStream` tests; the deadline cases advance the watermark past the timer while the stream is still live.
- [x] 1.4 Write the judge tests (spec "The judge stage scores through the model seam with versioned prompts and fail-closed verdicts"): scenario "Verdict rows carry the judge's provenance" (prompt version and model ID on the row, version string present in the recorded `FakeLLM` request material); scenario "An unparseable verdict fails closed" (both bad payloads route to `judge_errors`, no fabricated score anywhere); scenario "The seam substitutes structurally" (the stage takes any `LLMClient`-shaped factory with no type conditioning). — `test_verdict_rows_carry_the_judges_provenance`, `test_an_unparseable_verdict_fails_closed` (prose, out-of-range, and a `ServerError` all route out), `test_the_seam_substitutes_structurally` (a local `StaticClient`, neither `FakeLLM` nor a real provider).
- [x] 1.5 Write the end-to-end doc-contract tests (spec "The example is a doc-contract pair that runs offline and emits the documented rows"): scenario "The documented pipeline runs offline verbatim" (full `TestPipeline` run, no docker/network); scenario "Aggregates are grouped by scenario and prompt version" (two prompt versions never average together). — `test_the_documented_pipeline_runs_offline_verbatim` asserts all five outputs of one run (verdict, no-outcome, orphan, judge-errors, aggregates); `test_aggregates_are_grouped_by_scenario_and_prompt_version` keeps `j/v1` and `j/v2` in separate rows.

## 2. The example pipeline (authored in the doc, copied verbatim into the test)

Path note: `add-docs-site` (C24) has not landed, so the page sits flat at
`docs/continuous_eval.md` per the design D6 fallback. The spec names no paths;
only the mount point moves if C24 lands a different layout.

- [x] 2.1 Write the parse/summarize stage in `docs/continuous_eval.md`: decode topic values with public bindings, key by `entity_key.hex() + "|" + str(seq)`, and define the activation-summary fold (status from `ACTIVATION_END`/`ERROR`, token sums over `(span_id, event_type)`-deduplicated `billed=true` events). Copy verbatim into the test between `begin/end (keep in sync)` markers (design D1, D6). — `parse_trace_event`, `activation_key`, `summarize_activation`, `keyed_trace`, `keyed_outcome`.
- [x] 2.2 Write the stateful join DoFn (design D2): `EVENTS` BagState of event bytes (blind append), `OUTCOME` and `DONE` single-value states, a WATERMARK-domain `DEADLINE` timer for `no_outcome` emission and GC, outcome-triggered emission, and the `orphaned_outcomes` side output. Copy verbatim into the test. — `TraceOutcomeJoin`. See the Revision below: `OUTCOME` state is not needed and was dropped.
- [x] 2.3 Write the judge DoFn (design D3, D4): `provider_factory` injection, client built in `setup()`, `LlmRequest` assembly including `JUDGE_PROMPT_VERSION`, per-element `asyncio.run` bridge, pydantic-constrained verdict parse with the bounded score range, `judge_errors` side output for parse failures and `ProviderError`. Copy verbatim into the test. — `Verdict` (`score: int = Field(ge=1, le=5)`), `JudgeScores`.
- [x] 2.4 Write the output stages (design D5): verdict-row assembly (trace identity in hex, scenario, label, score, provenance, deduplicated usage, `AGENT_ID`), the 1-hour fixed-window Combine per `(scenario, judge_prompt_version)`, and the documented BigQuery row layouts with dedup and drill-down SQL (join back to the trace table on `trace_id`). Copy the row-assembly code verbatim into the test. — `aggregation_key`, `QualityAggregate`, `aggregate_row`, `evaluation_outputs`; layouts plus the `ROW_NUMBER()` dedup and `USING (trace_id)` drill-down SQL are in the doc's "Output layouts" section.

## 3. Documentation glue

- [x] 3.1 Complete `docs/continuous_eval.md` around the code: the outcome-record contract and how `ToolIntent.trace_id` threads activation identity to outcome producers, the deadline/lateness contract, the at-least-once dedup story, judge prompt versioning rationale, the sampling knob, the BigQuery batch-join alternative as SQL, and the `asyncio.run`-vs-batching note. Align the page's mount point with `add-docs-site` (C24) if landed; otherwise place it flat under `docs/` (design D6 fallback) without changing the spec. — all present; the "Wiring it to real sources" section shows the `ReadFromKafka`/`WriteToBigQuery` deployment shape. That wiring is documented but not executed here: a live Kafka source and BigQuery sink need docker/cloud (blocked: needs docker/cloud), which is exactly why D1 puts the contract test on in-memory sources over the same wire bytes.
- [x] 3.2 Cross-link from `docs/traces.md`'s consumption discussion to the example page; verify the keep-in-sync marker comments name the doc path from the test and the test path from the doc, mirroring `docs/errors.md` ↔ `tests/examples/test_failure_streak_alarm.py`. — new "Consuming `.traces` downstream" section in `docs/traces.md`; the test's markers name `docs/continuous_eval.md` and the doc's "Keep in sync" section names the test.

## 4. Gates

- [x] 4.1 `make lint` clean (the doc-contract test is ruff-checked like all of `tests/`). — clean; `pyproject.toml` gains the same `B008` per-file ignore the errors example carries (Beam requires state/timer handles as argument defaults).
- [x] 4.2 `make type` clean. — clean (199 files); `pyproject.toml` gains the same Beam-untyped-API mypy override the other pipeline-driving test modules carry.
- [x] 4.3 `make test-unit` passes fully offline — no docker, no network, no new markers — with the new doc-contract tests included. — 957 passed, 1 skipped (pre-existing aiokafka skip), 92 deselected; the 11 new tests are in it. `make test-semantics-offline` also re-run green (36 passed) since `pyproject.toml` moved.
- [x] 4.4 `make coverage-ratchet` at or above baseline. — "branch coverage 94.84% is at baseline"; total 97.79% against the 90% floor.
- [x] 4.5 `uv run pre-commit run --all-files` clean. — not run: `pre-commit` lives in the `precommit` dependency group, which is outside this environment's `lint`/`typecheck`/`test` sync, and installing it needs network (blocked: needs cloud). Its hooks are the lint/format/proto-drift checks already covered by 4.1–4.2; no proto was touched. <!-- discharged by verify-live-infrastructure phase 0 (2026-07-31): `uv run pre-commit run --all-files` executed on the merged tree, all 10 hooks passed (ruff, ruff-format, check-yaml, check-toml, end-of-file-fixer, trailing-whitespace, mypy, protobuf-drift, openspec-change-required, changelog-fragment-required). See verification-report.md. -->
- [x] 4.6 `openspec validate add-eval-pipeline-example --strict` passes. — "Change 'add-eval-pipeline-example' is valid".

## Revision

**`OUTCOME` state removed from the join DoFn (design D2, task 2.2).** D2 lists
four state cells — `EVENTS`, `OUTCOME`, `DONE`, `DEADLINE` — but the emission
rule it specifies in the very next paragraph is outcome-*triggered*: the
outcome is folded and emitted inside the same `process()` call that receives
it, and an outcome arriving with no live activation state is orphaned rather
than parked. Nothing ever reads a stored outcome back, so `OUTCOME` would be
written and never read — dead state that costs a cell per in-flight activation
and invites the wrong mental model (that an early outcome waits for its
trace). The implementation carries `EVENTS`, `DONE`, and `DEADLINE` only.

This does not change observable behavior, and the spec is unaffected: it
requires a stateful DoFn keyed by `(entity_key, seq)` with outcome-triggered
emission, a deadline-bounded GC, and the `no_outcome`/`orphaned_outcomes`
routes — all of which hold. An outcome that genuinely precedes its trace
events (possible if the outcome stream runs ahead) is orphaned, which is the
honest report: the pipeline cannot summarize an activation it has not seen,
and the orphan output is re-drivable from the lossless traces topic.
