## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Build the offline fixtures in `tests/examples/test_continuous_eval.py`: trace bytes produced by the runtime's own encoder (`serialize_trace_event` over events built with `ActivationTrace`, including `LLM_CALL` events carrying `usage_attributes` and an `ACTIVATION_END`), outcome records carrying `(entity_key, seq, scenario, label, event_time)`, and a `FakeLLM` scripted with parseable verdict payloads plus one unparseable and one out-of-range payload.
- [ ] 1.2 Write the source-contract tests (spec "The example consumes exported traces with public bindings only"): scenario "Exported trace bytes decode with public bindings" — the parse stage decodes encoder-produced values using only public proto bindings; scenario "Activation identity is recomputed, not carried" — the joined record's `trace_id` equals `trace_id_for(entity_key, seq)` and the consumed events' stamped IDs.
- [ ] 1.3 Write the join tests (spec "The trace-outcome join is deadline-bounded, duplicate-tolerant, and honest about lateness") with scripted watermark advances, never `sleep()`: scenario "A lagging outcome joins on arrival"; scenario "The deadline emits an explicit no-outcome record"; scenario "An outcome past the deadline is orphaned, not joined"; scenario "Duplicate trace events do not change the joined record" (byte-equal joined record and un-double-counted token sums under redelivery).
- [ ] 1.4 Write the judge tests (spec "The judge stage scores through the model seam with versioned prompts and fail-closed verdicts"): scenario "Verdict rows carry the judge's provenance" (prompt version and model ID on the row, version string present in the recorded `FakeLLM` request material); scenario "An unparseable verdict fails closed" (both bad payloads route to `judge_errors`, no fabricated score anywhere); scenario "The seam substitutes structurally" (the stage takes any `LLMClient`-shaped factory with no type conditioning).
- [ ] 1.5 Write the end-to-end doc-contract tests (spec "The example is a doc-contract pair that runs offline and emits the documented rows"): scenario "The documented pipeline runs offline verbatim" (full `TestPipeline` run, no docker/network); scenario "Aggregates are grouped by scenario and prompt version" (two prompt versions never average together).

## 2. The example pipeline (authored in the doc, copied verbatim into the test)

- [ ] 2.1 Write the parse/summarize stage in `docs/examples/continuous_eval.md`: decode topic values with public bindings, key by `entity_key.hex() + "|" + str(seq)`, and define the activation-summary fold (status from `ACTIVATION_END`/`ERROR`, token sums over `(span_id, event_type)`-deduplicated `billed=true` events). Copy verbatim into the test between `begin/end (keep in sync)` markers (design D1, D6).
- [ ] 2.2 Write the stateful join DoFn (design D2): `EVENTS` BagState of event bytes (blind append), `OUTCOME` and `DONE` single-value states, a WATERMARK-domain `DEADLINE` timer for `no_outcome` emission and GC, outcome-triggered emission, and the `orphaned_outcomes` side output. Copy verbatim into the test.
- [ ] 2.3 Write the judge DoFn (design D3, D4): `provider_factory` injection, client built in `setup()`, `LlmRequest` assembly including `JUDGE_PROMPT_VERSION`, per-element `asyncio.run` bridge, pydantic-constrained verdict parse with the bounded score range, `judge_errors` side output for parse failures and `ProviderError`. Copy verbatim into the test.
- [ ] 2.4 Write the output stages (design D5): verdict-row assembly (trace identity in hex, scenario, label, score, provenance, deduplicated usage, `AGENT_ID`), the 1-hour fixed-window Combine per `(scenario, judge_prompt_version)`, and the documented BigQuery row layouts with dedup and drill-down SQL (join back to the trace table on `trace_id`). Copy the row-assembly code verbatim into the test.

## 3. Documentation glue

- [ ] 3.1 Complete `docs/examples/continuous_eval.md` around the code: the outcome-record contract and how `ToolIntent.trace_id` threads activation identity to outcome producers, the deadline/lateness contract, the at-least-once dedup story, judge prompt versioning rationale, the sampling knob, the BigQuery batch-join alternative as SQL, and the `asyncio.run`-vs-batching note. Align the page's mount point with `add-docs-site` (C24) if landed; otherwise place it flat under `docs/` (design D6 fallback) without changing the spec.
- [ ] 3.2 Cross-link from `docs/traces.md`'s consumption discussion to the example page; verify the keep-in-sync marker comments name the doc path from the test and the test path from the doc, mirroring `docs/errors.md` ↔ `tests/examples/test_failure_streak_alarm.py`.

## 4. Gates

- [ ] 4.1 `make lint` clean (the doc-contract test is ruff-checked like all of `tests/`).
- [ ] 4.2 `make type` clean.
- [ ] 4.3 `make test-unit` passes fully offline — no docker, no network, no new markers — with the new doc-contract tests included.
- [ ] 4.4 `make coverage-ratchet` at or above baseline.
- [ ] 4.5 `uv run pre-commit run --all-files` clean.
- [ ] 4.6 `openspec validate add-eval-pipeline-example --strict` passes.
