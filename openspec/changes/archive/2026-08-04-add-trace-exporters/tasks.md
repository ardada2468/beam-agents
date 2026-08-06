## 1. Dependency and packaging

- [x] 1.1 Add the `otlp` optional extra (`opentelemetry-proto` with a version floor) to `pyproject.toml`, mirrored into the `test` dependency group (not `integration`, as first planned: the exporter's tests are offline unit tests that run — and are coverage-counted — in the required ci lane, which syncs only `lint`/`typecheck`/`test`), with a comment explaining why the runtime install still works without it.
- [x] 1.2 Add an offline import-boundary test asserting `import beam_agents` and `AgentConfig` construction with an `otlp://` traces sink succeed without `opentelemetry-proto` installed (monkeypatch the import to raise `ModuleNotFoundError`), and that sink *resolution* without it raises an actionable error naming the `otlp` extra.

## 2. TraceEvent → OTLP mapping

- [x] 2.1 Write the mapping tests first (`tests/observability/test_otlp.py`): IDs pass through byte-for-byte at 16/8/8 widths; `start_ms`/`end_ms` become unix nanos; attributes become string `KeyValue`s; span name is the lowercase event-type name; `ERROR` maps to `STATUS_CODE_ERROR` with `beam_agents.reason` present; mapping the same event twice yields byte-identical serialized spans; `service.name` defaults to `beam-agents` and honors an override.
- [x] 2.2 Write the activation-span election tests: `ACTIVATION_START` produces no span, `ACTIVATION_END` produces the activation span carrying status and kind, and each of `LLM_CALL`/`TOOL_CALL`/`INTENT_EMITTED`/`SUSPENDED`/`ERROR` produces exactly one span (spec "The activation span is exported once").
- [x] 2.3 Create `src/beam_agents/observability/otlp.py` with the pure mapping functions (`event_to_span`, batch → `ExportTraceServiceRequest` encoder) behind a lazy `opentelemetry-proto` import; module docstring documents the mapping table and the START-skip rule with its D4 rationale.

## 3. Batched non-blocking export DoFn

- [x] 3.1 Write the DoFn behavior tests first, against an injected `httpx.MockTransport` and a controllable fake clock where needed: `process()` performs no network I/O on the calling thread; a batch reaching `batch_size` is enqueued; a full queue drops the batch and counts its spans; `finish_bundle()` flushes the partial batch, waits at most `flush_deadline_s`, and counts undrained spans as dropped; a 400 response drops without retry; a connection failure retries with backoff only inside the deadline, then drops and counts.
- [x] 3.2 Write the never-fail tests: a bundle processed with a collector refusing every connection commits normally (no exception escapes `process()`/`finish_bundle()`); counters (`spans_exported`, `spans_dropped`, `export_failures`, `batches_sent`) are recorded via Beam `Metrics` only from `finish_bundle()` on the Beam thread — assert with a fake/inspectable metrics sink, mirroring `observability/metrics.py`'s pattern.
- [x] 3.3 Implement `_OtlpExportDoFn` in `observability/otlp.py`: `setup()` builds the `httpx.Client` and one daemon sender thread over a bounded `queue.Queue`; `process()` maps + batches + `put_nowait`; `finish_bundle()` drains within the deadline and records tallied counters; `teardown()` stops the sender via sentinel and closes the client. Plain-int tallies shared with the sender thread; no Beam calls off the Beam thread.
- [x] 3.4 Implement the `WriteTracesToOtlp` PTransform wrapping the DoFn, taking the parsed endpoint and knobs (`batch_size=512`, `flush_deadline_s=5`, `queue_batches=8`, `service_name="beam-agents"`, `tls`).
- [x] 3.5 Add a TestPipeline test running trace events through `WriteTracesToOtlp` against a failing transport, asserting the pipeline completes and drop counters are populated (spec "A dead collector does not fail bundles").

## 4. Resolver: the `otlp://` scheme

- [x] 4.1 Write the resolver tests first (`tests/core/test_transform.py`): `otlp://collector:4318` validates and resolves for `traces_to`; query params parse with defaults and reject unknown/unparseable values; `otlp://` with no host fails validation with the grammar in the message; `intents_to`/`errors_to` with an `otlp://` URI fail `AgentConfig` construction with the best-effort-only message; validation succeeds without `opentelemetry-proto` importable.
- [x] 4.2 Add `otlp` to `DefaultSinkResolver`: `_parse` arm for the grammar and query params (import-free), `validate` rejection for non-traces fields, `resolve` arm returning `WriteTracesToOtlp` (lazy import lives inside `otlp.py`). Update the resolver docstring's URI grammar table.

## 5. BigQuery schema and writer

- [x] 5.1 Write the schema tests first (`tests/observability/test_exporters.py`): schema field set equals encoded-row key set with matching types/modes (both directions, including the nested `attributes` record); `event_time` is derived from `start_ms` as epoch-millis UTC and identical across two encodings; existing row fields are byte-for-byte unchanged.
- [x] 5.2 Add `event_time` to `trace_event_to_row` and define `TRACE_TABLE_SCHEMA` beside it in `observability/exporters.py`, with the derivation and partition-column rationale (design D6) in the docstrings.
- [x] 5.3 Write the writer-configuration tests: a resolved `bigquery://` traces sink carries `TRACE_TABLE_SCHEMA`, `CREATE_IF_NEEDED`, `WRITE_APPEND`, and `additional_bq_parameters` with day partitioning on `event_time` and clustering on `trace_id`; `intents_to`/`errors_to` BigQuery resolution is unchanged.
- [x] 5.4 Implement the schema'd writer in `DefaultSinkResolver._write_transform`'s BigQuery arm for `traces_to` (via the existing `field_name` special-casing), leaving other fields' BigQuery writers untouched.

## 6. Docs and gates

- [x] 6.1 Document the two exporters where the capability's reader will look: URI grammar and knob defaults, the lossy-OTLP contract (drop-and-count, lossless alternatives), the START-skip mapping rule, and the BigQuery table layout with its partition/cluster keys; update `openspec/project.md`'s observability line if the shipped surface differs.
- [x] 6.2 `make lint` and `make type` clean (`mypy --strict` on `observability/otlp.py`; `opentelemetry-proto` stubs or targeted overrides as needed).
- [x] 6.3 Full unit tier passes offline with no docker and without the `otlp` extra in the core lane; offline semantics gates (`semantics and not integration`) still pass.
- [x] 6.4 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if it improves.
- [x] 6.5 Run `make mutation` and reconcile `mutation-baseline.toml`: the resolver growth moved `transform.py`'s no-tests ceiling 242 → 371 (documented in the baseline's comment, following the file's convention). A mutmut-selected split of the pipeline-free sink tests was tried first and reverted — mutmut's per-function reach turned 92 kafka/pubsub-arm mutants into survivors killable only by asserting cross-language writer internals.
- [x] 6.6 `uv run pre-commit run --all-files` clean.
