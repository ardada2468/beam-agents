## 1. Package scaffolding and dependencies

- [x] 1.1 Add an optional `effector` dependency group to `pyproject.toml` (`aiokafka`, `google-cloud-pubsub`, `redis`, `google-cloud-bigtable`); keep it out of the default install and out of `test`, and note the per-file `PLC0415` ignore for the adapter modules that import clients lazily.
- [x] 1.2 Create `src/beam_agents/effector/__init__.py` exporting the service, config, protocol, and runner types; confirm it is NOT re-exported from `beam_agents/__init__.py`.
- [x] 1.3 Create `tests/effector/__init__.py`.
- [x] 1.4 Write the import-boundary test first (spec: "The package imports with Beam unavailable" / "with no optional client libraries installed" / "absent from the public API"): import every effector module with `apache_beam` and all four client libraries blocked from `sys.modules`, and assert `beam_agents.__all__` carries no effector symbol.
- [x] 1.5 Add a `beam-agents-effector` console entry point in `pyproject.toml` pointing at `effector/__main__.py`.

## 2. Configuration

- [x] 2.1 Write config tests first (spec: unknown source scheme rejected; `lease_ms <= tool_timeout_ms` rejected; validation performs no client imports).
- [x] 2.2 Implement `effector/config.py`: frozen `EffectorConfig` with intents/results/approval/dedup URIs, consumer group id, `lease_ms`, `result_ttl_ms`, `tool_timeout_ms`, publish retry budget, `max_concurrent_partitions`.
- [x] 2.3 Implement import-free URI parsing for `kafka://<brokers>/<topic>`, `pubsub://<project>/<subscription|topic>`, `redis://`, `bigtable://<project>/<instance>/<table>`, `memory://`, reusing the grammar and error semantics from `core/transform.py::DefaultSinkResolver`.
- [x] 2.4 Implement `validate()` raising actionable `ValueError`s for malformed/unknown-scheme URIs and for `lease_ms <= tool_timeout_ms`; call it from `__post_init__`.

## 3. Dedup protocol and in-memory store

- [x] 3.1 Write dedup protocol tests first (spec: first claim granted; concurrent claims yield one owner; `Done` carries the stored result; non-owner completion refused; release frees the intent; expired lease re-claimable; unexpired lease not re-claimable; expired terminal record reads as unseen).
- [x] 3.2 Implement `effector/dedup.py`: the `DedupStore` protocol plus the `Claimed` / `InFlight` / `Done` outcome types (frozen dataclasses, `Claimed` carrying an opaque token, `Done` carrying a `ToolResult`).
- [x] 3.3 Implement `InMemoryDedupStore` with an injectable clock, so lease and TTL expiry are testable without sleeping.
- [x] 3.4 Run the protocol test suite against the in-memory store as the shared conformance suite that the Redis and Bigtable stores will also be run against.

## 4. Execution: EffectorToolRunner and `Tool.unwrap()`

- [x] 4.1 Write tests first for `Tool.unwrap()` (spec: returns the original callable; available for both tool kinds) and for the unchanged in-pipeline guard.
- [x] 4.2 Add `Tool.unwrap()` to `tools/registry.py` with a docstring naming the effector as its only sanctioned caller.
- [x] 4.3 Write `EffectorToolRunner` tests first (spec: side-effecting tool executes; read-only tool refused; async tool awaited; argument validation before invocation; malformed `args_json` rejected; timeout cancels and reports `ERROR`).
- [x] 4.4 Implement `effector/runner.py`: `EffectorToolRunner.run` — require `side_effect=True`, parse `args_json`, validate against the tool's Pydantic model, invoke via `unwrap()`, await awaitable results, wrap in `asyncio.wait_for(tool_timeout_ms)`.
- [x] 4.5 Implement the total intent → `ToolResult` status mapping (`OK`/`ERROR`/`EXPIRED`/`REJECTED`) with `REJECTED` reserved for never-invoked cases, plus canonical-JSON payload encoding and an `ERROR` result on encoding failure.

## 5. Sources and sinks

- [x] 5.1 Write source/sink protocol tests first against in-memory implementations (spec: the loop runs against in-memory implementations; results published under the originating entity key).
- [x] 5.2 Implement `effector/sources.py`: the `IntentSource` protocol (async iteration yielding intent + ack handle, plus `commit`) and an `InMemoryIntentSource` with a recording `commit`. The crash-injecting fake landed in `tests/effector/_fakes.py` rather than `src/`: it wraps the dedup store and the sinks (the phase boundaries that matter are `complete` and `publish`, not the source), and it raises a `BaseException` so the service's own retry wrapper cannot catch it — behavior that belongs to the test suite, not the shipped service.
- [x] 5.3 Implement `effector/sinks.py`: the `ResultSink` protocol, an `InMemoryResultSink`, and a raw-message sink for the approval channel.
- [x] 5.4 Implement the Kafka source adapter: consumer group with the configured group id, manual offset commit, one processing task per assigned partition, and a rebalance/revocation hook that releases unexecuted claims.
- [x] 5.5 Implement the Kafka result/approval sink adapter: publish keyed by `entity_key` with an idempotent producer.
- [x] 5.6 Implement the Pub/Sub source adapter: ordered subscription, one in-flight message per `ordering_key`, ack only on commit; log a warning at startup when the subscription's ordering flag is readable and false.
- [x] 5.7 Implement the Pub/Sub sink adapter: publish with `enable_message_ordering=True` and `ordering_key` derived from `entity_key`, matching `WriteIntents`' hex convention.

## 6. Service loop

- [x] 6.1 Write phase-order tests first (spec: expiry decided before the dedup store is touched; crash between completion and publication does not re-execute; crash before publication does not commit; in-flight is waited on, never skipped; `Done` republishes byte-identically).
- [x] 6.2 Write ordering tests first (spec: intents for one key execute in emission order; distinct partitions progress concurrently; a revoked partition releases unexecuted claims).
- [x] 6.3 Write retry tests first (spec: transient publish failure retried then committed; failed tool not re-invoked; exhausted publish retries leave the offset uncommitted).
- [x] 6.4 Implement `effector/service.py`: per-intent phases in order — `refuse_expired` → `claim` → route-or-execute → `complete` → publish → commit.
- [x] 6.5 Implement the `InFlight` wait loop with bounded exponential backoff, resolving to `Claimed` (lease expired) or `Done` (owner completed); never skip-and-commit.
- [x] 6.6 Implement approval-kind routing: publish the intent verbatim to the approval channel keyed by `entity_key`, mark terminal in the dedup store with a sentinel record, publish no `ToolResult`; treat `TOOL_KIND_UNSPECIFIED` as `TOOL`.
- [x] 6.7 Implement one task per assigned partition with sequential in-partition processing, bounded by `max_concurrent_partitions`.
- [x] 6.8 Implement bounded exponential-backoff retry for dedup RPCs and result publication only; ensure no code path re-invokes a tool callable within a claim.
- [x] 6.9 Implement the injectable metrics sink (counters for claimed / deduped / expired / rejected / errored and per-tool latency) with a no-op default.
- [x] 6.10 Implement `effector/__main__.py`: build `EffectorConfig` from CLI args/env, construct adapters, run the service with graceful shutdown (drain in-flight, release unexecuted claims, no uncommitted publish loss).

## 7. Redis dedup store

- [ ] 7.1 Write Redis-specific tests first (spec: claim/complete/re-claim round-trip; a stale owner cannot clobber the new owner's record) and register the store against the shared conformance suite from 3.4.
- [ ] 7.2 Implement `RedisDedupStore.claim` as `SET <intent_id> <tagged value> NX PX lease_ms`, decoding the existing value's tag to distinguish `InFlight` from `Done`.
- [ ] 7.3 Implement `complete` and `release` as server-side compare-and-set / compare-and-delete scripts against the ownership token; store the terminal result with `PX result_ttl_ms`.
- [ ] 7.4 Run the conformance suite plus the Redis-specific tests under `-m integration` against the compose Redis service.

## 8. Bigtable dedup store

- [ ] 8.1 Write Bigtable-specific tests first (spec: claiming is conditional in a single conditional mutation; lease expiry expressed as a value-range predicate; a completed row reports `Done`) and register the store against the shared conformance suite.
- [ ] 8.2 Implement the row layout: row key `intent_id`, column family `d`, columns `claim` (big-endian int64 lease expiry ‖ token) and `result` (serialized `ToolResult`); document the `maxage` GC rule matching `result_ttl_ms`.
- [ ] 8.3 Implement `claim` via `CheckAndMutateRow` with a live-claim filter chain (claim column present + `ValueRange` lower-bounded at big-endian `now_ms`); true branch → `InFlight`, false branch writes the claim, with a `result`-column check distinguishing `Done`.
- [ ] 8.4 Implement `complete` via `CheckAndMutateRow` conditional on the claim column still carrying the caller's token.
- [ ] 8.5 Verify big-endian encoding makes lexicographic value comparison agree with numeric comparison across the lease range used, with a property test.

## 9. Integration and semantics gates

- [ ] 9.1 Integration test (Redpanda + Redis): publish intents for two keys to the outbox topic, run the effector, assert per-key execution order, exactly one execution per `intent_id`, and one result per intent on the results topic.
- [ ] 9.2 Integration test (Pub/Sub emulator): ordered subscription end-to-end, asserting per-`ordering_key` sequencing and ack-after-publish.
- [ ] 9.3 Semantics gate (`semantics and not integration`, offline): replay an intent stream with kills injected at every phase boundary against the in-memory adapters; assert at most one tool invocation and exactly one terminal `ToolResult` status per `intent_id`.
- [ ] 9.4 Semantics gate: an expired intent never reaches the dedup store or a tool, for both `TOOL` and `APPROVAL` kinds.

## 10. Docs, quality, and wiring

- [ ] 10.1 Add `docs/effector.md`: deployment preconditions (topic keying, ordered subscription, dedup store provisioning and GC rule), the lease/TTL budget rules, the residual double-execution window, and the recommendation that tools use `intent_id` as their own idempotency key.
- [ ] 10.2 Update `docker/compose.yaml` only if the integration lane needs a Bigtable emulator service; otherwise note that Bigtable coverage is emulator-optional and conformance-tested in-memory.
- [ ] 10.3 Run `ruff` (incl. ASYNC), `mypy --strict` over `src/beam_agents/effector/`, and the offline unit suite with no docker; confirm all clean.
- [ ] 10.4 Re-check `mutation-baseline.toml` / `mutation-exclusions.toml` if `tools/registry.py` (a mutation-gated file) shifted with `Tool.unwrap()`, and update the ceiling if the gate requires it.
- [ ] 10.5 Confirm the coverage ratchet does not regress and that `make test-unit` passes with the `effector` dependency group uninstalled.
