## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/keys/test_shard_key.py`: derivation and round-trip units — same `(key, n, payload)` yields the identical physical key matching pinned golden values (pinning SHA-256 assignment against `PYTHONHASHSEED`/process drift); varied payloads with `n=8` reach every shard index and every result parses as `key#<digits>` in range; `n=1` still suffixes `#0`; `n<=0` raises `ValueError`; `unshard_key(shard_key(k, n, payload=p)) == k`; `unshard_key` on a suffix-free key raises `ValueError` naming the `key#<shard>` shape. Derived from "Shard-key derivation is deterministic across processes, workers, and retries" and "Shard keys round-trip through `unshard_key`".
- [ ] 1.2 `tests/keys/test_shard_keys_transform.py`: `TestPipeline` runs — every `ShardKeys(n=4)` output's KV key equals its envelope's `entity_key`, carries an in-range suffix, and unshards to the original logical key; a definitely-non-KV input raises `ValueError` at construction; two runs over the same elements produce element-for-element identical physical keys under the default hash assignment; `assignment="round_robin"` spreads identical payloads across shards (the skew case hash cannot serve). Derived from "`ShardKeys` rewrites the physical key consistently across the KV pair and the envelope".
- [ ] 1.3 `tests/keys/test_shard_keys_transform.py`: the divergent-memory demonstration — a memory-writing agent behind `ShardKeys(n=2)` (FakeLLM, DirectRunner) leaves independent `MemoryBlob`s under the two physical keys, neither containing the other's writes. Derived from the scenario "Sharding a memory-carrying agent splits its memory".
- [ ] 1.4 `tests/examples/test_shard_fanout.py`: the doc-contract test carrying the `docs/sharding.md` pipeline example verbatim (marked begin/end like `tests/examples/test_failure_streak_alarm.py`) — one hot logical key fanned by `ShardKeys(n=4)` over a memory-free FakeLLM agent; assert outputs span multiple shard keys and regroup under `unshard_key` to exactly the logical key's full output set. Derived from "The documented fan-out example runs as written".
- [ ] 1.5 `tests/test_import.py`: widen the public-surface set to include `shard_key`, `unshard_key`, `ShardKeys`, and keep the import-has-no-side-effects assertion covering the new module.

## 2. The keys module

- [ ] 2.1 Create `src/beam_agents/keys.py` with a module docstring that leads with the safety contract (memory-free agents only; no per-key ordering; do not shard HITL flows whose approvals are keyed by the logical entity) before any usage text, plus the `b"#"` delimiter constant.
- [ ] 2.2 Implement `shard_key(key: bytes, n: int, *, payload: bytes) -> bytes`: SHA-256 of `payload`, first 8 digest bytes reduced modulo `n`, ASCII-decimal suffix; `ValueError` for `n < 1`. Pure, import-side-effect-free, fully typed.
- [ ] 2.3 Implement `unshard_key(key: bytes) -> bytes`: strip exactly one trailing `#<digits>` group; `ValueError` with the expected-shape message when absent; document the `#<digits>`-suffixed-logical-key ambiguity on both functions (design D5).

## 3. The ShardKeys transform

- [ ] 3.1 Implement `ShardKeys(n, assignment="hash")` in `keys.py`: `PTransform` over `KV[bytes, AgentEnvelope]` rewriting the KV key and emitting an envelope copy with `entity_key` set to the same physical key; construction-time `ValueError` for invalid `n` or an unknown assignment mode.
- [ ] 3.2 Validate KV-shaped input at `expand` via the existing `is_kv_shaped` helper (`actions/write_intents.py`), mirroring `RunAgent`'s error-message style (actionable, names the expected element type).
- [ ] 3.3 Implement `assignment="round_robin"` (worker-local counter) as the explicit opt-in; its docstring states the bundle-retry nondeterminism caveat — unsafe for intent-emitting agents, defeats replay-cache stability across retries (design D3).
- [ ] 3.4 Docstring states placement: events branch only, after `WithKeys`, before the `Flatten` with tool-results/approvals — those streams already carry the physical key via `ToolIntent.entity_key` and must not be re-sharded (design D4).

## 4. Documentation

- [ ] 4.1 Write `docs/sharding.md`: the hot-key problem (per-key serialization, all five state cells under one key), the `key#shard_n` convention, and the corrected Dataflow-shape diagram showing `ShardKeys` on the events branch.
- [ ] 4.2 Throughput-math section in the runtime's metric vocabulary: per-key ceiling `1000 / E[activation_ms]` activations/sec; suspension dwell as latency-not-occupancy with the two-activations-per-logical-event ceiling for suspending flows; fan-out `λ_logical ≤ N × λ_key` bounded by runner parallelism, partition count, and hash uniformity.
- [ ] 4.3 Worked examples referencing the C33 benchmark harness's dimensions and measured single-key activation rates (figures filled from harness runs, cited with harness scenario names — no unattributed numbers).
- [ ] 4.4 When-NOT-to-shard section naming all three hazard classes: memory-carrying agents (with the divergent-memory failure mode), ordering-sensitive flows, HITL approval affinity (logically-keyed approval vs. shard-keyed continuation → `orphaned_result`); plus the round-robin caveat and the hash-skew warning with the verify-fan-out-first guidance.
- [ ] 4.5 Keep the doc's pipeline example and `tests/examples/test_shard_fanout.py` in sync (verbatim block, begin/end markers, "changing one without the other is a defect" header), matching the `docs/errors.md` precedent.

## 5. Public surface

- [ ] 5.1 Re-export `shard_key`, `unshard_key`, `ShardKeys` from `src/beam_agents/__init__.py` and add them to `__all__`, keeping the package import side-effect-free (no Beam pipeline machinery at import time beyond what `apache_beam` itself does).

## 6. Gates

- [ ] 6.1 `make lint` and `make type` clean (`mypy --strict`; no `Any` in the new public signatures).
- [ ] 6.2 `make test-unit` passes offline with no docker; both offline semantics selections unaffected.
- [ ] 6.3 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new module improves it.
- [ ] 6.4 `uv run pre-commit run --all-files` clean.
- [ ] 6.5 `openspec validate add-hot-key-sharding-guidance --strict` passes.
