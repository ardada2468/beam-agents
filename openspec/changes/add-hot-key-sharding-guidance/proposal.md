## Why

The runtime is keyed by construction: `RunAgent` refuses anything that is not a pre-keyed `PCollection[KV[bytes, AgentEnvelope]]` ([transform.py:456](../../../src/beam_agents/core/transform.py:456)), and every piece of durable state — `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ` ([dofn.py:199](../../../src/beam_agents/core/dofn.py:199)) — lives under that key. Per-key serialization is what makes working memory race-free ([dofn.py:3](../../../src/beam_agents/core/dofn.py:3)), and it has an unavoidable flip side: one key processes one element at a time, so a single hot `entity_key` caps its own throughput at the single-key activation rate — roughly `1000 / activation_ms` activations per second — no matter how many workers the runner adds. Cross-key parallelism scales; within-key parallelism does not exist, on purpose.

For agents that keep **no cross-event state** (no working memory, no per-key ordering requirement), that cap is pure waste: nothing about the computation actually needs one serial lane per logical entity. The standard escape is key sharding — fan one logical key across N physical keys `key#0..key#N-1` — but today every user would hand-roll the suffix convention, the deterministic assignment, and the regroup step, and nothing in the repo tells them when sharding is *unsafe* (memory-carrying agents silently split their memory N ways; nondeterministic assignment breaks the `intent_id` replay identity that the entire effectively-once argument rests on). The runtime should ship the convention once, correctly, with the throughput math and the anti-guidance written down.

## What Changes

- **New module `src/beam_agents/keys.py`** — the hot-key sharding utilities:
  - `shard_key(key, n, *, payload)`: pure function returning `key + b"#" + <shard>` where the shard index in `[0, n)` is derived from a SHA-256 hash of `payload` — deterministic across processes, workers, and bundle retries (never Python's salted `hash()`).
  - `unshard_key(key)`: strips exactly one trailing `#<digits>` shard suffix, recovering the logical key for downstream regrouping; raises `ValueError` on a key with no suffix.
  - `ShardKeys(n, assignment=...)`: a thin `PTransform` over `KV[bytes, AgentEnvelope]` that rewrites the KV key **and** the envelope's `entity_key` field ([beam_agents.proto:139](../../../protos/beam_agents.proto:139)) to the same physical shard key, validating KV-shaped input at `expand` time exactly as `RunAgent` does (reusing [`is_kv_shaped`](../../../src/beam_agents/actions/write_intents.py:193)). Default assignment is hash-of-payload; `round_robin` is an explicit opt-in that carries a documented bundle-retry caveat.
- **The memory-free-only safety contract, made explicit.** Sharding is safe only when the agent keeps no per-key memory and needs no per-key ordering. The contract is carried in the module and transform docstrings (which lead with it), in the docs anti-guidance, and in a test that demonstrates the failure mode (a memory-carrying agent behind `ShardKeys` accumulates N independent, divergent `MemoryBlob`s). The runtime performs no detection — see design D2 for why enforcement is documentary.
- **New `docs/sharding.md`** with the throughput math: the per-key ceiling derived from per-key serialization and `activation_ms` ([metrics.py:63](../../../src/beam_agents/observability/metrics.py:63)), how suspension dwell time affects effective throughput (occupancy vs. latency), the ×N fan-out formula and its limits (runner parallelism, source partition count, hash skew), worked examples referencing the C33 benchmark harness's dimensions, and the explicit when-NOT-to-shard list: memory-carrying agents, ordering-sensitive flows, and HITL flows whose approvals are keyed by the logical entity (an approval keyed `entity` cannot find a continuation stored under `entity#3` and dead-letters as `orphaned_result`).
- **The doc's worked example is held by a test**, following the repo's doc-contract pattern (`docs/errors.md` ↔ [test_failure_streak_alarm.py](../../../tests/examples/test_failure_streak_alarm.py)): `tests/examples/test_shard_fanout.py` carries the `docs/sharding.md` pipeline snippet verbatim and asserts the fan-out/regroup behavior it claims.
- **Public API addition:** `shard_key`, `unshard_key`, and `ShardKeys` are re-exported from [`beam_agents/__init__.py`](../../../src/beam_agents/__init__.py:24), and the public-surface test ([test_import.py:19](../../../tests/test_import.py:19)) is widened accordingly.

## Capabilities

### New Capabilities

- `key-sharding`: the `key#shard_n` convention for fanning one logical entity key across N physical shards — deterministic shard derivation and its round-trip (`shard_key`/`unshard_key`), the `ShardKeys` transform's KV contract and envelope consistency, the explicit memory-free-only safety contract with its documented anti-guidance, and the tested throughput-math documentation.

### Modified Capabilities

None. The runtime treats a sharded key as an ordinary `entity_key`: no requirement of `run-agent-transform`, `stateful-agent-runtime`, or any other capability changes. Sharding happens strictly upstream of `RunAgent`, on the caller's side of the existing KV contract.

## Impact

**Depends on:** C33 `add-benchmark-harness` (sibling proposal in this batch) — `docs/sharding.md`'s worked examples cite the harness's measured single-key activation rates and its benchmark dimensions rather than invented figures; the math section is written against the harness's terminology, and its numbers are filled in from harness runs during implementation. Also consumes, unchanged: `run-agent-transform` (the pre-keyed KV input contract this transform feeds), the `stateful-agent-runtime` state layout that motivates the whole change, and `runtime-metrics` (`activation_ms`/`overhead_ms`/`activations` are the instruments the math is expressed in).

**New code:** `src/beam_agents/keys.py` (two pure functions + `ShardKeys`), `tests/keys/test_shard_key.py`, `tests/keys/test_shard_keys_transform.py`, `tests/examples/test_shard_fanout.py`, `docs/sharding.md`.

**Modified code:** `src/beam_agents/__init__.py` (three re-exports) and `tests/test_import.py` (the public-surface set). No proto, wire, or state change: shard keys are ordinary `bytes` keys to every existing coder and state spec, so there is no `state_schema_version` implication and no golden-blob movement.

**CI/build:** no new workflow, marker, or dependency. All new tests are offline unit-tier (`TestPipeline` on DirectRunner with `FakeLLM`); the example test rides the existing `tests/examples/` collection.

**Gates:** `make lint`, `make type` (`mypy --strict`, no `Any` in the new public signatures), `make test-unit` offline, coverage ratchet at-or-above baseline (a small pure module should raise it), `uv run pre-commit run --all-files`. `keys.py` is outside `core/`, so the mutation gate's `core/` selection is untouched.
