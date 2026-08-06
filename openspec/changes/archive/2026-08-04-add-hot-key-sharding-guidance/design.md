## Context

Three existing facts shape this change.

**All state and all serialization hang off one key.** `RunAgent` consumes a pre-keyed `PCollection[KV[bytes, AgentEnvelope]]` and validates the shape at `expand` ([transform.py:480](../../../src/beam_agents/core/transform.py:480)). The DoFn's five state specs and two timers ([dofn.py:199](../../../src/beam_agents/core/dofn.py:199)) are all scoped to that key, and Beam stateful DoFns process one element at a time per key — the property the architecture leans on for race-free memory (correctness invariant 4). The consequence is a hard per-key throughput ceiling: a key whose activations average `activation_ms` of wall time sustains at most `1000 / activation_ms` activations per second, regardless of worker count. With a 2-second LLM call in the loop that is 0.5 activations/sec for the hottest entity.

**Replay identity is derived from the key.** `intent_id = uuid5(NAMESPACE, key + seq + step_index)` (invariant 2) and the replay-cache key includes `(key, seq)` (invariant 3). Anything that makes the physical key of an element differ between a bundle's first attempt and its retry re-mints different intent IDs (defeating the effector's dedup — duplicate side effects) and misses the replay cache (extra provider calls — exactly what the retry-determinism gate exists to forbid). Shard assignment is therefore not a load-balancing detail; it is a correctness input.

**Results and approvals re-enter on the key that emitted them.** `ToolIntent.entity_key` is stamped with the physical key, the effector publishes the `ToolResult` back under it, and `_KeyedWriteIntents` keys the outbox by it ([transform.py:193](../../../src/beam_agents/core/transform.py:193)). Resume admission looks up `CONTINUATION` under the arriving key and dead-letters a miss as `orphaned_result` ([dofn.py:117](../../../src/beam_agents/core/dofn.py:117)). So the re-injection path survives sharding automatically — but only for streams that carry the physical key. Anything an operator keys by the *logical* entity (a hand-wired approvals topic) will miss the shard and orphan.

The roadmap item asks for the escape hatch this implies: for agents with no cross-event state, fan one logical key across N physical shards `key#0..key#N-1` and multiply the ceiling by N — packaged as a utility plus documentation, with the safety boundary stated loudly enough that nobody shards a memory-carrying agent by accident.

## Goals / Non-Goals

**Goals:**
- One canonical `key#shard_n` convention: derivation, delimiter, and round-trip in a single module, so shard and unshard can never drift apart across hand-rolled copies.
- Deterministic-by-default shard assignment that preserves the retry-determinism and effectively-once invariants without the user thinking about them.
- The memory-free-only contract stated where it will be read: docstrings, docs, and a test that shows the failure mode.
- Throughput math an operator can act on, expressed in the runtime's own metric names and validated against the C33 benchmark harness's dimensions, with the worked example held by a doc-contract test.

**Non-Goals:**
- No runtime enforcement of "memory-free" (see D2), and no change to `RunAgent`, the DoFn, coders, or any proto — a sharded key is an ordinary `bytes` key to all of them.
- No automatic re-sharding, shard-count autoscaling, or hot-key detection. Choosing N is capacity planning; the docs give the formula, the operator gives the number.
- No cross-shard aggregation transform. Regrouping downstream of `.output` is ordinary Beam (`Map(unshard_key)` + whatever aggregation the caller wants); the module provides the key function, not an aggregation DSL.
- No framework-side sharding (LangGraph et al.): this is pipeline-shape guidance, squarely runtime territory.

## Decisions

### D1. A new top-level `beam_agents/keys.py`, not `core/` and not docs-only

Docs-only was considered and rejected: the convention has three coupled degrees of freedom (delimiter, digit encoding, hash function), and `unshard_key` only works if every producer used the exact same `shard_key`. A convention implemented by hand N times will drift; a 40-line module cannot. The inverse — burying it in `core/` — is wrong altitude: `core/` is the runtime engine, and sharding happens strictly on the caller's side of the KV contract, in the same layer as the `WithKeys(entity_id)` step the Dataflow shape already assigns to the caller. A top-level `keys.py` matches the precedent of `hitl.py` (small, public, caller-facing policy surface) and keeps the mutation gate's `core/` selection untouched. The three names are re-exported from the package root because the project defines the public API as exactly what `beam_agents/__init__.py` re-exports; the public-surface test widens in the same change.

### D2. The memory-free-only contract is documentary, not enforced at runtime

The utility is only safe when the agent keeps no per-key memory and needs no per-key ordering. Can the runtime enforce that? No, for two reasons. First, "memory-free" is a dynamic property of the agent's behavior, not a static property of the pipeline: the DoFn cannot know at graph-construction time whether an activation will call `ctx.remember(...)`. Second, the obvious runtime tripwire — the DoFn warning when a key containing `#` commits memory — couples the engine to a caller-side naming convention, false-positives on every logical key that legitimately contains `#`, and violates the runtime/framework altitude rule (the runtime does not police what agents compute).

A mandatory `ShardKeys(..., memory_free=True)` acknowledgment flag was also considered and rejected: an argument that admits exactly one value documents nothing at the call site and trains users to type it reflexively — an I-agree checkbox, not a contract.

So the contract is carried where contracts in this repo live: (a) the module and `ShardKeys` docstrings *lead* with it, before usage; (b) `docs/sharding.md` has a dedicated when-NOT-to-shard section covering the three hazard classes (memory-carrying agents, ordering-sensitive flows, HITL approval affinity); (c) a test demonstrates the failure mode positively — a memory-carrying agent behind `ShardKeys(n=2)` provably accumulates two independent, divergent `MemoryBlob`s — so the documented hazard is a pinned, observable behavior rather than prose. The spec makes all three requirements.

### D3. Hash-of-payload is the default assignment; round-robin is an explicit opt-in with a stated caveat

Deterministic assignment is a correctness requirement, not a preference (Context, second fact). Hash-of-payload — shard index `int.from_bytes(sha256(payload).digest()[:8]) % n` — is deterministic per element across processes, workers, and bundle retries, so a retried bundle reproduces the same physical keys, the same `(key, seq)` cache hits, and byte-identical intent IDs. SHA-256 rather than Python's `hash()` because the latter is salted per process (`PYTHONHASHSEED`), which would make assignment differ between workers — the exact failure D3 exists to prevent. The first 8 digest bytes are ample for a modulus that is realistically < 1024.

True round-robin (a worker-local counter) is *not* deterministic under retries: the retry's counter state differs, elements land on different shards, and both replay properties break. It still earns its opt-in slot because hash assignment has a real failure mode — low-entropy payloads (many identical events) hash to one shard and the fan-out collapses. The trade is explicit: `ShardKeys(n, assignment="round_robin")` is documented, in its docstring and in the docs, as forfeiting retry determinism on the sharded stream — acceptable only when the agent emits no intents (or its effects are idempotent independent of `intent_id`) and duplicate provider calls on a bundle retry are an accepted cost. The spec requires the caveat's presence, and the default stays `"hash"` so the safe mode is the zero-config mode.

### D4. `ShardKeys` rewrites both the KV key and `envelope.entity_key`; it belongs on the events branch only

The runtime stamps outputs from the KV key (errors, traces, intents), but the envelope also carries its own `entity_key` field ([beam_agents.proto:139](../../../protos/beam_agents.proto:139)) — the field `WithKeys` keyed from in the first place. Rewriting only the KV key would leave a split brain: state under `entity#3`, envelope claiming `entity`, and any component that re-keys or debugs from the envelope disagreeing with the state layout. `ShardKeys` therefore emits a copied envelope whose `entity_key` equals the new KV key; the logical key remains recoverable from either via `unshard_key`.

Placement: `ShardKeys` goes on the **events branch, after `WithKeys`, before the `Flatten`** with tool-results and approvals. Results and approvals must not pass through it — they already carry the physical shard key from `ToolIntent.entity_key` through the effector, and re-sharding them would either double-suffix the key or (hash mode, different payload) route them to the wrong shard and orphan the resume. The docs show the corrected Dataflow-shape diagram, and the transform's docstring states the placement rule.

### D5. `unshard_key` strips exactly one trailing `#<digits>` suffix and fails loudly otherwise

Regrouping downstream needs the inverse function, and the inverse is where delimiter ambiguity bites: a logical key `b"user#7"` is indistinguishable from shard 7 of `b"user"`. Resolution: `unshard_key` is defined only over keys produced by `shard_key` — it strips the final `#<digits>` group (rsplit once, digits required) and raises `ValueError` with an actionable message when no such suffix exists, rather than passing the key through and silently corrupting a downstream grouping. The residual ambiguity (logical keys that already end in `#<digits>`) cannot be eliminated while the roadmap-mandated `key#shard_n` shape is kept, so it is documented: don't feed keys ending in `#<digits>` to `ShardKeys`, or accept that regrouping merges them. Encoding the shard count into the suffix (`key#3of8`) to shrink the ambiguity window was considered and rejected — it leaks a deploy-time tuning parameter into every state key, so changing N would strand state and complicate `--update`, for a marginal disambiguation gain. Left as an open question whether a stricter delimiter variant is wanted later.

### D6. The throughput math ships as a tested document, in the runtime's own vocabulary

The math itself:

- **Per-key ceiling.** Per-key serialization means a key's sustainable input rate is `λ_key ≤ 1000 / E[activation_ms]` activations/sec, where `activation_ms` is the runtime's own distribution ([metrics.py:63](../../../src/beam_agents/observability/metrics.py:63)) — wall time including LLM and tool time, which is what occupies the key's serial lane. `overhead_ms` shows how much of that is runtime rather than provider.
- **Suspension dwell.** A suspended activation commits and releases the key — the key is idle during the dwell, so dwell time is *latency* per logical event, not occupancy. But a suspending flow costs two activations per logical event (start + resume), so its per-key ceiling is `1000 / (E[activation_ms_start] + E[activation_ms_resume])`: the dwell stretches end-to-end completion time while the two activations' wall time is what consumes the key's serial budget.
- **Fan-out.** N shards multiply the logical entity's ceiling: `λ_logical ≤ N × λ_key`, bounded above by runner parallelism, source partition count, and hash uniformity (low-entropy payloads collapse the effective N).

Two disciplines keep the document honest. First, worked examples use the C33 benchmark harness's dimensions and measured single-key activation rates — the doc points at reproducible harness output instead of inventing numbers, and the figures are filled from harness runs during implementation. Second, the doc's pipeline example is held by `tests/examples/test_shard_fanout.py` carrying the snippet verbatim, exactly as [test_failure_streak_alarm.py](../../../tests/examples/test_failure_streak_alarm.py) holds `docs/errors.md` — changing one without the other is a defect.

## Risks / Trade-offs

- **Someone shards a memory-carrying agent anyway** → memory silently splits N ways; each shard sees 1/N of the events. Mitigation: D2's three-layer contract, the divergent-memory test pinning the failure mode, and the fact that the symptom (per-shard `memory_bytes`, inconsistent recall) is observable in existing metrics and traces. The runtime cannot make this impossible without policing agent behavior.
- **HITL approval affinity** → an operator keying approvals by the logical entity orphans them against continuations stored under shard keys; the loss is visible as `orphaned_result` on `.errors` but is still a loss. Mitigation: named explicitly in the anti-guidance as a do-not-shard case; the intent→result path itself is unaffected (physical key rides `ToolIntent.entity_key`).
- **Hash skew** → identical payloads all land on one shard, and the promised ×N never materializes. Mitigation: documented, with round-robin as the eyes-open alternative and its caveat (D3); the docs tell the operator to verify fan-out via per-key element counts in the harness before trusting N.
- **Delimiter ambiguity** (D5) → logical keys ending `#<digits>` regroup incorrectly after `unshard_key`. Mitigation: documented restriction; `unshard_key` at least never invents a suffix where none exists.
- **Round-robin misuse** → a user opts in and then adds an intent-emitting tool later; bundle retries can now duplicate effects. Mitigation: the caveat lives on the assignment mode's docstring (read at the opt-in site), not only in the doc; no silent default path leads here.
- **Doc numbers rot** as the runtime gets faster/slower. Mitigation: the example code is test-held; the measured figures cite the harness version/dimensions so a stale number is traceable to a rerun rather than trusted.

## Migration Plan

Purely additive. No wire, state, coder, or runtime change: a shard key is an ordinary `bytes` key, so existing pipelines, `--update` compatibility, and golden blobs are untouched. Adopting sharding on a live pipeline is itself a state migration for that pipeline's keys (state under `entity` does not follow to `entity#i`) — the docs state that sharding a *stateful* key is out of contract anyway (D2), and for memory-free agents there is no state to strand, which is precisely why the contract is what it is. Rollback is deleting the `ShardKeys` step; in-flight suspensions under shard keys drain normally because their results carry the physical key.

## Open Questions

- Should assignment optionally take a caller-supplied discriminator (`assignment=by(lambda env: ...)`) so operators can shard by a stable sub-entity field instead of the whole payload — better uniformity than hash-of-payload, still deterministic? Deferred until a real workload asks; the seam is easy to add compatibly.
- Should the C33 harness grow a sharded-hot-key benchmark scenario (1 logical key × N shards) so the ×N claim is continuously measured rather than derived? That belongs to the harness's change, but the doc is written so the scenario can slot in.
- Is a stricter, collision-free suffix encoding worth a v2 of the convention (D5), or does the `key#shard_n` shape stay as the roadmap named it?
