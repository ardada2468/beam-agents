# key-sharding Specification

## Purpose
TBD - created by archiving change add-hot-key-sharding-guidance. Update Purpose after archive.
## Requirements
### Requirement: Shard-key derivation is deterministic across processes, workers, and retries

`shard_key(key, n, *, payload)` SHALL return `key + b"#" + <digits>` where `<digits>` is the ASCII decimal encoding of a shard index in `[0, n)` derived from a SHA-256 hash of `payload` reduced modulo `n`. The derivation SHALL depend only on `(payload, n)` — never on process identity, worker identity, element order, wall clock, or Python's per-process `hash()` salting — so that a bundle retry reproduces the same physical key, preserving `(key, seq)` replay-cache identity and byte-identical `intent_id`s. `n < 1` SHALL raise `ValueError` with an actionable message; `n = 1` SHALL still produce the `#0` suffix, so the shape of a sharded key never depends on the shard count.

#### Scenario: The same payload always lands on the same shard

- **WHEN** `shard_key` is called twice with the same `key`, `n`, and `payload` — including from separately started Python processes
- **THEN** both calls return the identical physical key, matching a pinned golden value for that input, so assignment cannot vary with `PYTHONHASHSEED` or process identity

#### Scenario: Varied payloads reach every shard

- **WHEN** `shard_key` is applied to a spread of distinct payloads with `n = 8`
- **THEN** every shard index in `[0, 8)` is produced by some payload, and every returned key parses as the original key plus a `#<digits>` suffix in range

#### Scenario: A non-positive shard count is rejected

- **WHEN** `shard_key` (or `ShardKeys`) is given `n = 0` or a negative `n`
- **THEN** `ValueError` is raised at the call (or transform-construction) site, before any pipeline runs

### Requirement: Shard keys round-trip through `unshard_key`

`unshard_key(key)` SHALL strip exactly one trailing `#<digits>` shard suffix and return the logical key, such that `unshard_key(shard_key(k, n, payload=p)) == k` for every valid input. A key carrying no trailing `#<digits>` suffix SHALL raise `ValueError` with an actionable message rather than being passed through, so a mis-wired regroup fails loudly instead of silently merging under wrong keys. The residual ambiguity — a logical key that itself ends in `#<digits>` is indistinguishable from a sharded key — SHALL be documented on both functions, with the guidance that such keys not be used with sharding.

#### Scenario: Sharding then unsharding is the identity on the logical key

- **WHEN** a logical key is sharded with any valid `n` and payload and the result is passed to `unshard_key`
- **THEN** the original logical key is returned exactly

#### Scenario: An unsharded key is refused

- **WHEN** `unshard_key` receives a key with no trailing `#<digits>` suffix
- **THEN** `ValueError` is raised, and its message names the expected `key#<shard>` shape

### Requirement: `ShardKeys` rewrites the physical key consistently across the KV pair and the envelope

`ShardKeys(n, assignment=...)` SHALL consume and produce `PCollection[KV[bytes, AgentEnvelope]]`, rewriting each element's KV key to the physical shard key and emitting an envelope copy whose `entity_key` field equals that same physical key, so no element leaves the transform with the state key and the envelope key disagreeing. The transform SHALL validate at `expand` (pipeline-construction) time that its input is KV-shaped, raising `ValueError` for a definite non-KV element type, mirroring `RunAgent`'s own input validation. Under the default hash assignment, reprocessing the same element SHALL yield the same physical key, so a retried bundle feeds `RunAgent` identical keys.

#### Scenario: KV key and envelope key agree after sharding

- **WHEN** a keyed envelope stream passes through `ShardKeys(n=4)`
- **THEN** every output element's KV key equals its envelope's `entity_key`, both carry a `#<digits>` suffix in `[0, 4)`, and `unshard_key` recovers the original logical key from either

#### Scenario: Non-KV input is rejected at construction time

- **WHEN** `ShardKeys` is applied to a `PCollection` whose element type is definitely not a KV pair
- **THEN** `ValueError` is raised during pipeline construction, before any element is processed

#### Scenario: Hash assignment is stable under reprocessing

- **WHEN** the same input elements are run through `ShardKeys` (default assignment) twice
- **THEN** both runs produce element-for-element identical physical keys

### Requirement: The memory-free-only safety contract is explicit, and round-robin's caveat is stated at its opt-in surface

The sharding utilities SHALL state, in the `beam_agents.keys` module docstring and the `ShardKeys` docstring ahead of any usage instructions, that sharding is safe only for agents that keep no per-key memory and require no per-key ordering, and that HITL flows whose approvals are keyed by the logical entity must not be sharded. The runtime SHALL NOT attempt to detect violations (a sharded key is an ordinary key to the DoFn); instead the documented failure mode SHALL be pinned by test: a memory-carrying agent behind `ShardKeys` accumulates independent per-shard memory. `assignment="round_robin"` SHALL be opt-in (never the default), and its documentation — docstring and `docs/sharding.md` — SHALL state that it forfeits deterministic shard assignment under bundle retries, and is therefore unsafe for intent-emitting agents and defeats replay-cache stability across retries.

#### Scenario: Sharding a memory-carrying agent splits its memory

- **WHEN** an agent that writes working memory runs behind `ShardKeys(n=2)` and processes events for one logical entity that hash to different shards
- **THEN** each physical shard key holds its own independent `MemoryBlob`, neither containing the other's writes — the documented reason the utility is restricted to memory-free agents

#### Scenario: The anti-guidance names all three do-not-shard cases

- **WHEN** a reader consults `docs/sharding.md`'s when-not-to-shard section
- **THEN** it names memory-carrying agents, ordering-sensitive flows, and HITL approval affinity (a logically-keyed approval orphans against a continuation stored under a shard key, surfacing as `orphaned_result`) as cases where sharding must not be used

#### Scenario: Round-robin requires an explicit opt-in that carries its caveat

- **WHEN** a caller constructs `ShardKeys` without specifying an assignment, or opts into `assignment="round_robin"`
- **THEN** the default is hash-of-payload, and the round-robin surface's docstring states the bundle-retry nondeterminism caveat

### Requirement: The throughput math is documented and its worked example is held by a test

`docs/sharding.md` SHALL derive the per-key throughput ceiling from per-key serialization and the runtime's own `activation_ms` metric (sustainable per-key rate at most `1000 / E[activation_ms]` activations/sec), SHALL explain how suspension dwell affects effective throughput (a suspended key is released — dwell is latency, not occupancy — but a suspending flow spends two activations per logical event), and SHALL state the fan-out formula (`N` shards multiply the logical entity's ceiling, bounded by runner parallelism, source partition count, and hash uniformity). Worked examples SHALL reference the benchmark harness's dimensions and measured single-key activation rates rather than unattributed figures. The document's pipeline example SHALL be carried verbatim by a test under `tests/examples/`, following the repo's doc-contract pattern, so the doc and the behavior cannot drift apart silently.

#### Scenario: The documented fan-out example runs as written

- **WHEN** the `docs/sharding.md` example — a memory-free agent, one hot logical key, `ShardKeys(n=4)`, FakeLLM — is executed by its `tests/examples/` doc-contract test
- **THEN** the pipeline's outputs span multiple physical shard keys for the one logical key, and regrouping them with `unshard_key` reassembles exactly the logical key's full output set

#### Scenario: The math is expressed in the runtime's metric vocabulary

- **WHEN** a reader follows the throughput-math section
- **THEN** the quantities it manipulates are the published `beam_agents.runtime` metrics (`activation_ms`, `overhead_ms`, `activations`), so the formula can be evaluated against a live pipeline's own dashboard
