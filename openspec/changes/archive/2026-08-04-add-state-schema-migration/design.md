## Context

The `state_schema_version` field is written but never read. Three write sites stamp `1` — `WorkingMemory.to_blob` ([facade.py:194](../../../src/beam_agents/memory/facade.py:194)), `ReplayCacheView.to_blob` ([replay_cache.py:203](../../../src/beam_agents/model/replay_cache.py:203)), `build_continuation` ([context.py:691](../../../src/beam_agents/core/context.py:691)) — and no read site inspects the value before interpreting fields. The keyed-state reads all live in `_AgentDoFn`: `_start` reads `MEMORY` and `LLM_CACHE` ([dofn.py:329](../../../src/beam_agents/core/dofn.py:329)–330), `_resume` reads `CONTINUATION` then `MEMORY`/`LLM_CACHE` ([dofn.py:387](../../../src/beam_agents/core/dofn.py:387), [dofn.py:410](../../../src/beam_agents/core/dofn.py:410)–411), and the two timer callbacks read `CONTINUATION` to interpret `seq`, `deadline_ms`, `escalations`, and `step_index` ([dofn.py:641](../../../src/beam_agents/core/dofn.py:641), [dofn.py:676](../../../src/beam_agents/core/dofn.py:676)). Seven read sites, all decoding through `DeterministicProtoCoder` ([coders.py:59](../../../src/beam_agents/core/coders.py:59)) via the three `ReadModifyWriteStateSpec`s ([dofn.py:199](../../../src/beam_agents/core/dofn.py:199)–201).

The golden-blob mechanism is single-version: flat `tests/core/golden/*.bin` v1 baselines, a manual generator whose `GOLDEN` dict is the single source of truth for bytes and expected values ([generate.py:217](../../../tests/core/golden/generate.py:217)), and a decode-equality test ([test_schema_compat.py:24](../../../tests/core/test_schema_compat.py:24)) that deliberately does not assert byte-identical re-encode (archived design D6 of `add-wire-schemas-and-coders`).

Beam realities that shape the design: user state cannot be enumerated (no "sweep all keys" job exists on any runner), so an eager migration pass is impossible; Dataflow `--update` restores state bytes verbatim and re-decodes them with the new binary's descriptor, so whatever the new binary does on *read* is the entire migration opportunity; and `TTL_TIMER` GC bounds idle-state age, but a hot key under a long-lived `--update`-ed pipeline keeps its state alive indefinitely — old-version blobs can surface arbitrarily long after a bump.

Only the three state blobs carry version fields. `PENDING` state holds `ToolIntent`s and `SEQ` holds a varint; neither is versioned, and `ToolIntent`/`ToolResult`/`TraceEvent`/`AgentEnvelope`/`ActivationErrorRecord` also travel Kafka/Pub/Sub topics read by the effector and external consumers.

## Goals / Non-Goals

**Goals:**
- A migration registry precise enough that a bump is mechanical: one constant edit plus the artifacts the gate demands.
- Lazy migration applied at every keyed-state read in `_AgentDoFn`, with rewrite deferred to the next commit — zero new write paths, invariant 1 untouched.
- Fail-fast, mutation-free handling of a version from the future.
- A per-version golden corpus whose completeness is asserted by CI, making "no breaking change without migration + corpus entry" an executable gate, not a review convention.
- Zero behavior and zero byte change for current-version state: at `CURRENT_STATE_SCHEMA_VERSION = 1` every read takes the identity fast path.

**Non-Goals:**
- No actual v2 schema ships here. The machinery lands against an all-identity landscape; the first real migration is written by the first breaking change.
- No versioning for the wire messages or for `PENDING`/`SEQ` state (D1).
- No migration of long-term MemoryStore rows (`memory/` stores) — those are outside keyed state and outside this regime.
- No Buf/`buf breaking` adoption; mechanical descriptor diffing stays a future tooling change (open question inherited from the archived schema change).
- No eager rewrite job, no state-spec renames, no coder format change.

## Decisions

### D1. The versioned set is exactly the three state blobs; everything else stays additive-only forever

Migration applies to `MemoryBlob`, `Continuation`, and `LlmCacheBlob` — the messages that carry `state_schema_version` and live in `ReadModifyWriteState`. The unversioned wire messages get no version field and no bump escape hatch, deliberately:

- `ToolIntent` is a cross-service contract: it sits in `PENDING` state briefly, but its authoritative life is on the outbox topic, consumed by the effector — a separate process that dedups on the *bytes-derived* `intent_id` and cannot participate in a lazy in-pipeline migration. Same for `ToolResult` (results topic), `TraceEvent` (OTLP/BigQuery exporters), `AgentEnvelope` (the pipeline's input contract), and `ActivationErrorRecord` (errors topic). A "migrated" reading that exists only inside `RunAgent` would desynchronize every external reader.
- `PENDING` needs no migration story of its own: pending intents are bounded by `expires_at_ms` and the HITL deadline, and the admission check already fails closed on anything stale. The corpus keeps pinning `ToolIntent` decode compatibility through the additive baseline exactly as today.

The alternative — adding `state_schema_version` to all eight messages "for symmetry" — would imply a migration promise the runtime cannot keep for messages it does not exclusively read. Breaking a wire message means a `beam_agents.v2` proto package and a topic cutover, which is a different (and intentionally expensive) event.

### D2. Registry shape: per-message chains of single-step pure functions, one shared current-version constant

`core/migration.py` defines:

- `CURRENT_STATE_SCHEMA_VERSION: Final[int] = 1`, which the three blob writers stamp from (replacing their literal `1`s). A bump is then one edit in one module — the same module the completeness test interrogates, so the constant and the gate cannot drift apart.
- A registry keyed `(message type, from_version)` holding functions `Callable[[M], M]` that take a version-`n` message and return a version-`n+1` message, registered via a decorator (`@migration(MemoryBlob, from_version=1)`). Registration happens at module import inside `core/migration.py` only — the registry is populated once, then read-only, matching the project's no-global-mutable-state rule in the same spirit as the coder registry's explicit registration.
- `migrate_to_current(msg)` — normalizes version `0` to `1`, returns current-version messages unchanged (identity fast path: one integer compare, no copy, no re-serialize), raises a typed error on a future version (D4), and otherwise walks the chain one step at a time, asserting each step advanced the stamp, until the message reads `CURRENT_STATE_SCHEMA_VERSION`. A missing step raises a typed error naming the message type and the gap.

Single-step chains beat the alternative of one `v_n → current` function per historical version: with chains, a bump to version `k` requires exactly three new functions (one per blob type), while direct-to-current functions would require rewriting every historical entry on every bump — O(versions) maintenance and a standing invitation to skew.

Migration functions are required to be **pure and deterministic**: no clocks, no randomness, no I/O. They run inside `process()` on the element path, and a replayed bundle must produce the same migrated view — feeding the same cache keys and intent IDs — as the original attempt, or the retry-determinism gate breaks.

Version `0` normalizes to `1` rather than failing: proto3 zero-defaults mean a hand-built or default-constructed blob reads `0`, the runtime has only ever written `1`, and the existing wire-schemas spec already designates `0` as "pre-versioned" — the baseline semantics are the only semantics it can have.

### D3. Lazy on first read, hooked in the DoFn — not in the coder, and never eager

The hook is `migrate_to_current()` applied at each of the seven keyed-state read sites in `_AgentDoFn`, before any field of the blob is interpreted — including the timer callbacks, which read `Continuation.deadline_ms`/`escalations` to make fail-closed decisions and must not misread a bumped layout.

**Not in the coder.** `DeterministicProtoCoder.decode` ([coders.py:75](../../../src/beam_agents/core/coders.py:75)) is shared by state specs, pipeline elements, and GBK keys. Migrating inside `decode` would run migration on wire elements that are not state (and mostly carry no version field), would make `decode(encode(x)) != x` for old-version values — breaking the coder's round-trip contract and its property tests — and would push version policy into a class whose entire job is to be a transparent, deterministic byte codec. The coder stays migration-invariant (spec'd in the `proto-coders` delta), which is also what keeps `--update` safe: restored state bytes decode exactly as written, and migration happens visibly, above the codec.

**Not eager.** Beam offers no way to enumerate keys in user state, so a "migrate everything at deploy" sweep cannot be built portably; lazy-on-read is not merely preferred, it is the only mechanism available.

**Rewrite only at the next commit.** Migration at read time mutates no state: `_resume`'s admission refusals, timer no-ops, and every activation failure must leave state untouched (invariant 1 — "a failed/timed-out activation mutates nothing" — and the timer callbacks' own mutate-nothing stale-handle rule). No write-back machinery is added at all, because none is needed: on a successful commit, `MEMORY` and `LLM_CACHE` are rewritten from `to_blob()` builders and a suspending activation rebuilds its `Continuation` via `build_continuation` — all stamping `CURRENT_STATE_SCHEMA_VERSION` — and the escalation path writes a `CopyFrom` of the *migrated* continuation. The consequence is that a key which only ever fails, or never activates again, keeps its old-version bytes until TTL GC wipes them; that is fine precisely because the migration chain is never pruned, so a later read still upgrades them.

### D4. A version from the future raises and fails the bundle — deliberately wedging the key

If `state_schema_version > CURRENT_STATE_SCHEMA_VERSION`, `migrate_to_current()` raises a typed `StateSchemaFromFutureError` naming the message type, the found version, and the binary's current version. The DoFn does not catch it: the bundle fails, the runner retries, and the key wedges until an operator rolls the binary forward.

This contradicts the module's usual "route element failures to `.errors`" posture on purpose, and the distinction is the same one the codebase already draws for the HITL policy (caught, because it is per-element user code that must not wedge a key) versus this case (deployment skew, where continuing *is* the failure):

- Dead-lettering the element would drop it while leaving the state unreadable, so every subsequent element on the key also dead-letters — a silent, unbounded data loss dressed up as graceful degradation.
- Processing anyway would interpret v(n+1) fields under v(n) semantics — exactly the corruption this whole change exists to prevent.
- A wedged key is loud (bundle retries surface on every runner's error reporting) and losslessly recoverable: retrying the same bundle under the rolled-forward binary succeeds, because nothing was mutated or emitted. Future-version state means a newer pipeline already ran here (e.g. `--update` forward then rollback); rolling forward again is always available.

Fail-fast happens before any field is interpreted, including in the timer callbacks — a fail-closed mechanism that misreads a future `deadline_ms` is not fail-closed.

### D5. Corpus mechanics: per-version directories, frozen builders, replay-to-current with field-level equality

`tests/core/golden/` is restructured from a flat v1 directory into a per-version corpus:

- `tests/core/golden/v1/*.bin` — the existing baseline blobs, moved (`git mv`) byte-for-byte; their meaning is unchanged.
- `generate.py` keeps its single-source-of-truth shape but becomes version-aware: a per-version mapping of builders to expected values, where a historical version's builders are **frozen** the moment a newer version exists, and `main()` only ever writes `v<CURRENT>/`. Regenerating history is therefore impossible by construction, not by convention — the committed historical bytes are the artifact, and the archived rule ("regenerating is only appropriate when intentionally establishing a new baseline") becomes "a new baseline is a new directory".
- The compat test becomes a corpus replay, parameterized over every `(version, fixture)`: decode the blob at its version, run `migrate_to_current()`, assert field-level equality against the expected *current-version* message. At `CURRENT = 1` the replay is the identity and the suite is exactly today's decode-equality check plus the migration call. Byte-identical re-encode remains deliberately un-asserted (protobuf-version serialization drift, archived D6): the corpus pins semantics, the coder property tests pin same-process byte determinism.
- Completeness meta-tests extend the existing fixture-inventory test: every version in `1..CURRENT` has a corpus directory; every versioned blob type has a fixture in every version directory since its introduction; every `(type, n)` for `n` in `1..CURRENT-1` has a registered migration step. These are the executable form of the gate (D6): bumping the constant without shipping the migration functions or freezing the outgoing corpus makes CI red with a message that names exactly what is missing.

The corpus tests carry the offline `semantics` marker (plus their plain unit-tier run): `project.md` names "state compat (golden blobs)" a semantics gate, and this change makes the implementation match the constitution. They need no docker, so they land in the required `ci` offline selection; `scripts/check_semantics_partition.py` keeps them from escaping both selections.

### D6. What a version bump may and may not do — the non-waivable parse-compatibility floor

Migration operates on *decoded* messages, therefore old bytes must always parse under the current descriptor before any migration can run. This yields a two-tier evolution rule, documented in `docs/state-migration.md` and spec'd in the `wire-schemas` delta:

- **Never, at any version:** retyping an existing field number, or reusing a reserved number. Those break parse itself; no `state_schema_version` bump can buy them back. `--update` makes this concrete: Dataflow restores state bytes verbatim and re-decodes with the new binary — a retyped tag turns restore into a parse error or, worse, a silently misread value, on every key at once.
- **With a bump + migration + corpus entry (versioned blobs only):** semantic reinterpretation of an existing field (units, encoding, invariants), moving data to a new field number (old number `reserved`, migration copies/transforms), or structural reshaping into new submessages. The old field's bytes still parse; the migration function is what gives them their new meaning.
- **Without a bump (everything, as today):** purely additive changes — new fields, new enum values, `reserved` removals.

Coder wire-compatibility is the other half of the `--update` story and is pinned in the `proto-coders` delta: `DeterministicProtoCoder` encodes every version as raw `SerializeToString(deterministic=True)` bytes and decodes without migrating, and the state spec IDs (`"memory"`, `"continuation"`, `"llm_cache"` at [dofn.py:199](../../../src/beam_agents/core/dofn.py:199)–201) are frozen — renaming a spec or changing its coder class is a state-compatibility break `--update` cannot survive, bump or no bump.

## Risks / Trade-offs

- **Hot-path cost of the hook** → seven call sites each pay one function call and one integer compare when state is current-version (the permanent case between bumps). No copy, no re-serialize on the fast path; the 15 ms p50 overhead budget is unthreatened, and `overhead_ms` would show it if not.
- **Wedged keys on future-version state** → deliberate (D4), but it does mean a rollback after an `--update` to a bumped binary stalls every key the new binary touched. Mitigation: the failure names itself precisely; `docs/state-migration.md` documents "roll forward, don't roll back" as the operational rule for crossing a version bump; and rollback before any key commits under the new version remains clean.
- **Migration functions are user-invisible but correctness-critical** → a buggy migration corrupts state at scale, lazily. Mitigation: purity requirement (D2), the corpus replay asserting field-level outcomes for every historical version, and `core/` mutation-gate coverage over `migration.py` itself.
- **Corpus growth** → linear in versions × versioned types, plus frozen builder code per version. Accepted: bumps are meant to be rare and expensive; the corpus growing is the visible price that keeps them so.
- **Cross-version protobuf serialization drift** → historical corpus bytes were written under a pinned `protobuf`; upgrades may change serialization details. Already handled: the corpus asserts semantic (field-level) equality, never byte equality, per archived D6.
- **Chain longevity** → chains are never pruned in v0.x, so `migrate_to_current` must stay correct from every historical version. Accepted; the completeness tests keep every link present, and TTL GC makes truly ancient state rare in practice without being load-bearing for correctness.

## Migration Plan

This change migrates nothing — it installs the machinery at `CURRENT_STATE_SCHEMA_VERSION = 1`, where every path is the identity:

1. Land registry + hook + corpus restructure in one change (they are one capability; a partial landing would claim a gate it cannot enforce).
2. Deployed pipelines see zero behavior change; `--update` across this change is safe (no coder, state-spec, or byte change).
3. Rollback is a plain revert: no v2 state exists to strand.
4. The first real use is the first breaking change, which follows the documented checklist: bump the constant, register the three migration steps, freeze `v<old>/` in the corpus, add `v<new>/` fixtures — with CI red until all artifacts exist.

## Open Questions

- Should `buf breaking` (or a descriptor-diff script) be added so *mechanical* breaking-change detection backs up the corpus gate? Inherited from the archived schema change; the corpus gate reduces its urgency but does not replace tag-level linting.
- Do long-term MemoryStore rows (Bigtable/Redis/Firestore/SQL, keyed by `(key, seq)`) eventually need a parallel version regime? They persist outside keyed state and outlive TTL GC; out of scope here but the registry design should be reusable.
- Should the effector validate a version marker on `ToolIntent` someday? Today intents are unversioned by design (D1); if the effector ever grows schema-dependent behavior, the topic contract needs its own versioning conversation.
- Should a `state_migrations` runtime counter (per-message-type migrations applied) be added under `beam_agents.runtime` so operators can watch a bump drain through a live pipeline? One-line follow-up to `add-runtime-metrics`; deliberately not folded in here.
