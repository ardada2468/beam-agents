# State schema migration

Keyed state is protobuf, never pickle, and it outlives the binary that wrote
it: Dataflow `--update` restores state bytes verbatim and re-decodes them with
the new binary's descriptor, and Beam offers no way to enumerate user-state
keys, so there is no "migrate everything at deploy" job on any runner. What the
new binary does on **read** is the entire migration opportunity. This document
is the policy for changing the state schemas, and `core/migration.py` plus the
golden corpus under `tests/core/golden/` are its executable form.

## The versioned set

Exactly three messages carry `state_schema_version` and participate in
migration — the keyed-state blobs the runtime alone reads and writes:

| Message | State spec | Written by |
|---|---|---|
| `MemoryBlob` | `MEMORY` | `Memory.to_blob` |
| `Continuation` | `CONTINUATION` | `ActivationContext.build_continuation` |
| `LlmCacheBlob` | `LLM_CACHE` | `ActivationContext` via `ReplayCache.to_blob` |

Every writer stamps `CURRENT_STATE_SCHEMA_VERSION` from `core/migration.py` —
the one authoritative constant.

The other five messages (`ToolIntent`, `ToolResult`, `TraceEvent`,
`AgentEnvelope`, `ActivationErrorRecord`) are **unversioned and additive-only,
with no bump escape hatch**. They cross service boundaries — the outbox topic,
the results topic, the errors topic, exporters, external producers — where a
lazy in-pipeline migration cannot reach; a "migrated" reading that exists only
inside `RunAgent` would desynchronize every external reader. Breaking one of
them means a `beam_agents.v2` proto package and a topic cutover: a different,
intentionally expensive event.

## The two-tier evolution rule

**Never, at any version:** retype an existing field number, or reuse a
`reserved` one. Migration operates on *decoded* messages — it runs only after a
successful parse — so previously written bytes must always parse under the
current descriptor. No `state_schema_version` bump can buy this back. Under
`--update` a retyped tag turns state restore into a parse error or a silently
misread value, on every key at once.

**Without a bump (all eight messages, the default):** purely additive changes —
new fields with new numbers, new enum values, removals via `reserved`. Old
bytes decode with the new field at its proto3 zero default; add a fixture to
the *current* corpus directory pinning the new field's encoding (the original
fixture keeps proving pre-field bytes still decode).

**With a bump (the three versioned blobs only):** semantic reinterpretation of
an existing field (units, encoding, invariants), moving data to a new field
number (old number `reserved`, the migration copies/transforms), or structural
reshaping into new submessages. The old field's bytes still parse; the
registered migration function is what gives them their new meaning.

## How migration runs

- **Lazily, on first read.** `_AgentDoFn` passes every `MEMORY` /
  `CONTINUATION` / `LLM_CACHE` read through `migrate_to_current()` before
  interpreting any field — the element paths and both timer callbacks.
  Current-version blobs take an identity fast path (one integer compare, no
  copy). A parsed version of `0` reads as the baseline `1` (proto3
  zero-default; the runtime has only ever written `1`).
- **Below the hook, the coder is migration-invariant.**
  `DeterministicProtoCoder` encodes every version as raw
  `SerializeToString(deterministic=True)` bytes and decodes without migrating,
  so encoded state stays a pure, version-agnostic function of message content
  and `--update` hands restored bytes to the hook unaltered. The state spec IDs
  (`"memory"`, `"continuation"`, `"llm_cache"`) and their coder class are
  frozen: a spec rename or coder swap is a state-compatibility break no bump
  can license.
- **Rewrite happens at the next commit, never at read time.** Migration writes
  nothing; the migrated view reaches durable state only through a successful
  activation's existing commit writes (which stamp the current version). A
  failed activation, refused resume, or stale timer fire leaves the old bytes
  untouched — the atomic-commit invariant applies to migration too. A key that
  never commits again keeps its old bytes until TTL GC; that is fine, because
  chains are never pruned and a later read still upgrades them.
- **Migration functions are pure single steps.** Registered per
  `(message type, from_version)`, each takes a version-`n` message and returns
  a new version-`n + 1` one. No clocks, no randomness, no I/O: they run on the
  element path, and a replayed bundle must produce an identical migrated view
  (feeding the same cache keys and intent IDs) or retry determinism breaks.
  `migrate_to_current` composes them one step at a time and verifies every
  step advanced the stamp; a gap raises a typed `MissingMigrationError`.

## A version from the future

A blob stamped above `CURRENT_STATE_SCHEMA_VERSION` raises
`StateSchemaFromFutureError` before any field is interpreted, and the DoFn does
not catch it: the bundle fails, the runner retries, and the key wedges until an
operator rolls the binary forward. This deliberately breaks the module's usual
route-to-`.errors` posture. Dead-lettering would drop the element while leaving
the state unreadable — every later element on the key would follow, silent and
unbounded — and processing anyway would interpret newer fields under older
semantics, the exact corruption this regime exists to prevent. A wedged key is
loud and losslessly recoverable: nothing was mutated or emitted, so the retry
under the rolled-forward binary succeeds.

**Operational rule: roll forward, don't roll back.** Future-version state means
a newer binary already ran on the key (an `--update` forward followed by a
rollback). Rolling back across a version bump stalls every key the bumped
binary committed on; rolling forward again is always available and always
clean. A rollback is safe only before any key commits under the new version.

## The bump checklist (CI-enforced)

A change that raises `CURRENT_STATE_SCHEMA_VERSION` from `n` to `n + 1` does
not merge until all of the following exist — the completeness tests in
`tests/core/test_schema_compat.py` go red naming exactly what is missing:

1. **Bump the constant** in `src/beam_agents/core/migration.py` (the writers
   follow automatically — they stamp from the constant).
2. **Register a migration step** `(type, n)` for *every* versioned message in
   `core/migration.py`, pure and single-step, even when it only re-stamps.
3. **Freeze the outgoing corpus**: leave `tests/core/golden/v<n>/` and its
   builder map in `generate.py` byte-for-byte untouched, forever.
4. **Add the incoming corpus**: a new builder map for `n + 1` in `generate.py`,
   then run `uv run python tests/core/golden/generate.py` — it writes only
   `v<n + 1>/` — and commit the new directory.
5. The corpus replay then proves every historical fixture decodes, migrates
   through the new chain, and lands field-equal on the expected current-version
   message.

## Dataflow `--update` implications

- Additive changes and bumps are both `--update`-safe by construction: coder
  bytes, state spec IDs, and coder classes never change, so restore always
  succeeds, and the read-path hook upgrades each key the first time it is
  touched. A hot key under a long-lived updated pipeline may surface
  old-version blobs arbitrarily long after a bump — the chain from every
  historical version stays registered for exactly this reason.
- Old readers rewriting newer state during an update window do not drop
  unknown fields: proto3 (3.5+) preserves unknown fields through re-encode,
  and the wire-schemas tests pin it.
- Crossing a bump, prefer draining or forward-only updates; see the
  from-the-future section above for what a rollback does.
