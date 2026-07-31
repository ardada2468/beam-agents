# State compatibility across releases

Keyed state in this runtime outlives the binary that wrote it. A Dataflow
streaming job upgraded with `--update` keeps every key's `MemoryBlob`,
`Continuation`, `LlmCacheBlob`, pending intents and activation counter, and
hands those bytes to a job built from a *different release* of `beam-agents`.
An operator planning that upgrade needs to know what is promised, what merely
happens to work, and which changes an author is allowed to make without
breaking a running production job.

This document is that contract. Its companion,
[`docs/state-migration.md`](state-migration.md), is the mechanism: the
`state_schema_version` regime, the registered lazy migrations, and the golden
corpus. This one is the promise those mechanics back.

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD and MAY are to be
interpreted as described in RFC 2119.

## What is promised

1. **Adjacent-release readability.** Keyed state written by release N SHALL be readable by release N+1
   — directly for additive schema changes, and through the registered lazy
   migration for a change that bumps `state_schema_version`.
2. **A stable wire format.** The keyed-state encoding SHALL remain exactly
   `SerializeToString(deterministic=True)` of the schema message, with no additional framing
   — no length prefix, no version byte, no compression, no envelope of any
   kind. Protobuf wire-compatibility rules therefore govern
   state compatibility directly, with nothing of ours layered on top.
3. **`--update` SHALL succeed.** Dataflow `--update` from a job running release
   N to a job built from release N+1 SHALL succeed, unless that release's notes
   declare it a migration release and state otherwise.

The promise is deliberately narrow — **adjacent releases, forward only, on
Dataflow** — because that is exactly what the nightly gate runs. A promise
wider than the evidence is folklore, which is the state this document exists to
end.

## What is not promised

- **Skip-level updates (N to N+k) are best-effort.** Additive changes compose
  trivially and lazy migrations chain through intermediate versions, so
  skipping releases will usually work. It is not gated — the nightly runs
  exactly one hop — so it is not promised. When state matters, step through
  releases one at a time.
- **Downgrades are unsupported.** Rolling a job back to an earlier release
  after newer state has been committed is unsupported at any version distance.
  Old code cannot interpret fields it has never heard of, and a blob stamped
  above the running binary's `CURRENT_STATE_SCHEMA_VERSION` deliberately wedges
  the key rather than corrupting it. Roll forward, do not roll back.
- **Cross-version byte-identity is not promised.** `deterministic=True`
  produces byte-identical output only within a pinned `protobuf` library
  version. The cross-release promise is *semantic decodability* — the same
  thing the golden corpus asserts (field equality, never byte equality) — not a
  stable byte string.
- **Flink savepoint compatibility is out of scope.** Flink carries state over a
  different mechanism with different failure modes; it deserves its own gate
  before it earns a promise.
- **Cross-runner state portability is out of scope.** State written by a job on
  one runner is not promised to be readable by a job on another.

## The compatibility surface

Two things must hold for `--update` to succeed, and only one of them is about
bytes.

**The bytes.** Restored state is re-decoded with the new binary's descriptor,
so previously written bytes must always parse. That is what makes the
additive-only rule non-negotiable and why a field number is never retyped or
reused at any version (see [`docs/state-migration.md`](state-migration.md)).

**The graph.** Before carrying anything over, Dataflow runs a compatibility
check: it matches steps between the old and new job graphs *by transform name*,
and matches the state carried into a step *by state spec id and coder*. So a
purely internal refactor inside `RunAgent`'s expansion — a renamed step, a
renamed state spec, a swapped coder class — breaks `--update` with zero
byte-level changes and no test of the stored bytes going red. The transform
names inside `RunAgent` and the state spec ids in `core/dofn.py` (`"memory"`,
`"continuation"`, `"llm_cache"`, `"pending"`, `"seq"`) are part of this
contract, and the table below gives that its own row.

## The compatibility table

Consult this before touching a proto, a coder, or the DoFn's graph shape.

| Change class | Old state readable? | `--update` safe? | Required action |
| --- | --- | --- | --- |
| Add an optional proto field (new tag number) | yes | yes | Add a golden fixture for the new field's encoding; the pre-field fixture stays, proving old bytes still decode. |
| Add an enum value (proto3 open enums) | yes | yes | Document the unknown-value behaviour at the read site, as `TraceEvent.SUSPENDED` does. |
| Add a new state spec (new keyed-state cell) | yes | yes | None — the new cell simply starts empty on every existing key. |
| Remove / renumber / retype a field | no | no | Forbidden. Use `reserved` plus a new tag number, or a versioned migration; no bump can buy a retyped tag back. |
| Change `DeterministicProtoCoder`'s encoding (framing, prefix, compression) | no | no | Forbidden — the raw-proto wire format is the contract, and every stored blob would stop parsing at once. |
| Rename a `RunAgent`-internal transform or a state spec id | yes | no | Avoid. Stored bytes are fine but step matching fails; if truly unavoidable, ship `--transform_name_mapping` guidance in the release notes. |
| Change a state cell's coder type | no | no | Forbidden without a versioned migration release; the coder is matched by the update compatibility check. |
| `state_schema_version` bump + lazy migration | via migration | yes | Follow the bump checklist in [`docs/state-migration.md`](state-migration.md): bump the constant, register a step for every versioned message, freeze the outgoing golden corpus, add the incoming one. |

The table is this document's contract. A change that invents a new
state-affecting class MUST add its row in the same pull request —
`tests/core/test_state_compat_doc.py` fails when a row goes missing.

## How the promise is verified

- **Offline, every PR:** the golden corpus (`tests/core/golden/`) decodes
  committed bytes from every historical schema version with the current
  bindings, migrates them through the registered chain, and asserts field
  equality. That covers every message shape, in process.
- **Nightly, on real Dataflow:** `tests/dataflow/test_update_compat.py`
  launches a streaming job at the **previous released version** (installed from
  PyPI), drives it to hold live keyed state — one key suspended mid-activation
  with a persisted `Continuation` and a pending `APPROVAL` intent, one key with
  populated working memory — then replaces the job in place with `--update` at
  current head and asserts from outside the pipeline that the suspension
  resumes with its pre-update snapshot, the memory key echoes its pre-update
  marker, and a fresh key completes. That is the promise, executed.

A replacement job refused by Dataflow's compatibility check fails while the
original keeps running, so the gate reports that shape as a **compatibility
failure** naming both versions and the service's reason — distinct from a
**state-loss failure** (the update took effect but the state did not survive)
and from **infrastructure failure** (quota, worker pools, PyPI, credentials),
which is never a verdict about compatibility. See
[`docs/ci.md`](ci.md#the-dataflow-update-compatibility-gate) for triage.

Until the first PyPI release exists, the nightly runs a **self-update** leg —
head launched, head updated — through the same phases and assertions. It proves
the harness and that head's graph is update-compatible with itself, and its
report is labelled `SELF-UPDATE (BOOTSTRAP)` in capitals so it can never be
mistaken for cross-version evidence. The cross-version leg arms itself
automatically the night after the first release is tagged.

## Release procedure

The `--update` gate is release-blocking.

1. Before tagging a release, confirm the most recent nightly `dataflow` run is green.
   A skipped run (no GCP project configured) is not a green run.
2. A red gate with a **compatibility** or **state-loss** classification blocks
   the tag. Resolve it by fixing the incompatibility, or by shipping the
   documented migration path and saying so in the release notes —
   never by weakening the gate. The gate carries no `xfail`, no flake-tolerant skip and
   no retry, and the `dataflow` make target fails on an empty test selection,
   so it cannot be quietly deselected instead.
3. A red gate classified as **infrastructure** is not a verdict: fix the
   environment and get a real run before tagging.
4. A release that bumps `state_schema_version` is a migration release. It ships
   its migrations and its golden corpus in the same release (see the bump
   checklist in [`docs/state-migration.md`](state-migration.md)), carries a
   `breaking` changelog entry, and requires a MINOR version bump per
   [`docs/releasing.md`](releasing.md).
