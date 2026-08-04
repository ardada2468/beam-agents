# Design: harden-memory-stores-offline

## D1 — Fake the client at the import seam, not with `importorskip`

The existing offline Bigtable file (`test_bigtable.py`) monkeypatches the *real* client class and
`importorskip`s the library. That pattern cannot transfer to Redis and Firestore: their clients are
mirrored only into the `integration` dependency group, and the `ci` unit lane
(`uv sync --group lint --group typecheck --group test`) and the `quality` lane (which produces the
ratcheted `coverage.xml`) never install them. An `importorskip`-based suite would skip in exactly
the lanes the coverage gap lives in, moving nothing.

Instead, the new suites inject fake client modules into `sys.modules` (via
`monkeypatch.setitem`/`setattr`), satisfying the constructors' lazy imports
(`from redis import asyncio`, `from google.cloud import firestore`,
`from google.cloud.firestore_v1.base_query import FieldFilter`) with typed fakes the test owns.
This works identically whether or not the real library is installed, so the suites run — and are
coverage-counted — in every lane. The Firestore fixture also patches the `google.cloud` package
attribute, because in a full environment the emulator suite's collection may have already bound the
real submodule there.

The repo rule "fake the external client, not the store" is preserved: the store's own code runs
unmodified end to end; only the wire is fake. Bigtable keeps its existing pattern (the client rides
into every lane via `apache-beam[gcp]`, and the file's fakes already sit behind the real class
path); the new tests extend that file in its own style.

## D2 — Call-shape tests, not behavioral fake servers

A fake Redis that evaluated the Lua script, or a fake Firestore that simulated transaction
contention, would quietly become a second — competing — definition of the guard semantics, and a
bug in the fake could mask a bug in the script. The emulator conformance suite
(`_conformance.py` under `-m integration`) remains the sole interchangeability authority.

The offline tests therefore assert only the two halves the client-side code owns:

- **What the store sends**: row keys / document IDs / hash keys and their entity scoping, the
  framed or celled value layout, filter and range construction, order/limit, the script's
  argument vector, the transaction's read-then-set sequencing.
- **How it decodes what it receives**: envelope extraction (frame slicing, cell selection, field
  reads), the absent-row `None` paths, the applied/not-applied mapping from the backend's reply,
  and result assembly (client-side sort, startswith belt, limit truncation).

D-3 lived entirely in the first half: the document ID for `case/2` contained a `/`. The Firestore
suite pins that class of defect as *encoding invariants* — for any key the conformance suite could
accept (including `/`, `#`, `%`, unicode), the document ID contains no `/`, never collides with
Firestore's reserved forms, is injective, and round-trips — rather than as examples only.

## D3 — Property tests where an ordering or injectivity contract exists

Two helpers carry contracts that examples under-specify, so they get hypothesis properties in
addition to pinned examples:

- `bigtable._prefix_successor(p)`: every byte string prefixed by `p` sorts strictly below the
  successor (and at/above `p`), and an all-`0xff` prefix yields `None` — this is what makes the
  row-range scan exactly "keys with this prefix".
- `firestore._doc_id`: injective over `(entity_key, key)` and never produces `/` — the D-3
  invariant itself.

## D4 — No `src/` refactor

Verified before writing tests: everything the offline suites need is already reachable. The pure
helpers are module-level functions or staticmethods importable without any client
(`beam_agents.memory.stores.firestore` imports clean by design — that is the import-boundary
requirement), and every I/O path is reachable through the constructor's lazy import seam. So this
change adds no `src/` code, which keeps it in the same class as `close-core-mutation-gaps`:
coverage for behavior the specs already require.
