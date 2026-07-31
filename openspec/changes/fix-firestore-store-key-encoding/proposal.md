## Why

The `verify-live-infrastructure` run executed `memory/stores/firestore.py` against the Firestore
emulator for the first time and found the backend cannot store a record key containing `/`:

```
FAILED tests/memory/stores/test_firestore_emulator.py::TestFirestoreMemoryStoreConformance::test_prefix_search_returns_ordered_bounded_entity_scoped_results
FAILED tests/memory/stores/test_firestore_emulator.py::TestFirestoreMemoryStoreConformance::test_search_round_trips_the_full_record
E  ValueError: A document must have an even number of path elements
   path = ('ltm-636857925c27', '656e746974792d61#case', '2')
```

`FirestoreMemoryStore._doc_id` built the document ID as `f"{entity_key.hex()}#{key}"`. Firestore
treats `/` inside a document ID as a **path separator**, so a key of `case/2` produces a
three-element path — an invalid document reference.

Hierarchical keys are ordinary, not exotic: the shared `MemoryStoreConformance` suite
(`tests/memory/stores/_conformance.py`) deliberately uses `case/1`, `case/2`, `case/3`, `note/1`, and
builds its prefix-search requirement on them. The Redis and Bigtable backends pass that identical
suite. The three backends were therefore **not interchangeable**, which is the specific guarantee a
shared conformance suite exists to provide.

The same run also found that `make type` fails whenever the `integration` group is installed, because
`AsyncClient.close()` is untyped and `--strict` rejects the call. No CI lane installs `typecheck` and
`integration` together, so the gate has been green by group selection rather than by typechecking.

## What Changes

- `FirestoreMemoryStore._doc_id` percent-encodes the record key into the document ID. Only the
  document ID is encoded; the `key` **field** — which `search` orders and range-scans over — keeps the
  key verbatim, so prefix semantics are unchanged.
- `FirestoreMemoryStore.close` casts the client's `close` attribute to a typed callable, so the module
  typechecks both with and without the Firestore client installed.

## Capabilities

### Modified Capabilities

- `memory-stores`: the Firestore backend now satisfies the shared store conformance suite for
  hierarchical keys, matching the Redis and Bigtable backends.

## Impact

- **Modified code:** `src/beam_agents/memory/stores/firestore.py` only.
- **Data layout:** document IDs for keys containing characters outside the unreserved set change
  shape. The backend could not previously store such keys at all, so no existing readable data is
  invalidated. Keys that were already storable (no `/`, no reserved characters) are unaffected apart
  from percent-encoding of non-unreserved characters.
- **Gates:** unblocks the two failing cells in `tests/memory/stores/test_firestore_emulator.py`, and
  makes `make type` pass under `uv sync --all-groups`.
