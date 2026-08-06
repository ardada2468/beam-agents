# Tasks: fix-firestore-store-key-encoding

## 1. Fix

- [x] 1.1 Percent-encode the record key into the Firestore document ID in
  `FirestoreMemoryStore._doc_id`, leaving the `key` field verbatim.
- [x] 1.2 Cast the client's `close` attribute to a typed callable so the module typechecks with and
  without `google-cloud-firestore` installed.
- [x] 1.3 Record the layout decision in the module docstring, including why only the ID is encoded.

## 2. Verify

- [x] 2.1 `tests/memory/stores/test_firestore_emulator.py` passes in full against the emulator
  (requires the `firestore-emulator` service).
- [x] 2.2 `make type` passes under `uv sync --locked --all-groups`, and still passes under CI's
  `lint typecheck test` selection.
- [x] 2.3 `make lint` clean.
- [x] 2.4 The Redis and Bigtable emulator conformance suites still pass, confirming the shared suite
  is satisfied identically across all three backends.
