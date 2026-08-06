Closed the offline blind spot `coverage-baseline.toml`'s M2 entry records: the Redis, Bigtable and
Firestore `MemoryStore` backends' client-side logic — document-ID/row-key encoding, request
shaping, response decoding, teardown — is now exercised by offline unit tests against faked
clients, moving the three modules from 0% to 100% offline branch coverage. The class of defect
that previously hid until the first emulator run (D-3, the Firestore `/`-in-key encoding bug) is
now pinned by property tests in the required ci lane. Test-only — no runtime behavior changed.
