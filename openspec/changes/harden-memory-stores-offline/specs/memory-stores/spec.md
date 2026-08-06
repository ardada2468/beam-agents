## ADDED Requirements

### Requirement: Backend client-side logic is exercised offline

Every live-client `MemoryStore` backend (Redis, Bigtable, Firestore) SHALL have offline unit
coverage of the logic it executes client-side — key/row/document-ID encoding, request shaping
(filters, ranges, ordering, limits, script argument vectors), response decoding (envelope
extraction, absent-row `None` paths, applied/not-applied mapping), and client teardown — driven
through faked clients injected at the backend's lazy-import seam. The suites SHALL run with no
backend client library installed and no docker, so the offline lane can catch a
client-side defect (such as an invalid document-ID encoding) before any emulator run. The fakes
SHALL NOT reimplement a backend's atomic guard semantics; the shared emulator conformance suite
remains the sole interchangeability authority.

#### Scenario: The offline suites run with every backend client absent

- **WHEN** the Redis and Firestore offline suites run in an environment without the `redis` or
  `google-cloud-firestore` packages (the `ci`/`quality` lane environment)
- **THEN** they construct each store against a faked client module and pass, rather than skipping

#### Scenario: Firestore document-ID encoding invariants hold for every accepted key

- **WHEN** document IDs are derived for keys the shared conformance suite accepts — including
  keys containing `/`, `#`, `%`, and non-ASCII characters (property-based over keys)
- **THEN** no ID contains `/` or collides with Firestore's reserved forms, distinct
  `(entity_key, key)` pairs yield distinct IDs, the encoding round-trips, and the `key` field in
  the save payload carries the key verbatim

#### Scenario: Request shaping is pinned offline per backend

- **WHEN** `save` and `search` run against a recording fake client
- **THEN** the Redis leg pins the prefixed entity hash key, the `[key, big-endian seq, envelope]`
  script arguments, and the escaped literal `HSCAN` glob; the Bigtable leg pins the row range
  `[row_key(entity, prefix), prefix-successor)` with the latest-cells filter and forwarded limit;
  and the Firestore leg pins the entity equality filter, the `[prefix, prefix + U+FFFF)` key
  bounds, the key ordering, and the limit

#### Scenario: Decode paths are pinned offline per backend

- **WHEN** scripted client replies are fed through `load` and `search`
- **THEN** an absent row returns `None` on every backend, the Redis frame decodes the envelope
  from byte 8 on, the Bigtable read decodes the `rec` cell and skips rows without one, and each
  backend's `applied` result reflects the backend reply rather than a client-side re-check
