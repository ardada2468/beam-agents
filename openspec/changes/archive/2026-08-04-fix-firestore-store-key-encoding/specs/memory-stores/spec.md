## MODIFIED Requirements

### Requirement: The Firestore store guards upserts with a transaction

The Firestore `MemoryStore` SHALL guard every upsert with a transaction, running the
read-compare-write inside one so the sequence guard holds atomically. It SHALL additionally encode
the record key into its document ID such that any key the shared store conformance suite accepts is
storable — in particular a key containing `/`, which Firestore would otherwise interpret as a
document-path separator. The stored `key` field SHALL retain the key verbatim, so ordered prefix
search is unaffected by the encoding.

#### Scenario: Transactional guard under the conformance suite

- **WHEN** the shared conformance suite runs against the Firestore emulator
- **THEN** the seq-guard and prefix-search requirements hold, and a stale-seq save observed mid-transaction never overwrites a newer document

#### Scenario: A hierarchical key round-trips

- **WHEN** a record is saved under a key containing `/`, such as `case/2`
- **THEN** the save succeeds, `load` returns the record, and the key reported on the loaded record is
  `case/2` rather than an encoded form

#### Scenario: Prefix search is unaffected by document-ID encoding

- **WHEN** records are saved under `case/1`, `case/2`, `case/3` and `note/1` for one entity, and a
  prefix search for `case/` is issued with a limit of 2
- **THEN** the search returns `case/1` and `case/2` in that order, scoped to the entity

#### Scenario: The three live backends stay interchangeable

- **WHEN** the shared `MemoryStoreConformance` suite runs against the Redis, Bigtable and Firestore
  backends
- **THEN** all three satisfy it identically, with no backend rejecting a key shape the others accept
