## ADDED Requirements

### Requirement: MemoryStore is an async ABC with load, save, and search

`beam_agents.memory.stores` SHALL provide a `MemoryStore` abstract base class with three async methods: `load(entity_key, key)` returning the stored record for `(entity_key, key)` or `None`; `save(record)` performing the seq-guarded idempotent upsert; and `search(entity_key, prefix, limit)` returning matching records. The base class SHALL own the shared, backend-independent behavior — record envelope encoding/decoding, the seq-guard comparison rule, and argument validation (non-empty `key`, non-negative `seq`, positive `limit`) — so every backend inherits one definition of the contract's semantics. A `close()` method SHALL release backend clients. The subpackage SHALL be importable, and the ABC and in-memory store usable, with no backend client library installed.

#### Scenario: Load returns the saved record or None

- **WHEN** a record is saved for `(entity_key, "profile")` and `load` is called for that pair and for an absent key
- **THEN** the first call returns a record field-equal to the saved one and the second returns `None`

#### Scenario: Invalid arguments are rejected before any I/O

- **WHEN** `save` is called with an empty `key` or a negative `seq`, or `search` is called with a non-positive `limit`
- **THEN** a `ValueError` naming the offending argument is raised and no backend operation is attempted

#### Scenario: The subpackage imports without any client library

- **WHEN** every `beam_agents.memory.stores` module is imported in an environment where the Bigtable, Redis, Firestore, and SQLAlchemy packages are all blocked
- **THEN** the imports succeed, and only constructing a backend store raises the missing-dependency error naming the `memory-stores` extra

### Requirement: Records are versioned envelopes stored byte-identically across backends

Every backend SHALL store the same value bytes for a given record: a `LongTermRecord` protobuf message carrying `state_schema_version`, `key`, opaque `value` bytes, `seq`, and `updated_at_ms`, serialized deterministically. `seq` SHALL additionally be stored backend-natively as an 8-byte big-endian unsigned integer wherever the backend's guard primitive compares raw bytes, so that lexicographic comparison agrees with numeric comparison. The proto addition SHALL be additive only: no existing message or field in `protos/beam_agents.proto` changes, and no `state_schema_version` bump is required for existing blobs.

#### Scenario: Envelope bytes are pinned by a golden test

- **WHEN** a fixed `LongTermRecord` is encoded by the shared envelope encoder
- **THEN** its bytes equal the committed golden bytes, and decoding them yields a field-equal record

#### Scenario: Big-endian seq encoding preserves numeric order

- **WHEN** any two non-negative seq values are encoded (property-based)
- **THEN** the lexicographic order of the encodings equals the numeric order of the values

### Requirement: Save is an idempotent upsert guarded by seq

`save(record)` SHALL apply the write if and only if the incoming `seq` is greater than or equal to the seq currently stored for `(entity_key, key)`; a lower incoming seq SHALL leave the stored row unchanged and report that it did not apply. An equal-seq save SHALL be accepted (a replayed activation legitimately rewrites its own row with byte-identical content). The guard SHALL be enforced atomically by each backend's own primitive, so that a duplicate or delayed flush can never regress a newer row. Every implementation SHALL pass one shared conformance suite exercising the full seq-pair matrix (lower / equal / higher against absent and present rows).

#### Scenario: Replayed flush converges on the identical row

- **WHEN** the same record (same `entity_key`, `key`, `seq`, bytes) is saved twice, as a retried bundle's flush would
- **THEN** both saves report applied and the stored row after the second save is byte-identical to after the first

#### Scenario: A stale seq cannot regress a newer row

- **GIVEN** a row stored at seq 7
- **WHEN** a save arrives for the same `(entity_key, key)` at seq 5
- **THEN** the save reports not-applied and `load` still returns the seq-7 record

#### Scenario: A newer seq overwrites

- **GIVEN** a row stored at seq 5
- **WHEN** a save arrives at seq 7 with different value bytes
- **THEN** the save applies and `load` returns the seq-7 record

### Requirement: Search is a bounded per-entity key-prefix scan

`search(entity_key, prefix, limit)` SHALL return at most `limit` records belonging to `entity_key` whose `key` starts with `prefix`, ordered by `key` ascending. An empty `prefix` SHALL match all of the entity's records (still bounded by `limit`). Records of other entities SHALL never be returned. Backends whose scan primitive takes a pattern SHALL treat the prefix as a literal (metacharacters such as SQL `%`/`_` are escaped).

#### Scenario: Prefix search returns ordered, bounded, entity-scoped results

- **GIVEN** records `case/1`, `case/2`, `case/3`, and `note/1` for entity A and `case/9` for entity B
- **WHEN** `search(A, "case/", limit=2)` is called
- **THEN** exactly `case/1` and `case/2` are returned, in that order

#### Scenario: Prefix metacharacters are literal

- **GIVEN** records under keys `a%b` and `axb` for one entity
- **WHEN** `search` is called with prefix `a%`
- **THEN** only the `a%b` record is returned

### Requirement: A URI-scheme factory builds stores with import-free validation

`build_memory_store(scheme, parts)` SHALL construct the store a parsed URI names: `memory://` (the in-memory store), `redis://`, `bigtable://<project>/<instance>/<table>`, `firestore://<project>/<collection>`, and any other scheme treated as a SQLAlchemy async URL. URI-grammar validation SHALL be performable without importing any client library, raising an actionable `ValueError` at configuration-construction time for malformed URIs; client libraries SHALL be imported only inside the chosen store's constructor. The in-memory store SHALL implement the full contract with an injectable clock and no external process, and SHALL be the default store the offline conformance suite runs against.

#### Scenario: Each scheme builds its store

- **WHEN** the factory is called with each recognized scheme and a SQLAlchemy-style URL
- **THEN** it returns the matching store type, and validation of the URI grammar performed no client import

#### Scenario: A malformed URI fails at construction time

- **WHEN** a `bigtable://` URI missing its table segment is validated
- **THEN** a `ValueError` naming the field and the expected grammar is raised before any pipeline exists

### Requirement: The Bigtable store guards upserts with CheckAndMutateRow

The Bigtable implementation SHALL key rows as `hex(entity_key) + "#" + key` in a single column family holding a `seq` column (big-endian u64) and a `rec` column (the envelope bytes). `save` SHALL be a single `CheckAndMutateRow` whose predicate matches a stored seq strictly greater than the incoming seq, evaluated against the most recent cell version only; the predicate-true branch SHALL write nothing and the predicate-false branch SHALL write both cells. `search` SHALL be a row-range prefix scan over the entity's key range.

#### Scenario: The conditional mutation enforces the guard in one RPC

- **GIVEN** a row stored at seq 7
- **WHEN** saves arrive at seq 5 and then seq 8
- **THEN** the seq-5 save takes the predicate-true branch and writes nothing, and the seq-8 save takes the false branch and replaces both cells

#### Scenario: Only the latest seq cell decides the predicate

- **GIVEN** a row whose seq column carries superseded older cell versions beneath the latest
- **WHEN** a save's predicate is evaluated
- **THEN** only the most recent cell version participates, so a stale older cell can never satisfy or defeat the guard

### Requirement: The Redis store guards upserts with a server-side script

The Redis implementation SHALL store each entity's records in one hash keyed by a prefixed `hex(entity_key)`, with the field set to `key` and the value framed as the 8-byte big-endian seq followed by the envelope bytes. `save` SHALL execute as a server-side compare-and-set script that reads the field, compares the stored seq prefix to the incoming seq, and writes only when the incoming seq is greater than or equal — so the guard holds without client-side read-modify-write races. `search` SHALL scan the entity's hash with the prefix as a literal match and return results ordered by key.

#### Scenario: The script applies the seq-pair matrix atomically

- **WHEN** the shared conformance suite's seq-pair matrix runs against a live Redis
- **THEN** lower seqs report not-applied and leave the value untouched, and equal or higher seqs replace the framed value in a single scripted operation

### Requirement: The Firestore store guards upserts with a transaction

The Firestore implementation SHALL store one document per `(entity_key, key)` under the configured collection, carrying the native seq and the envelope bytes. `save` SHALL run the read-compare-write inside a Firestore transaction, which is the backend's atomic guard. `search` SHALL be an ordered range query over the entity's keys bounded by `limit`.

#### Scenario: Transactional guard under the conformance suite

- **WHEN** the shared conformance suite runs against the Firestore emulator
- **THEN** the seq-guard and prefix-search requirements hold, and a stale-seq save observed mid-transaction never overwrites a newer document

### Requirement: The SQLAlchemy store guards upserts with a portable transaction

The SQLAlchemy implementation SHALL use an async engine and a table with primary key `(entity_key, key)` plus `seq`, envelope, and `updated_at_ms` columns. `save` SHALL perform a transactional read-compare-write that is portable across dialects (no dialect-specific upsert clause in the base implementation), acquiring a row lock where the dialect supports it. `search` SHALL use an escaped `LIKE` prefix with `ORDER BY key` and a `LIMIT`. The required DDL SHALL be shipped as a documented statement, not executed implicitly at runtime. The store SHALL pass the full conformance suite offline against `sqlite+aiosqlite`.

#### Scenario: The conformance suite passes offline on sqlite

- **WHEN** the shared conformance suite runs against `sqlite+aiosqlite://` with no docker
- **THEN** every load/save/search and seq-guard scenario passes in the unit tier

#### Scenario: Store operations never block the event loop

- **WHEN** any store method runs under the async lint rules and against the async engine
- **THEN** no synchronous driver call executes on the bridge loop (enforced by ruff ASYNC checks and the async-engine-only implementation)
