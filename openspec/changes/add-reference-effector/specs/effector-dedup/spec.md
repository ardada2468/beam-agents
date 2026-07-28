## ADDED Requirements

### Requirement: DedupStore is a three-state claim protocol

The effector SHALL define a `DedupStore` protocol whose `claim(intent_id, lease_ms)` returns exactly one of three outcomes: `Claimed` (carrying an opaque ownership token), `InFlight` (a live lease is held elsewhere), or `Done` (carrying the terminal `ToolResult` already stored for that `intent_id`). The protocol SHALL also provide `complete(intent_id, token, result, ttl_ms)`, which stores the terminal result only while the caller still owns the claim, and `release(intent_id, token)`, which voluntarily abandons an unexecuted claim. Claiming SHALL be atomic: two concurrent claims on the same `intent_id` SHALL yield at most one `Claimed`.

#### Scenario: A first claim on an unseen intent is granted

- **WHEN** `claim` is called for an `intent_id` the store has never seen
- **THEN** the outcome is `Claimed` with a non-empty ownership token

#### Scenario: Concurrent claims yield a single owner

- **GIVEN** two workers claiming the same `intent_id` concurrently
- **WHEN** both calls return
- **THEN** exactly one receives `Claimed` and the other receives `InFlight`

#### Scenario: A completed intent reports Done with its stored result

- **GIVEN** an `intent_id` completed with a `ToolResult` of status `OK` and a payload
- **WHEN** `claim` is called again for that `intent_id`
- **THEN** the outcome is `Done` and the carried result compares field-equal to the stored one

#### Scenario: Completion by a non-owner is refused

- **GIVEN** an `intent_id` claimed with token `A`
- **WHEN** `complete` is called with a different token `B`
- **THEN** the store refuses the write and the record still reflects token `A`'s claim

#### Scenario: Release frees the intent for a new owner

- **GIVEN** an `intent_id` claimed with token `A` and not yet executed
- **WHEN** `release(intent_id, A)` is called and another worker claims it
- **THEN** the second claim returns `Claimed` with a new token

### Requirement: A Done record republishes rather than re-executes

When `claim` returns `Done`, the effector SHALL publish the stored `ToolResult` verbatim and SHALL NOT invoke the tool. Republishing SHALL be safe to repeat: a duplicate `ToolResult` on the results topic is correlated downstream by `intent_id` and discarded by the runtime when no live continuation matches.

#### Scenario: A redelivered completed intent republishes without execution

- **GIVEN** an `intent_id` already `Done` in the dedup store
- **WHEN** the same intent is redelivered and processed
- **THEN** the stored result is published, the tool callable is never invoked, and the offset is committed

#### Scenario: The republished result is byte-identical to the stored one

- **WHEN** a stored result is republished
- **THEN** its serialized bytes equal the bytes stored at completion time

### Requirement: An in-flight claim is waited on, never skipped

When `claim` returns `InFlight`, the effector SHALL retry the claim with bounded backoff until the claim resolves to `Claimed` (the prior lease expired) or `Done` (the prior owner completed). It SHALL NOT commit the offset for an intent whose claim it never acquired and whose result it never observed.

#### Scenario: Waiting resolves once the prior owner completes

- **GIVEN** an `intent_id` held `InFlight` by another worker
- **WHEN** that worker completes the intent while the waiting worker is backing off
- **THEN** the waiting worker's next claim returns `Done` and it republishes the stored result

#### Scenario: Waiting resolves once the lease expires

- **GIVEN** an `intent_id` held by a worker that never completes
- **WHEN** the lease expires
- **THEN** the waiting worker's next claim returns `Claimed` and it proceeds to execute

#### Scenario: An in-flight intent is never skipped

- **WHEN** an intent's claim returns `InFlight` for the entire processing attempt
- **THEN** no offset is committed for that intent and no result is published for it

### Requirement: Lease and result TTLs bound in-flight and terminal records

A claim SHALL carry a lease of `lease_ms` after which it is no longer live and the `intent_id` becomes re-claimable. A terminal record SHALL carry a TTL of `result_ttl_ms` after which it is removed and a redelivered intent is treated as unseen. Configuration SHALL enforce `lease_ms > tool_timeout_ms` so that a live lease implies a live owner.

#### Scenario: An expired lease is re-claimable

- **GIVEN** a claim taken with `lease_ms` and never completed
- **WHEN** `lease_ms` has elapsed and a new `claim` is issued
- **THEN** the outcome is `Claimed` with a new token

#### Scenario: An unexpired lease is not re-claimable

- **GIVEN** a claim taken with `lease_ms` and never completed
- **WHEN** a new `claim` is issued before `lease_ms` elapses
- **THEN** the outcome is `InFlight`

#### Scenario: An expired terminal record reads as unseen

- **GIVEN** a terminal record stored with `result_ttl_ms`
- **WHEN** `result_ttl_ms` has elapsed and the intent is redelivered
- **THEN** `claim` returns `Claimed` and the intent is executed again

### Requirement: The Redis dedup store implements the protocol with SET NX PX

The Redis implementation SHALL take a claim with a single `SET <intent_id> <claim-tagged value> NX PX <lease_ms>` and SHALL distinguish `InFlight` from `Done` by decoding the tag of the existing value. `complete` and `release` SHALL be executed as server-side compare-and-set / compare-and-delete against the ownership token, so a worker whose lease expired cannot overwrite the record of a subsequent owner. Lease and TTL semantics SHALL come from Redis key expiry rather than client-side clocks.

#### Scenario: Claim, complete, and re-claim round-trip against Redis

- **GIVEN** a running Redis instance
- **WHEN** an `intent_id` is claimed, completed with a `ToolResult`, and claimed again
- **THEN** the outcomes are `Claimed`, then `Done` carrying the field-equal result

#### Scenario: A stale owner cannot clobber the new owner's record

- **GIVEN** worker `A`'s claim has expired and worker `B` has claimed the same `intent_id`
- **WHEN** worker `A` calls `complete` with its stale token
- **THEN** the write is refused and `B`'s claim remains intact

### Requirement: The Bigtable dedup store implements the protocol with CheckAndMutateRow

The Bigtable implementation SHALL use `CheckAndMutateRow` on a row keyed by `intent_id` to make claiming conditional: the predicate SHALL match a live claim (the claim column present with a lease expiry not yet reached, encoded big-endian so lexicographic value comparison agrees with numeric comparison), the true branch SHALL report `InFlight`, and the false branch SHALL write the new claim. A stored terminal result SHALL be reported as `Done` only while it is unexpired. `complete` SHALL be conditional on the claim column still carrying the caller's token.

Terminal-record expiry SHALL be decided by a read-time predicate over an explicit expiry column, encoded big-endian exactly as the lease expiry is, and `complete` SHALL write that column from its `ttl_ms` argument. The column family's age-based garbage-collection rule reclaims the space afterwards; it SHALL NOT be the mechanism that decides expiry. A GC rule is table-level, so it cannot express a per-call `result_ttl_ms`, and it is asynchronous and best-effort, so a record can be served arbitrarily long after its TTL has elapsed — neither is compatible with "an expired terminal record reads as unseen".

Every value predicate SHALL be evaluated against the most recent cell version only. Bigtable columns are versioned, and a re-claim after a lease expiry leaves the superseded owner token in place as an older cell; an ownership predicate that can match any version would let a worker whose lease expired complete over its successor's claim.

#### Scenario: Claiming is conditional in a single conditional mutation

- **GIVEN** a Bigtable table with the effector's column family
- **WHEN** two workers claim the same `intent_id` concurrently
- **THEN** exactly one conditional mutation writes a claim and the other observes the live claim as `InFlight`

#### Scenario: Lease expiry is expressed as a value-range predicate

- **GIVEN** a claim whose encoded lease expiry precedes the current time
- **WHEN** a new claim is attempted
- **THEN** the live-claim predicate does not match, the false branch writes the new claim, and the outcome is `Claimed`

#### Scenario: A completed row reports Done

- **GIVEN** a row whose result column holds a serialized `ToolResult`
- **WHEN** `claim` is called for that `intent_id`
- **THEN** the outcome is `Done` and the parsed result compares field-equal to the stored one

#### Scenario: Terminal-record expiry is a read-time predicate, not a GC rule

- **GIVEN** a terminal record completed with a `ttl_ms` shorter than the column family's GC `maxage`, so garbage collection cannot have removed it
- **WHEN** `ttl_ms` has elapsed and the intent is redelivered
- **THEN** the live-result predicate does not match, the false branch writes a new claim, and the outcome is `Claimed`

#### Scenario: A superseded owner cell cannot satisfy the ownership predicate

- **GIVEN** a row re-claimed after a lease expiry, so the claim and owner columns each carry the new cell over the superseded one
- **WHEN** the worker holding the superseded token calls `complete`
- **THEN** the ownership predicate matches only the most recent cell, the conditional mutation takes its false branch, and `complete` returns false
