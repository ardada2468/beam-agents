## MODIFIED Requirements

### Requirement: Effectively-once execution under induced worker kills

Across a run of 10,000 events with worker processes killed at randomized points throughout, execution SHALL be counted at the side effect itself — a Redis ledger the test tool writes — not inferred from message counts, since duplicate messages are expected by design. The gate's `charge` tool SHALL declare the keyword-only `intent: IntentInfo` parameter and record two countings per intent: an **effective execution** via first-writer-wins keyed on `intent_id` (a `SETNX`-style write that succeeds exactly once per key), and a raw **attempt** via an always-increment counter. The gate SHALL assert the strong form for idempotent tools: **zero lost effects** (every minted tool `intent_id` has an attempt count of at least one) and **exactly one effective execution per `intent_id`** — including under induced kills, because a crash-window re-execution re-arrives with the identical `intent_id` and loses the first-writer race. Raw attempts retain the crash-window bound demonstrated by this gate's earlier form (a `SIGKILL` between the tool's effect and the effector's durable completion record unavoidably re-invokes after lease expiry): no member's attempt count may exceed `1 + kills`, and the number of members with more than one attempt may not exceed `kills × max_concurrent_partitions`. In a run with **no** kills, attempts SHALL also be exactly one per intent. This is the honest exactly-once contract: the runtime supplies deterministic intent IDs and at-most-one completed execution per `intent_id`; a tool keying its downstream effect on `intent_id` gets true exactly-once effects; a tool that does not remains at-least-once across crash recovery.

#### Scenario: A killed effector worker never loses an effect, and effective executions stay exactly-once

- **WHEN** effector worker processes are `SIGKILL`ed at randomized points while 10,000 events' intents flow through them, and the killed workers' claims are recovered by lease expiry and consumer-group rebalance
- **THEN** the ledger records at least one attempt for every minted tool `intent_id` (no lost effect), exactly one effective execution for every minted tool `intent_id` (first-writer-wins on the deterministic `intent_id` collapses every crash-window re-invocation), and every attempt count exceeding one is attributable to the crash window (bounded by the number of kills and the in-flight limit)

#### Scenario: A killed TaskManager restores checkpointed state without duplicating pre-kill work

- **WHEN** the Flink TaskManager is killed mid-run and the job restores from its last completed checkpoint
- **THEN** every output committed before the kill appears without duplication after the restore, and the restore itself adds no ledger attempts and no effective executions

#### Scenario: A wiped pipeline replayed from zero does not double-mint an effect

- **WHEN** the job is cancelled after a kill and the identical pipeline is resubmitted against the same immutable spool with completely fresh pipeline state, replaying every event from the beginning (in-place source resumption after restore is unavailable on this runner — a documented limitation)
- **THEN** every replayed activation re-mints a byte-identical `intent_id`, the effector collapses every redelivered intent, the ledger's attempt and effective-execution counts are byte-for-byte unchanged from before the replay (the replay adds zero of either), and every event is accounted for after recovery
