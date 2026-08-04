## ADDED Requirements

### Requirement: Working-memory GC never preempts a live suspension's deadline

When an activation suspends, the DoFn SHALL arm `TTL_TIMER` at `max(activation clock, the persisted Continuation's deadline_ms) + ttl_ms`, so the working-memory garbage collector cannot fire while the suspension is still awaiting its answer. A completing activation SHALL continue to arm `TTL_TIMER` at the activation clock plus `ttl_ms`, unchanged.

When the HITL fallback escalates and rewrites the continuation with a later `deadline_ms`, the DoFn SHALL re-arm `TTL_TIMER` past the new deadline as well, so a bounded escalation chain cannot outlive working memory. The `Deny` and `Drop` routes end the suspension and SHALL NOT re-arm it.

#### Scenario: A HITL window longer than the memory TTL still reports its timeout

- **WHEN** an activation suspends with a HITL window longer than the configured `ttl_ms`, and both the watermark and processing time then advance past the deadline
- **THEN** the configured timeout route runs and emits its result, rather than the suspension being garbage-collected with nothing emitted on any output

#### Scenario: The timeout is reported regardless of which clock advances first

- **WHEN** a suspension whose HITL window exceeds `ttl_ms` times out, with the watermark advanced before processing time in one run and after it in another
- **THEN** both runs emit the same timeout output, so the outcome does not depend on the order the two clocks advance

#### Scenario: An escalation carries the memory TTL forward with the deadline

- **WHEN** the fallback escalates, extending `deadline_ms` beyond the mark the original suspension armed, and the watermark then advances past that original mark
- **THEN** the continuation and its pending intents survive, and an answer to the escalation still resumes the activation

#### Scenario: A completing activation's TTL mark is unchanged

- **WHEN** an activation completes without suspending
- **THEN** `TTL_TIMER` is armed at the activation clock plus `ttl_ms`, and the existing wipe-and-re-arm behavior is unaffected

### Requirement: A TTL fire over a live suspension is reported, not silent

`TTL_TIMER` is watermark-domain and `HITL_TIMER` is real-time, so ordering their marks in one quantity cannot prevent event time from passing the mark before real time reaches the deadline. When `TTL_TIMER` fires while a `Continuation` is still live, the DoFn SHALL emit an `ActivationError` on `.errors` with reason `ttl_wiped_suspension`, carrying the entity key and a detail naming the dropped suspension's `seq` and `deadline_ms`, in addition to the state wipe it already performs. A TTL fire with no live continuation SHALL continue to emit nothing.

#### Scenario: A TTL fire over a live suspension emits a dead-letter record

- **WHEN** `TTL_TIMER` fires while a `Continuation` is live
- **THEN** an `ActivationError` with reason `ttl_wiped_suspension` is emitted on `.errors` naming the key, `seq`, and `deadline_ms`, and every state spec is still cleared

#### Scenario: A TTL fire with no live suspension stays silent

- **WHEN** `TTL_TIMER` fires for a key with no `Continuation` in state
- **THEN** every state spec is cleared and nothing is emitted on any output
