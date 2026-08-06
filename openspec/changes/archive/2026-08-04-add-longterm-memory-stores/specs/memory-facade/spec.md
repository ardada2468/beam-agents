## ADDED Requirements

### Requirement: Long-term store access is explicit via memory.longterm

The `Memory` facade SHALL expose a `longterm` property returning an activation-scoped long-term handle when `AgentConfig.longterm_memory` is configured, and raising an actionable error naming that configuration field when it is not. Access SHALL be explicit only: no working-tier operation (`get`/`set`/`delete`/`append`/`ring`), no compaction path, and no runtime code path SHALL consult the long-term store implicitly, and the facade's own methods SHALL remain free of external I/O — the handle is the only surface that reaches a store. The handle SHALL be constructed per activation with the activation's frozen `entity_key`, `seq`, and `now_ms`, over a store client built once per DoFn instance in `setup()` and closed in `teardown()`.

#### Scenario: Unconfigured pipelines behave exactly as today

- **WHEN** an agent touches only working memory on a pipeline with no `longterm_memory` configured
- **THEN** the activation completes with no store constructed and no external I/O, and accessing `ctx.memory.longterm` raises an error naming `AgentConfig.longterm_memory`

#### Scenario: Working-tier operations never reach the store

- **WHEN** an agent performs working-memory reads and writes, including a compaction-triggering write, on a pipeline with a long-term store configured
- **THEN** the store records no operation — only explicit `longterm` calls reach it

### Requirement: Long-term saves stage in the activation and flush only on success

`longterm.save(key, value)` SHALL perform no store I/O when called; it SHALL stage an upsert record stamped with the activation's `seq` and `now_ms`. Staged upserts SHALL be flushed through the store only after the agent returns successfully, in the commit tail before the DoFn commits the bundle-atomic effects; a failed or timed-out activation SHALL flush nothing. A flush failure SHALL fail the activation closed (routed to the errors output, nothing committed). Because a replayed activation deterministically re-stages byte-identical upserts and the store's upsert is seq-guarded, duplicate flushes from bundle retries SHALL converge on identical rows — this is the sanctioned invariant-5 exception, and it is the only in-pipeline external write path.

#### Scenario: A failed activation flushes nothing

- **WHEN** the agent raises after staging a long-term save
- **THEN** the element is routed to errors, the store records no write, and keyed state is unchanged

#### Scenario: A bundle retry across a completed flush converges

- **GIVEN** an activation whose flush succeeded but whose bundle commit was forced to fail (chaos harness)
- **WHEN** the bundle retries and the activation replays
- **THEN** the retry stages byte-identical upserts, the re-flush applies via the equal-seq guard, the stored rows are byte-identical to the first attempt's, and the activation's intents are byte-identical

#### Scenario: A flush failure fails the activation closed

- **WHEN** the store raises during the commit-tail flush
- **THEN** the activation fails, the element is routed to errors with a typed error record, and no output, intent, or state mutation commits

### Requirement: Long-term reads are point-in-time with a read-your-writes overlay

`longterm.load` and `longterm.search` SHALL execute inline within the activation and SHALL consult the activation's staged upserts first, merging them over store results, so an agent observes its own writes in program order before any flush. Reads SHALL be documented as point-in-time, and the replay discipline SHALL be normative: a long-term write MUST be computed from replay-stable inputs (the event, working memory, replay-cached model output) and MUST NOT be conditioned on a same-activation long-term read of the same key. A read failure SHALL propagate and fail the activation closed.

#### Scenario: Staged saves are visible to reads before any flush

- **WHEN** an agent stages `longterm.save("profile", v)` and then calls `longterm.load("profile")` and `longterm.search` with a matching prefix in the same activation
- **THEN** both reads reflect the staged record, and the store has still performed no write

#### Scenario: Blind upserts keep replay path-stable

- **GIVEN** an agent following the discipline (its saves are computed from the event and working memory, never from a same-key long-term read)
- **WHEN** the retry-determinism chaos gate forces a bundle retry after a completed flush
- **THEN** the replayed activation emits byte-identical intents and stages byte-identical upserts whether or not its reads observe the first attempt's flushed rows
