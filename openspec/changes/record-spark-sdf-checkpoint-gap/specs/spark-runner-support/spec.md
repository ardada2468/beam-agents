## ADDED Requirements

### Requirement: Scenarios blocked by the Spark SDF bundle-checkpoint gap are declared skips naming it

Every conformance scenario whose only obstacle on the spark leg is the Spark portable runner's lack of a registered bundle checkpoint handler SHALL be declared `Skip` on that leg, and its reason SHALL name four things: the missing runner capability; the concrete mechanism that requires it (the leg's `unbounded_per_element` spool-source SDF calling `defer_remainder` to self-checkpoint while tailing the spool); that the gap is runner-level and therefore identical for every adapter; and the date of the job-server run that observed it. The declaration SHALL NOT be recorded as an infrastructure failure: a job server that starts a SparkContext, builds its graph, and then fails the invocation on a missing runner capability has produced a runner verdict, not a stack failure. No scenario blocked solely by this gap SHALL remain declared runnable on the spark leg.

#### Scenario: The gap is recorded as a runner verdict, not an infra failure

- **WHEN** the spark leg's submission-stall classifier reports an infrastructure failure but the job-server log shows the invocation failing on the absent bundle checkpoint handler
- **THEN** the outcome is triaged as a portable-runner capability gap and converted into declared skips, rather than retried as a stack problem or recorded as an adapter failure

#### Scenario: Every affected scenario carries the specific reason

- **WHEN** the spark skip inventory is read for the scenarios blocked by this gap
- **THEN** each reason names the missing bundle checkpoint handler, the SDF ingest path that requires it, its adapter-independence, and the observation date, and none of them is a generic "unsupported"

#### Scenario: A skip is lifted only by evidence

- **WHEN** a change proposes returning any of these scenarios to runnable on the spark leg
- **THEN** it SHALL cite either a Spark portable runner that registers a bundle checkpoint handler or a replacement non-SDF ingest path for the leg, and SHALL NOT convert the leg's pipeline to batch mode to obtain a passing run

### Requirement: A spark leg with zero executing cells is never promotion-ready

While every conformance scenario is declared a skip on the spark leg, the leg SHALL be reported as executing zero cells, and the promotion gate SHALL NOT be satisfiable regardless of how many consecutive green scheduled weekly runs accumulate: a run that submits no pipeline asserts nothing about the Spark runner. The weekly status report SHALL continue to reset the promotion window when a spark skip is added inside it, so the additions that produced the all-skip state restart the clock on their own. The leg SHALL keep running on its weekly cadence so that the state is re-checked and re-published rather than forgotten, and the reduction in coverage SHALL be legible at the declaration site rather than inferable only from a status run.

#### Scenario: Vacuous green cannot promote

- **WHEN** four consecutive green scheduled weekly runs accumulate while every spark scenario is declared a skip
- **THEN** the promotion verdict is not ready, on the grounds that the leg executed no cells, and no promotion change may cite those runs as gate evidence

#### Scenario: Adding the skips resets the window

- **WHEN** the run-to-skip conversions land inside the trailing promotion window
- **THEN** the weekly status step reports the added skip declarations as coverage shrinkage and the promotion clock restarts from zero

#### Scenario: The consequence is stated where the declarations live

- **WHEN** a reader opens the scenario declarations after the conversion
- **THEN** the file states that the spark leg now has zero executing cells, that this makes promotion unreachable by design, and what would restore executing cells
