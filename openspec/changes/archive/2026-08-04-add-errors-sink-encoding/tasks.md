# Tasks: add-errors-sink-encoding

## 1. Wire schema

- [x] 1.1 Write failing wire-schema tests: `ActivationErrorRecord` round-trip of all four fields, deterministic serialization byte-stability, and the importable-bindings test updated to expect eight top-level classes (scenarios: "All fields round-trip", "Deterministic serialization is byte-stable", "Bindings are importable from the installed package")
- [x] 1.2 Add `ActivationErrorRecord` (entity_key bytes, reason string, detail string, event_time_ms int64) to `protos/beam_agents.proto` with the envelope-convention comment; regenerate via `scripts/gen_proto.sh`, commit bindings, verify diff-clean regen and re-export from `beam_agents._protos`

## 2. ActivationError event time

- [x] 2.1 Write failing dofn tests: element-path dead letters carry the element's `event_time_ms`, HITL/TTL timer-path dead letters carry the scheduled firing time, and a repeated failure path yields equal records (scenarios: "Element-path dead letters carry the element's event time", "Timer-path dead letters carry the scheduled firing time", "Replay produces identical records")
- [x] 2.2 Add `event_time_ms: int = 0` to `ActivationError`; thread a deterministic timestamp through the `_dead_letter` chokepoint and `_error` builder and populate every emission site (element path, HITL timer, TTL timer, orphan/admission paths) — no wall-clock reads; metrics accounting unchanged

## 3. Error encoders and resolver

- [x] 3.1 Write failing encoder tests for `core/error_records.py`: envelope encoding round-trips through `AgentEnvelope`→`ActivationErrorRecord`, keyed by `entity_key`, byte-identical on repeat; BigQuery row has hex `entity_key` and native `reason`/`detail`/`event_time_ms` (scenarios: "Encoded record round-trips through AgentEnvelope", "Encoding is deterministic", "Row carries all triage fields")
- [x] 3.2 Implement `core/error_records.py` (`serialize_error_envelope`, `activation_error_to_row`) with deterministic serialization and `tuple[bytes, bytes]` output typing, modeled on `observability/exporters.py`
- [x] 3.3 Write failing resolver/transform tests: `resolve("errors_to", ...)` feeds the scheme writer encoded elements for kafka and row mappings for bigquery, and a `RunAgent` run with a stub-wrapped default encoding delivers encoded records while `.errors` still exposes `ActivationError` (scenarios: "errors_to kafka URI resolves to an encoding writer", "errors_to bigquery URI resolves to a row-encoding writer", "A configured errors sink receives encoded records, not dataclasses"); update the `test_transform.py` comment that documented the old gap
- [x] 3.4 Implement `_WriteErrors` in `core/transform.py` and special-case `errors_to` in `DefaultSinkResolver.resolve`

## 4. Intent dead-letter unification

- [x] 4.1 Write failing tests: with `intents_to` + kafka `errors_to`, a serialization-failed intent arrives envelope-encoded with reason `intent_dead_letter` and detail JSON carrying reason/`intent_id`/`seq`/`tool_name` and `event_time_ms == created_at_ms`; with bigquery `errors_to` it arrives as a row; with `errors_to` unset, `dead_letter` stays exposed with no write attached (scenarios: "A dead-lettered intent reaches the errors sink as a unified record", "BigQuery errors sinks accept intent dead letters", "Dead letters and activation errors share one sink schema", "dead_letter stays exposed without an errors sink")
- [x] 4.2 Add `REASON_INTENT_DEAD_LETTER` to the reason vocabulary; rewrite the `RunAgent.expand` dead-letter branch to map dead letters into `ActivationError` and reuse the single resolved errors sink; delete `encode_intent_dead_letter` from `actions/write_intents.py`

## 5. Failure-streak alarm example

- [x] 5.1 Write the failing TestStream example test (`tests/examples/test_failure_streak_alarm.py`): encoder-produced envelope bytes decoded with public parsing only, threshold-`N` alarm fires exactly once, `N-1` stays silent, `2N-1` alarms once (scenarios: "The alarm fires once at the threshold", "Below-threshold keys stay silent and counts reset after alarming", "The example consumes the documented wire format")
- [x] 5.2 Write `docs/errors.md`: errors-sink record schema per scheme, the envelope-is-a-convention note, and the complete failure-streak alarm pipeline whose DoFn the test embeds verbatim (cross-referencing comments in both files)

## 6. Gates

- [x] 6.1 Full offline suite: `pytest`, `ruff`, `mypy --strict`, diff-clean proto regen, `scripts/check_semantics_partition.py`; confirm no docker needed by any new test and coverage does not decrease
- [x] 6.2 Mutation gate on the touched core files: cover `intent_dead_letter_to_error` from inside the selection, exclude the four equivalent `deterministic=` survivors with reasons, and raise `transform.py`'s no-tests ceiling for the pipeline-only sink wiring
