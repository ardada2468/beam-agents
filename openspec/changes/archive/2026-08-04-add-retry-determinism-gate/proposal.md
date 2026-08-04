## Why

`project.md` names the retry-determinism gate as a release-blocking `-m semantics` correctness check — "chaos wrapper forcing bundle retries: zero extra FakeLLM calls, byte-identical intents" — and correctness invariants #2 and #3 stake the entire effectively-once argument on it. But nothing exercises it: there is no chaos wrapper, no semantics test, and `make test-semantics` runs only inside the docker-gated `integration` workflow with a `test $? -eq 0 -o $? -eq 5` tolerance that lets an empty selection pass green. The invariant is asserted in prose and nowhere in CI. This change builds the chaos bundle-retry wrapper, writes the semantics test that *is* the promised scenario, and wires it as a required, offline CI gate so a runtime that quietly re-calls the provider or emits drifting intents on a bundle retry fails the build.

## What Changes

- Introduce a **chaos commit-failure helper** (`beam_agents.testing.chaos`, test infrastructure — never re-exported from the root API): a context manager that monkeypatches `_AgentDoFn._commit` for its duration to fail the first commit matching a caller-supplied predicate over `ActivationResult`, then lets every subsequent commit — including Beam's own retry of that same bundle — proceed normally. No production code changes: Beam's own `BundleBasedDirectRunner` already retries a failed bundle (up to 4 attempts) with genuine per-key state rollback, verified empirically against this runtime; the helper only needs to trigger one targeted failure.
- Add the semantics test `tests/semantics/test_retry_determinism.py`, marked `-m semantics`, encoding the actually-achievable shape of the invariant: an activation calls the model, stages an intent, and suspends; a resume re-issues the identical request (same `seq`, so it reads the `LLM_CACHE` committed at suspend time) before completing; the resume's own first commit attempt is chaos-forced to fail. The test asserts, from the pipeline's committed `.traces` and `.intents` output, that the total real-provider-call count is unaffected by the forced retry and the committed intent matches the deterministic `intent_id_for(entity_key, seq, step_index)` formula.
- **Wire it as a required CI check:** split an offline semantics selection (`-m "semantics and not integration"`) into a new required `ci.yml` step/job so the gate runs on every PR without docker, and remove the exit-5 "no tests collected" tolerance for that selection so an empty or accidentally-deselected gate fails instead of passing. Docker-backed semantics tests (future effectively-once/state-compat gates) stay in `integration.yml`.

## Capabilities

### New Capabilities
- `retry-determinism-gate`: The chaos commit-failure helper (predicate-targeted, fail-once, zero production-code surface), and the `-m semantics` retry-determinism test proving a chaos-forced retry of a resumed activation adds zero real provider calls (asserted via `.traces` cache-hit attributes) and commits the deterministically-expected `ToolIntent` — plus its required, offline CI wiring.

### Modified Capabilities
<!-- The stateful-agent-runtime, agent-context, llm-replay-cache, and fake-llm specs are consumed unchanged; this change verifies their existing requirements rather than altering any of them. -->
(none — the runtime, context, replay-cache, and FakeLLM contracts are consumed and verified as-is)

## Impact

- **Depends on C09** (the stateful `_AgentDoFn` runtime — the atomic-commit tail and suspend/resume path this gate exercises); also consumes `agent-context` (deterministic `intent_id`/`ToolIntent` emission via `intent_id_for`) and `fake-llm`/`ActivationContext.call_model`'s cache-hit trace attribute. The gate is a pure verifier: it provides no mechanism, only forces one targeted commit to fail and asserts the invariant the runtime already promises. A runtime whose resume path doesn't read the suspend-committed `LLM_CACHE` will (correctly) fail this gate.
- **New code:** `src/beam_agents/testing/chaos.py` (the commit-failure context manager), `tests/semantics/test_retry_determinism.py` (new semantics tier directory). No changes to `core/dofn.py`, `core/context.py`, or any other production module — the fault is injected by monkeypatching from the test module only.
- **CI/build:** `Makefile` gains an offline semantics target (`test-semantics-offline` selecting `-m "semantics and not integration"`, no exit-5 tolerance); `.github/workflows/ci.yml` runs it as a required step; `integration.yml` keeps the docker-backed `make test-semantics`. `project.md`'s required-checks/testing-tier notes are updated to record the offline retry-determinism gate.
- **Dependencies:** `apache-beam` `TestPipeline`/`TestStream` and the classic (streaming-capable) DirectRunner; `FakeLLM`. No new third-party dependencies, no docker for this gate.
- **Verification surface:** the semantics test itself is the verification; additionally a small unit test asserts the chaos helper fails exactly the first matching commit (not zero times, not every time) and leaves non-matching commits untouched.
