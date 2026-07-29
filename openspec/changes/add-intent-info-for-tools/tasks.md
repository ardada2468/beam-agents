# Tasks: add-intent-info-for-tools

TDD order per project convention: each group writes the scenario-derived tests first, watches them fail for the right reason, then implements.

## 1. IntentInfo and registry recognition (tool-registry spec)

- [x] 1.1 Write failing tests in `tests/tools/` from the tool-registry delta scenarios: IntentInfo frozen/hashable/standalone-import; declaring side-effect tool gets `accepts_intent=True` with `intent` excluded from argument model and JSON schema; non-declaring tool byte-identical schema/behavior; `ToolDefinitionError` for positional `IntentInfo`, misnamed keyword-only `IntentInfo`, and `intent: IntentInfo` on a `side_effect=False` tool; `intent: str` stays an ordinary argument; string-annotation (`from __future__ import annotations`) recognition; an `"intent"` key in a declaring tool's args fails validation (resolve the Pydantic extra-key mechanism per design)
- [x] 1.2 Implement `src/beam_agents/tools/intent_info.py` (`@dataclass(frozen=True, slots=True) IntentInfo`: `intent_id`, `entity_key`, `seq`, `step_index`, `attempt`) and export it from `beam_agents.tools.__init__`
- [x] 1.3 Implement recognition in `src/beam_agents/tools/registry.py`: annotation resolution with string-annotation fallback, `Tool.accepts_intent`, `_build_argument_model` exclusion, and the three `ToolDefinitionError` near-miss cases; all pre-existing tool-registry tests pass unmodified
- [x] 1.4 `ruff` + `mypy --strict` clean on `src/beam_agents/tools/`

## 2. Effector injection (effector-execution spec)

- [x] 2.1 Write failing tests in `tests/effector/` from the effector-execution delta scenarios: declaring tool receives `IntentInfo` equal to the executing intent's wire fields; non-declaring tool invoked with no `intent` keyword; validation unaffected (valid args pass, invalid args still `REJECTED` pre-invoke); `"intent"` inside `args_json` of a declaring tool → `REJECTED` without invocation; re-executed same `intent_id` receives byte-identical identity; async declaring tool injected and awaited
- [x] 2.2 Implement injection in `src/beam_agents/effector/runner.py`: `EffectorToolRunner.run(..., intent_info: IntentInfo | None = None)`, `_invoke` adds `intent=` iff `accepts_intent`, `execute_intent` builds `IntentInfo` from the `ToolIntent`; all pre-existing effector tests pass unmodified
- [x] 2.3 `ruff` + `mypy --strict` clean on `src/beam_agents/effector/`

## 3. Docs: the honest exactly-once contract

- [x] 3.1 Update `docs/effector.md`: state the two-sided contract (runtime: deterministic intent IDs + at-most-one completed execution per `intent_id`; tool keying its downstream on `intent_id` → exactly-once effects, e.g. Stripe `Idempotency-Key`, Redis `SETNX`, keyed upsert; otherwise at-least-once across crash recovery); replace the args-derived-key example with the `*, intent: IntentInfo` form; delete the "natural follow-up" paragraph

## 4. E2E gate strong form (effectively-once-e2e-gate spec; requires `add-effectively-once-e2e-gate` harness landed)

- [x] 4.1 Update `tests/semantics/_e2e/ledger.py`: two-counter ledger keyed by `intent_id` — unconditional attempt `INCR` plus first-writer-wins effective `SETNX` — with unit coverage in the gate's offline harness tests
- [x] 4.2 Update the `charge` tool in `tests/semantics/_e2e/agent.py` to declare `*, intent: IntentInfo` and record attempt + effective through the new ledger
- [x] 4.3 Update `tests/semantics/_e2e/assertions.py` and `tests/semantics/test_effectively_once_e2e.py` to the strong form: effective executions exactly 1 per minted tool `intent_id`; attempts ≥ 1, per-member ≤ `1 + kills`, duplicated members ≤ `kills × max_concurrent_partitions`, exactly 1 when kills = 0; keep the intents-topic per-key/per-intent cross-check
- [x] 4.4 Run the docker-backed gate and confirm the strong assertions hold (passed at BEAM_AGENTS_E2E_EVENTS=600, SEED=42: 538/538 intents exactly one attempt and one effective execution across 3 effector kills + TM kill + full replay; the kills=0 sub-claim is exercised by phase B, which fires no kills — the harness has no zero-kill mode; full 10,000-event volume runs in CI's integration lane); offline `pytest -m "semantics and not integration"` stays green

## 5. Wrap-up

- [x] 5.1 Full local gates: `pytest` 704 passed (sole failure was the docker gate running concurrently with itself — resolved by the dedicated passing run), ruff + `mypy --strict` clean on `src/`, semantics-partition check OK, `git diff` shows no `_pb2`/proto edits; coverage ratchet is enforced by CI's quality lane (all new code paths carry direct tests)
- [x] 5.2 Validate and archive coordination: `openspec validate add-intent-info-for-tools`; note in the PR that archive must follow `add-reference-effector` and `add-effectively-once-e2e-gate`
