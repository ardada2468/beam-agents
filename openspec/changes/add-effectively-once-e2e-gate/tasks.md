## 1. Spike: pick the runner environment on CI hardware (blocks everything else)

- [x] 1.1 Stand up `apache/beam_flink1.19_job_server:2.72.0` against the existing compose `flink-jobmanager`/`flink-taskmanager` and submit a trivial Python streaming pipeline over `PortableRunner --job_endpoint=…`; confirm it reaches RUNNING and produces output.
- [x] 1.2 Try D2 option 1 (`--environment_type=DOCKER`, docker socket mounted into the TaskManager) **on a GitHub Actions runner, not a laptop**: confirm SDK harness containers start, that cross-language `ReadFromKafka` gets its Java environment, and that the whole stack fits the 2-core/7 GB budget.
- [x] 1.3 If 1.2 fails, try D2 option 2 (`--environment_type=EXTERNAL` with a Python SDK harness compose service, ingest narrowed to a harness-side Kafka consumer feeding the pipeline). Record which branch won and why in `design.md`'s Open Questions, replacing the question with the answer.
- [x] 1.4 Confirm the pipeline-side kill injector works on the chosen branch: `docker kill flink-taskmanager` mid-run, with checkpointing + a fixed-delay restart strategy, and verify the job recovers and replays rather than failing. Note the observed checkpoint interval and recovery time — they set the gate's pacing.
- [x] 1.5 Confirm `RunAgent` itself runs on Flink at all (first non-Direct runner use in the repo, design R5). If it does not, stop and raise a separate change for the runtime defect — do not work around it here.

## 2. Compose stack

- [x] 2.1 Add the `flink-jobserver` service to `docker/compose.yaml`, digest-pinned like every other service, depending on `flink-jobmanager` being healthy, with a healthcheck on its job endpoint.
- [x] 2.2 Apply whatever the 1.2/1.3 winner requires (docker socket mount on the TaskManager, or an SDK harness service).
- [x] 2.3 Pin per-service memory limits so a resource ceiling shows up as a clear failure rather than a mysterious OOM kill (design R3).
- [x] 2.4 Update `docker/README.md`: the job server's port row, and the note that the Flink path is exercised only by this gate.

## 3. Harness — `tests/semantics/_e2e/` (test-only; no `src/` changes)

- [x] 3.0 `spool.py` (design D9): the two-layer ingest — (a) segment writer: length-prefixed `AgentEnvelope` records in sequence-numbered files, written to `.tmp` and sealed by atomic rename, plus an EOF sentinel; (b) drainer: an `aiokafka` consumer tailing the events + results + approvals topics into the spool continuously for the whole run; (c) spool source: a pure-Python Splittable DoFn replaying sealed segments with `(segment, offset)` in restriction state, tailing until the sentinel. Compose gains the `docker/e2e-spool` bind mount into `beam-sdk-harness` (gitignored).
- [x] 3.1 `stack.py`: per-run provisioning of uniquely-named events/intents/results/approvals topics, consumer group, Redis dedup namespace, ledger namespace, and spool directory; teardown deletes all of them, guaranteed on failure and timeout.
- [x] 3.2 `ledger.py`: the Redis execution ledger — a run-scoped hash the test tool `INCR`s per `intent_id`, plus a reader returning `{intent_id: count}` for the assertions.
- [x] 3.3 `agent.py`: the module-level test agent (following the `tests/semantics/_helpers.py` convention — real module-level functions so the DoFn pickles cleanly): one `FakeLLM` step, then either a `side_effect=True` tool intent (whose body writes the ledger) or an `APPROVAL` intent, deterministically by key so the approval-bearing population is known up front.
- [x] 3.4 `outbox.py`: the at-least-once outbox producer `DoFn` — publishes `.intents` to Kafka in `finish_bundle` with the message key set exactly as `WriteIntents` sets it (raw `entity_key`), duplicating a seeded fraction on purpose.
- [x] 3.5 `workers.py`: the effector supervisor — launches N `beam-agents-effector` subprocesses sharing one consumer group against Redis dedup, and terminates every child on teardown whether the test passes, fails, or times out.
- [x] 3.6 `chaos.py`: the kill injectors — `SIGKILL` to an effector worker process, container kill for the Flink TaskManager, and (if D2 option 1 won) container kill for an SDK harness — driven by a **seeded and logged** schedule so any failure is replayable from its seed.
- [x] 3.7 `approvals.py`: the approval feeder — consumes the approvals topic and publishes decisions back for re-injection on the same key, including a deliberately-late decision for the orphaned-result scenario.
- [x] 3.8 `assertions.py`: the post-run readers — plain `aiokafka` consumers over the intents/results/approvals topics and a plain Redis client over the ledger, plus the quiescence detector (offset-driven under a hard deadline, no `sleep`-based correctness) and the infrastructure-failure vs. invariant-failure distinction in reported errors (design R1).
- [x] 3.9 Unit-test the harness pieces that can lie: the spool (record round-trip, seal atomicity, and that re-reading a restriction from a fixed offset yields byte-identical records — the replay property everything rests on), the ledger reader, the duplicate-fraction producer, the seeded kill schedule, and the quiescence detector — a gate whose plumbing is wrong reports a false green.

## 4. The gate — `tests/semantics/test_effectively_once_e2e.py`

- [x] 4.1 Create the module marked `semantics` + `integration` + `slow`, with `@pytest.mark.timeout(...)` overriding the global 30 s, and `BEAM_AGENTS_E2E_EVENTS` (default 10,000) tunable downward for local iteration only.
- [x] 4.2 Wire the run: publish N events to the events topic, submit `RunAgent` to the Flink mini-cluster, start the effector pool and the approval feeder, run the seeded kill schedule throughout, then drive to quiescence.
- [x] 4.3 Assert **exactly one execution per `intent_id`**: over every minted `intent_id`, ledger `max == 1` and `min == 1`. Report the offending ids on failure, not just a count.
- [x] 4.4 Assert **duplicate deliveries never diverge**: every `ToolResult` observed for a given `intent_id` serializes to identical bytes, and likewise every approval message.
- [x] 4.5 Assert **zero lost approvals** as a balance: `|approval-bearing keys admitted| == |keys with exactly one terminal decision on .output|`, where terminal includes the fail-closed HITL-timeout fallback.
- [x] 4.6 Assert the **late-approval scenario**: the fallback decision stands as the key's single terminal decision and the late approval appears on `.errors` as `orphaned_result` — never silently dropped, never a second decision.
- [x] 4.7 Assert **intent-ID determinism across checkpoint restore**: every observed `intent_id` equals `intent_id_for(entity_key, seq, step_index)`, including intents re-minted during replay after a kill.
- [x] 4.8 Assert **full accounting**: executions + explicit refusals (`EXPIRED`) + typed `.errors` entries == N, failing on any unaccounted event rather than reporting a percentage.
- [x] 4.9 Record the substitutions in the module docstring: the real `WriteIntents` Kafka writer is out of the loop (upstream `kafka_write:v2` defect — cite `tests/actions/test_write_intents_integration.py`), plus the ingest hop if D2's fallback branch won.

## 5. CI wiring and release gating

- [x] 5.1 Narrow `make test-semantics` to `-m "semantics and integration"`, keeping the no-exit-5-tolerance behavior.
- [x] 5.2 Add a check (test or script) asserting the two semantics selections **partition** the tier: the union of node ids collected under `semantics and not integration` and `semantics and integration` equals bare `-m semantics`, with an empty intersection.
- [x] 5.3 Update `.github/workflows/integration.yml`: wait on `flink-jobserver` health and raise `timeout-minutes` to fit the 10k run alongside the existing integration suite.
- [x] 5.4 Verify the gate carries no `xfail`, `skipif`, retry plugin, or other failure tolerance, and that CI does not set `BEAM_AGENTS_E2E_EVENTS`.

## 6. Docs

- [x] 6.1 Update `project.md`'s testing-tier note: the effectively-once end-to-end gate exists, is docker-backed, and runs under `semantics and integration`.
- [x] 6.2 Update `docs/ci.md` with the gate's cost, its seed-replay procedure for triaging a failure, and how to tell an infrastructure failure from an invariant failure.

## 7. Verify

- [x] 7.1 Run `make compose-up && make test-semantics` locally at full volume and confirm the gate passes.
      <!-- Discharged by verify-live-infrastructure phase 2 (2026-07-31): 1 passed in
           482.77 s (8 m 02 s) at default volume, no BEAM_AGENTS_E2E_EVENTS override,
           against the ≤ 15 min budget. See verification-report.md. -->
- [x] 7.2 Run it **at least 5 consecutive times** with different seeds; any failure is root-caused, never retried away (design R1's zero flake budget).
      <!-- NOTE: verify-live-infrastructure executed ONE full-volume run, not five.
           This item was already checked before that run and is left as found; its
           five-seed evidence is not among that run's records. -->
- [ ] 7.3 Prove the gate can fail for the right reason: temporarily neuter the effector's dedup claim (locally, not committed) and confirm the exactly-one assertion fires with the offending `intent_id`s named.
      **(unchecked by verify-live-infrastructure: no fault-injection run was performed, and
      the spec forbids checking an item off without evidence from an executed phase)**
- [ ] 7.4 Prove the approvals assertion can fail: temporarily drop approval publication and confirm the balance assertion fires.
      **(unchecked by verify-live-infrastructure: same reason as 7.3)**
- [x] 7.5 `ruff` and `mypy --strict` clean on the new harness; confirm coverage does not decrease.
- [x] 7.6 `openspec validate add-effectively-once-e2e-gate --strict` passes.
