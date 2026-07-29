"""Effectively-once, end to end, on real infrastructure (release gate).

The closed loop under test: events on Kafka → `RunAgent` on the Flink
mini-cluster (portable runner, checkpointing) → intents on Kafka (with
manufactured duplicate deliveries) → a pool of real `beam-agents-effector`
worker processes with Redis dedup → results/approvals on Kafka → re-injection
→ terminal outputs. Chaos: effector workers are SIGKILLed mid-run, then the
Flink TaskManager container is killed, the job cancelled, and the identical
pipeline resubmitted with fresh state, replaying every event from the
immutable spool. Asserted, over the whole population:

- the strong-form exactly-once contract (the Redis ledger, counted at the
  side effect itself, keyed by the injected `intent.intent_id`): zero lost
  effects, **exactly one effective execution per minted intent** (the charge
  tool is intent-keyed idempotent via first-writer-wins, so crash-window
  re-invocations lose the race), raw attempts duplicated only within the
  SIGKILL crash window (bounded; exactly one attempt with zero kills), and
  the full pipeline replay adding zero attempts and zero effective
  executions;
- duplicate deliveries never diverge (byte-identical results per intent_id);
- zero lost approvals (every approval key reaches a terminal decision,
  including the fail-closed HITL-timeout fallback);
- a late decision surfaces as `orphaned_result`, never as a second decision
  and never silently dropped;
- every observed `intent_id` equals `intent_id_for(entity_key, seq,
  step_index)`;
- full accounting: every event's key reaches a terminal output.

Substitutions, both forced by this stack and documented in design D4/D9/F7:
the production `WriteIntents(kafka://…)` writer is upstream-broken (Beam
2.60-2.72 leaks the Java-native `kafka_write:v2` urn into the cross-language
expansion response — root cause in tests/actions/test_write_intents_integration.py)
and cross-language Kafka IO cannot run here at all (no Java SDK environment on
the TaskManager), so intents are published by the harness's deliberately
at-least-once producer (message key = raw entity_key, exactly as WriteIntents
sets it) and the pipeline reads its topics through the replayable segment
spool (tests/semantics/_e2e/spool.py) instead of ReadFromKafka.

Pacing: recovery from an effector SIGKILL is lease-driven (LEASE_MS = 15 s),
so quiescence deadlines are generous multiples of the lease, condition-driven,
and never bare sleeps. The kill schedule is seeded and logged; rerun a failure
with BEAM_AGENTS_E2E_SEED=<seed>. Volume is pinned at 10,000 in CI;
BEAM_AGENTS_E2E_EVENTS tunes it down for local iteration only.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time
from collections.abc import Awaitable, Callable

import pytest

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from beam_agents.core.agent import intent_id_for
from beam_agents.core.dofn import REASON_ORPHANED
from tests.semantics._e2e.agent import (
    APPROVAL_PREFIX,
    DECISION_OUT,
    LATE_PREFIX,
    TOOL_PREFIX,
)
from tests.semantics._e2e.approvals import ApprovalFeeder
from tests.semantics._e2e.assertions import (
    ErrorsWatcher,
    OutputWatcher,
    distinct_by_key,
    group_by_key,
    make_infra_check,
    read_topic_all,
    topic_end_offsets,
)
from tests.semantics._e2e.chaos import ChaosExecutor, build_schedule, kill_taskmanager
from tests.semantics._e2e.drainer import Drainer
from tests.semantics._e2e.ledger import ExecutionLedger
from tests.semantics._e2e.pipeline import run_pipeline
from tests.semantics._e2e.spool import SpoolWriter
from tests.semantics._e2e.stack import HOST_BROKERS, InfraFailure, RunConfig, Stack, new_run
from tests.semantics._e2e.workers import LEASE_MS, EffectorPool

pytestmark = [pytest.mark.semantics, pytest.mark.integration, pytest.mark.slow]

_LOG = logging.getLogger("beam_agents.e2e")

EVENTS = int(os.environ.get("BEAM_AGENTS_E2E_EVENTS", "10000"))
SEED = int(os.environ.get("BEAM_AGENTS_E2E_SEED", str(random.SystemRandom().randrange(2**31))))

LATE_KEYS = 2
APPROVAL_EVERY = 10  # every 10th non-late event is approval-bearing
EFFECTOR_POOL_SIZE = 3
EFFECTOR_KILLS = 3

# Deadlines: condition-driven, generous, lease-aware. A kill can stall one
# partition for a full lease; several kills compound.
PHASE_A_DEADLINE_S = 240 + EVENTS / 25 + (EFFECTOR_KILLS + 1) * (LEASE_MS / 1000)
PHASE_B_DEADLINE_S = 240 + EVENTS / 25
SETTLE_ROUNDS = 3
# Phase-B submission-stall handling (design F12): how long a freshly submitted
# replay job gets to publish its first intent before the submission is judged
# stalled (all vertices at zero), and how many submissions to try. Generous:
# environment spin-up alone is ~30-60 s.
REPLAY_STALL_WINDOW_S = 120 + EVENTS / 50
REPLAY_SUBMIT_ATTEMPTS = 3
# The F12 stall's *partial* form, observed ~50% of CI runs: the job starts,
# a few dozen keys reach terminal, then every vertex freezes forever. It is
# reclassified as a runner stall — and the job cancelled and replayed from
# the spool, phase B's own mechanism, which the gate itself proves adds zero
# executions — only when BOTH hold: no new terminal for the whole window AND
# the job's per-vertex counters are byte-identical across a 10 s probe. A
# slow-but-moving run never trips it, and a hang in *our* code cannot hide
# behind it: activation timeouts route wedged activations to `.errors`
# (which are progress), so a total vertex freeze is runner-level by
# construction. Bounded: past PHASE_A_RESUBMITS the run is InfraFailure.
PHASE_A_FREEZE_WINDOW_S = 150
PHASE_A_RESUBMITS = 1
# Mirrors the effector's default max_concurrent_partitions: the most in-flight
# executions one SIGKILL can strand mid-crash-window.
_EFFECTOR_MAX_CONCURRENT_PARTITIONS = 8


def _now_ms() -> int:
    return int(time.time() * 1000)


def _populations(config: RunConfig) -> tuple[list[bytes], list[bytes], list[bytes]]:
    """(tool_keys, approval_keys, late_keys) — the run's whole population."""
    tool_keys: list[bytes] = []
    approval_keys: list[bytes] = []
    late_keys = [config.entity_key(LATE_PREFIX, i) for i in range(LATE_KEYS)]
    for i in range(EVENTS - LATE_KEYS):
        if i % APPROVAL_EVERY == 0:
            approval_keys.append(config.entity_key(APPROVAL_PREFIX, i))
        else:
            tool_keys.append(config.entity_key(TOOL_PREFIX, i))
    return tool_keys, approval_keys, late_keys


async def _publish_events(config: RunConfig, keys: list[bytes]) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers=HOST_BROKERS)
    await producer.start()
    try:
        for key in keys:
            envelope = AgentEnvelope(entity_key=key, event_time_ms=_now_ms(), external_event=b"go")
            await producer.send(config.events_topic, key=key, value=envelope.SerializeToString())
        await producer.flush()
    finally:
        await producer.stop()


def _start_pipeline_thread(config: RunConfig, job_name: str) -> threading.Thread:
    def target() -> None:
        try:
            run_pipeline(config, job_name=job_name)
        except Exception:  # the gate cancels the job; the client raising is expected
            _LOG.info("pipeline client for %s ended", job_name, exc_info=True)

    thread = threading.Thread(target=target, name=f"pipeline-{job_name}", daemon=True)
    thread.start()
    return thread


def _intents_nonempty(config: RunConfig) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        offsets = await topic_end_offsets(config.intents_topic)
        return sum(offsets.values()) > 0

    return check


def _outputs_advanced(config: RunConfig, baseline: int) -> Callable[[], Awaitable[bool]]:
    """Progress signal for a phase-A resubmission: the replay re-publishes
    already-known terminals long before it produces a NEW key's terminal, so
    raw output-topic offsets moving past the pre-resubmit baseline is the
    earliest honest sign the replacement job is executing user code."""

    async def check() -> bool:
        offsets = await topic_end_offsets(config.output_topic)
        return sum(offsets.values()) > baseline

    return check


async def _submit_with_stall_retry(
    stack: Stack,
    config: RunConfig,
    *,
    job_prefix: str,
    progressed: Callable[[], Awaitable[bool]],
    infra_check: Callable[[], None],
) -> str:
    """Submit the pipeline until it demonstrably starts processing.

    The portable Flink runner on this stack stochastically never starts a
    submitted streaming job's source: the job goes RUNNING and checkpoints,
    but every vertex sits at in=0/out=0 forever — zero user code executes
    (design F12). A stalled submission therefore says nothing about the
    invariants under test; it is retried (bounded), each stall logged with the
    per-vertex counter snapshot and a captured TaskManager thread dump for the
    upstream report. Exhaustion is InfraFailure, never a red invariant. A
    deadline breach *after* progress began still fails the gate as before.
    """
    for attempt in range(1, REPLAY_SUBMIT_ATTEMPTS + 1):
        job_name = f"{job_prefix}{attempt}"
        _start_pipeline_thread(config, job_name)
        submitted = time.monotonic()
        while time.monotonic() - submitted < REPLAY_STALL_WINDOW_S:
            infra_check()
            if await progressed():
                return job_name
            await asyncio.sleep(2)
        vertices = stack.job_vertex_summary(job_name)
        dump = stack.capture_tm_thread_dump(f"{config.run_id}-{job_name}-stall")
        _LOG.warning(
            "submission %s stalled with zero-progress vertices (%s); "
            "thread dump: %s — cancelling and resubmitting",
            job_name,
            vertices,
            dump,
        )
        stack.await_no_running_jobs()
        stack.fresh_harness()
    raise InfraFailure(
        f"{job_prefix}* never started processing in {REPLAY_SUBMIT_ATTEMPTS} "
        "submissions (source stuck at in=0/out=0 each time — runner-level "
        "submission stall, see the captured thread dumps in docker/e2e-spool/)"
    )


# 40 min hard cap: base phases fit in ~15, but one phase-A freeze
# resubmission legitimately adds a freeze window plus a full replay.
@pytest.mark.timeout(2400)
async def test_exactly_one_execution_and_zero_lost_approvals_under_kills() -> None:
    _LOG.info("run seed=%d events=%d (rerun with BEAM_AGENTS_E2E_SEED=%d)", SEED, EVENTS, SEED)
    config = new_run(SEED, EVENTS)
    stack = Stack(config)
    tool_keys, approval_keys, late_keys = _populations(config)
    all_keys = set(tool_keys) | set(approval_keys) | set(late_keys)

    stack.freshen_flink()
    await stack.create_topics()
    stack.provision_spool()

    ledger = ExecutionLedger(config.run_id)
    writer = SpoolWriter(config.host_spool)
    drainer = Drainer(
        writer,
        brokers=HOST_BROKERS,
        events_topic=config.events_topic,
        results_topic=config.results_topic,
        decisions_topic=config.decisions_topic,
        group=config.drainer_group,
    )
    pool = EffectorPool(config, EFFECTOR_POOL_SIZE)
    watcher = OutputWatcher(config)
    errors_watcher = ErrorsWatcher(config)
    # Late answers release on the OBSERVED fail-closed terminal, not a wall
    # clock: a fixed delay races the HITL timer's real-time firing under CI
    # load (losing turns the "late" decision into a normal resume) and the
    # phase A→B boundary (losing lets the TM kill race the orphan's
    # emission). Gating on the timeout terminal makes the orphan a
    # deterministic phase-A fact, which `phase_a_done` below then awaits.
    feeder = ApprovalFeeder(
        config,
        late_ready=lambda key: (
            (DECISION_OUT + key + b"|timeout") in watcher.terminals.get(key, set())
        ),
    )
    schedule = build_schedule(SEED, effector_kills=EFFECTOR_KILLS)
    expected_executions = max(1, len(tool_keys))
    chaos = ChaosExecutor(
        schedule,
        progress=lambda: len(ledger.attempts()) / expected_executions,
        on_kill_effector=lambda victim: _LOG.info(
            "chaos killed effector pid=%d", pool.kill_one(victim)
        ),
    )

    drainer_task = asyncio.create_task(drainer.run())
    feeder_task = asyncio.create_task(feeder.run())

    try:
        pool.start()
        await _publish_events(config, sorted(all_keys))
        await watcher.start()
        await errors_watcher.start()
        chaos.start()

        infra_check = make_infra_check(pool_healthy=pool.check_healthy)

        # The F12 submission stall (source never emits, zero user code runs)
        # can hit ANY submission on this stack, including the first: submit
        # phase A with the same stall-classified retry as phase B. Progress
        # signal: the first intent reaching the intents topic.
        job_name = await _submit_with_stall_retry(
            stack,
            config,
            job_prefix=f"e2e-{config.run_id}-a",
            progressed=_intents_nonempty(config),
            infra_check=infra_check,
        )

        # ---- Phase A: full population to terminal, under effector kills ----
        # Terminal coverage alone is not enough: the late keys' fail-closed
        # timeout IS their terminal, so phase A could otherwise end (and the
        # TM kill fire) before the gated late decision has been fed, drained,
        # replayed into the pipeline, and surfaced as `orphaned_result`. The
        # orphan is part of phase A's contract, so phase A waits for it.
        def late_orphans_done() -> bool:
            return all(
                errors_watcher.has_reason(key, REASON_ORPHANED.encode()) for key in late_keys
            )

        async def phase_a_done() -> bool:
            await watcher.poll()
            await errors_watcher.poll()
            return watcher.keys_with_terminal() >= all_keys and late_orphans_done()

        def progress() -> str:
            missing = all_keys - watcher.keys_with_terminal()
            return (
                f"{len(watcher.keys_with_terminal())}/{len(all_keys)} keys terminal; "
                f"first missing: {sorted(missing)[:5]}; late orphans observed: "
                f"{late_orphans_done()}"
            )

        start = time.monotonic()
        deadline_s = PHASE_A_DEADLINE_S
        resubmits = 0
        last_terminals = -1
        last_progress_t = time.monotonic()
        while True:
            infra_check()
            if await phase_a_done():
                break
            terminals = len(watcher.keys_with_terminal())
            if terminals != last_terminals:
                last_terminals = terminals
                last_progress_t = time.monotonic()
            elif time.monotonic() - last_progress_t > PHASE_A_FREEZE_WINDOW_S:
                # Suspected partial F12 freeze — corroborate against the
                # runner's own counters before blaming it (see the constant's
                # comment for why this cannot mask a hang in our code).
                before = stack.job_vertex_summary(job_name)
                await asyncio.sleep(10)
                after = stack.job_vertex_summary(job_name)
                if before != after:
                    last_progress_t = time.monotonic()  # moving, just slow
                    continue
                if resubmits >= PHASE_A_RESUBMITS:
                    raise InfraFailure(
                        f"phase A froze {resubmits + 1}x with byte-identical vertex "
                        f"counters ({after}) — runner-level stall persisted through "
                        f"replay; not an invariant verdict. {progress()}"
                    )
                resubmits += 1
                dump = stack.capture_tm_thread_dump(f"{config.run_id}-{job_name}-freeze")
                _LOG.warning(
                    "phase A froze at %d terminals with frozen vertices (%s); "
                    "thread dump: %s — cancelling and replaying from the spool",
                    terminals,
                    after,
                    dump,
                )
                stack.await_no_running_jobs()
                stack.fresh_harness()
                baseline = sum((await topic_end_offsets(config.output_topic)).values())
                job_name = await _submit_with_stall_retry(
                    stack,
                    config,
                    job_prefix=f"e2e-{config.run_id}-a-r{resubmits}",
                    progressed=_outputs_advanced(config, baseline),
                    infra_check=infra_check,
                )
                # The replacement replays the whole spool: extend the budget
                # by a freeze window plus one full-replay allowance.
                deadline_s += PHASE_A_FREEZE_WINDOW_S + EVENTS / 25 + 120
                last_progress_t = time.monotonic()
                continue
            if time.monotonic() - start > deadline_s:
                raise AssertionError(
                    "phase A did not reach full terminal coverage with healthy "
                    f"infrastructure — liveness/invariant failure. {progress()}"
                )
            await asyncio.sleep(2)

        chaos.stop()
        chaos.join(10)
        assert len(chaos.executed) == EFFECTOR_KILLS, (
            f"chaos executed {len(chaos.executed)}/{EFFECTOR_KILLS} scheduled kills; "
            "a gate that skipped its chaos proves nothing"
        )

        # Ledger after phase A: the strong form — zero lost effects, exactly
        # one effective execution per minted intent (first-writer-wins keyed
        # on the injected intent_id collapses crash-window re-invocations),
        # and raw attempts duplicated only within the SIGKILL crash window
        # (spec: bounded by kills and the in-flight limit; exactly one
        # attempt when kills = 0). Each t-… key stages exactly one tool
        # intent at (seq 0, step 1), so the expected ledger members are the
        # deterministic formula's ids — the same formula the intents-topic
        # cross-check below holds every observed intent to.
        attempts_a = ledger.attempts()
        effective_a = ledger.effective()
        expected_ledger = {intent_id_for(k, 0, 1) for k in tool_keys}
        _assert_strong_form(
            attempts_a, effective_a, expected_ledger, phase="A", kills=len(chaos.executed)
        )

        # ---- Phase B: TM kill, cancel, resubmit, full replay from the spool ----
        # Order is load-bearing (F8): recover the TM, let the restored -a job
        # land and be cancelled against the OLD worker pool, and only then
        # give the replay job a factory-fresh pool — a pool that has already
        # served (and torn down) another job's workers can be permanently dead
        # by the time -b asks for an environment.
        kill_taskmanager()
        stack.recover_taskmanager()
        stack.await_no_running_jobs()
        stack.fresh_harness()
        intents_before = await topic_end_offsets(config.intents_topic)

        async def replay_progressed() -> bool:
            now = await topic_end_offsets(config.intents_topic)
            return sum(now.values()) > sum(intents_before.values())

        await _submit_with_stall_retry(
            stack,
            config,
            job_prefix=f"e2e-{config.run_id}-b",
            progressed=replay_progressed,
            infra_check=infra_check,
        )

        # Replay quiescence: intents topic stable for SETTLE_ROUNDS polls.
        stable = 0
        last = await topic_end_offsets(config.intents_topic)
        start = time.monotonic()
        while stable < SETTLE_ROUNDS:
            infra_check()
            if time.monotonic() - start > PHASE_B_DEADLINE_S:
                raise AssertionError("replay did not quiesce within the deadline")
            await asyncio.sleep(4)
            now = await topic_end_offsets(config.intents_topic)
            stable = stable + 1 if now == last else 0
            last = now

        # THE assertion: the full replay added zero attempts and zero
        # effective executions. No kills fire during phase B, so both
        # countings must be byte-for-byte what phase A left — replay
        # determinism plus effector dedup, with no crash-window allowance
        # at all.
        attempts_b = ledger.attempts()
        effective_b = ledger.effective()
        _assert_strong_form(
            attempts_b,
            effective_b,
            expected_ledger,
            phase="B (after full replay)",
            kills=len(chaos.executed),
        )
        changed = {
            k: (attempts_a.get(k), attempts_b.get(k))
            for k in set(attempts_a) | set(attempts_b)
            if attempts_a.get(k) != attempts_b.get(k)
        }
        assert not changed, f"the pipeline replay changed ledger attempt counts: {changed}"
        changed_effective = {
            k: (effective_a.get(k), effective_b.get(k))
            for k in set(effective_a) | set(effective_b)
            if effective_a.get(k) != effective_b.get(k)
        }
        assert not changed_effective, (
            f"the pipeline replay changed effective executions: {changed_effective}"
        )

        # Capture every topic now — teardown deletes them.
        intents = [ToolIntent.FromString(v) for _, v in await read_topic_all(config.intents_topic)]
        results_raw = await read_topic_all(config.results_topic)
        requests = [
            ToolIntent.FromString(v)
            for _, v in await read_topic_all(config.approval_requests_topic)
        ]
        errors = await read_topic_all(config.errors_topic)
        outputs = distinct_by_key(
            [
                (v.split(b"|")[1], v)
                for _, v in await read_topic_all(config.output_topic)
                if v.count(b"|") >= 2
            ]
        )

    finally:
        chaos.stop()
        feeder.stop()
        drainer.stop()
        for task in (drainer_task, feeder_task):
            task.cancel()
        await asyncio.gather(drainer_task, feeder_task, return_exceptions=True)
        await watcher.stop()
        await errors_watcher.stop()
        pool.terminate_all()
        await stack.teardown()
        ledger.close()

    # ---- Post-run, everything from the captured topics (D6) ----
    # Intent-ID determinism, including replay re-mints: every observed id is
    # the formula's id, and every key carries exactly one distinct intent.
    by_key: dict[bytes, set[bytes]] = {}
    for intent in intents:
        assert intent.intent_id == intent_id_for(
            intent.entity_key, intent.seq, intent.step_index
        ), f"non-deterministic intent_id for key {intent.entity_key!r}"
        by_key.setdefault(intent.entity_key, set()).add(
            intent.SerializeToString(deterministic=True)
        )
    for key, blobs in by_key.items():
        assert len(blobs) == 1, (
            f"key {key!r} minted {len(blobs)} distinct intents across replay — "
            "seq/step_index derivation drifted"
        )
    assert set(by_key) == all_keys, (
        f"intent coverage mismatch: {len(by_key)} keys minted intents, expected {len(all_keys)}"
    )

    # Duplicate deliveries never diverge: byte-identical results per intent_id.
    results_by_id: dict[str, set[bytes]] = {}
    for _key, value in results_raw:
        result = ToolResult.FromString(value)
        results_by_id.setdefault(result.intent_id, set()).add(value)
    for intent_id, blobs in results_by_id.items():
        assert len(blobs) == 1, f"intent {intent_id} produced {len(blobs)} distinct results"

    # Zero lost approvals, part 1: every approval intent reached the channel.
    requested_ids = {r.intent_id for r in requests}
    for key in list(approval_keys) + list(late_keys):
        (blob,) = by_key[key]
        intent = ToolIntent.FromString(blob)
        assert intent.kind == ToolIntent.APPROVAL
        assert intent.intent_id in requested_ids, (
            f"approval intent for key {key!r} never reached the approvals channel"
        )

    # Zero lost approvals, part 2 + full accounting: terminal identity per key.
    missing = all_keys - set(outputs)
    assert not missing, f"{len(missing)} keys reached no terminal output: {sorted(missing)[:5]}"
    for key in tool_keys:
        assert outputs[key] == {b"result|" + key + b"|OK"}, (
            f"tool key {key!r} terminal set diverged: {outputs[key]!r}"
        )
    for key in approval_keys:
        index = int(key.rsplit(b"-", 1)[-1])
        verdict = b"approved" if index % 2 == 0 else b"denied"
        assert outputs[key] == {DECISION_OUT + key + b"|" + verdict}, (
            f"approval key {key!r} terminal set diverged: {outputs[key]!r}"
        )
    for key in late_keys:
        # The fail-closed timeout decision must be present. The replay may add
        # the (deterministic) fed decision as a second terminal — wall-clock
        # racing under replay is expected and documented — but nothing else.
        index = int(key.rsplit(b"-", 1)[-1])
        allowed = {
            DECISION_OUT + key + b"|timeout",
            DECISION_OUT + key + (b"|approved" if index % 2 == 0 else b"|denied"),
        }
        assert DECISION_OUT + key + b"|timeout" in outputs[key], (
            f"late key {key!r} never fail-closed: {outputs[key]!r}"
        )
        assert outputs[key] <= allowed, f"late key {key!r} produced {outputs[key]!r}"

    # The late decision surfaced as orphaned_result, and no unexpected error
    # reason appeared anywhere (orphaned_result is legitimate on any key under
    # replay: re-delivered answers to already-completed suspensions).
    errors_by_key = group_by_key(errors)
    for key in late_keys:
        reasons = {v.split(b"|")[0] for v in errors_by_key.get(key, [])}
        assert REASON_ORPHANED.encode() in reasons, (
            f"late key {key!r}'s late decision vanished instead of surfacing "
            f"as orphaned_result (got {reasons!r})"
        )
    unexpected = {
        (k, v)
        for k, vs in errors_by_key.items()
        for v in vs
        if not v.startswith(REASON_ORPHANED.encode())
    }
    assert not unexpected, f"unexpected error records: {sorted(unexpected)[:5]}"


def _assert_strong_form(
    attempts: dict[str, int],
    effective: dict[str, int],
    expected_members: set[str],
    *,
    phase: str,
    kills: int,
) -> None:
    """The honest exactly-once contract, asserted in its strong form.

    Zero lost effects (every minted intent attempted at least once), exactly
    one effective execution per minted intent — the charge tool keys a
    first-writer-wins write on the injected `intent_id`, so a crash-window
    re-invocation replays the same key and loses the race — and raw attempts
    duplicated only within the SIGKILL crash window. A kill between a tool's
    effect and the durable completion record re-executes after lease expiry
    (design F13); each kill can strand at most `max_concurrent_partitions`
    in-flight executions, and a member can gain at most one extra attempt per
    kill, so with zero kills attempts are exactly one per intent too.
    """
    missing = expected_members - set(attempts)
    assert not missing, (
        f"[phase {phase}] lost effects — no attempt recorded for: {sorted(missing)[:10]}"
    )
    extra = set(attempts) - expected_members
    assert not extra, f"[phase {phase}] attempts for unknown members: {sorted(extra)[:10]}"

    # The strong form: exactly one effective execution per minted intent. The
    # first-writer-wins write can succeed at most once per intent_id by
    # construction, so presence is the whole assertion — plus attribution:
    # the winning attempt must be one this ledger actually counted.
    missing_effective = expected_members - set(effective)
    assert not missing_effective, (
        f"[phase {phase}] intents with no effective execution: {sorted(missing_effective)[:10]}"
    )
    extra_effective = set(effective) - expected_members
    assert not extra_effective, (
        f"[phase {phase}] effective executions for unknown members: {sorted(extra_effective)[:10]}"
    )
    unattributed = {
        k: (winner, attempts[k])
        for k, winner in effective.items()
        if not 1 <= winner <= attempts[k]
    }
    assert not unattributed, (
        f"[phase {phase}] effective executions won by an attempt the ledger never counted "
        f"(winner, attempts): {sorted(unattributed.items())[:10]}"
    )

    duplicated = {k: c for k, c in attempts.items() if c > 1}
    max_duplicated_members = kills * _EFFECTOR_MAX_CONCURRENT_PARTITIONS
    assert len(duplicated) <= max_duplicated_members, (
        f"[phase {phase}] {len(duplicated)} members re-attempted, but at most "
        f"{max_duplicated_members} are attributable to the crash window of "
        f"{kills} kills — something beyond the documented window re-invokes "
        f"tools: {sorted(duplicated.items())[:10]}"
    )
    over_cap = {k: c for k, c in duplicated.items() if c > 1 + kills}
    assert not over_cap, (
        f"[phase {phase}] attempt counts exceed 1+kills({1 + kills}) — not "
        f"explainable by crash-window re-execution: {sorted(over_cap.items())[:10]}"
    )
    if duplicated:
        _LOG.warning(
            "[phase %s] crash-window re-attempts (within bound, %d kills; all "
            "collapsed to one effective execution): %s",
            phase,
            kills,
            sorted(duplicated.items()),
        )
