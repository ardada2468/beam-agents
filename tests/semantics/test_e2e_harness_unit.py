"""Unit tests for the e2e gate's harness — the plumbing that could lie.

A gate whose own machinery is wrong reports a false green: a spool that
mutates records would fake intent determinism, a duplicate producer that
never duplicates would fake the duplicate-tolerance property, an unseeded
kill schedule would make failures unreplayable, and a quiescence detector
that returns early would assert against a half-finished run.

Unmarked (unit tier): everything here is pure or filesystem-local, no docker.
The Redis-backed ledger reader is covered by the docker-backed gate itself.
"""

from __future__ import annotations

import sys
import time
import types
from pathlib import Path

import pytest

from beam_agents._protos import AgentEnvelope, ToolIntent, ToolResult
from beam_agents.core.agent import intent_id_for
from beam_agents.effector.__main__ import build_parser, config_from_args
from beam_agents.intent_signing import (
    IntentSigner,
    VerificationResult,
    load_keyring,
    sign_intent,
    verify_intent,
)
from tests.semantics._e2e.agent import APPROVAL_PREFIX, LATE_PREFIX, TOOL_PREFIX
from tests.semantics._e2e.approvals import split_due
from tests.semantics._e2e.assertions import await_condition
from tests.semantics._e2e.chaos import ChaosExecutor, KillAction, build_schedule
from tests.semantics._e2e.drainer import Drainer, should_seal
from tests.semantics._e2e.forgery import FORGED_PREFIX, FORGED_TTL_MS, forged_intent
from tests.semantics._e2e.ledger import ExecutionLedger
from tests.semantics._e2e.outbox import _SignAndEncodeIntent, duplicate_decision
from tests.semantics._e2e.pipeline import intent_signer
from tests.semantics._e2e.spool import SpoolWriter, eof_total, read_segment
from tests.semantics._e2e.stack import HOST_BROKERS, SIGNING_KEY_ID, InfraFailure, RunConfig, Stack
from tests.semantics._e2e.workers import VERIFY_INTENTS, effector_argv


def _env(key: bytes, payload: bytes, t_ms: int = 1_700_000_000_000) -> AgentEnvelope:
    return AgentEnvelope(entity_key=key, event_time_ms=t_ms, external_event=payload)


# -- spool: the replay property everything rests on -----------------------------


def test_spool_round_trips_records_byte_identically(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path)
    envelopes = [_env(b"k1", b"a"), _env(b"k2", b"b" * 1000), _env(b"k3", b"")]
    for envelope in envelopes:
        writer.append(envelope)
    writer.seal()

    payloads = list(read_segment(tmp_path / "00000000.seg"))
    assert payloads == [e.SerializeToString() for e in envelopes]


def test_rereading_a_sealed_segment_yields_identical_bytes(tmp_path: Path) -> None:
    """The replay argument: a sealed segment reads the same forever."""
    writer = SpoolWriter(tmp_path)
    for i in range(50):
        writer.append(_env(f"k{i}".encode(), b"x" * i))
    writer.seal()

    first = list(read_segment(tmp_path / "00000000.seg"))
    second = list(read_segment(tmp_path / "00000000.seg"))
    assert first == second


def test_unsealed_segments_are_invisible_and_eof_counts_sealed_only(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path)
    writer.append(_env(b"k1", b"sealed"))
    writer.seal()
    writer.append(_env(b"k2", b"pending"))  # open, never visible as .seg

    assert sorted(p.name for p in tmp_path.glob("*.seg")) == ["00000000.seg"]
    assert eof_total(tmp_path) is None
    writer.close()
    assert eof_total(tmp_path) == 2  # close seals the tail first
    assert sorted(p.name for p in tmp_path.glob("*.seg")) == ["00000000.seg", "00000001.seg"]


def test_a_truncated_record_is_an_error_not_a_short_read(tmp_path: Path) -> None:
    writer = SpoolWriter(tmp_path)
    writer.append(_env(b"k1", b"whole"))
    writer.seal()
    segment = tmp_path / "00000000.seg"
    segment.write_bytes(segment.read_bytes()[:-3])

    with pytest.raises(ValueError, match="truncated record"):
        list(read_segment(segment))


# -- duplicate producer: deterministic, seed-scoped, actually duplicates --------


def test_duplicate_decision_is_deterministic_and_seed_scoped() -> None:
    payloads = [f"payload-{i}".encode() for i in range(2000)]
    first = [duplicate_decision(7, p, 0.05) for p in payloads]
    second = [duplicate_decision(7, p, 0.05) for p in payloads]
    assert first == second, "the duplicate schedule must be reproducible"
    other_seed = [duplicate_decision(8, p, 0.05) for p in payloads]
    assert first != other_seed, "the seed must actually participate"


def test_duplicate_decision_edges_and_rate() -> None:
    payloads = [f"payload-{i}".encode() for i in range(5000)]
    assert not any(duplicate_decision(1, p, 0.0) for p in payloads)
    assert all(duplicate_decision(1, p, 1.0) for p in payloads)
    rate = sum(duplicate_decision(1, p, 0.05) for p in payloads) / len(payloads)
    assert 0.02 < rate < 0.09, f"5% target produced {rate:.3f} — the hash bucketing is off"


# -- kill schedule: seeded, replayable, in-window --------------------------------


def test_kill_schedule_is_reproducible_from_its_seed() -> None:
    a = build_schedule(42, effector_kills=5)
    b = build_schedule(42, effector_kills=5)
    assert a == b
    c = build_schedule(43, effector_kills=5)
    assert a != c


def test_kill_schedule_shape() -> None:
    schedule = build_schedule(7, effector_kills=4)
    assert len(schedule) == 4
    assert all(0.05 <= s.at_progress <= 0.95 for s in schedule), (
        "kills must land strictly inside the run, never before traffic or after it"
    )
    assert schedule == sorted(schedule, key=lambda s: s.at_progress)


def test_chaos_executor_fires_as_progress_crosses_thresholds() -> None:
    progress = {"value": 0.0}
    killed: list[int] = []
    executor = ChaosExecutor(
        [KillAction(0.3, 1), KillAction(0.6, 2)],
        progress=lambda: progress["value"],
        on_kill_effector=killed.append,
    )
    executor.start()
    time.sleep(0.4)
    assert killed == [], "no kill before its threshold"
    progress["value"] = 0.35
    time.sleep(0.6)
    assert killed == [1], "first threshold crossed, second still pending"
    progress["value"] = 1.0
    executor.join(3)
    assert killed == [1, 2]
    assert len(executor.executed) == 2


# -- quiescence detector: never returns early, classifies infra ------------------


async def test_await_condition_returns_when_condition_holds() -> None:
    calls = {"n": 0}

    def condition() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2

    await await_condition("two polls", condition, deadline_s=10, poll_s=0.01)
    assert calls["n"] == 2


async def test_await_condition_deadline_is_an_invariant_failure() -> None:
    start = time.monotonic()
    with pytest.raises(AssertionError, match="invariant/liveness"):
        await await_condition(
            "never true",
            lambda: False,
            deadline_s=0.05,
            poll_s=0.01,
            progress=lambda: "0/10 done",
        )
    assert time.monotonic() - start < 5


async def test_await_condition_surfaces_infra_failure_immediately() -> None:
    def dead_stack() -> None:
        raise InfraFailure("worker pool is dead")

    with pytest.raises(InfraFailure, match="worker pool"):
        await await_condition(
            "anything", lambda: False, deadline_s=30, poll_s=0.01, infra_check=dead_stack
        )


# -- drainer wrapping: re-injection envelopes carry the right payload ------------


def test_drainer_wraps_each_topic_into_the_right_envelope_field(tmp_path: Path) -> None:
    drainer = Drainer(
        SpoolWriter(tmp_path),
        brokers="unused:0",
        events_topic="events",
        results_topic="results",
        decisions_topic="decisions",
        group="g",
    )

    event = _env(b"k1", b"raw")
    wrapped_event = drainer._wrap("events", b"k1", event.SerializeToString())
    assert wrapped_event == event

    result = ToolResult(intent_id="i-1", entity_key=b"k1", status=ToolResult.OK)
    wrapped_result = drainer._wrap("results", b"k1", result.SerializeToString())
    assert wrapped_result.WhichOneof("payload") == "tool_result"
    assert wrapped_result.tool_result == result
    assert wrapped_result.entity_key == b"k1"

    decision = AgentEnvelope.Approval(intent_id="i-2", approved=True, approver="t")
    wrapped_decision = drainer._wrap("decisions", b"k2", decision.SerializeToString())
    assert wrapped_decision.WhichOneof("payload") == "approval"
    assert wrapped_decision.approval == decision
    assert wrapped_decision.entity_key == b"k2"


# -- drainer sealing: a finished burst must become visible ------------------------


def test_should_seal_covers_burst_then_silence() -> None:
    # The original bug: a burst inside the seal interval followed by silence
    # never sealed (seal required new messages AND an elapsed interval at once).
    assert should_seal(5, idle=True, elapsed=False), "idle with pending must seal"
    assert should_seal(5, idle=False, elapsed=True), "elapsed with pending must seal"
    assert not should_seal(0, idle=True, elapsed=True), "nothing pending, nothing to seal"
    assert not should_seal(5, idle=False, elapsed=False), (
        "mid-burst inside the interval keeps batching"
    )


# -- feeder late-release gating: the orphan's determinism rests on it ------------


def _late_intent(key: bytes = b"late-r-00000") -> ToolIntent:
    return ToolIntent(intent_id="i-late", entity_key=key, kind=ToolIntent.APPROVAL)


def test_split_due_time_mode_releases_at_the_recorded_deadline() -> None:
    queue = [(10.0, _late_intent(b"late-r-00000")), (20.0, _late_intent(b"late-r-00001"))]
    due, kept = split_due(queue, now=15.0, late_ready=None)
    assert [t for t, _ in due] == [10.0]
    assert [t for t, _ in kept] == [20.0]


def test_split_due_gated_mode_releases_only_on_the_predicate() -> None:
    # Wall clock is irrelevant in gated mode: an "overdue" entry stays queued
    # until the fail-closed terminal is observed, and a not-yet-due entry
    # releases the moment it is.
    ready_keys = {b"late-r-00001"}
    queue = [(10.0, _late_intent(b"late-r-00000")), (99.0, _late_intent(b"late-r-00001"))]
    due, kept = split_due(queue, now=50.0, late_ready=lambda k: k in ready_keys)
    assert [i.entity_key for _, i in due] == [b"late-r-00001"]
    assert [i.entity_key for _, i in kept] == [b"late-r-00000"]


# -- ledger: the two countings the strong form rests on --------------------------
#
# `record_attempt` must always increment (the raw at-least-once measurement)
# and `record_effective` must be first-writer-wins (the modeled idempotent
# downstream keyed on intent_id). A ledger that got either wrong would fake
# the gate's exactly-one-effective-execution assertion.


class _FakeRedis:
    """Dict-backed stand-in implementing exactly the ledger's client calls."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, int]] = {}

    def hincrby(self, key: str, field: str, amount: int) -> int:
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = int(bucket.get(field, 0)) + amount
        return bucket[field]

    def hsetnx(self, key: str, field: str, value: int) -> int:
        bucket = self.hashes.setdefault(key, {})
        if field in bucket:
            return 0
        bucket[field] = int(value)
        return 1

    def hgetall(self, key: str) -> dict[bytes, bytes]:
        return {k.encode(): str(v).encode() for k, v in self.hashes.get(key, {}).items()}

    def delete(self, *keys: str) -> None:
        for key in keys:
            self.hashes.pop(key, None)

    def close(self) -> None:
        return None


def _fake_ledger(
    monkeypatch: pytest.MonkeyPatch, run_id: str = "run-1"
) -> tuple[ExecutionLedger, _FakeRedis]:
    # A whole fake `redis` module in sys.modules, not a monkeypatched
    # attribute: the ledger imports redis lazily inside its constructor, and
    # the ci unit lane has no real redis to patch (its absence there is the
    # import-boundary proof).
    fake = _FakeRedis()
    fake_module = types.ModuleType("redis")
    fake_module.Redis = types.SimpleNamespace(  # type: ignore[attr-defined]
        from_url=lambda url: fake
    )
    monkeypatch.setitem(sys.modules, "redis", fake_module)
    return ExecutionLedger(run_id), fake


def test_ledger_attempts_always_increment(monkeypatch: pytest.MonkeyPatch) -> None:
    ledger, _ = _fake_ledger(monkeypatch)
    assert ledger.record_attempt("intent-1") == 1
    assert ledger.record_attempt("intent-1") == 2
    assert ledger.record_attempt("intent-2") == 1
    assert ledger.attempts() == {"intent-1": 2, "intent-2": 1}


def test_ledger_effective_is_first_writer_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    ledger, _ = _fake_ledger(monkeypatch)
    assert ledger.record_effective("intent-1", attempt=1) is True
    assert ledger.record_effective("intent-1", attempt=2) is False
    assert ledger.record_effective("intent-2", attempt=5) is True
    # The stored value is the winning attempt, for post-run attribution.
    assert ledger.effective() == {"intent-1": 1, "intent-2": 5}


def test_ledger_clear_removes_both_countings(monkeypatch: pytest.MonkeyPatch) -> None:
    ledger, fake = _fake_ledger(monkeypatch)
    ledger.record_attempt("intent-1")
    ledger.record_effective("intent-1", attempt=1)
    ledger.clear()
    assert ledger.attempts() == {}
    assert ledger.effective() == {}
    assert fake.hashes == {}


# -- signing: the reference deployment's `require` posture ------------------------
#
# The e2e gate is the repository's evidence that intent-signature verification
# is *enforced* (1.0 release gate condition 2.4). These pin the plumbing that
# evidence rests on without docker: the per-run keyring both halves resolve,
# the outbox signer's parity with `WriteIntents`, the forgery's shape, and the
# effector pool's command line.


def _run_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RunConfig:
    # `host_spool` hangs off the module-level spool root; point it at tmp so
    # the keyring lands nowhere near docker/e2e-spool.
    monkeypatch.setattr("tests.semantics._e2e.stack.HOST_SPOOL_ROOT", tmp_path)
    return RunConfig(run_id="unit-run", seed=7, events=10)


def test_keyring_is_minted_per_run_and_resolves_through_a_file_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _run_config(tmp_path, monkeypatch)
    stack = Stack(config)
    stack.provision_spool()
    key = stack.provision_keyring()

    assert len(key) == 32, "docs/security.md prescribes 32 CSPRNG bytes"
    # Both halves read it the production way: a `file:` reference, never bytes
    # in configuration or on argv.
    assert load_keyring(f"file:{config.host_keyring}") == {SIGNING_KEY_ID: key}
    assert intent_signer(config).key_id == SIGNING_KEY_ID
    assert intent_signer(config).key_reference == f"file:{config.container_keyring}"
    assert config.container_keyring.endswith("/" + config.host_keyring.name)
    # Owner-only on the host; the keyring is not a spool segment.
    assert config.host_keyring.stat().st_mode & 0o777 == 0o600
    assert not config.host_keyring.name.endswith(".seg")
    # Fresh material every run: a committed or reused key would be theater.
    assert stack.provision_keyring() != key


def test_outbox_signer_matches_write_intents_byte_for_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _run_config(tmp_path, monkeypatch)
    stack = Stack(config)
    stack.provision_spool()
    key = stack.provision_keyring()
    intent = ToolIntent(
        intent_id="i-1", entity_key=b"t-1", tool_name="charge", args_json="{}", expires_at_ms=5
    )

    # Resolve from the host path here (the container path only exists inside
    # beam-sdk-harness); the DoFn is otherwise exactly what the pipeline runs.
    dofn = _SignAndEncodeIntent(
        IntentSigner(key_id=SIGNING_KEY_ID, key_reference=f"file:{config.host_keyring}")
    )
    dofn.setup()
    ((out_key, payload),) = list(dofn.process((b"t-1", intent)))

    assert out_key == b"t-1"
    assert payload == sign_intent(intent, key_id=SIGNING_KEY_ID, key=key).SerializeToString(
        deterministic=True
    )
    assert verify_intent(ToolIntent.FromString(payload), {SIGNING_KEY_ID: key}) is (
        VerificationResult.OK
    )
    # Signed on a copy: the element (keyed-state bytes, in production) is untouched.
    assert not intent.signature
    # Without a signer the publisher is the pre-signing outbox, byte for byte.
    unsigned = _SignAndEncodeIntent(None)
    unsigned.setup()
    assert list(unsigned.process((b"t-1", intent))) == [
        (b"t-1", intent.SerializeToString(deterministic=True))
    ]


def test_forgery_is_well_formed_and_refused_only_for_its_missing_signature() -> None:
    config = RunConfig(run_id="unit-run", seed=7, events=10)
    forged = forged_intent(config, now_ms=1_700_000_000_000)

    # Nothing malformed to hide behind: a registered tool, canonical args, the
    # runtime's own deterministic id, an hour of TTL, a key outside every
    # population the gate enumerates.
    assert forged.tool_name == "charge"
    assert forged.intent_id == intent_id_for(forged.entity_key, forged.seq, forged.step_index)
    assert forged.entity_key.startswith(FORGED_PREFIX)
    assert not forged.entity_key.startswith((TOOL_PREFIX, APPROVAL_PREFIX, LATE_PREFIX))
    assert forged.expires_at_ms - forged.created_at_ms == FORGED_TTL_MS
    assert verify_intent(forged, {SIGNING_KEY_ID: b"\x01" * 32}) is VerificationResult.UNSIGNED


def test_effector_pool_enforces_require_with_a_keyring_reference_and_dead_letters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _run_config(tmp_path, monkeypatch)
    argv = effector_argv(config)

    def flag(name: str) -> str:
        return argv[argv.index(name) + 1]

    assert VERIFY_INTENTS == "require"
    assert flag("--verify-intents") == "require"
    assert flag("--signing-keys") == f"file:{config.host_keyring}"
    assert flag("--dead-letters-to") == f"kafka://{HOST_BROKERS}/{config.dead_letters_topic}"
    assert config.dead_letters_topic in config.all_topics
    # No key material on the command line (argv is visible in process listings).
    assert not any("=" in arg for arg in argv)
    # The command line is a valid `require`-mode effector configuration as the
    # real CLI parses it — `python -m beam_agents.effector <flags>`.
    assert argv[1:3] == ["-m", "beam_agents.effector"]
    effector_config = config_from_args(build_parser().parse_args(argv[3:]))
    assert effector_config.verify_intents == "require"
    assert effector_config.signing_keys == f"file:{config.host_keyring}"
    assert effector_config.dead_letters_to == f"kafka://{HOST_BROKERS}/{config.dead_letters_topic}"
