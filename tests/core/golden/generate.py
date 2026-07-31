"""Manual-run generator for the per-version golden corpus.

Run once, by hand, to (re)produce the current version's committed fixtures:

    uv run python tests/core/golden/generate.py

NEVER invoked by CI (the file is not a ``test_*`` module, so pytest does not
collect it). Fixtures live one directory per ``state_schema_version``
(``v1/``, ``v2/``, ...); ``main()`` only ever writes
``v<CURRENT_STATE_SCHEMA_VERSION>/``, so regenerating a historical version is
impossible by construction — the committed historical bytes ARE the artifact,
and a new baseline is a new directory.

``CORPUS`` maps each version to that version's builders and is the single
source of truth for both the committed bytes (written here) and the expected
field values (imported by ``test_schema_compat``). A version's builders are
**frozen** the moment a newer version exists: on a
``CURRENT_STATE_SCHEMA_VERSION`` bump, leave the outgoing version's builder map
untouched, add a new map for the incoming version, and run this generator to
write the new directory (see ``docs/state-migration.md`` for the full bump
checklist). Each message is fully populated with fixed, documented values so
field-level equality is meaningful.
"""

from __future__ import annotations

from pathlib import Path

from google.protobuf.message import Message

from beam_agents._protos import (
    AgentEnvelope,
    Continuation,
    LlmCacheBlob,
    MemoryBlob,
    ToolIntent,
    ToolResult,
    TraceEvent,
)
from beam_agents.core.migration import CURRENT_STATE_SCHEMA_VERSION
from beam_agents.intent_signing import sign_intent

GOLDEN_DIR = Path(__file__).parent

# Fixed timestamps (unix epoch ms) reused across fixtures for legibility.
_T0 = 1_700_000_000_000
_T1 = 1_700_000_000_500


def _memory_blob() -> MemoryBlob:
    blob = MemoryBlob(state_schema_version=1, total_value_bytes=6)
    blob.entries.add(key="alpha", value=b"aa", last_access_ms=_T0)
    blob.entries.add(key="beta", value=b"bb", last_access_ms=_T0 + 1)
    blob.entries.add(key="gamma", value=b"cc", last_access_ms=_T0 + 2)
    return blob


def _tool_intent() -> ToolIntent:
    return ToolIntent(
        intent_id="11111111-2222-5333-8444-555555555555",
        entity_key=b"entity-1",
        seq=7,
        step_index=2,
        tool_name="http.post",
        args_json='{"body":{"n":1},"url":"https://example.test"}',
        created_at_ms=_T0,
        expires_at_ms=_T0 + 60_000,
        attempt=1,
    )


def _tool_result() -> ToolResult:
    return ToolResult(
        intent_id="11111111-2222-5333-8444-555555555555",
        entity_key=b"entity-1",
        seq=7,
        status=ToolResult.OK,
        payload=b"\x00\x01\x02result",
        error_message="",
        completed_at_ms=_T1,
    )


def _trace_event() -> TraceEvent:
    event = TraceEvent(
        trace_id=bytes.fromhex("0123456789abcdef0123456789abcdef"),
        span_id=bytes.fromhex("0011223344556677"),
        parent_span_id=bytes.fromhex("7766554433221100"),
        entity_key=b"entity-1",
        seq=7,
        step_index=2,
        event_type=TraceEvent.LLM_CALL,
        start_ms=_T0,
        end_ms=_T1,
    )
    event.attributes["gen_ai.request.model"] = "claude-opus-4-8"
    event.attributes["gen_ai.usage.input_tokens"] = "1234"
    event.attributes["gen_ai.usage.output_tokens"] = "567"
    return event


def _agent_envelope() -> AgentEnvelope:
    return AgentEnvelope(
        entity_key=b"entity-1",
        event_time_ms=_T0,
        approval=AgentEnvelope.Approval(
            intent_id="11111111-2222-5333-8444-555555555555",
            approved=True,
            approver="alice@example.test",
            decided_at_ms=_T1,
        ),
    )


def _llm_cache_blob() -> LlmCacheBlob:
    blob = LlmCacheBlob(state_schema_version=1, total_response_bytes=11)
    # A fully-stored entry: response retained, digest populated.
    blob.entries.add(
        cache_key="0" * 64,
        response=b"hello world",
        response_digest=bytes.fromhex(
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        ),
        created_at_ms=_T0,
        last_access_ms=_T0,
        digest_only=False,
    )
    # A digest-only entry: oversized response dropped, only the digest kept.
    # The digest is sha256 of the (dropped) original response, here "hi".
    blob.entries.add(
        cache_key="f" * 64,
        response=b"",
        response_digest=bytes.fromhex(
            "8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4"
        ),
        created_at_ms=_T0 + 1,
        last_access_ms=_T1,
        digest_only=True,
    )
    return blob


def _tool_intent_approval() -> ToolIntent:
    """A `kind = APPROVAL` intent: the human-in-the-loop baseline.

    `kind` was added after the v1 baseline blobs were written, so this fixture
    (not `tool_intent`, which is deliberately left as the pre-`kind` bytes)
    is what pins the new field's encoding.
    """
    return ToolIntent(
        intent_id="aaaaaaaa-bbbb-5ccc-8ddd-eeeeeeeeeeee",
        entity_key=b"entity-1",
        seq=7,
        step_index=3,
        tool_name="approval",
        args_json='{"amount":9000,"reason":"refund"}',
        created_at_ms=_T0,
        expires_at_ms=_T0 + 3_600_000,
        attempt=0,
        kind=ToolIntent.APPROVAL,
    )


def _tool_intent_traced() -> ToolIntent:
    """An intent carrying its activation's `trace_id`.

    `trace_id` was added after the v1 baseline blobs were written, so this
    fixture (not `tool_intent`, which is deliberately left as the pre-`trace_id`
    bytes) is what pins the new field's encoding. The value is the 16-byte
    width the schema requires, not an arbitrary-length blob.
    """
    return ToolIntent(
        intent_id="cccccccc-dddd-5eee-8fff-000000000000",
        entity_key=b"entity-1",
        seq=7,
        step_index=4,
        tool_name="http.post",
        args_json='{"url":"https://example.test"}',
        created_at_ms=_T0,
        expires_at_ms=_T0 + 60_000,
        attempt=0,
        kind=ToolIntent.TOOL,
        trace_id=bytes.fromhex("0123456789abcdef0123456789abcdef"),
    )


# The fixed, non-secret HMAC key the signed golden fixture is signed with. A
# corpus fixture must be reproducible from this file alone, so the key is
# committed here rather than read from the environment; it authenticates
# nothing and exists only to pin the signing input's encoding.
GOLDEN_SIGNING_KEY_ID = "golden-k1"
GOLDEN_SIGNING_KEY = bytes(range(32))


def _tool_intent_signed() -> ToolIntent:
    """An intent carrying its HMAC-SHA256 signature envelope.

    The signature fields were added after the v1 baseline blobs were written,
    so this fixture (not `tool_intent`, which is deliberately left as the
    pre-signature bytes) is what pins their encoding. Because the signature is
    recomputed by this builder and compared against the committed bytes, the
    fixture also pins the *signing input* — cleared-signature-fields
    deterministic serialization — which is the definition both the signer and
    the verifier have to agree on.
    """
    return sign_intent(
        ToolIntent(
            intent_id="dddddddd-eeee-5fff-8000-111111111111",
            entity_key=b"entity-1",
            seq=7,
            step_index=5,
            tool_name="http.post",
            args_json='{"url":"https://example.test"}',
            created_at_ms=_T0,
            expires_at_ms=_T0 + 60_000,
            attempt=0,
            kind=ToolIntent.TOOL,
        ),
        key_id=GOLDEN_SIGNING_KEY_ID,
        key=GOLDEN_SIGNING_KEY,
    )


def _tool_intent_future_field() -> ToolIntent:
    """A signed intent carrying an unknown field numbered above the signature.

    Stands in for a message minted by a *newer* schema generation and read by
    these bindings: field 500 is unknown here, and protobuf preserves it and
    re-emits it after the known fields. That placement is what keeps the
    cleared-fields signing input stable across schema skew, so this blob is the
    committed artifact behind the "a future-field intent still yields a stable
    signing input" scenario.
    """
    unsigned = ToolIntent(
        intent_id="eeeeeeee-ffff-5000-8111-222222222222",
        entity_key=b"entity-1",
        seq=7,
        step_index=6,
        tool_name="http.post",
        args_json='{"url":"https://example.test"}',
        created_at_ms=_T0,
        expires_at_ms=_T0 + 60_000,
        attempt=0,
        kind=ToolIntent.TOOL,
    )
    # A varint field 500 (tag = 500 << 3 | 0 = 4000, value 42), appended as raw
    # bytes and parsed back so the message genuinely carries it as an unknown
    # field. It is added *before* signing, because the fixture stands in for a
    # message minted by a newer schema generation: the newer signer covered the
    # field, and these older bindings must still reproduce that signing input.
    unknown = bytes([0xA0, 0x1F, 0x2A])
    carrier = ToolIntent()
    carrier.ParseFromString(unsigned.SerializeToString(deterministic=True) + unknown)
    return sign_intent(carrier, key_id=GOLDEN_SIGNING_KEY_ID, key=GOLDEN_SIGNING_KEY)


def _continuation() -> Continuation:
    return Continuation(
        state_schema_version=1,
        seq=7,
        step_index=2,
        pending_intent_ids=[
            "11111111-2222-5333-8444-555555555555",
            "aaaaaaaa-bbbb-5ccc-8ddd-eeeeeeeeeeee",
        ],
        adapter="langgraph",
        snapshot=bytes(range(64)),
        suspended_at_ms=_T0,
        deadline_ms=_T0 + 600_000,
    )


def _continuation_escalated() -> Continuation:
    """A continuation that has already escalated once.

    Pins the encoding of `escalations`, added after the v1 baseline; the
    `continuation` fixture stays as the pre-`escalations` bytes.
    """
    return Continuation(
        state_schema_version=1,
        seq=7,
        step_index=4,
        pending_intent_ids=["aaaaaaaa-bbbb-5ccc-8ddd-eeeeeeeeeeee"],
        adapter="langgraph",
        snapshot=bytes(range(64)),
        suspended_at_ms=_T0,
        deadline_ms=_T0 + 1_200_000,
        escalations=1,
    )


# name -> fully-populated message, for the v1 fixture set. Filenames are
# `<name>.bin` under `v1/`. Every message type has at least one fixture; a type
# gains a second one when a field is added (additively) within the version, so
# the original blob keeps proving that pre-field bytes still decode while the
# new one pins the new field.
#
# FROZEN once version 2 exists: these builders pair with the committed `v1/`
# bytes and may never change after a bump — the migration chain, not a
# regenerated baseline, is what carries v1 state forward.
GOLDEN_V1: dict[str, Message] = {
    "memory_blob": _memory_blob(),
    "tool_intent": _tool_intent(),
    "tool_intent_approval": _tool_intent_approval(),
    "tool_intent_traced": _tool_intent_traced(),
    "tool_result": _tool_result(),
    "trace_event": _trace_event(),
    "agent_envelope": _agent_envelope(),
    "continuation": _continuation(),
    "continuation_escalated": _continuation_escalated(),
    "llm_cache_blob": _llm_cache_blob(),
    "tool_intent_signed": _tool_intent_signed(),
    "tool_intent_future_field": _tool_intent_future_field(),
}

# version -> that version's fixture builders. One entry per schema version up
# to CURRENT_STATE_SCHEMA_VERSION; the completeness meta-tests in
# `test_schema_compat` fail the build when a bump lands without its entry.
CORPUS: dict[int, dict[str, Message]] = {
    1: GOLDEN_V1,
}


def write_current(golden_dir: Path) -> list[Path]:
    """Write the current version's fixtures under ``v<CURRENT>/``, nothing else.

    Historical directories are never touched: only
    ``CORPUS[CURRENT_STATE_SCHEMA_VERSION]`` is serialized.
    """
    version_dir = golden_dir / f"v{CURRENT_STATE_SCHEMA_VERSION}"
    version_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for name, message in CORPUS[CURRENT_STATE_SCHEMA_VERSION].items():
        path = version_dir / f"{name}.bin"
        path.write_bytes(message.SerializeToString(deterministic=True))
        written.append(path)
    return written


def main() -> None:
    for path in write_current(GOLDEN_DIR):
        print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
