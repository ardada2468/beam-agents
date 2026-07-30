## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Wire-schema tests from the wire-schemas delta scenarios: "Signature fields round-trip", "A pre-signature intent decodes as unsigned" (against a committed pre-signature golden blob), and "A future-field intent still yields a stable signing input" (a blob with an injected unknown high-numbered field must re-serialize to the signer's cleared-fields bytes).
- [ ] 1.2 Signing-module tests from "ToolIntents are signed at the outbox writer": deterministic signatures (same intent + key → same bytes, twice), verify round-trip, constant-time comparison used, keyring loading from `env:`/`file:` references, malformed reference rejected.
- [ ] 1.3 `WriteIntents` signer tests: "A signed intent verifies against the signing key", "A retried bundle re-signs to byte-identical messages", "No signer configured preserves today's output" (byte-compare against current `_SerializeIntent` output), "Key bytes never enter the pipeline graph" (pickle the transform, assert the key material is absent).
- [ ] 1.4 Effector verification tests from "The effector verifies intent signatures before any other phase": valid signed intent executes end to end; tampered intent → no tool invocation, no `ToolResult`, no dedup claim (assert the in-memory store is empty for that id); unknown key id dead-letters with its distinct reason; tampered-and-expired intent dead-letters for the signature, publishing no `EXPIRED` result.
- [ ] 1.5 Dead-letter tests from "Verification failures are dead-lettered and never produce a ToolResult": verbatim payload on the dead-letter sink, offset committed, next intent on the partition unblocked; no-channel case logs identity fields only (assert `args_json` content absent from the log record) and still commits.
- [ ] 1.6 Mode tests from "Verification mode governs unsigned intents across the rollout": `off` is byte-for-byte today's behavior on signed and unsigned streams; `permissive` accepts+counts unsigned, still dead-letters `bad_signature`; `require` dead-letters unsigned; verifying mode without a keyring fails at startup; `require` without `dead_letters_to` warns.
- [ ] 1.7 Transport-security tests from "Broker transport security is configurable with credentials by reference": settings reach a fake/captured Kafka client constructor on source, sinks, and `WriteIntents`; malformed reference rejected import-free; resolved secret absent from config attributes, `repr`, and raised errors.
- [ ] 1.8 Redaction tests from "Secrets embedded in URIs never appear in errors or reprs": credentialed malformed URI → exception chain free of the password; `repr(EffectorConfig)` masked; `beam-agents-effector` startup error path prints redacted output.
- [ ] 1.9 Docs tests from "The security baseline is documented...": `docs/security.md` exists and contains the secrets-in-args contract and the per-principal least-privilege matrix (same doc-assertion style as existing docs-backed tests).
- [ ] 1.10 Semantics gate addition (`semantics and not integration`, offline): under `require`, a mixed stream of genuine signed, tampered, and forged intents — with kills injected at phase boundaries — yields exactly one execution per genuine `intent_id`, zero executions and zero published results for tampered/forged deliveries, and every tampered/forged delivery accounted for on the dead-letter sink.

## 2. Wire schema

- [ ] 2.1 Add `SignatureScheme` enum and the `signature_scheme` (12), `signing_key_id` (13), `signature` (14) fields to `ToolIntent` in `protos/beam_agents.proto`, with comments stating the transported-never-computed rule and the cleared-fields signing-input definition.
- [ ] 2.2 Regenerate bindings via `scripts/gen_proto.sh`; commit; confirm the diff-clean regen gate passes.
- [ ] 2.3 Extend the golden-blob fixtures: keep the existing pre-signature `ToolIntent` blob (it now doubles as the unsigned-decode fixture) and add a signed-intent blob plus the unknown-future-field blob for the signing-input stability test.

## 3. Signing module

- [ ] 3.1 Implement `src/beam_agents/intent_signing.py`: `sign_intent(intent, key_id, key) -> ToolIntent`, `verify_intent(intent, keyring) -> VerificationResult` (ok / unsigned / bad_signature / unknown_signing_key), cleared-fields deterministic serialization helper, and `load_keyring(reference)` for `env:`/`file:` references — stdlib `hmac`/`hashlib` only, no Beam import, no client-library import.
- [ ] 3.2 Verify the module passes the effector's import-boundary tests (AST walk + blocked-import subprocess) so both `actions/` and `effector/` may import it.

## 4. Outbox signer (pipeline side)

- [ ] 4.1 Add a frozen signer-spec dataclass (scheme, `signing_key_id`, key reference) accepted by `WriteIntents(uri, signer=...)`; validate the reference eagerly and import-free at construction.
- [ ] 4.2 Resolve the key in the serializing DoFn's `setup()`; stamp signature fields in `_SerializeIntent` before `SerializeToString(deterministic=True)`; leave the unsigned path byte-identical.

## 5. Effector verification and dead-letter path

- [ ] 5.1 Extend `EffectorConfig`: `verify_intents` mode (`off` default), keyring reference, optional `dead_letters_to` URI; startup validation per the mode requirement (verifying mode requires keyring; `require` without dead-letter channel warns), all import-free.
- [ ] 5.2 Implement the verification phase in `EffectorService.process` ahead of `refuse_expired`; on failure route to the dead-letter `MessageSink` (or log-and-count), increment per-reason counters (`unsigned_intent`, `bad_signature`, `unknown_signing_key`, `unsigned_intents_accepted`), and commit.
- [ ] 5.3 Wire `dead_letters_to` through `__main__.py` (flag + `EFFECTOR_DEAD_LETTERS_TO` env var) and `build_service`, reusing `build_message_sink`.
- [ ] 5.4 Update `docs/effector.md`: phase order gains the verify step; point to `docs/security.md` for modes, keys, and rollout.

## 6. Broker transport security

- [ ] 6.1 Implement the transport-security block (frozen dataclass: protocol, SASL mechanism, TLS paths, credential references) with eager import-free validation of reference syntax; add it to `EffectorConfig` and as a `WriteIntents` parameter.
- [ ] 6.2 Thread it into `KafkaIntentSource`, `KafkaMessageSink`, and `_build_kafka_writer`'s producer config, resolving references at client construction and storing nothing resolved back on the config.
- [ ] 6.3 Add flags/env vars in `__main__.py` for the effector-side block.
- [ ] 6.4 Integration lane: a SASL-enabled Redpanda profile in `docker/compose.yaml` and an `-m integration` test proving the effector consumes and publishes through authenticated listeners.

## 7. Secret-handling review closure

- [ ] 7.1 Add a `redact_uri()` helper and apply it in every `EffectorConfigError`/`ValueError` message in `effector/config.py` and in `__main__.py`'s stderr path.
- [ ] 7.2 Implement `EffectorConfig.__repr__` with userinfo redaction on the dedup and transport URIs.
- [ ] 7.3 Sweep the effector and outbox writer for other interpolation sites (log statements, adapter constructors) that could render a credentialed URI; redact or remove.

## 8. Documentation

- [ ] 8.1 Write `docs/security.md` per the documentation requirement: enforce-vs-document boundary, Kafka SASL/mTLS + ACLs, Pub/Sub IAM least-privilege matrix, secret-manager pattern, signing-key provisioning and rotation, `off`→`permissive`→`require` rollout with the counters to alarm on (`unsigned_intents_accepted`, per-reason dead-letter counts), the `result_ttl_ms` ≥ max intent TTL replay rule, and the secrets-never-in-args contract with the effector-side credential-resolution example.

## 9. Gates

- [ ] 9.1 `make lint` (ruff, incl. ASYNC rules) clean.
- [ ] 9.2 `make type` (`mypy --strict`) clean, including the new `intent_signing.py` and config surfaces.
- [ ] 9.3 `make test-unit` green offline with no docker and no optional client libraries installed; new semantics addition passes under the offline `semantics and not integration` selection.
- [ ] 9.4 `make test-integration` green (SASL Redpanda leg included) where docker is available.
- [ ] 9.5 Coverage ratchet holds or ratchets up; regen diff-clean gate passes on `src/beam_agents/_protos/`.
- [ ] 9.6 `uv run pre-commit run --all-files` clean.
- [ ] 9.7 `openspec validate add-effector-security --strict` passes.
