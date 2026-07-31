## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 Wire-schema tests from the wire-schemas delta scenarios: "Signature fields round-trip", "A pre-signature intent decodes as unsigned" (against a committed pre-signature golden blob), and "A future-field intent still yields a stable signing input" (a blob with an injected unknown high-numbered field must re-serialize to the signer's cleared-fields bytes). — `tests/core/test_wire_schemas.py` (3 scenarios) + `tests/core/test_schema_compat.py` (committed-blob halves, incl. the pre-signature `v1/tool_intent.bin`).
- [x] 1.2 Signing-module tests from "ToolIntents are signed at the outbox writer": deterministic signatures (same intent + key → same bytes, twice), verify round-trip, constant-time comparison used, keyring loading from `env:`/`file:` references, malformed reference rejected. — `tests/test_intent_signing.py`, 26 tests; `intent_signing.py` lands at 100% branch coverage.
- [x] 1.3 `WriteIntents` signer tests: "A signed intent verifies against the signing key", "A retried bundle re-signs to byte-identical messages", "No signer configured preserves today's output" (byte-compare against current `_SerializeIntent` output), "Key bytes never enter the pipeline graph" (pickle the transform, assert the key material is absent). — `tests/actions/test_write_intents_signing.py`; the pickle test peels Beam's base64+bz2 envelope and asserts the *reference* is present so the absence assertions cannot pass vacuously. Also carries the keyed-state byte-identity test (staged intents + `Continuation` unchanged across signing).
- [x] 1.4 Effector verification tests from "The effector verifies intent signatures before any other phase": valid signed intent executes end to end; tampered intent → no tool invocation, no `ToolResult`, no dedup claim (assert the in-memory store is empty for that id); unknown key id dead-letters with its distinct reason; tampered-and-expired intent dead-letters for the signature, publishing no `EXPIRED` result. — `tests/effector/test_verification.py`; the no-claim assertion uses `RecordingDedupStore.calls == []`, which is stronger than "empty for that id".
- [x] 1.5 Dead-letter tests from "Verification failures are dead-lettered and never produce a ToolResult": verbatim payload on the dead-letter sink, offset committed, next intent on the partition unblocked; no-channel case logs identity fields only (assert `args_json` content absent from the log record) and still commits. — `tests/effector/test_verification.py`.
- [x] 1.6 Mode tests from "Verification mode governs unsigned intents across the rollout": `off` is byte-for-byte today's behavior on signed and unsigned streams; `permissive` accepts+counts unsigned, still dead-letters `bad_signature`; `require` dead-letters unsigned; verifying mode without a keyring fails at startup; `require` without `dead_letters_to` warns. — `tests/effector/test_verification.py`; the unresolvable-keyring half of startup validation is in `tests/effector/test_main.py` (proved to run before any client by leaving `aiokafka` uninstalled).
- [x] 1.7 Transport-security tests from "Broker transport security is configurable with credentials by reference": settings reach a fake/captured Kafka client constructor on source, sinks, and `WriteIntents`; malformed reference rejected import-free; resolved secret absent from config attributes, `repr`, and raised errors. — `tests/effector/test_transport_security.py`.
- [x] 1.8 Redaction tests from "Secrets embedded in URIs never appear in errors or reprs": credentialed malformed URI → exception chain free of the password; `repr(EffectorConfig)` masked; `beam-agents-effector` startup error path prints redacted output. — `tests/effector/test_redaction.py`; the chain check walks `__cause__`/`__context__` and asserts on the password's *absence*, so a leak that changes shape still fails.
- [x] 1.9 Docs tests from "The security baseline is documented...": `docs/security.md` exists and contains the secrets-in-args contract and the per-principal least-privilege matrix (same doc-assertion style as existing docs-backed tests). — `tests/effector/test_security_docs.py`.
- [x] 1.10 Semantics gate addition (`semantics and not integration`, offline): under `require`, a mixed stream of genuine signed, tampered, and forged intents — with kills injected at phase boundaries — yields exactly one execution per genuine `intent_id`, zero executions and zero published results for tampered/forged deliveries, and every tampered/forged delivery accounted for on the dead-letter sink. — `tests/semantics/test_effector_signing.py`, 5 cells; see Revision 2 for the exactly-once wording.

## 2. Wire schema

- [x] 2.1 Add `SignatureScheme` enum and the `signature_scheme` (12), `signing_key_id` (13), `signature` (14) fields to `ToolIntent` in `protos/beam_agents.proto`, with comments stating the transported-never-computed rule and the cleared-fields signing-input definition. — additive only; no existing field renumbered or retyped, no `state_schema_version` bump.
- [x] 2.2 Regenerate bindings via `scripts/gen_proto.sh`; commit; confirm the diff-clean regen gate passes. — regenerated with the *locked* grpcio-tools (the `precommit` group); see Revision 1.
- [x] 2.3 Extend the golden-blob fixtures: keep the existing pre-signature `ToolIntent` blob (it now doubles as the unsigned-decode fixture) and add a signed-intent blob plus the unknown-future-field blob for the signing-input stability test. — `v1/tool_intent_signed.bin`, `v1/tool_intent_future_field.bin`; regeneration left all ten pre-existing blobs byte-identical, which is itself the additive-change proof.

## 3. Signing module

- [x] 3.1 Implement `src/beam_agents/intent_signing.py`: `sign_intent(intent, key_id, key) -> ToolIntent`, `verify_intent(intent, keyring) -> VerificationResult` (ok / unsigned / bad_signature / unknown_signing_key), cleared-fields deterministic serialization helper, and `load_keyring(reference)` for `env:`/`file:` references — stdlib `hmac`/`hashlib` only, no Beam import, no client-library import.
- [x] 3.2 Verify the module passes the effector's import-boundary tests (AST walk + blocked-import subprocess) so both `actions/` and `effector/` may import it. — added to `_ALLOWED_INTERNAL` in `tests/effector/test_boundary.py`; the AST and blocked-import subprocess checks both pass.

## 4. Outbox signer (pipeline side)

- [x] 4.1 Add a frozen signer-spec dataclass (scheme, `signing_key_id`, key reference) accepted by `WriteIntents(uri, signer=...)`; validate the reference eagerly and import-free at construction. — `IntentSigner` in `intent_signing.py` (frozen, slots, `__post_init__` validation).
- [x] 4.2 Resolve the key in the serializing DoFn's `setup()`; stamp signature fields in `_SerializeIntent` before `SerializeToString(deterministic=True)`; leave the unsigned path byte-identical. — `sign_intent` returns a *copy*, so the staged proto Beam passes by reference is never mutated.

## 5. Effector verification and dead-letter path

- [x] 5.1 Extend `EffectorConfig`: `verify_intents` mode (`off` default), keyring reference, optional `dead_letters_to` URI; startup validation per the mode requirement (verifying mode requires keyring; `require` without dead-letter channel warns), all import-free.
- [x] 5.2 Implement the verification phase in `EffectorService.process` ahead of `refuse_expired`; on failure route to the dead-letter `MessageSink` (or log-and-count), increment per-reason counters (`unsigned_intent`, `bad_signature`, `unknown_signing_key`, `unsigned_intents_accepted`), and commit. — `_verified`/`_dead_letter`; also increments an `intents_dead_lettered` total for a single alarm.
- [x] 5.3 Wire `dead_letters_to` through `__main__.py` (flag + `EFFECTOR_DEAD_LETTERS_TO` env var) and `build_service`, reusing `build_message_sink`. — the keyring loads *first* in `build_service`, so a verifying mode with an unresolvable keyring fails before any client is constructed.
- [x] 5.4 Update `docs/effector.md`: phase order gains the verify step; point to `docs/security.md` for modes, keys, and rollout.

## 6. Broker transport security

- [x] 6.1 Implement the transport-security block (frozen dataclass: protocol, SASL mechanism, TLS paths, credential references) with eager import-free validation of reference syntax; add it to `EffectorConfig` and as a `WriteIntents` parameter. — `TransportSecurity` in `effector/config.py`; `write_intents.py` takes it as an annotation-only import so the pipeline's runtime closure never pulls the effector package.
- [x] 6.2 Thread it into `KafkaIntentSource`, `KafkaMessageSink`, and `_build_kafka_writer`'s producer config, resolving references at client construction and storing nothing resolved back on the config. — `client_kwargs()` (aiokafka) and `java_producer_config()` (Beam's cross-language Java client); `_kafka_producer_config` is split out so the offline lane can assert the settings without an expansion service.
- [x] 6.3 Add flags/env vars in `__main__.py` for the effector-side block. — `--kafka-security-protocol`, `--kafka-sasl-mechanism`, `--kafka-sasl-{username,password}-reference`, `--kafka-ssl-{ca,cert,key}`, each with an `EFFECTOR_KAFKA_*` env fallback.
- [ ] 6.4 Integration lane: a SASL-enabled Redpanda profile in `docker/compose.yaml` and an `-m integration` test proving the effector consumes and publishes through authenticated listeners. **(blocked: needs docker)** — the test is written (`tests/effector/test_transport_security_integration.py`) and skips cleanly until `EFFECTOR_SASL_BOOTSTRAP`/`EFFECTOR_SASL_USER`/`EFFECTOR_SASL_PASSWORD` name a listener. The `docker/compose.yaml` profile is deliberately NOT committed: it cannot be brought up or verified here, and an unverified profile in the shared integration lane would fail other agents' runs rather than this one's.

## 7. Secret-handling review closure

- [x] 7.1 Add a `redact_uri()` helper and apply it in every `EffectorConfigError`/`ValueError` message in `effector/config.py` and in `__main__.py`'s stderr path. — the helper redacts URI userinfo *anywhere in a string*, so it applies to whole messages and covers interpolation sites added later by default.
- [x] 7.2 Implement `EffectorConfig.__repr__` with userinfo redaction on the dedup and transport URIs. — renders every field through `redact_uri(repr(...))`, so a credentialed URI in any field is covered, not just the two known ones.
- [x] 7.3 Sweep the effector and outbox writer for other interpolation sites (log statements, adapter constructors) that could render a credentialed URI; redact or remove. — the sweep also found that `redis://user:password@` (all userinfo, no host) passed validation, so its credential never even reached a message to redact; see Revision 3.

## 8. Documentation

- [x] 8.1 Write `docs/security.md` per the documentation requirement: enforce-vs-document boundary, Kafka SASL/mTLS + ACLs, Pub/Sub IAM least-privilege matrix, secret-manager pattern, signing-key provisioning and rotation, `off`→`permissive`→`require` rollout with the counters to alarm on (`unsigned_intents_accepted`, per-reason dead-letter counts), the `result_ttl_ms` ≥ max intent TTL replay rule, and the secrets-never-in-args contract with the effector-side credential-resolution example. — added to the mkdocs nav under "Operating the runtime".

## 9. Gates

- [x] 9.1 `make lint` (ruff, incl. ASYNC rules) clean.
- [x] 9.2 `make type` (`mypy --strict`) clean, including the new `intent_signing.py` and config surfaces. — two pre-existing `unused-ignore` errors in `effector/sources.py`/`sinks.py` (introduced by the M2 merge making `google-cloud-pubsub` resolvable in the typecheck env, verified against `HEAD`) had to be fixed for the gate to pass; both files are in this change's Impact.
- [x] 9.3 `make test-unit` green offline with no docker and no optional client libraries installed; new semantics addition passes under the offline `semantics and not integration` selection. — 1662 passed / 9 skipped; `make test-semantics-offline` 72 passed / 5 skipped; `scripts/check_semantics_partition.py` OK (73 offline + 29 docker).
- [ ] 9.4 `make test-integration` green (SASL Redpanda leg included) where docker is available. **(blocked: needs docker)**
- [x] 9.5 Coverage ratchet holds or ratchets up; regen diff-clean gate passes on `src/beam_agents/_protos/`. — ratcheted **up** to 0.9090 (from 0.9028); `scripts/gen_proto.sh` is diff-clean on a second run.
- [ ] 9.6 `uv run pre-commit run --all-files` clean. **(blocked: pre-commit hooks fetch their environments from the network)** — its two locally-checkable gates were run directly instead: ruff (9.1) and the protobuf-drift regen check (9.5).
- [x] 9.7 `openspec validate add-effector-security --strict` passes.

## Revision 1 — regenerate protos with the locked toolchain, not a fresh install

`uv pip install grpcio-tools` (as the task list assumed) resolves the newest
`grpcio-tools`, which pulls `protobuf` 7.x into the environment and stamps the
generated modules with a 7.x gencode version. The locked runtime is `protobuf`
6.33.6, and the generated bindings then refuse to import at all
(`VersionError: Runtime version cannot be older than the linked gencode
version`) — every test in the suite fails at collection.

The fix is to take `grpcio-tools` from the lockfile's `precommit` group, which
is where it already lives for the protobuf-drift pre-commit hook, and which is
therefore also what CI regenerates with:

```sh
uv sync --locked --group lint --group typecheck --group test --group precommit
scripts/gen_proto.sh
```

No artifact change is needed beyond this note; the task text is unchanged
because `scripts/gen_proto.sh` is still the regeneration command.

## Revision 2 — the semantics gate asserts at-least-once under kills, exactly-once without

Task 1.10 and this change's proposal both say the gate should yield "exactly one
execution per genuine `intent_id`" with kills injected at phase boundaries. That
is stronger than the guarantee the runtime actually makes, and stronger than the
existing effectively-once gate asserts: a worker killed *between* invoking a tool
and writing its durable completion record re-executes on redelivery. That window
is documented in `docs/effector.md` ("What is guaranteed, and what is not") and
in `project.md`'s semantics-tier description ("duplicates bounded to the SIGKILL
crash window between a tool's effect and its durable completion record").

Signing neither widens nor narrows that window, so the gate now asserts:

- `set(calls) == {the genuine intents}` under every kill point — nothing
  adversarial ever executes, which is the property this change adds; and
- exact once-each execution in the no-kill control cell.

Asserting exactly-once under kills would have meant asserting a guarantee the
runtime does not make, and the only way to make it pass would have been to weaken
the kill injection — the wrong direction.

The gate also delivers each intent on its own partition. With a single partition,
a mid-stream kill leaves the dispatcher blocked feeding a dead worker's bounded
queue: a deadlock in the offline harness rather than a property of the service
(real scale-out spreads keys across partitions), and the pre-existing
effectively-once gate sidesteps it by using one intent per pass.

## Revision 3 — `redis://user:password@` was not malformed enough to be caught

The `effector-security` spec's redaction scenario says: "WHEN `EffectorConfig` is
constructed with a malformed dedup URI containing `redis://user:secret@host`".
Under the shipped parser that URI is *valid* — `parse_dedup_uri` accepted any
non-empty `netloc`, and `redis://user:password@` has the truthy netloc
`user:password@` with no host at all. So the credential-carrying typo the
scenario describes did not raise, and there was no message to redact.

The redis check now requires `parsed.hostname` rather than `parsed.netloc`, which
makes an all-userinfo URI malformed (as it should be) and makes the scenario
literally testable. The pre-existing `redis://` case still raises, and every
valid form (`redis://host`, `redis://host:port/db`, `redis://:pw@host:port`)
still parses. `tests/effector/test_redaction.py` covers both the redis and the
bigtable malformed-and-credentialed shapes.

## Revision 4 — `DeliveredIntent` carries the raw delivered bytes

The dead-letter requirement says the effector publishes "the raw delivered
payload under the raw delivered key". `DeliveredIntent` carried only the *parsed*
`ToolIntent`, and re-serializing it would publish something subtly different from
what arrived — precisely what a forensic record must not do, and what would make
a dead letter unusable for re-driving after a keyring fix.

`DeliveredIntent` therefore gains `payload`/`key` fields (defaulting to empty,
with `raw_payload()`/`raw_key()` falling back to a deterministic re-encode) and
the Kafka and Pub/Sub sources populate them. This is additive on a plain
dataclass, not a wire message, so no schema question arises.
