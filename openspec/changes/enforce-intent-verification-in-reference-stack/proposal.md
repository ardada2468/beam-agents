## Why

The 1.0 release gate (`openspec/changes/add-1-0-0-release`, design D1(d)) requires that effector intent signing ship "with rollout complete: verification *enforced*, not merely available". `add-effector-security` shipped the whole mechanism — the additive signature envelope, `IntentSigner` on `WriteIntents`, verification as the effector's phase zero, dead-lettering that never publishes a `ToolResult`, and an offline semantics gate (`tests/semantics/test_effector_signing.py`) — but `EffectorConfig.verify_intents` defaults to `off`, and nothing the repository runs ever turned the dial. The gate therefore recorded condition 2.4 as `pending (rollout)`: moving a deployment to `require` was an operator step no artifact here could evidence.

The repository does have a deployment of its own: the docker compose stack in `docker/` plus the pipeline and `beam-agents-effector` worker pool the effectively-once end-to-end gate (`tests/semantics/test_effectively_once_e2e.py`) runs against it, on every `flink-minicluster` CI job. It ran unsigned, at `off`. Making *that* deployment enforce — and making the gate fail if it ever stops — turns "enforced" into a fact CI re-checks rather than a rollout asserted from memory.

## What Changes

- **The e2e gate's pipeline signs every intent.** The harness's outbox publisher (`tests/semantics/_e2e/outbox.py`, the stand-in for the upstream-broken `WriteIntents(kafka://…)` writer) gains a signing step that mirrors `WriteIntents._SerializeIntent` exactly: the production `IntentSigner` carrying only a `file:` key *reference*, resolved worker-side in `setup()` inside the `beam-sdk-harness` container, `sign_intent` on a copy. The key is minted fresh per run (32 CSPRNG bytes) by `Stack.provision_keyring` and written as a `key_id=base64(key)` keyring into the run's spool directory — the one filesystem the pipeline container and the host already share — and removed at teardown. Nothing about the key is committed.
- **The e2e gate's effector pool runs `--verify-intents require`**, with `--signing-keys file:<the same keyring>` and `--dead-letters-to kafka://…/<run>-dead-letters` (a new run-scoped topic). The command line is a module-level function (`effector_argv`) so the unit tier can pin the posture without spawning a process.
- **The gate injects one unsigned forgery and asserts on it.** Between phase A and phase B (chaos complete, no replacement worker joining, so the dead-letter publish has no crash window), the host publishes one unsigned `charge` intent with a formula-minted `intent_id` for a key outside every population. The gate asserts: it is dead-lettered within a lease-aware deadline; it is the *only* record on the dead-letter topic (raw key and raw payload, verbatim, once); no `ToolResult` is published for it; it never reaches the execution ledger in either phase; and every genuine intent on the wire verifies under the run's key. Under `off` or `permissive` the forgery executes and the gate is red — on the ledger's unknown-member check and on the empty dead-letter topic alike.
- **The reference stack's posture is documented**: `docs/security.md` gains "The reference stack runs `require`"; `docs/ci.md`, `docs/effector.md`, `docs/index.md`'s guarantee table, `docker/README.md`, and a comment on the `e2e-spool` bind mount in `docker/compose.yaml` point at it.
- **Release gate condition 2.4 is flipped to met** in `openspec/changes/add-1-0-0-release/tasks.md` and the `### Release gate` table in `CHANGELOG.md`, citing the gate test.
- **Not changing**: `EffectorConfig.verify_intents` stays `off` by default (flipping it would be a 1.0 breaking change for every deployment without keys); no `src/` file is touched; the conformance matrix's Flink and Spark legs, which share the outbox's `publish_tagged`, are unaffected (`publish_intents`' signer is optional and only the e2e pipeline passes one). No effector is added to `docker/compose.yaml` or `docker/compose.console.yaml`: neither runs one today — the gate SIGKILLs its workers, so they are host processes by design — and a compose effector nothing consumes from would be configuration theater.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `effector-security`: gains a requirement that the repository's reference deployment enforces verification (`require`) end to end on real infrastructure and proves it with an injected unsigned intent, so the 1.0 gate's "enforced, not merely available" condition has repository-checkable evidence. The three existing verification-mode requirements are unchanged.

## Impact

**Tests (the whole implementation)**
- `tests/semantics/_e2e/stack.py` — `dead_letters_topic`, `host_keyring`/`container_keyring`, `SIGNING_KEY_ID`, `Stack.provision_keyring`.
- `tests/semantics/_e2e/outbox.py` — `_SignAndEncodeIntent`, `publish_intents(..., signer=)`.
- `tests/semantics/_e2e/pipeline.py` — `intent_signer(config)` wired into `publish_intents`.
- `tests/semantics/_e2e/workers.py` — `VERIFY_INTENTS = "require"`, `effector_argv`.
- `tests/semantics/_e2e/forgery.py` — new: the unsigned forgery and its host-side injection.
- `tests/semantics/test_effectively_once_e2e.py` — keyring provisioning, injection, dead-letter wait, post-run authenticity assertions.
- `tests/semantics/test_e2e_harness_unit.py` — four offline pins: per-run keyring round-trip, outbox-signer parity with `WriteIntents`, forgery shape, effector argv posture parsed by the real CLI.

**Docs / config**: `docs/security.md`, `docs/ci.md`, `docs/effector.md`, `docs/index.md`, `docker/README.md`, `docker/compose.yaml` (comment only).

**Release records**: `openspec/changes/add-1-0-0-release/tasks.md` (2.4), `CHANGELOG.md` (`### Release gate` row). `tests/release/test_release_1_0_0.py` stays green: it checks the archival and Spark rows against the tree and only requires the signing row to exist.

**Cost**: the gate gains one intent publish and one lease-bounded wait (seconds) on a ~10-minute run. Signing adds one HMAC per intent in the harness container.

**Risk**: the "exactly one dead letter" assertion is strict. It is placed where no effector kill and no rebalance is in flight, and the effector's dead-letter publish is retried only on failure, so a duplicate there would be a real finding, not noise.
