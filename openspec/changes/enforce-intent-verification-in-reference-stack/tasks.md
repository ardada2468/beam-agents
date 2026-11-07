## 1. Harness — `tests/semantics/_e2e/` (test-only; no `src/` changes)

- [x] 1.1 `stack.py`: add the run-scoped `dead_letters_topic` (in `all_topics`), `SIGNING_KEY_ID`, `host_keyring`/`container_keyring`, and `Stack.provision_keyring` — 32 CSPRNG bytes, `key_id=base64(key)` line, written into the spool directory with mode `0600`, returned so the gate can verify the wire independently (design D1)
- [x] 1.2 `outbox.py`: `_SignAndEncodeIntent` mirroring `WriteIntents._SerializeIntent` (reference-only `IntentSigner`, `resolve_key()` in `setup()`, `sign_intent` on a copy, deterministic bytes); `publish_intents(..., signer=None)` so the conformance legs are unaffected (design D2)
- [x] 1.3 `pipeline.py`: `intent_signer(config)` → `IntentSigner(SIGNING_KEY_ID, "file:<container keyring>")`, passed to `publish_intents`
- [x] 1.4 `workers.py`: `VERIFY_INTENTS = "require"`; `effector_argv(config)` adds `--verify-intents require --signing-keys file:<host keyring> --dead-letters-to kafka://…/<dead-letters topic>`; `_spawn_one` uses it
- [x] 1.5 `forgery.py`: `forged_intent(config, now_ms)` — unsigned `charge` intent, formula-minted `intent_id`, key outside every population, one-hour TTL, asserted `UNSIGNED` at construction — and `inject(config, intent)` publishing it once from the host, keyed as the outbox keys genuine intents (design D3)

## 2. The gate — `tests/semantics/test_effectively_once_e2e.py`

- [x] 2.1 Provision the keyring after the spool; keep the key for the post-run wire check
- [x] 2.2 Between phase A's strong-form assertion and the TaskManager kill: inject the forgery, await its dead letter under `FORGERY_DEADLINE_S` (lease-aware; the failure message names the dial and the ledger count found), and assert it is absent from the ledger
- [x] 2.3 Capture the dead-letter topic before teardown; split the forgery off the captured intents so population assertions stay about the pipeline's output
- [x] 2.4 Post-run: every genuine intent on the wire verifies under the run's key; the forgery on the wire is the single unsigned intent injected; the dead-letter topic is exactly `[(raw key, raw payload)]`; no `ToolResult` carries the forged `intent_id`; the forged `intent_id` is in neither ledger counting
- [x] 2.5 Module docstring: the enforcement bullet and the signing half of the outbox substitution

## 3. Offline pins — `tests/semantics/test_e2e_harness_unit.py`

- [x] 3.1 Per-run keyring: 32 bytes, loads via `load_keyring("file:…")`, the signer names the container path, mode `0600`, not a `.seg`, fresh per call
- [x] 3.2 Outbox signer parity: output bytes equal `sign_intent(...).SerializeToString(deterministic=True)`, verify `OK`, element not mutated, `signer=None` is the pre-signing publisher
- [x] 3.3 Forgery shape: registered tool, formula id, prefix outside the populations, one-hour TTL, `UNSIGNED` under any keyring
- [x] 3.4 Effector argv: `require`, `file:` keyring reference, dead-letter URI, no `=` on argv, and the real CLI (`build_parser` + `config_from_args`) accepts it as a `require`-mode configuration

## 4. Docs and reference-stack notes

- [x] 4.1 `docs/security.md`: "The reference stack runs `require`" under the rollout dial, stating the default stays `off` and what the gate asserts
- [x] 4.2 `docs/ci.md` (gate section), `docs/effector.md` (inert-until-turned-on paragraph), `docs/index.md` (guarantee row), `docker/README.md` (effector is not a compose service; keyring via the spool mount), `docker/compose.yaml` (comment on the `e2e-spool` bind mount)
- [x] 4.3 `changelog.d/enforce-intent-verification-in-reference-stack.docs.md` — a `docs` fragment: no `src/` change, the user-visible delta is the documented posture

## 5. Release records

- [x] 5.1 `openspec/changes/add-1-0-0-release/tasks.md` 2.4: box checked with an addendum preserving the original finding and citing the gate; §3's status prose gets a matching addendum
- [x] 5.2 `CHANGELOG.md` `### Release gate`: the signing row's evidence rewritten and its verdict `pending (rollout)` → `pass`

## 6. Verify

- [x] 6.1 `make lint`, `make type`, `make test-unit`, `make test-semantics-offline` green; `tests/release/test_release_1_0_0.py` green
- [x] 6.2 `openspec validate enforce-intent-verification-in-reference-stack --strict`
- [x] 6.3 Local docker run: `make compose-up` (rebuilding `beam-sdk-harness`, which bakes `tests/` in), then `BEAM_AGENTS_E2E_EVENTS=600 uv run pytest tests/semantics/test_effectively_once_e2e.py` alone on an idle machine, then `make compose-down`; record the run's seed and the dead-letter assertion passing
  - *Run (2026-11-07):* 1 passed in 247 s at 600 events, seed `732179486`, 3 effector SIGKILLs plus one F12 submission resubmit; the forgery (`forged-<run>-00000`) was dead-lettered within the window, absent from both ledger countings, and every genuine intent on the wire verified under the run's key. Teardown removed the run's spool directory, keyring included
- [ ] 6.4 CI: the PR's `flink-minicluster` job runs the gate under `require` at full volume
