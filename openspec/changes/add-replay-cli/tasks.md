## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 Wire-schema round-trip tests (spec: "All four variants round-trip"; "An envelope written before export_request still decodes"; "A populated snapshot round-trips"; "Embedded blobs keep their own schema versions") — including new golden blobs for `AgentEnvelope` with `export_request` and for `StateSnapshot`, and the predating-golden compat assertion.
- [ ] 1.2 Export-route DoFn tests with a fake state handle, inside the mutmut selection (spec: "Snapshot captures the blobs a subsequent activation would load"; "State and seq are untouched by an export"; "An export produces no activation outputs"; "A retried bundle re-emits a byte-identical snapshot").
- [ ] 1.3 Transform/sink tests (spec: "A configured snapshots sink receives serialized snapshots keyed by entity"; "No sink configured still constructs and runs") using `TestPipeline` and the in-memory sink pattern the traces-sink tests use.
- [ ] 1.4 Replay-bundle tests (spec: "A resume replay is reconstructed from the continuation"; "A mismatched envelope is refused"; "An older-schema snapshot replays after migration"; "A newer-schema snapshot fails closed") against fixture snapshots and trace streams built in-test.
- [ ] 1.5 Cache-only provider tests (spec: "A cache miss aborts loudly instead of calling a provider"; "A digest-only entry is not silently refetched"; "Cache entries do not expire at replay time"; "Replay makes zero provider calls") — assert `CacheOnlyLLMClient.complete` raises unconditionally, and that a full replay over a populated cache blob never invokes it.
- [ ] 1.6 Diff tests (spec: "A divergent re-run produces a diff and exit code 1"; "Cache-hit normalization does not report false divergence"; "Unrepresented fields are reported, not diffed").
- [ ] 1.7 End-to-end reproduction test, `-m "semantics and not integration"` (spec: "A replayed activation reproduces the traced outcome"; "A failed activation replays to its traced failure position"): run an activation under `TestPipeline` with `FakeLLM` whose agent reads the event, calls the model, emits an intent, and writes memory only after its calls (the exact-replay shape per design D2); capture its committed state via an `export_request`, collect `.traces`, then replay through `beam_agents.replay` and assert byte-identical trace events, identical `intent_id`s, exit 0, and zero provider `complete` calls; repeat with an agent that raises before any model call and assert the failure position matches.

## 2. Proto and generated bindings

- [ ] 2.1 Add `StateExportRequest` and the `export_request = 6` oneof variant to `AgentEnvelope`, and the `StateSnapshot` message, in `protos/beam_agents.proto`; regenerate `_pb2.py` and confirm regen is diff-clean in CI.
- [ ] 2.2 Re-export the new messages from `beam_agents._protos` and commit the new golden blobs under `tests/core/golden/`.

## 3. Snapshot export runtime

- [ ] 3.1 Route the `export_request` payload variant in `core/dofn.py::process` to a read-only handler: read `MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`; build `StateSnapshot` with `snapshot_at_ms = envelope.event_time_ms` and the echoed `request_id`; yield on the `.snapshots` tag; touch no state, timer, or counter.
- [ ] 3.2 Expose `.snapshots` as a tagged output on the transform and add `snapshots_to` to `AgentConfig`, resolved through `DefaultSinkResolver` with a deterministic-serialization step keyed by `entity_key`, mirroring `traces_to`.
- [ ] 3.3 Re-check `mutation-baseline.toml` ceilings for `core/dofn.py` / `core/transform.py` after the new branches land.

## 4. Replay package

- [ ] 4.1 Implement `replay/provider.py`: `ReplayCacheMissError` (carrying the cache key and, for digest-only, the stored digest hex) and `CacheOnlyLLMClient` with an unconditionally raising `complete`, no transport, no endpoint.
- [ ] 4.2 Implement `replay/bundle.py`: load and parse the `StateSnapshot` file and the varint-length-delimited `TraceEvent` stream; select the target `(entity_key, seq)` (highest traced seq by default, `--seq` override); recover `now_ms` from the target `ACTIVATION_START.start_ms`; parse the envelope file and cross-check `entity_key`; derive `step_index`/adapter snapshot/resume payload for resumes from the embedded `Continuation`.
- [ ] 4.3 Implement schema-version checking and migration-on-load in `bundle.py`, delegating to the `state-schema-migration` capability's per-blob migration functions (sibling change `add-state-schema-migration`); refuse newer-than-supported versions with both versions named.
- [ ] 4.4 Implement the re-run: build `run_activation` arguments from the bundle, inject `CacheOnlyLLMClient` and the imported agent/registry/decode, and drive it with `asyncio.run`; map `ActivationFailed` to the traced-failure comparison rather than a crash.

## 5. Diff and CLI

- [ ] 5.1 Implement `replay/diff.py`: deterministic-bytes comparison of replayed vs traced `TraceEvent`s with the closed cache-hit/billed normalization; structured status and intent-attribute comparison; digest-and-size reporting for outputs and the memory blob; first-divergence rendering.
- [ ] 5.2 Implement `replay/__main__.py` following `effector/__main__.py`: `build_parser` (`--snapshot`, `--traces`, `--event`, `--agent`, `--registry`, `--decode`, `--seq`, `--log-level`), `module:attribute` import helper reuse, and the exit-code contract (0 reproduced / 1 diverged / 2 usage or version refusal / 3 irreproducible).
- [ ] 5.3 Add the `beam-agents-replay = "beam_agents.replay.__main__:main"` console script to `pyproject.toml`; keep `beam_agents/__init__.py` free of replay symbols (the CLI is a tool surface, not the public API).
- [ ] 5.4 Add `docs/replay.md`: the export workflow (publish request → collect snapshot from the sink → dump traces → fetch the envelope from the bus), the D2 pre-image table (what replays exactly vs. what reports irreproducible), TTL/eviction promptness guidance, and the events-topic trust note.

## 6. Gates

- [ ] 6.1 `make lint`
- [ ] 6.2 `make type`
- [ ] 6.3 `make test-unit`
- [ ] 6.4 Coverage ratchet: branch coverage does not decrease; update `coverage-baseline.toml` only to lock in a gain.
- [ ] 6.5 `uv run pre-commit run --all-files`
- [ ] 6.6 `openspec validate add-replay-cli --strict`
