## 1. Proto schema and generation plumbing

- [x] 1.1 Write `protos/beam_agents.proto` (proto3, package `beam_agents.v1`) with `MemoryBlob`, `ToolIntent`, `ToolResult`, `TraceEvent`, `AgentEnvelope` (+ nested `Approval`), and `Continuation` per design D4: int64 `*_ms` timestamps, `_UNSPECIFIED = 0` enum zeros, hot fields in tag range 1–15
- [x] 1.2 Update `scripts/gen_proto.sh` to emit `--python_out`/`--pyi_out` into `src/beam_agents/_protos/` and create `src/beam_agents/_protos/__init__.py` re-exporting the six message classes
- [x] 1.3 Run `make proto`, commit generated `beam_agents_pb2.py`/`.pyi`, and verify the pre-commit drift hook passes (re-run is diff-clean — scenario "Regeneration is diff-clean")
- [x] 1.4 Confirm `mypy --strict` and `ruff` pass on `src/` with the generated package (add per-module mypy override for `beam_agents._protos.*` only if the generated stubs require it)

## 2. Wire-schema tests (write first, must fail only before 1.x lands)

- [x] 2.1 Add `tests/core/test_wire_schemas.py` covering import/round-trip scenarios: bindings importable from `beam_agents._protos`; `MemoryBlob` entry order and `state_schema_version`; `ToolIntent` field round-trip incl. exact `args_json`; `ToolResult` status enum coverage; `TraceEvent` GenAI attributes; `AgentEnvelope` oneof exclusivity and all three variants; `Continuation` round-trip with byte-identical `snapshot`
- [x] 2.2 Add unknown-field forward-compat tests: hand-encode a blob with an extra field number, assert parse succeeds with known fields intact (scenario "Unknown fields are tolerated on decode"), and assert re-serializing the parsed message preserves the unknown field's bytes (scenario "Unknown fields survive re-encode")

## 3. Golden-blob compat baseline

- [x] 3.1 Write `tests/core/golden/generate.py` (manual-run generator, never invoked by CI) producing one fully-populated `.bin` fixture per message type with fixed, documented field values
- [x] 3.2 Run the generator once and commit the six golden blobs under `tests/core/golden/`
- [x] 3.3 Add `tests/core/test_schema_compat.py` decoding every golden blob and asserting field-level equality against expected values — explicitly NOT asserting byte-identical re-encode (design D6)

## 4. Coder tests (write first, fail for the right reason)

- [x] 4.1 Add `tests/core/test_coders.py` determinism tests: hypothesis strategies for all six message types; encode-twice byte-equality; `TraceEvent` attributes-map insertion-order independence
- [x] 4.2 Add round-trip tests: `decode(encode(msg)) == msg` for property-based instances of all six types, including default values, oneof cases, and opaque bytes
- [x] 4.3 Add registration tests: import-alone does not touch the registry; `register_coders()` makes registry lookups resolve the deterministic coder (not pickle fallback); double registration is harmless; `is_deterministic()` is True
- [x] 4.4 Add `TestPipeline` tests: each message type flows through a GroupByKey shuffle boundary intact after registration, and a `ToolIntent` works as a GBK key without a non-deterministic-coder error

## 5. Coder implementation

- [x] 5.1 Create `src/beam_agents/core/__init__.py` and `src/beam_agents/core/coders.py` with a `DeterministicProtoCoder` (parameterized by message type) using `SerializeToString(deterministic=True)`, `is_deterministic() == True`, and a docstring documenting the pinned-protobuf-version scope of the determinism claim (design D3)
- [x] 5.2 Implement idempotent `register_coders()` registering all six types; no import-time registry mutation (design D5)
- [x] 5.3 Make all section 2–4 tests pass; confirm nothing is re-exported from `beam_agents/__init__.py`

## 6. Verification and gates

- [x] 6.1 Full local gate: `make fmt lint type test-unit` all green; default pytest run passes offline with no docker
- [x] 6.2 Verify scenario→test traceability: every `#### Scenario:` in both spec files maps to a named test; record the mapping in the PR description
- [x] 6.3 Re-run `scripts/gen_proto.sh` and `git diff --exit-code` as a final drift check; confirm coverage ratchet does not decrease
