## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/yaml/test_refs.py`: the reference grammar and resolution — a valid `module:object` reference resolves to the module-level object (including a dotted attribute path after the colon); no-colon and empty-side forms raise `ValueError` quoting the reference and the grammar; an unimportable module raises `ValueError` naming the module, chained from the `ImportError`; a missing attribute raises `ValueError` naming attribute and module; a resolved non-agent (a module, a string) is rejected stating what was found. Derived from "Agent and callable config values are module:object references resolved at construction time" (all five scenarios).
- [ ] 1.2 `tests/yaml/test_config_mapping.py`: the YAML-config → `AgentConfig` round-trip — scalar knobs, verbatim sink URIs, `hitl` mapping onto `HitlPolicy` (including an `on_timeout` reference), `decode`/`tool_registry` references; the built `provider_factory` pickles and calls the referenced callable with exactly the `provider_config` kwargs; unknown top-level and nested `hitl` keys raise `ValueError` listing valid keys; a bad sink scheme and a non-positive knob propagate the delegated `ValueError`; a misspelled `provider_config` kwarg fails at construction via signature introspection. Derived from "YAML config maps totally onto AgentConfig and rejects unknown keys" (all four scenarios).
- [ ] 1.3 `tests/yaml/test_transform.py`: the row boundary and multi-output naming on a DirectRunner `TestPipeline` — rows with default `key`/`payload` fields reach the wrapped `RunAgent` as `KV[bytes, AgentEnvelope]` (str key UTF-8-encoded, payload on `external_event`); `event_time_field` overrides the element timestamp; a row missing `key_field` dead-letters to `errors` naming the field while the rest of the bundle processes; `output`/`intents`/`traces`/`errors` are addressable by name; tagged streams carry rows (intent scalar fields, the existing trace/error row shapes), never raw protos; a configured `intents_to` (stub resolver, offline) leaves the `intents` named output consumable. Derived from the row-boundary and multi-output requirements (all five scenarios).
- [ ] 1.4 `tests/yaml/test_yaml_e2e.py`: a complete YAML pipeline document — provider mapping for `beam_agents.yaml.run_agent`, `agent` referencing a module-level test agent, `provider` referencing a module-level `FakeLLM` factory — parsed with `apache_beam.yaml` and run on DirectRunner: completes offline, agent output observed, `FakeLLM` recorded the calls. Also asserts `beam_agents.yaml` itself never imports `apache_beam.yaml` (design D1). Derived from "An end-to-end YAML pipeline runs RunAgent offline with FakeLLM".
- [ ] 1.5 `tests/yaml/test_docs_example.py`: the docs' example pipeline and provider declaration parse, and every fully-qualified path and config key they use is one the shipped constructor accepts. Derived from "The documented example matches the shipped surface".

## 2. Reference resolution

- [ ] 2.1 Create `src/beam_agents/yaml/_refs.py`: parse the `module:object` grammar (dotted attribute path allowed after the colon), resolve via `importlib.import_module` + iterated `getattr`, and raise the four enumerated `ValueError` forms with the offending reference in every message. Import-side-effect-free.
- [ ] 2.2 Add the shallow structural agent check (callable or has `activate`) and per-position expectation messages so the same resolver serves agent, provider, decode, `on_timeout`, and `tool_registry` references.

## 3. Config mapping

- [ ] 3.1 Create `src/beam_agents/yaml/_config.py`: the fixed keyword surface (design D4 table), shape validation with unknown-key rejection (top-level and nested `hitl`), and construction of `AgentConfig`/`HitlPolicy` by delegation — no duplicated range or URI validation.
- [ ] 3.2 Build `provider_factory` as `functools.partial(resolved, **provider_config)`; probe picklability with `pickle.dumps` and validate kwarg names against `inspect.signature` when introspectable, both failing with `ValueError` naming the reference at construction.

## 4. The YAML-facing transform

- [ ] 4.1 Create `src/beam_agents/yaml/transform.py`: the wrapping `beam.PTransform` — input rows → `KV[bytes, AgentEnvelope]` (configured `key_field`/`payload_field`/`event_time_field`, UTF-8 key encoding, element-timestamp fallback), malformed rows routed to `errors` as malformed-input records, then the wrapped `RunAgent` with the mapped config.
- [ ] 4.2 Row-ify the outputs: reuse `trace_event_to_row`/`activation_error_to_row` for `traces`/`errors`, add the `ToolIntent` scalar-field row mapping for `intents`, and emit `output` as key + opaque-bytes rows; surface all four under Beam YAML's multi-output convention with the `RunAgentOutputs` names (verify the exact declaration mechanism against the pinned Beam version — design Open Questions — and record the answer in the module docstring).
- [ ] 4.3 Create `src/beam_agents/yaml/__init__.py` exposing `run_agent` at its stable fully-qualified path, with the capability-overview docstring; no `apache_beam.yaml` import anywhere in the package.
- [ ] 4.4 Add the per-module mypy Beam-untyped-API overrides in `pyproject.toml` (same relaxation set as `core/transform.py`) and any test-group dependency the e2e suite needs to import `apache_beam.yaml` (resolve the design's test-environment open question here).

## 5. Provider packaging and docs

- [ ] 5.1 Ship the static provider-listing file under `src/beam_agents/yaml/` mapping `RunAgent` → `beam_agents.yaml.run_agent`, in the form the pinned Beam version's provider-inclusion mechanism accepts (verified, not assumed).
- [ ] 5.2 Write `docs/yaml.md`: the verified `providers:` block (with the C25 `beam-agents==0.1.0` package pin), the full config-key table, the reference convention and its trust-boundary note (references import installed code; no dynamic code in the document; keep secrets out via templating), the multi-output addressing example, and a complete example pipeline (source → `RunAgent` → `intents_to` outbox URI).

## 6. Gates

- [ ] 6.1 `make lint` and `make type` clean (`mypy --strict`; no `Any` in public signatures).
- [ ] 6.2 `make test-unit` passes offline with no docker; the e2e YAML suite runs in the required lane, not behind a skip.
- [ ] 6.3 `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new package improves it.
- [ ] 6.4 `uv run pre-commit run --all-files` clean.
- [ ] 6.5 `openspec validate add-yaml-provider --strict` passes.
