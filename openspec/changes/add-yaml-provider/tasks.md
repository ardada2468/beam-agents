## 1. Tests (written first, must fail for the right reason)

- [x] 1.1 `tests/yaml/test_refs.py`: the reference grammar and resolution — a valid `module:object` reference resolves to the module-level object (including a dotted attribute path after the colon); no-colon and empty-side forms raise `ValueError` quoting the reference and the grammar; an unimportable module raises `ValueError` naming the module, chained from the `ImportError`; a missing attribute raises `ValueError` naming attribute and module; a resolved non-agent (a module, a string) is rejected stating what was found. Derived from "Agent and callable config values are module:object references resolved at construction time" (all five scenarios). *(15 tests; also pins that a `lambda …`/`def …`/`os:system('…')` "reference" is rejected by grammar or import, never evaluated.)*
- [x] 1.2 `tests/yaml/test_config_mapping.py`: the YAML-config → `AgentConfig` round-trip — scalar knobs, verbatim sink URIs, `hitl` mapping onto `HitlPolicy` (including an `on_timeout` reference), `decode`/`tool_registry` references; the built `provider_factory` pickles and calls the referenced callable with exactly the `provider_config` kwargs; unknown top-level and nested `hitl` keys raise `ValueError` listing valid keys; a bad sink scheme and a non-positive knob propagate the delegated `ValueError`; a misspelled `provider_config` kwarg fails at construction via signature introspection. Derived from "YAML config maps totally onto AgentConfig and rejects unknown keys" (all four scenarios). *(17 tests; also pins that the factory is not invoked at construction and that an unpicklable reference fails at the boundary.)*
- [x] 1.3 `tests/yaml/test_transform.py`: the row boundary and multi-output naming on a DirectRunner `TestPipeline` — rows with default `key`/`payload` fields reach the wrapped `RunAgent` as `KV[bytes, AgentEnvelope]` (str key UTF-8-encoded, payload on `external_event`); `event_time_field` overrides the element timestamp; a row missing `key_field` dead-letters to `errors` naming the field while the rest of the bundle processes; `output`/`intents`/`traces`/`errors` are addressable by name; tagged streams carry rows (intent scalar fields, the existing trace/error row shapes), never raw protos; a configured `intents_to` (stub resolver, offline) leaves the `intents` named output consumable. Derived from the row-boundary and multi-output requirements (all five scenarios). *(18 tests; the D1 no-`apache_beam.yaml`-import rule is checked against each module's AST imports.)*
- [x] 1.4 `tests/yaml/test_yaml_e2e.py`: a complete YAML pipeline document — provider mapping for `beam_agents.yaml.run_agent`, `agent` referencing a module-level test agent, `provider` referencing a module-level `FakeLLM` factory — parsed and run on DirectRunner: completes offline, agent output observed, `FakeLLM` served the calls (proven fail-closed). Also asserts `beam_agents.yaml` itself never imports `apache_beam.yaml` (design D1). Derived from "An end-to-end YAML pipeline runs RunAgent offline with FakeLLM". *(7 tests. Executed through `tests/yaml/_yaml_driver.py` rather than `apache_beam.yaml`, whose extra is absent from the locked environment — see Revision 1.)*
- [x] 1.5 `tests/yaml/test_docs_example.py`: the docs' example pipeline and provider declaration parse, and every fully-qualified path and config key they use is one the shipped constructor accepts. Derived from "The documented example matches the shipped surface". *(21 tests; also pins the documented `beam-agents==X.Y.Z` pin against the installed version and asserts every accepted key is documented, so the page cannot fall behind the code.)*

## 2. Reference resolution

- [x] 2.1 Create `src/beam_agents/yaml/_refs.py`: parse the `module:object` grammar (dotted attribute path allowed after the colon), resolve via `importlib.import_module` + iterated `getattr`, and raise the four enumerated `ValueError` forms with the offending reference in every message. Import-side-effect-free.
- [x] 2.2 Add the shallow structural agent check (callable or has `activate`) and per-position expectation messages so the same resolver serves agent, provider, decode, `on_timeout`, and `tool_registry` references. *(`resolve_agent` / `resolve_callable` / `resolve_instance`; a module is rejected as an agent even when it carries an `activate` attribute.)*

## 3. Config mapping

- [x] 3.1 Create `src/beam_agents/yaml/_config.py`: the fixed keyword surface (design D4 table), shape validation with unknown-key rejection (top-level and nested `hitl`), and construction of `AgentConfig`/`HitlPolicy` by delegation — no duplicated range or URI validation. *(`CONFIG_KEYS`/`HITL_KEYS` are the published key sets; unset knobs are omitted so `AgentConfig`'s own defaults apply.)*
- [x] 3.2 Build `provider_factory` as `functools.partial(resolved, **provider_config)`; probe picklability with `pickle.dumps` and validate kwarg names against `inspect.signature` when introspectable, both failing with `ValueError` naming the reference at construction. *(A `**kwargs`-taking or C-implemented callable skips the name check rather than guessing.)*

## 4. The YAML-facing transform

- [x] 4.1 Create `src/beam_agents/yaml/transform.py`: the wrapping `beam.PTransform` — input rows → `KV[bytes, AgentEnvelope]` (configured `key_field`/`payload_field`/`event_time_field`, UTF-8 key encoding, element-timestamp fallback), malformed rows routed to `errors` as malformed-input records, then the wrapped `RunAgent` with the mapped config. *(Keying uses the documented `beam.WithKeys(...).with_output_types(tuple[bytes, AgentEnvelope])` idiom; the timestamp fallback maps Beam's unstamped sentinel to 0 — Revision 3.)*
- [x] 4.2 Row-ify the outputs: reuse `trace_event_to_row`/`activation_error_to_row` for `traces`/`errors`, add the `ToolIntent` scalar-field row mapping for `intents`, and emit `output` as opaque-bytes rows; surface all four under Beam YAML's multi-output convention with the `RunAgentOutputs` names. *(Mechanism verified against Beam 2.72.0 and recorded in the module docstring: `expand` returns `dict[str, PCollection]`. `output` carries no key — Revision 2.)*
- [x] 4.3 Create `src/beam_agents/yaml/__init__.py` exposing `run_agent` at its stable fully-qualified path, with the capability-overview docstring; no `apache_beam.yaml` import anywhere in the package. *(Also exports `PROVIDER_LISTING`, the packaged listing's path inside the wheel.)*
- [x] 4.4 Add the per-module mypy Beam-untyped-API overrides in `pyproject.toml` (same relaxation set as `core/transform.py`) and any test-group dependency the e2e suite needs. *(Overrides added for `beam_agents.yaml.transform` and `tests.yaml.*`, plus `ignore_missing_imports` for the unstubbed `yaml` module. **No dependency was added** — see Revision 1.)*

## 5. Provider packaging and docs

- [x] 5.1 Ship the static provider-listing file under `src/beam_agents/yaml/` mapping `RunAgent` → `beam_agents.yaml.run_agent`, in the form the pinned Beam version's provider-inclusion mechanism accepts (verified, not assumed). *(`providers.yaml`, a YAML **list** of provider specs — the shape `yaml_provider.load_providers` requires — pulled in with `providers: [{include: <path>}]`. Verified present in a built wheel.)*
- [x] 5.2 Write `docs/yaml.md`: the verified `providers:` block (with the C25 `beam-agents==0.1.0` package pin), the full config-key table, the reference convention and its trust-boundary note, the multi-output addressing example, and a complete example pipeline (source → `RunAgent` → `intents_to` outbox URI). *(Added to the mkdocs nav under "Operating the runtime".)*

## 6. Gates

- [x] 6.1 `make lint` and `make type` clean (`mypy --strict`; no `Any` in public signatures). *(`ruff check`/`ruff format --check` clean over 310 files; mypy clean over 305 source files.)*
- [x] 6.2 `make test-unit` passes offline with no docker; the e2e YAML suite runs in the required lane, not behind a skip. *(78 `tests/yaml` tests, all unconditional — no `importorskip`, no marker.)*
- [x] 6.3 `make coverage-ratchet` — **pre-existing failure, improved by this change; baseline deliberately not touched.** See Revision 5.
- [x] 6.4 `uv run pre-commit run --all-files` clean — all ten hooks Passed, including C25's `changelog-fragment-required`, satisfied by `changelog.d/add-yaml-provider.added.md`.
- [x] 6.5 `openspec validate add-yaml-provider --strict` passes.

## 7. Revisions

Numbered corrections made during implementation, each after reading the real
source rather than reasoning from the plan.

### Revision 1 — the e2e suite cannot import `apache_beam.yaml`, and no dependency could be added to fix it

**Planned** (tasks 1.4 / 4.4, design "Test-environment dependency"): declare whatever `apache_beam.yaml` needs in the `test` dependency group so the e2e suite parses the document with Beam's own expander and never skips.

**Reality.** `apache_beam.yaml` is not importable under the plain `apache-beam[gcp]` pin. `apache_beam/yaml/__init__.py` imports `yaml_transform`, which imports `jinja2` at module scope; `yaml_transform` then imports `yaml_provider`, which imports `apache_beam.dataframe.io` → `pandas`. Both belong to Beam's `yaml` extra (`docstring-parser`, `jinja2`, `virtualenv-clone`, `js2py`, `jsonschema`, `pandas`), of which the locked environment installs only `docstring-parser` and `jsonschema`. Adding any of the rest to the `test` group changes `uv.lock` (confirmed: `uv lock --check` reports the lockfile needs updating), and this change is not permitted to move the lockfile.

**Resolution.** No dependency was added and nothing is skipped. `tests/yaml/_yaml_driver.py` executes the *same document the docs publish* on the DirectRunner, mirroring the three — and only three — Beam YAML mechanisms this change couples to, each read out of the installed Beam 2.72.0 source and cited in the driver's docstring:

1. `type: python` with no `packages:` resolves a fully-qualified constructor in-process via `PythonCallableWithSource.load_from_source` (`yaml_provider.py::python`) — the driver calls Beam's own function, not a re-implementation;
2. `InlineProvider.create_transform` constructs it as `factory(**config)`;
3. `expand_leaf_transform` names outputs from a `dict[str, PCollection]` return, addressed downstream as `Name.output` (`Scope.get_pcollection`).

`test_beam_yaml_contract_still_matches_this_driver` asserts all three are still present in the installed Beam source, so a Beam upgrade that moves any of them fails the offline lane instead of silently invalidating the harness. The cost is honest and bounded: the e2e gate proves the constructor, the references, the config mapping, the row boundary, the named outputs, and an offline `FakeLLM`-served activation from a real YAML document — it does not prove Beam's own document *preprocessing* (jinja templating, `chain`/`composite` desugaring, schema validation), which is Beam's code, not ours. Re-pointing the suite at `apache_beam.yaml.expand_pipeline` is a one-import change once Beam's `yaml` extra can be added to the lockfile.

### Revision 2 — the main output carries no entity key

**Planned** (design D5, spec "The four outputs are addressable by name"): emit the main stream as `Row(key: bytes, output: bytes)`.

**Reality.** `RunAgent`'s main output is an unkeyed `PCollection[bytes]`: `_AgentDoFn._commit` does `yield from result.outputs`, where `ActivationResult.outputs` is a `list[bytes]`. The entity key is simply not on that stream, and recovering it would mean changing `core/`, which this change's Impact excludes.

**Resolution.** `output` rows are `Row(output: bytes)`. `design.md` D5 and the spec requirement were corrected, a scenario was added pinning the absence, and `docs/yaml.md` states the consequence with the workaround: an agent that needs its key downstream emits it inside its own output bytes. The e2e test's agent does exactly that, so the key that `key_field` supplied is still observed end to end.

### Revision 3 — the element-timestamp fallback needs an unstamped-element rule

**Planned** (design D5): `event_time_ms` comes from `event_time_field` when configured, else the element timestamp — with no further qualification.

**Reality.** Beam stamps an element that never carried a time (a bounded `Create`, an unstamped source) with `MIN_TIMESTAMP`, ≈292 million years before the epoch. Carried into `AgentEnvelope.event_time_ms` it poisons everything derived from it: the first `traces` row raised `OverflowError: date value out of range` inside the shipped `trace_event_to_row`, and `expires_at_ms` and the TTL watermark mark would have been just as wrong, silently.

**Resolution.** The fallback maps the sentinel to `0` (epoch) and is otherwise unchanged. It reads no clock, so it stays replay-deterministic. Recorded in `design.md` D5, in the spec's row-boundary requirement, in the helper's docstring, and steered against in `docs/yaml.md`, which tells a pipeline that cares to configure `event_time_field`.

### Revision 4 — no conditional `dead_letter` named output

**Planned** (design D6, flagged in Open Questions): expose `RunAgentOutputs.dead_letter` as a fifth named output when it exists.

**Reality.** Beam YAML resolves a transform's outputs once, by name, from the `expand` return; a document referencing `RunAgent.dead_letter` would be valid or invalid depending on whether `intents_to` happened to resolve to the outbox writer — the implicit coupling the fixed key set exists to prevent.

**Resolution.** The output set is constant at four. Malformed input rows (Revision 3's neighbour, spec's row-boundary requirement) merge onto `errors` alongside the activation dead letters, so one stream is the whole failure story. `design.md` D6 and the Open Questions list record the decision. The intents dead letter remains reachable from Python via `RunAgentOutputs.dead_letter`; nothing was removed from the core surface.

### Revision 5 — the coverage ratchet was already failing on the integration branch

**Planned** (task 6.3): `make coverage-ratchet` at or above baseline; raise `coverage-baseline.toml` if the new package improves it.

**Reality.** `coverage-baseline.toml` records `branch_rate = 0.9497`, last set by `add-state-schema-migration`. Measured on the merged integration branch **with this change stashed**, the tree's actual branch rate is **0.8984** (805/896) — the ratchet already fails by ~5 points before a line of this change is applied. The baseline file's own comments say why: every earlier merge re-measured the combined tree rather than picking a side's number, and the merge that brought C24/C25/C26/C29/C31/C39 together did not.

**Resolution.** Nothing was changed here, deliberately. This change *improves* the number — 0.8984 → **0.9000** (855/950), with the new package landing at 96%/97%/96% statement coverage across `transform.py`/`_refs.py`/`_config.py` — and the hard `fail_under = 90` floor in `[tool.coverage.report]` passes (95.02% combined). Raising `coverage-baseline.toml` to 0.90 would silently ratify the pre-existing ~5-point regression under this change's name, which is exactly what the file exists to prevent; lowering it would be worse. Re-measuring and re-committing the baseline belongs to whoever owns the integration merge, in one deliberate edit with the merge-resolution note the file's convention asks for.
