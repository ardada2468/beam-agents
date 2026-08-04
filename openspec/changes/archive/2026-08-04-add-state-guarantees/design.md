## Context

Keyed state in this runtime is protobuf end to end: every state cell in `core/dofn.py` (`MEMORY`, `CONTINUATION`, `LLM_CACHE`, `PENDING`, `SEQ`) is encoded by `DeterministicProtoCoder`, whose wire format is exactly `SerializeToString(deterministic=True)` with no length prefix, version byte, or framing of any kind ([coders.py:73](../../../src/beam_agents/core/coders.py:73)). The three blob messages carry `state_schema_version` ([beam_agents.proto:29](../../../protos/beam_agents.proto:29), [:179](../../../protos/beam_agents.proto:179), [:221](../../../protos/beam_agents.proto:221)), the proto file's header pins additive-only evolution, and C32 (`add-state-schema-migration`) is building the lazy-migration machinery for versioned changes.

Dataflow `--update` replaces a running streaming job with a new job graph while carrying over in-flight data and persisted keyed state. Before the handoff, the service runs a compatibility check: steps are matched between the old and new graphs (by transform name, with `--transform_name_mapping` for renames), and the state carried into a matched step must be decodable by the new step's coders. If the check fails, the replacement job fails and the old job keeps running — no data is lost, but the upgrade is refused. So the runtime's `--update` story rests on three legs: proto wire compatibility (additive changes decode), coder identity (the encoding function never changes shape), and graph stability (`RunAgent`'s internal transform names and state spec ids persist across releases).

What exists to verify this today: the golden-blob suite decodes committed v1 bytes with current bindings, in-process; the conformance matrix's `restart_mid_suspension` cell proves a suspension survives a same-version restart on DirectRunner and Flink. What does not exist: any statement a user can rely on about what beam-agents promises across releases, and any test that has ever run Dataflow's compatibility check or handed live state from one release's job to another's. The nightly `dataflow` job is plumbed and authenticated (Workload Identity Federation, gated on `vars.GCP_PROJECT_ID`) but runs an empty selection — no `dataflow`-marked test exists, which is why `make test-dataflow` tolerates exit 5.

One more constraint shapes everything: the package is version `0.0.0` and has never been released to PyPI. "Install the previous released version" has no referent until the first release ships.

## Goals / Non-Goals

**Goals:**
- One published document that separates promise from accident: what state compatibility beam-agents guarantees across releases, what it treats as best-effort, and a change-class table an author can consult before touching a proto, a coder, or the DoFn's graph shape.
- A nightly gate that runs the real thing: a previous-release streaming job on Dataflow with live keyed state — a mid-suspension `Continuation` with a `PENDING` intent, a populated `MemoryBlob` — updated in place to current head, with state survival asserted from outside the pipeline.
- An honest two-version harness: previous version genuinely installed from PyPI, head genuinely built from the checkout, identical launcher source producing both graphs.
- Bounded cost and guaranteed cleanup: one small worker, a hard wall-clock budget, unconditional teardown, and a sweeper for prior crashed runs.
- A defined bootstrap path so the gate is useful before the first release and arms itself the night after it.

**Non-Goals:**
- Building migration machinery. `state_schema_version` bumps, lazy migration, and golden-blob movement rules are C32's scope; this change documents and exercises them, and its table cites C32's path as the required action for versioned changes.
- Flink savepoint compatibility. Flink's state carry-over is a different mechanism with different failure modes; it deserves its own change once the Dataflow contract is pinned. The doc marks it explicitly as not-yet-promised.
- Cross-runner state portability (state written on Dataflow read on Flink). Never promised, and the doc says so.
- N → N+k skip-level updates as a promise. See D1.
- Performance or drain-latency characteristics of `--update` itself.
- A general Dataflow e2e suite. This gate tests exactly one property; other `dataflow`-tier tests (roadmap siblings) arrive on their own changes.

## Decisions

### D1. The guarantee boundary: adjacent-release readability is promised; everything else is classified best-effort or unsupported

The document's core promise is deliberately narrow: **state written by release N SHALL be readable by release N+1, and `--update` from N to N+1 SHALL succeed, provided the release notes do not declare a migration release.** Adjacent releases only, forward only, Dataflow only.

Everything else gets an explicit non-promise classification rather than silence:

- *Skip-level updates (N → N+k):* best-effort. Additive proto changes compose trivially, and C32's lazy migrations are written to chain through intermediate versions, so skipping will usually work — but it is not gated (the nightly tests exactly one hop), so it is not promised. The doc tells operators to step through releases when state matters.
- *Downgrade (N+1 → N):* unsupported. Old code cannot be expected to read new fields it has never heard of into semantics it does not have; proto3 will silently drop unknown fields on re-serialize in the old binary, which is state loss.
- *Byte-identity across versions:* explicitly NOT promised. `deterministic=True` guarantees byte-identical encoding only within a pinned protobuf library version (already documented in the coder's docstring); the cross-release promise is semantic decodability, matching what the golden suite asserts (field equality, never byte equality).
- *Flink savepoints, cross-runner portability:* not promised (non-goals above).

Drawing the line at one hop is what makes the promise testable: the nightly gate runs exactly the promised transition and nothing it does not run is claimed. Widening the promise later (say, N → N+2 once the release cadence justifies it) is a doc-plus-gate change, not a retroactive reinterpretation.

### D2. The compatibility table classifies change classes, and the compatibility surface includes graph shape, not just bytes

The table in `docs/state-compat.md` has one row per change class an author can actually make, with three columns: *old state readable?*, *`--update` safe?*, *required action*. Initial rows:

| Change class | Readable | `--update` | Required action |
|---|---|---|---|
| Add an optional proto field (new tag number) | yes | yes | golden fixture for the pre-field bytes (existing practice) |
| Add an enum value (proto3 open enums) | yes | yes | document the unknown-value behavior at the read site (existing practice, e.g. `TraceEvent.SUSPENDED`) |
| Add a new state spec (new cell) | yes | yes | none — new cell starts empty per key |
| Remove / renumber / retype a field | no | no | forbidden; use `reserved` + new tag, or a versioned migration (C32) |
| Change `DeterministicProtoCoder`'s encoding (framing, compression, prefix) | no | no | forbidden — the raw-proto wire format is the contract |
| Rename a `RunAgent`-internal transform or state spec id | yes (bytes fine) | no (step matching fails) | avoid; if unavoidable, ship `--transform_name_mapping` guidance in release notes |
| Change a state cell's coder type | no | no | forbidden without a versioned migration release (C32) |
| `state_schema_version` bump + lazy migration | via migration | yes | C32's checklist: migration registered, old-version golden fixture kept, compat test extended |

The second-to-last rows are the ones nobody writes down: Dataflow's check matches steps by name and state by spec id, so a purely internal refactor of `_AgentDoFn` — new step name, renamed state spec — breaks `--update` with zero byte-level changes. Making graph shape an explicit row is what turns "the refactor looked harmless" into a reviewable violation. The nightly gate enforces it mechanically: any such rename makes the self- or cross-version update fail.

The table is the doc's contract; the spec requires every state-affecting change class to have a row, so a future change that invents a new class (say, a new blob message) must extend the table in the same PR.

### D3. Two interpreters, one launcher source: prev from PyPI into a venv, head as a wheel from the checkout

The harness must run two versions of beam-agents in one CI job without either contaminating the other. Mechanics:

- **Resolve prev:** query PyPI for the latest released version (`pip index versions beam-agents` / the JSON API through the session's pip). Download its wheel with `pip download --no-deps`. The resolved version string is logged and attached to the test report — a compat failure is meaningless without knowing which two versions collided.
- **Prev environment:** `python -m venv` under the scratch dir with the same interpreter minor version the job already uses (3.11); install the downloaded wheel plus its own pinned `apache-beam[gcp]` resolution. The prev leg's Beam version may lag head's — that skew is the real user upgrade path (users bump beam-agents and apache-beam together), so the harness does not force them equal.
- **Head environment:** the job's existing `uv sync` environment, plus `uv build` producing the head wheel.
- **One launcher, run twice:** `tests/dataflow/_update/pipeline.py` is a self-contained module invoked by file path from each interpreter (`<venv>/bin/python .../pipeline.py --run-id ... --phase launch|update ...`). It imports only `apache_beam` and the `beam_agents` public API plus the packaged `FakeLLM`, defines its agent and matchers at module level, and passes `--save_main_session` so workers can unpickle them. Identical source under two interpreters means both job graphs differ only in library versions — exactly the variable under test. It also means the launcher is restricted to API that must be stable across adjacent releases, which makes the gate a cheap public-API-stability canary as a side effect.
- **Worker payload:** each leg ships its own beam-agents to workers via `--extra_packages` (prev: the PyPI wheel; head: the built wheel). No custom SDK container image — default Beam containers keep the harness free of an image-build pipeline, at the cost of worker startup installing one wheel.

Alternative rejected: running prev from a git checkout of the release tag. It tests the tag's source, not the artifact users install, and quietly diverges the day packaging (data files, entry points, generated protos) matters — which for `_protos/` bindings is precisely a plausible failure mode.

### D4. The state under test is a live suspension plus populated memory, and every assertion reads the output topic

The scenario is chosen to cover the two state cells whose loss is catastrophic and observable:

- **Phase 1 (prev job):** three keys. `K-suspend` receives an event whose scripted agent stages an `APPROVAL` intent and suspends — leaving a `Continuation`, a `PENDING` entry, and a live HITL timer (deadline set hours out, far beyond the test window). `K-memory` receives an event whose agent writes a known marker into working memory and completes. `K-canary` completes trivially, proving the job is live before the update begins. Phase 1 ends when the canary's and `K-memory`'s outputs appear on the output topic.
- **Update:** head launcher submits the replacement job with `--update` and the same `--job_name`. The harness polls until the old job reaches its updated/terminal state and the replacement reaches `RUNNING`.
- **Phase 2 (head job):** the harness injects the approval for `K-suspend`'s pending intent (an `AgentEnvelope` with the approval payload, published to the events topic on the same key); the scripted agent's resume output embeds data recoverable only from the pre-update continuation snapshot, so its appearance proves the suspension resumed rather than restarted. A second event to `K-memory` makes the agent echo the stored marker, proving `MemoryBlob` survived and decoded. A fresh `K-post` key completes normally, proving the updated job handles new work. Ordering note: the `SEQ` cell is implicitly covered too — the resumed and echoed activations mint seq-dependent outputs that would be wrong if the counter reset.

All assertions are pull-subscription reads on the output topic with hard deadlines. Nothing inspects worker internals, and the model is the packaged in-process `FakeLLM` (scripted matcher responses) pickled into each job — not FakeLLM-over-HTTP. HTTP-scripted models earn their keep when a test must observe or count provider requests from outside; this gate asserts on outputs only (provider-call counting belongs to the offline retry-determinism gates), and an in-process fake removes a deployed endpoint, its egress path, and its failure modes from a test that is already the flakiest-by-construction in the repo.

### D5. A refused compatibility check is the gate's primary red, and failure classification is explicit

`--update` has an asymmetric failure mode: when Dataflow's compatibility check refuses the replacement graph, the *new* job fails while the *old* job keeps running. A naive harness reports that as "job failed to start" — infrastructure noise — when it is in fact the exact defect this gate exists to catch.

The poller therefore classifies every failure before reporting:

- **Compatibility failure (the gate's red):** replacement job terminal in `FAILED` while the prior job is still healthy, or the update error message names the compatibility check / coder mismatch / unmatched step. Reported with both version strings, both job ids, and the service's stated reason.
- **State-loss failure (also red):** update succeeded, but a phase-2 assertion timed out or produced wrong bytes — the suspension restarted instead of resuming, the memory echo missed, the fresh key failed.
- **Infrastructure failure (not a compat verdict):** quota exhaustion, worker-pool startup failure, PyPI/network errors during provisioning, WIF token trouble. Reported distinctly so a red night is triaged in minutes, and never silently retried into a green.

No retry decorators, no `xfail`, no flake tolerance: the same stance as the semantics gates. A genuinely flaky infrastructure step gets fixed in the harness, not tolerated in the verdict.

### D6. Cost and cleanup: one worker, hard budgets, unconditional teardown, and a sweeper for the previous crash

- **Job shape:** Streaming Engine on (state lives in the service, and it is the configuration under which `--update` is the supported upgrade path), `--max_num_workers=1`, smallest supported streaming machine type, no public IPs if the project's network allows. Two jobs exist but never concurrently for long — the update replaces the first.
- **Budgets:** every poller takes a deadline; the test's own budget is ≤ 35 minutes (launch ~5–8, phase 1 ~5, update ~5–10, phase 2 ~5, teardown ~2), inside the raised 50-minute job timeout. The current 30-minute `timeout-minutes` ([nightly.yml:35](../../../.github/workflows/nightly.yml:35)) cannot absorb two sequential streaming-job startups.
- **Naming and labels:** run id = date + short random suffix; job name, topics, subscriptions, and GCS temp prefix all embed it; both jobs carry a `beam-agents-update-compat` label.
- **Teardown:** a `finally`-guaranteed sequence force-cancels (not drains — drain waits on watermarks the test does not care about) both job ids, deletes the topics/subscriptions, and removes the temp prefix. Runs on pass, fail, and pytest timeout.
- **Sweeper:** before provisioning, list jobs bearing the label older than two hours and force-cancel them; delete same-labeled stale topics. A crashed runner (OOM, cancelled workflow) can orphan a streaming job that bills until someone notices; the sweeper bounds that to one night. This is the same "guaranteed on failure and timeout" discipline the e2e gate's stack teardown established.

### D7. Bootstrap: a self-update leg until PyPI has a release, arming automatically afterward

Until the first release exists, the resolver finds nothing on PyPI. Skipping the gate until then would ship the harness untested and guarantee its first real run — the night after the first release — is also its shakedown run. Instead:

- **No release found:** the gate runs head → head. Same harness, same phases, same assertions; the launch leg installs the head wheel instead of a PyPI wheel. This proves the Dataflow mechanics, the pollers, the teardown, and one real property: head's job graph is update-compatible with itself, which catches nondeterministic transform naming and any construction-time randomness in the graph — a genuine `--update` prerequisite. The report labels the run `SELF-UPDATE (bootstrap)` in capitals so a green bootstrap night is never mistaken for cross-version evidence.
- **Release found:** the resolver returns it and the cross-version leg runs with no configuration change. The first true N → head run happens automatically the night after `0.1.0` is tagged.

The alternative — skip until release — was rejected because it converts the highest-stakes first run into an untested one; the bootstrap leg costs the same nightly budget and retires the harness risk early.

### D8. `make test-dataflow` stops tolerating an empty selection

The `; test $$? -eq 0 -o $$? -eq 5` suffix existed because the `dataflow` marker had no tests. Once this gate lands, an empty `dataflow` collection means the release-blocking gate was deselected — the same reasoning that gave `test-semantics` its no-exit-5 stance. The target drops the tolerance. Local runs without GCP configuration stay green because the gate *skips* (a collected, skipped test exits 0) on missing `GCP_PROJECT_ID`/`GCP_REGION`/`GCP_DATAFLOW_TEMP_BUCKET` env — skip-on-missing-credentials is the smoke tier's established pattern and is visible in the report, unlike deselection.

## Risks / Trade-offs

- **This is the repo's flakiest test by construction** — real Dataflow, real Pub/Sub, PyPI, worker-pool spin-up, twice. Mitigation: D5's failure classification keeps infrastructure noise out of the compat verdict; deadline-driven polling everywhere; nightly-only and not a PR check, so a red night costs a morning's triage, not a blocked merge queue. Residual: a genuinely red compat night the day before a planned release delays the release — which is the feature, not the bug.
- **Nightly cost.** Two sequential streaming jobs at one small worker for ~25 job-minutes plus Streaming Engine and Pub/Sub — small but nonzero, every night, forever. Mitigation: single worker cap, force-cancel teardown, sweeper for leaks; the job continues to no-op entirely on forks/repos without `GCP_PROJECT_ID`.
- **Beam SDK version skew inside the update.** The prev leg may run an older `apache-beam` than head, so the gate's update crosses two version boundaries at once (beam-agents and Beam). That compounds diagnosis when red — but it is the real user path, and holding Beam constant would test a transition users never perform. Mitigation: both legs log their full `pip freeze`; a suspected Beam-side incompatibility is reproducible by pinning head's leg to the prev Beam version locally.
- **The launcher is frozen against two API versions.** `pipeline.py` must run under prev and head, so a public-API break strands it. Mitigation: that *is* an adjacent-release compatibility break and should surface here; for a deliberate, release-noted API change, the launcher grows a version guard in the same release, and the doc's table gains the row that classifies the break.
- **Dataflow update semantics are a moving target** (service-side compatibility-check behavior, machine-type availability, Streaming Engine defaults). Mitigation: the harness pins nothing service-side and asserts only documented behavior (job states, state survival); a service-side change that breaks the gate is classified as infrastructure until understood.
- **A green gate can overclaim.** One scenario, three keys, one hop — passing does not prove every state shape migrates. Mitigation: the doc's promise is scoped to what the golden suite plus this gate actually verify; the gate covers the two cells whose loss is unrecoverable (`CONTINUATION`, `MEMORY`) and leans on the offline golden suite for full per-message coverage. `LLM_CACHE` loss, by contrast, is self-healing (a refetch), so it is deliberately not asserted here.
- **PyPI is in the loop.** An outage or a yanked release breaks resolution. Mitigation: classified as infrastructure; the bootstrap self-update leg is the automatic fallback when resolution fails outright (logged as such, never silently).

## Migration Plan

1. Land after (or alongside) C32 — the doc's versioned-change rows cite C32's migration path, and the spec requirement about migration releases presumes the machinery exists.
2. Doc first (`docs/state-compat.md`), reviewed against C32's spec so the two never disagree about what a version bump requires.
3. Harness unit tests (offline, in the default tier), then the gate module, then CI wiring (`nightly.yml` variables + timeout, Makefile tolerance drop).
4. First nightly runs are bootstrap self-update legs; treat the first three green nights as the harness burn-in window before declaring the gate stable.
5. After the first PyPI release, verify the resolver armed the cross-version leg (the report says so) and add the release-procedure checkbox: latest nightly `dataflow` leg green before tagging.
6. Rollback: delete the test and the doc; no state, wire, or `src/` implications.

## Open Questions

- Should the compatibility table also gate mechanically pre-release — e.g., a script that diffs `protos/beam_agents.proto` against the last release tag and fails CI when a change class with a "forbidden" row appears without a version bump? Powerful, but it needs release tags to diff against; revisit after `0.1.0`.
- When the release cadence produces N and N+1 on PyPI, should the gate run two hops (N → head and N-1 → head) to give skip-level updates evidence before promising them? Costs a second full run per night.
- Where should the release procedure live long-term — `docs/state-compat.md` owns the compat checkbox for now, but a future `RELEASING.md` may subsume it.
- Flink savepoint compatibility: same promise shape, different mechanism — its own change, once someone needs it.
