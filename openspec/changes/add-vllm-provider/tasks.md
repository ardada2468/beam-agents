## 1. Tests (written first, must fail for the right reason)

- [ ] 1.1 `tests/model/test_vllm.py` — endpoint mode over `httpx.MockTransport`: one non-streaming POST to `<base_url>/chat/completions` returning raw bytes; no `Authorization` header without an API key and a required `base_url`; `Authorization: Bearer` with a key; 429/503/timeout raise `RateLimitError`/`ServerError(503)`/`ProviderTimeout` through the shared mapper. Derived from "vLLM endpoint mode is a preset over the OpenAI-compatible provider" (all four scenarios).
- [ ] 1.2 Fake engine + `Shared` singleton tests: two providers built from one sidecar factory construct the engine exactly once and share it; a changed engine-config tag constructs a distinct engine; a generation submitted from a bridge-style loop reaches the engine loop without blocking the caller loop. Derived from "vLLM sidecar engine is a per-worker-process singleton acquired via Shared" (all three scenarios).
- [ ] 1.3 Health-probe tests: an engine that raises on init or probe fails provider construction (and therefore `setup`) before any element; a probe exceeding its deadline raises a message naming the deadline; a second instance's probe reuses the live engine without re-loading. Derived from "Sidecar engine construction is health-checked during DoFn setup and fails fast" (all three scenarios).
- [ ] 1.4 Response-shape and cache tests: sidecar bytes decode with `openai_compat.decode` yielding the engine's `TokenUsage` and text; an `LlmFacade` call served from the replay cache after a sidecar response makes zero additional engine calls and decodes identically. Derived from "Sidecar responses are cacheable chat-completions-shaped bytes decoded by the OpenAI-compatible Decode" (both scenarios).
- [ ] 1.5 Engine-failure taxonomy tests over the fake engine: saturation → `RateLimitError`; internal failure → `ServerError(500)`; deadline → `ProviderTimeout`; invalid request material → `ProviderRequestError(400)` not caught by `except ProviderError`. Derived from "Sidecar engine failures map onto the provider-error taxonomy" (all four scenarios).
- [ ] 1.6 Lifecycle tests: releasing one of two providers leaves the engine serving the survivor; releasing the last runs the shutdown finalizer exactly once (engine loop stopped, engine thread joined). Derived from "The sidecar engine shuts down gracefully when the last holder releases it" (both scenarios).
- [ ] 1.7 Packaging tests: `beam_agents.model` imports with no side effects and endpoint mode works without the `vllm` extra; sidecar construction without the extra raises an error naming the extra. Derived from "The vllm extra gates the sidecar and verification is offline via an engine seam" (first two scenarios).

## 2. Endpoint mode

- [ ] 2.1 Create `src/beam_agents/model/vllm.py` with `VllmEndpointProvider`: required `base_url`, optional `api_key` (omit the `Authorization` header when absent), per-request timeout; delegate the POST/raw-bytes/taxonomy path to a contained `OpenAICompatProvider` per design D1, threading the mock-transport seam through for tests.
- [ ] 2.2 Document that both vLLM modes pair with the existing `openai_compat.decode` as `AgentConfig.decode` (no new decoder).

## 3. Engine seam, Shared singleton, health probe

- [ ] 3.1 Define the internal engine seam (async generate over request material → text + token counts, plus a readiness coroutine) and the engine handle owning the dedicated engine thread + asyncio loop (design D3); import `vllm` lazily inside the real constructor only, raising the actionable missing-extra error otherwise (design D7).
- [ ] 3.2 Implement `vllm_sidecar_factory(...)`: build the `Shared` handle once at pipeline-construction time, derive the acquisition tag from a digest of the engine configuration, and return a `provider_factory` closure capturing both (design D2).
- [ ] 3.3 Implement the bounded readiness probe in `VllmSidecarProvider.__init__`: submit the readiness coroutine to the engine loop and block with the configurable health deadline; raise actionably on failure or overrun (design D4).
- [ ] 3.4 Register the `weakref.finalize` shutdown on the engine handle (stop loop, join thread, release engine) and hold the strong reference on the provider for the DoFn lifetime (design D5).

## 4. Sidecar provider

- [ ] 4.1 Implement `VllmSidecarProvider.complete`: submit generation via `run_coroutine_threadsafe` from the bridge loop, await through `asyncio.wrap_future`, and enforce the per-request deadline.
- [ ] 4.2 Serialize engine output into the canonical chat-completions-shaped body (sorted keys, compact separators, UTF-8) returned as `LlmResponse` bytes (design D6).
- [ ] 4.3 Map engine failures onto the taxonomy per design D8, with the conventional-status stand-ins documented at the mapping site.

## 5. Packaging and exports

- [ ] 5.1 Add the `vllm` extra to `pyproject.toml` `[project.optional-dependencies]` with a version floor and an explanatory comment following the `effector`/`langgraph`/`otlp` pattern; regenerate the lockfile (apply environment markers only if universal resolution requires them — design open question).
- [ ] 5.2 Re-export `VllmEndpointProvider`, `VllmSidecarProvider`, and `vllm_sidecar_factory` from `beam_agents/model/__init__.py`; confirm `beam_agents/__init__.py`'s public surface is unchanged.

## 6. Smoke (nightly, hardware-gated)

- [ ] 6.1 Add `tests/smoke/test_vllm_sidecar.py`: one `smoke`-marked test loading a small model on a real engine, running one `complete`, asserting a decodable non-empty response; skip without the `vllm` extra or a visible GPU. Derived from the "Real-engine tests are smoke-marked and hardware-gated" scenario. No workflow change: the existing nightly smoke lane runs it and it skips on GPU-less runners.

## 7. Gates

- [ ] 7.1 `make lint` clean (ruff, including ASYNC rules on the cross-loop submission path).
- [ ] 7.2 `make type` clean (`mypy --strict`; the lazy `vllm` import stays behind the typed engine seam).
- [ ] 7.3 `make test-unit` passes offline with no docker, no GPU, and no `vllm` installed; no `smoke` test runs in the unit tier.
- [ ] 7.4 `make coverage-ratchet` at or above baseline. Mutation gate not applicable: no `core/` file is touched by this change.
- [ ] 7.5 `uv run pre-commit run --all-files` clean.
- [ ] 7.6 `openspec validate add-vllm-provider --strict` passes.
