# Model providers

A provider is anything satisfying the async
[`LLMClient`](api.md#model-beam_agentsmodel) protocol — one method,
`async def complete(request) -> LlmResponse`, transport only: no retry, no
caching, no breaker. Everything resilient sits *around* the provider in the
runtime's facade, which is why writing a new one is small and why every
provider ships with a paired **decoder** rather than a vendor SDK.

`RunAgent` gets its provider from `AgentConfig`:

```python
from beam_agents import AgentConfig
from beam_agents.model import AnthropicProvider, anthropic_decode

config = AgentConfig(
    provider_factory=lambda: AnthropicProvider(api_key=...),
    decode=anthropic_decode,
)
```

`provider_factory` is a zero-argument callable built once per worker — never
a client instance, which would have to survive pickling. `decode` is the
provider's paired response decoder; it is optional, and unset means "token
counts unknown": `LLM_CALL` traces then omit their usage attributes rather
than report zeros (token budgets require it, so
`max_tokens_per_activation` without `decode` is a construction-time
`ValueError`).

## The shipped providers

All four live in `beam_agents.model` and need **no extra** beyond the core
install — the HTTP providers ride the core `httpx` dependency, deliberately
with no vendor SDK. Only the vLLM *sidecar* has an extra, for the engine
itself.

| Provider | Endpoint | Decode |
| --- | --- | --- |
| `AnthropicProvider` | Anthropic Messages API | `anthropic_decode` |
| `OpenAICompatProvider` | any `/chat/completions`-shaped endpoint | `openai_compat_decode` |
| `VllmEndpointProvider` | a separately served vLLM OpenAI endpoint | `openai_compat_decode` |
| `VllmSidecarProvider` | an in-worker vLLM engine (`beam-agents[vllm]`) | `openai_compat_decode` |

**`AnthropicProvider`** — non-streaming, single-shot: one POST per `complete`
call, the raw response body returned verbatim as `LlmResponse.response`. The
cache payload must be byte-identical to what the provider sent, so there is
no SDK and no re-serialization anywhere in the path.

**`OpenAICompatProvider`** — the same shape over `/chat/completions`. Covers
OpenAI, vLLM's OpenAI server, and any compatible gateway by base URL alone;
it differs from the Anthropic provider only in endpoint shape and auth
header. There is no Vertex/Gemini-specific provider; an OpenAI-compatible
gateway in front of one works today, and the ADK adapter routes
`google-genai` clients through this same replay-cached path.

**vLLM, two ways** ([details](api.md#providers)) —
`VllmEndpointProvider` is a preset over the OpenAI-compatible provider for a
vLLM server you run separately: `base_url` is required (no OpenAI default to
mis-hit) and the API key is optional, matching unauthenticated vLLM
deployments. `VllmSidecarProvider` runs the engine *inside* the Beam worker
as one worker-local shared singleton (`vllm_sidecar_factory` builds it);
construction runs a bounded readiness probe so a broken engine fails worker
setup before any element. The sidecar serializes engine output into canonical
chat-completions-shaped bytes, so the one decoder serves both modes. The
`vllm` extra (torch/CUDA-heavy, GPU-oriented) is needed only for the sidecar;
endpoint mode and `import beam_agents.model` never import it.

## `FakeLLM`: the scripted double

[`FakeLLM`](api.md#beam_agentsmodelfake) is a deterministic in-process
`LLMClient` — ordered first-match-wins rules mapping a request matcher
(`match_any`, `match_contains`, `match_model_id`) to a behavior
(`respond_with`, `raise_error`, `fail_then_succeed`), recording every
request. An unmatched request raises `UnmatchedRequestError` rather than
serving a default: a test that reaches it has a gap.

It is the default model in every test tier but the nightly smoke run, and it
is what makes the [examples](index.md#start-here) runnable offline. Its
latency is an injected hook, never a wall clock, so nothing driven by it
flakes.

## The replay cache, and why bytes are opaque

Every provider call goes through the cache-first facade path, against the
per-key [`ReplayCache`](api.md#beam_agentsmodelreplay_cache) persisted in
keyed state: the request's content hash (`compute_cache_key` — one canonical
JSON document over model id, messages, tools schema, sampling params, and the
activation's `entity_key`/`seq`) is looked up before any transport is
touched. This is correctness invariant 3 — **a
retried bundle adds zero provider calls and commits byte-identical
results** — and it is a release gate
([`tests/semantics/test_retry_determinism.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_retry_determinism.py)),
which is why providers must return the provider's bytes verbatim: the cache
stores and replays exactly what was received.

Bounds, enforced by the cache itself: 64 entries per key (LRU), a 6-hour
TTL, and a 100 KiB blob cap with a digest-only fallback that preserves
divergence detection for oversized responses. The same cache is what powers
[state export and replay](replay.md) — a replayed activation's model calls
are served from the snapshot's `llm_cache`, and reaching a real transport
*is* a cache miss.

Around a cache miss, the facade adds the resilience the protocol keeps out of
providers: the typed provider-error taxonomy (`ProviderTimeout`,
`RateLimitError`, `ServerError` retryable; `ProviderRequestError` not),
full-jitter exponential backoff with a Retry-After floor, a worker-local
per-endpoint circuit breaker, and the per-activation token budget. All of it
is configuration on [`AgentConfig`](api.md#beam_agentscoretransform), none of
it is provider code.

## Writing your own

Implement `complete`, return the provider's raw bytes, map failures onto the
error taxonomy, and pair it with a `Decode` that extracts text and
`TokenUsage` from those bytes. The two vLLM modes are the worked example of
both directions — a preset over an existing provider, and a from-scratch
transport that reuses an existing decoder.
