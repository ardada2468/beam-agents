# API reference

This page enumerates the **frozen public surface** of `beam_agents`: every name
that carries a compatibility promise, grouped by the module that defines it,
with one line of contract each.

The machine-readable form of the same surface is
[`public-surface.toml`](https://github.com/ardada2468/beam-agents/blob/main/public-surface.toml),
generated from the sources and checked on every run of `make test-unit`. A drift
test asserts that every name frozen there appears on this page, so the reference
cannot silently fall behind the code.

Two tiers, and one rule for everything else:

- **Root tier** — the sixteen names `beam_agents` re-exports. Import these from
  the package root.
- **Module tier** — names that are contract at their dotted path. Each such
  module declares an `__all__` naming exactly its contract.
- **Everything else is private.** A name with a leading underscore, or any name
  inside a `_`-prefixed module or package (`beam_agents._protos`,
  `beam_agents.adapters._transport`, `beam_agents.model._http`,
  `beam_agents.yaml._config`, `beam_agents.yaml._refs`), is internal machinery
  with no compatibility promise. Do not import it.

Removing or renaming anything on this page requires a deprecation window — see
[Contributing](https://github.com/ardada2468/beam-agents/blob/main/CONTRIBUTING.md).

---

## Root namespace — `beam_agents`

| Name | Contract |
| --- | --- |
| `RunAgent` | The transform. `events \| RunAgent(agent, config=...)` turns an agent into a keyed, stateful Beam step. |
| `AgentConfig` | Everything the transform needs that is not the agent: provider factory, timeouts, tool registry, HITL policy, sink URIs. Validated at construction. |
| `RunAgentOutputs` | The four named outputs: `.output`, `.intents`, `.traces`, `.errors`. |
| `StreamAgent` | The protocol an agent (or adapter) implements to be accepted by `RunAgent`. |
| `tool` | The decorator that registers a callable as a tool, carrying its `side_effect` flag. |
| `HitlPolicy` | Human-in-the-loop configuration: suspension timeout, intent TTL, approval channel, timeout route. |
| `FallbackContext` | What a `HitlPolicy` route function is handed when a suspension times out. |
| `Deny` | Timeout route: emit a denial payload downstream. |
| `Drop` | Timeout route: discard the suspended activation silently. |
| `Escalate` | Timeout route: re-raise the timeout as an error record. |
| `ShardKeys` | Caller-side hot-key escape hatch: fan one logical entity across N physical keys. Memory-free agents only. |
| `shard_key` | Derive a physical shard key from a logical entity key. |
| `unshard_key` | Recover the logical entity key from a physical shard key. |
| `LangGraphAgent` | The LangGraph adapter class. Resolves lazily; needs the `langgraph` extra. |
| `AdkAgent` | The Google ADK adapter class. Resolves lazily; needs the `adk` extra. |
| `PydanticAIAgent` | The Pydantic AI adapter class. Resolves lazily; needs the `pydantic-ai` extra. |

Accessing an adapter class without its extra installed raises `ImportError`
naming the extra to install. Importing `beam_agents` itself performs no I/O,
spawns no threads, and imports no optional framework.

---

## Agent authoring — `beam_agents.core`

`beam_agents.core` re-exports `AgentContext`, `AgentResult`, `FunctionAgent`
and `StreamAgent`.

### `beam_agents.core.agent`

| Name | Contract |
| --- | --- |
| `StreamAgent` | Protocol: `async def activate(ctx) -> None`. Everything the agent does goes through `ctx` and is staged, never applied directly. |
| `FunctionAgent` | Adapts a plain `async def fn(ctx) -> None` into a `StreamAgent`. |
| `Agent` | The runtime driver contract: `async def __call__(ctx) -> Outcome`. What adapters implement. |
| `Outcome` | `Complete \| Suspend` — the result of one activation. |
| `Complete` | The activation finished; carries the payload emitted on `.output`. |
| `Suspend` | The activation staged an intent and yielded; the runtime persists a continuation and resumes on re-injection. |
| `FallbackContext` | The context a HITL timeout route receives. |
| `intent_id_for` | The deterministic intent ID: `uuid5(NAMESPACE, key + seq + step_index)` (correctness invariant 2). |

### `beam_agents.core.context`

| Name | Contract |
| --- | --- |
| `AgentContext` | What an agent sees: the event, keyed memory, `act()`, `call_model()`, `request_approval()`. All effects are staged. |
| `ActivationContext` | The runtime-facing context the driver holds: the staged intents, traces, upserts, and the step cursor. |
| `AgentResult` | The value an agent returns from a completed activation. |
| `MonotonicNs` | The injectable monotonic clock, in nanoseconds — the seam that keeps latency measurement testable. |

### `beam_agents.core.transform`

| Name | Contract |
| --- | --- |
| `RunAgent` | The `PTransform`. Raises `ValueError` at construction on non-KV input. |
| `AgentConfig` | The transform's configuration dataclass; `validate()` runs at construction and performs no I/O. |
| `RunAgentOutputs` | The four named outputs. |
| `SinkResolver` | Protocol for resolving a sink URI to a writer transform; `validate` must stay import-free. |
| `DefaultSinkResolver` | The shipped resolver: `kafka://`, `pubsub://`, `bigquery://`, `otlp://`. |
| `UnknownSinkSchemeError` | Raised at pipeline-construction time for an unusable sink URI. |
| `INTENTS_TAG` | Tag of the intents output. |
| `TRACES_TAG` | Tag of the traces output. |
| `ERRORS_TAG` | Tag of the errors output. |
| `SNAPSHOTS_TAG` | Tag of the state-snapshot output. |

### `beam_agents.core.loop`

| Name | Contract |
| --- | --- |
| `run_activation` | The loop driver: runs one activation, applies compaction and long-term flush, returns an `ActivationResult`. |
| `ActivationResult` | What the driver returns: the outcome plus everything staged. |
| `ActivationFailed` | The activation raised; the element routes to `.errors` and nothing commits. |
| `LongtermFlushFailed` | A long-term upsert failed during the commit tail. |
| `FailureContext` | The diagnostic bundle attached to a failure: step, model calls, staged intents. |
| `DEFAULT_HITL_TIMEOUT_MS` | Default suspension timeout when a `HitlPolicy` does not set one. |

### `beam_agents.core.dofn`

Error-record reasons and details, as they appear on the `.errors` output. See
[Errors and dead letters](errors.md) for what each one means operationally.

| Name | Contract |
| --- | --- |
| `ActivationError` | The typed error proto wrapper the errors output carries. |
| `REASON_ERROR` | The agent raised. |
| `REASON_TIMEOUT` | The activation exceeded `activation_timeout_s`. |
| `REASON_ORPHANED` | A result or approval arrived with no matching continuation. |
| `REASON_HITL_TIMEOUT` | A suspension timed out and the policy escalated. |
| `REASON_BUDGET_EXCEEDED` | The activation's token budget tripped. |
| `REASON_BATCH_OVERFLOW` | The adaptive batch buffer overflowed. |
| `REASON_INTENT_DEAD_LETTER` | An intent could not be written to the outbox. |
| `REASON_TTL_WIPED_BATCH` | Memory GC wiped a key with a buffered batch. |
| `REASON_TTL_WIPED_SUSPENSION` | Memory GC wiped a key with a live suspension. |
| `DETAIL_UNKNOWN_INTENT` | Detail: the intent id is not one this key emitted. |
| `DETAIL_NO_CONTINUATION` | Detail: no continuation is stored for this key. |
| `DETAIL_INTENT_EXPIRED` | Detail: the intent's `expires_at` had passed. |
| `DETAIL_DEADLINE_PASSED` | Detail: the suspension deadline had passed. |
| `HITL_TIMEOUT_OUTPUT` | The payload a default `Deny` route emits. Re-exported from `beam_agents.hitl`. |

### `beam_agents.core.batching`

Adaptive batching; see [Adaptive batching](batching.md).

| Name | Contract |
| --- | --- |
| `BatchPolicy` | Caller-facing batching configuration on `AgentConfig`. |
| `BatchSettings` | The resolved, validated settings the DoFn runs on. |
| `resolve_batch_settings` | Turn a `BatchPolicy` (or `None`) into `BatchSettings`. |
| `should_flush_on_size` | Whether the buffer has reached its size trigger. |
| `should_flush_on_timer` | Whether the flush timer should fire now. |
| `buffer_is_full` | Whether one more event would overflow the buffer. |
| `TRIGGER_SIZE` | Flush-trigger discriminant: size. |
| `TRIGGER_TIMER` | Flush-trigger discriminant: timer. |
| `DEFAULT_MAX_BATCH_SIZE` | Default events per batch. |
| `DEFAULT_MAX_WAIT_MS` | Default maximum buffering delay. |
| `BUFFER_HEADROOM` | Multiplier bounding the buffer above `max_batch_size`. |
| `TRACE_BATCH_SIZE` | Trace attribute: the flushed batch's size. |
| `TRACE_BATCH_TRIGGER` | Trace attribute: which trigger flushed the batch. |

### `beam_agents.core.migration`

State-schema evolution; see [State migration](state-migration.md).

| Name | Contract |
| --- | --- |
| `CURRENT_STATE_SCHEMA_VERSION` | The version every writer stamps. Bumping it is the breaking-change signal. |
| `VERSIONED_MESSAGE_TYPES` | The proto messages that carry a `state_schema_version`. |
| `migration` | Decorator registering one lazy migration step for a message type and source version. |
| `migrate_to_current` | Apply the registered steps in order, bringing a stored message to the current version. |
| `StateMigrationError` | Base class for migration failures. |
| `MissingMigrationError` | No registered step bridges a stored version to the next. |
| `MigrationStepError` | A step ran but did not produce the version it promised. |
| `StateSchemaFromFutureError` | The stored blob is newer than this build understands. |

### `beam_agents.core.snapshot`, `coders`, `bridge`, `error_records`

| Name | Contract |
| --- | --- |
| `build_snapshot` | Assemble the `StateSnapshot` emitted for offline replay. |
| `serialize_snapshot` | Deterministically serialize a snapshot for the snapshots sink. |
| `DeterministicProtoCoder` | The Beam coder for proto state: `deterministic=True` always. |
| `register_coders` | Register the proto coders with Beam's registry. Idempotent. |
| `MESSAGE_TYPES` | The proto message types `register_coders` registers. |
| `AsyncBridge` | The per-DoFn background thread and asyncio loop that runs activations. |
| `ActivationTimeout` | Raised when an activation outlives `activation_timeout_s`; nothing commits. |
| `serialize_error_envelope` | Deterministically serialize an error envelope for the errors sink. |
| `activation_error_to_row` | Map an activation error to a BigQuery row. |
| `intent_dead_letter_to_error` | Map an undeliverable intent to an activation error record. |

---

## Tools — `beam_agents.tools`

| Name | Contract |
| --- | --- |
| `tool` | The decorator. Derives a Pydantic argument model from the signature; `side_effect=True` marks a tool that may only run via `ctx.act(...)`. |
| `Tool` | One registered tool: name, schema, `side_effect` flag, and the guarded callable. |
| `ToolRegistry` | The name → `Tool` map handed to `AgentConfig`. Duplicate names are refused. |
| `ToolRunner` | Validates arguments and runs a read-only tool inline. |
| `IntentInfo` | The runtime-injected parameter a tool may declare to see its own intent id. |
| `ToolError` | Base class for tool failures. |
| `ToolNotFoundError` | The registry has no tool under that name. |
| `ToolArgumentError` | Arguments failed the tool's Pydantic model. |
| `ToolDefinitionError` | The tool could not be defined — e.g. an un-annotated parameter. |
| `SideEffectToolError` | A `side_effect` tool was called directly instead of via `ctx.act(...)` (invariant 5). |

---

## Model — `beam_agents.model`

### `beam_agents.model.client`

| Name | Contract |
| --- | --- |
| `LLMClient` | Protocol: `async def complete(request) -> LlmResponse`. Transport only — no retry, no caching, no breaker. |
| `LlmRequest` | One completion request: model id, messages, tools schema, sampling params. |
| `LlmResponse` | The provider's raw response bytes plus its latency. |
| `ProviderError` | Base class for provider failures, classified by type and never by message. |
| `ProviderTimeout` | The request outlived its transport timeout. Retryable. |
| `ProviderRequestError` | The provider rejected the request (4xx). Not retryable. |
| `RateLimitError` | The provider rate-limited the call (429). Retryable with backoff. |
| `ServerError` | The provider failed (5xx). Retryable. |

### `beam_agents.model.facade`

| Name | Contract |
| --- | --- |
| `LlmFacade` | The cache-first call path: replay cache, retry policy, circuit breaker, token budget, trace staging. |
| `FacadeResult` | What the facade returns: the response plus whether it came from cache. |
| `RetryPolicy` | Attempts, backoff, and which `ProviderError`s are retryable. |
| `CircuitBreaker` | Worker-local, per-endpoint breaker. Never keyed Beam state. |
| `CircuitState` | `CLOSED`, `OPEN`, `HALF_OPEN`. |
| `CircuitOpenError` | Raised while the endpoint's breaker is open. |
| `TokenBudget` | A per-activation token ceiling; latches once tripped. |
| `BudgetExceeded` | Raised when a call would exceed the budget. |
| `TokenUsage` | Prompt, completion, and total tokens for one call. |
| `DecodedResponse` | A provider response decoded into text, tool calls, and usage. |
| `OutputSchemaError` | A constrained-JSON decode did not validate against its schema. |
| `StagingSink` | Protocol the facade stages trace events and usage through. |
| `Decode` | The provider decode function type — the seam a new provider plugs into. |
| `Sleep` | The injectable sleep used by the retry policy, so backoff is testable without waiting. |

### `beam_agents.model.replay_cache`

| Name | Contract |
| --- | --- |
| `ReplayCache` | Keyed-state memoization of model calls. Bundle retries incur zero additional provider calls (invariant 3). |
| `ReplayEntry` | One cached response, or its digest when the response exceeded the blob cap. |
| `compute_cache_key` | `sha256(model_id, canonical_json(messages), tools_schema, sampling_params, key, seq, step_index)`. |
| `MAX_ENTRIES` | LRU bound on cached entries per key. |
| `TTL_MS` | How long a cached entry stays valid. |
| `BLOB_CAP_BYTES` | Per-response size cap; larger responses are kept by digest only. |

### Providers

| Name | Contract |
| --- | --- |
| `AnthropicProvider` | `LLMClient` over the Anthropic Messages API (httpx, no vendor SDK). |
| `OpenAICompatProvider` | `LLMClient` over any OpenAI-compatible `/chat/completions` endpoint. |
| `VllmEndpointProvider` | `LLMClient` against a remote vLLM server. |
| `VllmSidecarProvider` | `LLMClient` against a vLLM engine running in the Beam worker. |
| `vllm_sidecar_factory` | Build the worker-local sidecar provider, sharing one engine per worker. |
| `anthropic_decode` | Decode an Anthropic response into a `DecodedResponse`. Exported from `beam_agents.model`. |
| `openai_compat_decode` | Decode an OpenAI-compatible response into a `DecodedResponse`. Exported from `beam_agents.model`. |

### `beam_agents.model.fake`

The default model in every test tier but nightly smoke.

| Name | Contract |
| --- | --- |
| `FakeLLM` | Scripted `LLMClient`: matcher → behavior, recording every request. |
| `Matcher` | Predicate over an `LlmRequest`. |
| `Behavior` | What a matched request yields: a response, an error, or a sequence. |
| `match_any` | Matcher that accepts every request. |
| `match_contains` | Matcher on a substring of the request's messages. |
| `match_model_id` | Matcher on the request's model id. |
| `respond_with` | Behavior: return these response bytes. |
| `raise_error` | Behavior: raise this `ProviderError`. |
| `fail_then_succeed` | Behavior: fail N times, then succeed — the retry-path fixture. |
| `UnmatchedRequestError` | No matcher applied. A test that reaches this has a gap, not a default. |

---

## Memory — `beam_agents.memory`

See [Memory](memory.md).

| Name | Contract |
| --- | --- |
| `Memory` | Working memory: keyed scalars and bounded rings. Every write is staged and commits with the bundle. |
| `MemoryOverflow` | A write would take working memory past its hard cap. |
| `HARD_CAP_BYTES` | The per-key working-memory hard cap. |
| `LongtermMemory` | The explicit long-term tier handle. No working-tier path consults it implicitly. |
| `Compactor` | Tier-1 protocol: shrink memory synchronously, without model calls. |
| `DropOldestCompactor` | Shipped tier-1 compactor: LRU eviction down to a target size, honoring protected prefixes. |
| `Summarizer` | Tier-2 protocol: fold old content into a summary, using the activation's cached model path. |
| `SummarizeCompactor` | Shipped tier-2 summarizer: rolling summary of each source ring's oldest items. |
| `SummarizationView` | What a `Summarizer` is handed: the memory plus a replay-cached `call_model`. |
| `ExpireHook` | Protocol invoked by the TTL timer before keyed state is wiped. |
| `ExpiringMemory` | What an `ExpireHook` is handed: the final blob, entity key, and seq. |
| `FlushToLongterm` | The shipped hook: one idempotent, seq-guarded upsert of the final blob. |
| `DEFAULT_TRIGGER_BYTES` | Size at which compaction runs. |
| `DEFAULT_TARGET_BYTES` | Size compaction reduces to. |
| `DEFAULT_KEEP_RECENT` | Ring items a summarizer leaves untouched. |
| `DEFAULT_SUMMARY_KEY` | Memory key the rolling summary is written to. |
| `DEFAULT_EXPIRY_KEY` | Long-term key `FlushToLongterm` writes under. |
| `DEFAULT_PROTECTED_PREFIXES` | Key prefixes compaction never evicts. |

### `beam_agents.memory.stores`

| Name | Contract |
| --- | --- |
| `MemoryStore` | The long-term store protocol: `save` (idempotent, seq-guarded), `load`, `search`, `close`. |
| `MemoryRecord` | One stored record: entity key, key, value bytes, and seq. |
| `InMemoryMemoryStore` | Process-local store. Backs the offline lane. |
| `build_memory_store` | Construct the backend a parsed store URI names. |
| `parse_memory_store_uri` | Parse and validate a store URI without importing its client. |
| `BigtableMemoryStore` | Bigtable backend, seq-guarded via `CheckAndMutateRow`. |
| `RedisMemoryStore` | Redis backend. |
| `FirestoreMemoryStore` | Firestore backend. |
| `SqlMemoryStore` | SQLAlchemy backend (async drivers). |
| `DDL` | The table definition `SqlMemoryStore` expects. |

---

## Human-in-the-loop — `beam_agents.hitl`

| Name | Contract |
| --- | --- |
| `HitlPolicy` | Suspension timeout, intent TTL, approval channel, and the route taken on timeout. |
| `Route` | The routing function type: `FallbackContext -> Deny \| Drop \| Escalate`. |
| `Deny` | Emit a denial payload downstream. |
| `Drop` | Discard the timed-out suspension. |
| `Escalate` | Route the timeout to `.errors`. |
| `deny` | The default route: always `Deny`. |
| `intent_expired` | Whether an intent's `expires_at` has passed, given a clock reading. |
| `refuse_expired` | The effector-side guard: refuse an intent past its TTL (invariant 6, second layer). |
| `HITL_TIMEOUT_OUTPUT` | The payload a default `Deny` emits: `b"__hitl_timeout__"`. |
| `REASON_HITL_TIMEOUT` | The error reason an `Escalate` produces. |
| `DEFAULT_HITL_TIMEOUT_MS` | Default suspension timeout. |
| `DEFAULT_INTENT_TTL_MS` | Default intent TTL. |
| `DEFAULT_APPROVAL_CHANNEL` | Default approval channel name. |

---

## Key sharding — `beam_agents.keys`

See [Sharding](sharding.md). Safe for memory-free agents only.

| Name | Contract |
| --- | --- |
| `ShardKeys` | The transform that fans one logical entity across N physical keys. |
| `shard_key` | Derive the physical key for a logical key and shard index. |
| `unshard_key` | Recover the logical key from a physical key. |
| `Assignment` | The shard assignment for one element: logical key and chosen shard. |
| `SHARD_DELIMITER` | The byte separating logical key from shard index. |

---

## Intents and the outbox — `beam_agents.actions`

| Name | Contract |
| --- | --- |
| `WriteIntents` | Writes keyed `ToolIntent`s to the outbox topic. Requires KV input. |
| `WriteIntentsResult` | Its outputs: the written intents and the dead-letter output. |
| `UnknownIntentsSchemeError` | The outbox URI names no supported scheme. |
| `DEAD_LETTER_TAG` | Tag of the undeliverable-intent output. |

---

## Observability — `beam_agents.observability`

See [Runtime metrics](metrics.md) and [Trace delivery](traces.md), which give
each constant its operational meaning.

### `beam_agents.observability.traces`

| Name | Contract |
| --- | --- |
| `ActivationTrace` | Builds the OTel-GenAI-shaped trace events for one activation, correlated by derived ids. |
| `trace_id_for` | The activation's trace id, derived from `(entity_key, seq)` so a replay lands in the same trace. |
| `span_id_for` | The span id for one event within an activation. |
| `role_for_event_type` | Which role (`activation` or `timer`) an event type belongs to. |
| `usage_attributes` | Map a `TokenUsage` onto the OTel GenAI usage attributes. |
| `ROLE_ACTIVATION` | Role value for activation-driven events. |
| `ROLE_TIMER` | Role value for timer-driven events. |
| `OPERATION_NAME` | OTel GenAI attribute: operation name. |
| `OPERATION_CHAT` | Value of `OPERATION_NAME` for a chat completion. |
| `REQUEST_MODEL` | OTel GenAI attribute: requested model id. |
| `USAGE_INPUT_TOKENS` | OTel GenAI attribute: prompt tokens. |
| `USAGE_OUTPUT_TOKENS` | OTel GenAI attribute: completion tokens. |
| `ACTIVATION_KIND` | Attribute: `start` or `resume`. |
| `ACTIVATION_STATUS` | Attribute: the activation's terminal status. |
| `ADAPTER` | Attribute: which adapter drove the activation. |
| `ATTEMPTS` | Attribute: provider attempts a call consumed. |
| `BILLED` | Attribute: whether the call reached the provider. |
| `CACHE_HIT` | Attribute: whether the replay cache served the call. |
| `CIRCUIT_STATE` | Attribute: the breaker's state at call time. |
| `DEADLINE_MS` | Attribute: a suspension's deadline. |
| `ERROR_TYPE` | Attribute: the classified error type. |
| `EXPIRES_AT_MS` | Attribute: an intent's expiry. |
| `INTENT_ID` | Attribute: the deterministic intent id. |
| `INTENT_KIND` | Attribute: tool call or approval. |
| `PENDING_INTENT_IDS` | Attribute: intent ids a suspension is waiting on. |
| `REASON` | Attribute: why an activation ended the way it did. |
| `TOOL_NAME` | Attribute: the tool an intent names. |
| `FAILURE_STEP` | Failure attribute: the step index reached. |
| `FAILURE_LLM_CALLS` | Failure attribute: model calls made before failing. |
| `FAILURE_STAGED_INTENTS` | Failure attribute: intents staged before failing. |
| `FAILURE_LAST_EVENT` | Failure attribute: the last trace event produced. |

### `beam_agents.observability.metrics`

| Name | Contract |
| --- | --- |
| `MetricsSink` | Protocol: `incr` and `observe`. |
| `RuntimeMetrics` | Beam-backed sink; handles built once per recorder. |
| `NullMetrics` | No-op sink for components constructed outside a pipeline. |
| `ActivationTally` | Per-activation counters and timings the DoFn records on commit. |
| `NAMESPACE` | The Beam metrics namespace. |
| `COUNTERS` | Every declared counter name. |
| `DISTRIBUTIONS` | Every declared distribution name. |
| `COUNTER_ACTIVATIONS` | Activations processed. |
| `COUNTER_AGENT_ERRORS` | Activations that failed. |
| `COUNTER_LLM_CALLS` | Model calls that reached a provider. |
| `COUNTER_TOOL_CALLS` | Read-only tool calls executed inline. |
| `COUNTER_INTENTS_EMITTED` | Intents written to the outbox. |
| `COUNTER_SUSPENSIONS` | Activations that suspended. |
| `COUNTER_ORPHANED_RESULTS` | Results with no matching continuation. |
| `COUNTER_LONGTERM_UPSERTS` | Long-term store upserts. |
| `COUNTER_EVENTS_BUFFERED` | Events held in adaptive-batch buffers. |
| `COUNTER_BATCH_FLUSHES_SIZE` | Batches flushed by the size trigger. |
| `COUNTER_BATCH_FLUSHES_TIMER` | Batches flushed by the timer trigger. |
| `DISTRIBUTION_ACTIVATION_MS` | Wall time per activation. |
| `DISTRIBUTION_OVERHEAD_MS` | Runtime overhead excluding model and tool time — the latency budget's measure. |
| `DISTRIBUTION_LLM_MS` | Wall time per model call. |
| `DISTRIBUTION_ITERATIONS` | Steps consumed per activation. |
| `DISTRIBUTION_MEMORY_BYTES` | Working-memory size at commit. |
| `DISTRIBUTION_TOKENS` | Total tokens per activation. |
| `DISTRIBUTION_PROMPT_TOKENS` | Prompt tokens per activation. |
| `DISTRIBUTION_COMPLETION_TOKENS` | Completion tokens per activation. |
| `DISTRIBUTION_BATCH_SIZE` | Events per flushed batch. |

### `beam_agents.observability.exporters` and `otlp`

| Name | Contract |
| --- | --- |
| `serialize_trace_event` | Deterministically serialize a trace event for a lossless sink. |
| `trace_event_to_row` | Map a trace event onto the BigQuery trace row. |
| `TRACE_TABLE_SCHEMA` | The BigQuery schema `trace_event_to_row` writes against. |
| `WriteTracesToOtlp` | Batched, non-blocking OTLP/HTTP exporter. Best-effort: drops rather than blocking the pipeline. |
| `DEFAULT_BATCH_SIZE` | Spans per OTLP request. |
| `DEFAULT_FLUSH_DEADLINE_S` | How long a partial batch waits before being sent. |
| `DEFAULT_QUEUE_BATCHES` | Bound on queued batches before dropping. |
| `DEFAULT_SERVICE_NAME` | The `service.name` resource attribute. |
| `COUNTER_SPANS_EXPORTED` | Spans successfully exported. |
| `COUNTER_SPANS_DROPPED` | Spans dropped by backpressure or failure. |
| `COUNTER_BATCHES_SENT` | OTLP requests sent. |
| `COUNTER_EXPORT_FAILURES` | OTLP requests that failed. |

---

## Adapters — `beam_agents.adapters`

Each adapter's framework is an optional extra. Importing `beam_agents` never
imports one.

| Name | Contract |
| --- | --- |
| `LangGraphAgent` | Runs a compiled LangGraph graph as an activation. Needs the `langgraph` extra. |
| `BeamCheckpointSaver` | LangGraph checkpointer backed by keyed working memory, so checkpoints commit with the bundle. |
| `BeamToolNode` | LangGraph tool node that runs read-only tools inline and stages side-effecting ones as intents. |
| `AdkAgent` | Runs a Google ADK agent as an activation. Needs the `adk` extra. |
| `BeamSessionService` | ADK session service backed by keyed working memory. |
| `BeamFunctionTool` | ADK function tool bound to a beam-agents registry entry. |
| `BeamLongRunningTool` | ADK long-running tool that stages a `ToolIntent` and suspends. |
| `BeamApprovalTool` | ADK tool that requests human approval. |
| `beam_tools` | Build the ADK tool list from a `ToolRegistry`. |
| `PydanticAIAgent` | Runs a Pydantic AI agent as an activation. Needs the `pydantic-ai` extra. |
| `BeamToolset` | Pydantic AI toolset exposing a `ToolRegistry`, staging side effects as intents. |

---

## Replay — `beam_agents.replay`

See [State export and replay](replay.md).

| Name | Contract |
| --- | --- |
| `ReplayBundle` | A snapshot plus its trace: everything one activation needs to be re-run offline. |
| `build_bundle` | Assemble a bundle from a snapshot and a trace stream. |
| `run_replay` | Re-run the bundled activation against its own replay cache. |
| `ReplayOutcome` | The result of a replay: reproduced, diverged, or irreproducible. |
| `ReplayError` | Base class for replay failures. |
| `ReplayUsageError` | The inputs were unusable (bad snapshot, mismatched trace). |
| `ReplayIrreproducibleError` | The replay needed a model call the cache cannot serve. |
| `load_snapshot` | Read a `StateSnapshot` from bytes. |
| `load_envelope` | Read the `AgentEnvelope` a snapshot references. |
| `parse_trace_stream` | Parse a length-framed trace stream. |
| `frame_trace_events` | Write trace events in that framing. |
| `compare` | Diff a replay's events against the recorded ones. |
| `DiffReport` | The comparison result, renderable for the CLI. |
| `Difference` | One divergence: its kind and detail. |
| `normalize_event` | Strip the attributes that legitimately differ between runs. |
| `NORMALIZED_ATTRIBUTES` | Which attributes `normalize_event` strips. |
| `CacheOnlyLLMClient` | An `LLMClient` that serves only from the snapshot's cache. |
| `ReplayCacheMissError` | The cache-only client was asked for an uncached call. |
| `digest_only_digests` | The digests of responses the cache kept by digest alone. |
| `build_parser` | The `beam-agents-replay` CLI parser. |
| `main` | The CLI entry point. |
| `import_object` | Resolve a `module:attr` reference from a CLI flag. |
| `EXIT_REPRODUCED` | Exit status: byte-identical replay. |
| `EXIT_DIVERGED` | Exit status: the replay differed from the trace. |
| `EXIT_IRREPRODUCIBLE` | Exit status: the cache could not serve a call. |
| `EXIT_USAGE` | Exit status: bad invocation. |

---

## Test harness — `beam_agents.testing.chaos`

The bundle-retry chaos wrapper behind the retry-determinism gate.

| Name | Contract |
| --- | --- |
| `fail_first_matching_commit` | Force a bundle retry at the first commit matching a predicate. |
| `fail_first_hitl_fire` | Force a bundle retry at the first HITL timer firing. |
| `ChaosBundleFailure` | The exception the wrapper raises to trigger the retry. |
| `Matcher` | Predicate over a commit, selecting where to inject the failure. |
| `match_any` | Matcher that accepts the first commit. |

---

## Beam YAML provider — `beam_agents.yaml`

See [Beam YAML provider](yaml.md).

| Name | Contract |
| --- | --- |
| `run_agent` | The YAML transform constructor Beam YAML calls. |
| `RunAgentFromYaml` | The `PTransform` it builds: rows in, named outputs out. |
| `PROVIDER_LISTING` | The provider listing Beam YAML discovers this package through. |
| `OUTPUT_NAMES` | The names the provider exposes its outputs under. |
| `MALFORMED_TAG` | Output tag for rows that could not be mapped to an envelope. |
| `REASON_MALFORMED_ROW` | The reason recorded on a malformed row. |

---

## Effector — `beam_agents.effector`

The effector is a **separate reference service**, not a pipeline transform: it
imports no Beam. Its real contract is its CLI and config; see
[Running the effector](effector.md).

| Name | Contract |
| --- | --- |
| `EffectorConfig` | The service's validated configuration. |
| `EffectorConfigError` | The configuration is unusable — raised before anything connects. |
| `parse_transport_uri` | Parse and validate a source/sink URI without importing its client. |
| `parse_dedup_uri` | Parse and validate a dedup-store URI. |
| `DEFAULT_LEASE_MS` | Default claim lease duration. |
| `DEFAULT_RESULT_TTL_MS` | Default retention for terminal dedup records. |
| `DEFAULT_TOOL_TIMEOUT_MS` | Default per-tool execution timeout. |
| `build_parser` | The `beam-agents-effector` CLI parser. |
| `config_from_args` | Turn parsed arguments into a validated `EffectorConfig`. |
| `main` | The CLI entry point. |
| `EffectorService` | The loop: consume → dedup → execute → publish → commit. |
| `EffectorToolRunner` | Executes an intent's tool, exactly once per `intent_id`. |
| `ReadOnlyToolError` | The intent named a tool that is not `side_effect` — the effector refuses it. |
| `PublishFailedError` | A result could not be published within its retry budget. |
| `MetricsSink` | The service's metrics protocol (`incr`, `observe`). |
| `CountingMetrics` | Default in-process `MetricsSink`. |
| `DedupStore` | Atomic claim/complete/release over `intent_id`. The execution-side half of effectively-once. |
| `ClaimOutcome` | `Claimed \| InFlight \| Done`. |
| `Claimed` | Exclusive ownership, carrying the token later calls must present. |
| `InFlight` | Another worker holds a live lease. |
| `Done` | A terminal record exists — the dedup decision itself. |
| `InMemoryDedupStore` | Process-local store; backs the offline lane. |
| `RedisDedupStore` | Redis store: `SET NX PX` plus token-checking Lua scripts. |
| `BigtableDedupStore` | Bigtable store: one `CheckAndMutateRow` per claim. |
| `build_dedup_store` | Construct the store a parsed dedup URI names. |
| `IntentSource` | Where intents arrive from, with explicit commit and partition revocation. |
| `DeliveredIntent` | One delivered intent plus the handle used to commit it. |
| `RevocationHandler` | Callback invoked when partitions are revoked mid-flight. |
| `InMemoryIntentSource` | Scripted source recording what was committed. |
| `KafkaIntentSource` | Kafka consumer source; commits offset + 1. |
| `PubSubIntentSource` | Pub/Sub streaming-pull source. |
| `build_intent_source` | Construct the source a parsed transport URI names. |
| `MessageSink` | Durable publish of `(key, payload)`, ordered by key. |
| `ResultSink` | Publishes a `ToolResult` under its own entity key. |
| `ProtoResultSink` | Serializes a `ToolResult` onto a `MessageSink`. |
| `InMemoryMessageSink` | Recording sink; backs the offline lane. |
| `InMemoryResultSink` | Recording result sink; backs the offline lane. |
| `KafkaMessageSink` | Idempotent Kafka producer; `send_and_wait` before the offset commits. |
| `PubSubMessageSink` | Pub/Sub publisher using the entity key as the ordering key. |
| `build_message_sink` | Construct the message sink a parsed transport URI names. |
| `build_result_sink` | Construct the result sink a parsed transport URI names. |
