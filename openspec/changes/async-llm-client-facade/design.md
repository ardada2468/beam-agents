## Context

The codebase already defines provider-neutral value types, a protocol surface, and replay-cache primitives, but the runtime behavior around provider execution is fragmented and underspecified. Upcoming provider adapters need one shared async facade that enforces consistent request shape, constrained JSON output behavior, retry policy, backoff timing, endpoint-level protection, usage accounting, and tracing.

Constraints:
- Preserve provider-neutral contracts and avoid coupling to one vendor API.
- Reuse the existing replay-cache keying and blob semantics.
- Keep behavior deterministic enough for fake-provider and replay-based tests.

Stakeholders:
- Runtime/agent loop implementers
- Provider adapter implementers
- Observability and reliability owners

## Goals / Non-Goals

**Goals:**
- Define a single async `complete()` facade contract that accepts messages, tools, and optional output schema constraints.
- Standardize retry behavior with jittered exponential backoff while honoring provider `Retry-After`.
- Define per-endpoint circuit-breaker state transitions and rejection behavior.
- Define token usage accounting as part of completion outcomes.
- Define replay-cache lookup/write integration points around provider calls.
- Define trace emission points and required attributes at each point.

**Non-Goals:**
- Implementing every provider adapter in this change.
- Introducing durable/shared circuit-breaker state across activations.
- Designing new trace transport or storage systems.
- Changing replay-cache size/TTL limits.

## Decisions

1. **Facade-level message contract instead of provider request pass-through**
   - Decision: `complete()` accepts provider-neutral messages, optional tool schema, optional JSON output schema, and sampling options.
   - Rationale: Keeps adapters thin and makes retry/cache/trace behavior testable in one place.
   - Alternative considered: Keep only raw provider requests in adapters. Rejected because behavior would diverge and duplicate resilience logic.

2. **Constrained JSON is a first-class request concern**
   - Decision: Add optional `output_schema` to request material and require invalid outputs to fail with explicit provider/facade error classification.
   - Rationale: Structured outputs are central for tool and agent workflows, and cache keys must reflect schema-constrained response shape.
   - Alternative considered: Post-parse unconstrained text. Rejected due to weak guarantees and brittle parsing.

3. **Retry policy prioritizes server hints before local backoff**
   - Decision: For retryable failures, delay uses `Retry-After` when present/valid; otherwise use capped exponential backoff with jitter.
   - Rationale: Aligns with provider throttling semantics while preventing synchronized retry storms.
   - Alternative considered: Pure fixed backoff. Rejected due to poor tail behavior and higher error amplification.

4. **Circuit breaker keyed per endpoint**
   - Decision: Maintain independent breaker state per endpoint/model route key with closed/open/half-open transitions.
   - Rationale: Isolates unhealthy upstream partitions without globally disabling all traffic.
   - Alternative considered: Global breaker. Rejected because one failing endpoint would unnecessarily block healthy ones.

5. **Usage accounting and cache provenance included in completion result**
   - Decision: Completion outcomes include input/output token counts (when known) and whether served from replay cache.
   - Rationale: Needed for cost controls, SLO analysis, and deterministic test assertions.
   - Alternative considered: Emit usage only in traces/logs. Rejected because callers need synchronous access to usage signals.

6. **Trace points emitted at semantic boundaries**
   - Decision: Emit trace events for completion start, cache decision, provider attempt, retry scheduling, breaker short-circuit, and completion end/error.
   - Rationale: Supports debugging reliability behavior without over-instrumenting internal helpers.
   - Alternative considered: Single coarse LLM_CALL event. Rejected because retries/cache/breaker behavior would be opaque.

## Risks / Trade-offs

- **[Risk] Schema-constrained outputs vary across providers** → **Mitigation:** keep facade contract provider-neutral and map provider-specific mechanisms in adapters with explicit fallback errors.
- **[Risk] Added metadata in completion outcomes can break existing callers** → **Mitigation:** evolve value types in a backward-compatible way and document required vs optional fields.
- **[Risk] Jitter/backoff timing introduces flaky tests** → **Mitigation:** inject clock/RNG sources for deterministic tests.
- **[Risk] Circuit breakers can over-trip under transient bursts** → **Mitigation:** configure threshold/window/cooldown defaults conservatively and require half-open probe success to close.

## Migration Plan

1. Land new/modified specs for facade behavior, model-client contract, and replay-cache integration.
2. Update `model` interfaces and fake provider to satisfy the revised contract.
3. Introduce retry/backoff + breaker helpers with deterministic test seams.
4. Integrate replay-cache read/write in the completion flow and expose cache provenance.
5. Emit trace events/attributes at defined points and update tests/fixtures.
6. Rollback strategy: keep feature-gated facade path and revert to direct provider invocation while preserving value-type compatibility.

## Open Questions

- Should output-schema validation failures be classified as non-retryable provider errors or dedicated facade validation errors?
- Which endpoint key dimensions (base URL, deployment name, model_id) must be mandatory for breaker partitioning?
- For providers that omit token usage, should estimates be allowed or must usage fields remain explicitly unknown?
