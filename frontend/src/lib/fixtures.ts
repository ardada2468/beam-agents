/**
 * A deterministic dataset covering the runtime's full event vocabulary, plus a
 * `fetch` interceptor that serves it.
 *
 * This exists so every page is buildable, screenshottable, and reviewable with
 * no backend running — which is what makes the UI work parallelizable against a
 * backend being built at the same time.
 *
 * It is a development affordance, not a product feature. `installFixtures()`
 * returns immediately unless `import.meta.env.DEV` is set, so the production
 * bundle never serves generated data — and when it is active the shell shows an
 * unmistakable banner, because a UI silently displaying fake records is a trap.
 *
 * The data is generated, not hand-written, from a seeded PRNG — so the same
 * screenshot comes out of the same commit — and it deliberately includes the
 * *unpleasant* cases, because those are what most of the UI exists to render: a
 * suspension awaiting approval, a tool that failed, a budget exhausted, a TTL
 * that wiped a live suspension, an activation known only through the lossy OTLP
 * path.
 */

import type {
  ActivationDetail,
  ActivationSummary,
  ApprovalSummary,
  AttemptSummary,
  BucketPoint,
  EntitySummary,
  ErrorGroup,
  ErrorRecord,
  EventRecord,
  Health,
  IntentSummary,
  ModelSummary,
  Overview,
  Page,
  SearchHit,
  SpanNode,
  StoreStatus,
  ToolSummary,
  TraceDetail,
  TraceSummary,
} from './api-types';

/* -- Deterministic generation --------------------------------------------- */

/** mulberry32: small, seeded, and stable across engines. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * The instant the fixture dataset is anchored on.
 *
 * Rounded to the hour rather than pinned to a date: a fixed past timestamp made
 * every generated record permanently stale, so a "last 24 hours" window showed
 * rows days old and every pending approval rendered overdue. Rounding keeps a
 * screenshot stable for an hour, which is what reproducibility here is actually
 * for, without lying about when the data is from.
 */
export const FIXTURE_NOW = Math.floor(Date.now() / 3_600_000) * 3_600_000;

const random = rng(20260731);

function pick<T>(items: readonly T[]): T {
  return items[Math.floor(random() * items.length)] as T;
}

function hex(bytes: number): string {
  let out = '';
  for (let i = 0; i < bytes; i += 1) {
    out += Math.floor(random() * 256)
      .toString(16)
      .padStart(2, '0');
  }
  return out;
}

function toHex(text: string): string {
  return Array.from(new TextEncoder().encode(text))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/* -- Vocabulary, taken from the runtime, not invented ---------------------- */

const MODELS = [
  'claude-opus-5',
  'claude-sonnet-5',
  'claude-haiku-4-5-20251001',
  'gpt-4o-mini',
] as const;

const TOOLS = [
  'lookup_account',
  'fetch_risk_score',
  'charge_card',
  'send_notification',
  'freeze_account',
  'open_ticket',
] as const;

/** The runtime's closed reason vocabulary (`core/dofn.py`, `hitl.py`). */
const REASONS = [
  'activation_error',
  'activation_timeout',
  'orphaned_result',
  'hitl_timeout',
  'budget_exceeded',
  'intent_dead_letter',
  'ttl_wiped_suspension',
  'batch_buffer_overflow',
] as const;

const ERROR_TYPES = [
  'ToolExecutionError',
  'ValidationError',
  'TimeoutError',
  'BudgetExceeded',
  'ConnectionError',
] as const;

const ENTITY_NAMES = [
  'acct-10442',
  'acct-20981',
  'acct-33517',
  'user-7781',
  'user-9024',
  'device-4471',
  'session-88123',
  'merchant-551',
  'merchant-902',
  'card-6612',
] as const;

/* -- Generation ------------------------------------------------------------ */

interface Generated {
  activations: ActivationSummary[];
  details: Map<string, ActivationDetail>;
  errors: ErrorRecord[];
  approvals: ApprovalSummary[];
}

function activationKey(entityKey: string, seq: number): string {
  return `${entityKey}/${seq}`;
}

function generate(): Generated {
  const activations: ActivationSummary[] = [];
  const details = new Map<string, ActivationDetail>();
  const errors: ErrorRecord[] = [];
  const approvals: ApprovalSummary[] = [];

  const entityKeys = ENTITY_NAMES.map(toHex);

  for (let i = 0; i < 140; i += 1) {
    const entityKey = pick(entityKeys);
    const seq = Math.floor(random() * 40) + 1;
    const traceId = hex(16);
    const startedMs = FIXTURE_NOW - Math.floor(random() * 22 * 3_600_000);

    // Weighted so the list is mostly healthy but every state is reachable —
    // a fixture set of pure failures teaches the wrong shape.
    const roll = random();
    const status: ActivationSummary['status'] =
      roll < 0.62 ? 'completed' : roll < 0.78 ? 'suspended' : roll < 0.93 ? 'error' : 'in_flight';

    const resumed = status !== 'in_flight' && random() < 0.22;
    const model = pick(MODELS);
    const llmCalls = 1 + Math.floor(random() * 4);
    const toolCalls = Math.floor(random() * 5);
    const tools = Array.from({ length: toolCalls }, () => pick(TOOLS)).filter(
      (tool, index, all) => all.indexOf(tool) === index,
    );
    const promptTokens = 400 + Math.floor(random() * 5200);
    const completionTokens = 40 + Math.floor(random() * 900);
    const wallMs = status === 'in_flight' ? null : 180 + Math.floor(random() * 5200);
    const cacheHits = random() < 0.3 ? Math.floor(random() * llmCalls) : 0;

    // One activation in twelve arrives only over OTLP, which carries no
    // ACTIVATION_START. The UI has to say so rather than render it as complete.
    const otlpOnly = random() < 0.08;

    const errorCount = status === 'error' ? 1 : random() < 0.06 ? 1 : 0;
    const reasons: string[] = [];
    if (errorCount > 0) {
      reasons.push(status === 'error' ? pick(REASONS) : 'orphaned_result');
    }

    const summary: ActivationSummary = {
      entity_key: entityKey,
      seq,
      trace_id: traceId,
      status,
      kind: otlpOnly ? 'unknown' : resumed ? 'resume' : 'start',
      attempts: resumed ? 2 : 1,
      started_ms: startedMs,
      ended_ms: wallMs === null ? null : startedMs + wallMs,
      wall_ms: wallMs,
      model,
      llm_calls: llmCalls,
      tool_calls: toolCalls,
      intents: Math.floor(random() * 3),
      errors: errorCount,
      // A cache-only activation records no usage at all — null, not zero.
      prompt_tokens: cacheHits === llmCalls ? null : promptTokens,
      completion_tokens: cacheHits === llmCalls ? null : completionTokens,
      total_tokens: cacheHits === llmCalls ? null : promptTokens + completionTokens,
      cache_hits: cacheHits,
      tools,
      reasons,
      provenance: otlpOnly ? ['otlp'] : ['native'],
      complete_provenance: !otlpOnly,
    };
    activations.push(summary);

    if (errorCount > 0) {
      const reason = reasons[0] ?? 'activation_error';
      const hasContext = reason === 'activation_error';
      errors.push({
        entity_key: entityKey,
        seq,
        reason,
        detail:
          reason === 'activation_error'
            ? `ToolExecutionError: charge_card returned 502 (step 3, 2 staged intents, 2 llm calls)`
            : reason === 'hitl_timeout'
              ? `approval window elapsed at ${startedMs + 86_400_000}`
              : `${reason} for seq=${seq}`,
        error_type: hasContext ? pick(ERROR_TYPES) : null,
        event_time_ms: startedMs + (wallMs ?? 900),
        failure_step: hasContext ? Math.floor(random() * 6) : null,
        failure_last_event: hasContext ? pick(['LLM_CALL', 'TOOL_CALL', 'INTENT_EMITTED']) : null,
        failure_staged_intents: hasContext ? Math.floor(random() * 3) : null,
        failure_llm_calls: hasContext ? llmCalls : null,
      });
    }

    if (status === 'suspended') {
      approvals.push({
        intent_id: hex(16),
        entity_key: entityKey,
        seq,
        tool_name: pick(['charge_card', 'freeze_account', 'send_notification']),
        step_index: 1 + Math.floor(random() * 4),
        requested_ms: startedMs + 400,
        deadline_ms: startedMs + 3_600_000,
        expires_at_ms: startedMs + 3_600_000,
        escalations: random() < 0.2 ? 1 : 0,
        // One draw, not two: drawing the decision and its timestamp
        // independently produced `Pending` rows carrying a decided time.
        ...(() => {
          const decided = random() >= 0.7;
          return {
            decision: decided
              ? pick(['approved', 'denied', 'expired'] as const)
              : ('pending' as const),
            decided_ms: decided ? startedMs + 120_000 : null,
          };
        })(),
      });
    }

    details.set(
      activationKey(entityKey, seq),
      buildDetail(
        summary,
        errors.filter((e) => e.entity_key === entityKey && e.seq === seq),
      ),
    );
  }

  activations.sort((a, b) => b.started_ms - a.started_ms);
  errors.sort((a, b) => b.event_time_ms - a.event_time_ms);
  return { activations, details, errors, approvals };
}

function buildDetail(summary: ActivationSummary, own: ErrorRecord[]): ActivationDetail {
  const events: EventRecord[] = [];
  const spans: SpanNode[] = [];
  const intents: IntentSummary[] = [];
  const rootSpan = hex(8);
  let order = 0;

  // Every event is stamped with the same clock read on both ends: spans are
  // zero-width by design, and the fixtures must reproduce that faithfully or
  // the UI will be built against a shape the real runtime never produces.
  const push = (
    eventType: string,
    stepIndex: number,
    attributes: Record<string, string>,
    spanId: string,
    parent: string,
    atMs: number,
  ): EventRecord => {
    const event: EventRecord = {
      trace_id: summary.trace_id,
      span_id: spanId,
      parent_span_id: parent,
      entity_key: summary.entity_key,
      seq: summary.seq,
      step_index: stepIndex,
      event_type: eventType,
      start_ms: atMs,
      end_ms: atMs,
      attributes,
      provenance: summary.complete_provenance ? 'native' : 'otlp',
    };
    events.push(event);
    spans.push({
      span_id: spanId,
      parent_span_id: parent,
      role:
        eventType === 'ACTIVATION_START' || eventType === 'ACTIVATION_END'
          ? 'activation'
          : eventType,
      step_index: stepIndex,
      depth: parent === '' ? 0 : 1,
      order: order++,
      events: [event],
    });
    return event;
  };

  const t = summary.started_ms;
  const step = Math.max(1, Math.floor((summary.wall_ms ?? 900) / 6));

  if (summary.complete_provenance) {
    push('ACTIVATION_START', 0, { 'beam_agents.activation.kind': summary.kind }, rootSpan, '', t);
  }

  for (let i = 0; i < summary.llm_calls; i += 1) {
    const cached = i < summary.cache_hits;
    const attributes: Record<string, string> = {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': summary.model ?? 'unknown',
      'beam_agents.cache_hit': cached ? 'true' : 'false',
      'beam_agents.billed': cached ? 'false' : 'true',
      'beam_agents.attempts': '1',
    };
    // Usage attributes are omitted, not zeroed, when nothing was decoded.
    if (!cached && summary.prompt_tokens !== null) {
      attributes['gen_ai.usage.input_tokens'] = String(
        Math.floor(summary.prompt_tokens / summary.llm_calls),
      );
      attributes['gen_ai.usage.output_tokens'] = String(
        Math.floor((summary.completion_tokens ?? 0) / summary.llm_calls),
      );
    }
    push('LLM_CALL', i, attributes, hex(8), rootSpan, t + step * (i + 1));
  }

  summary.tools.forEach((tool, i) => {
    push(
      'TOOL_CALL',
      i,
      { 'beam_agents.tool_name': tool },
      hex(8),
      rootSpan,
      t + step * (summary.llm_calls + i + 1),
    );
  });

  for (let i = 0; i < summary.intents; i += 1) {
    const intentId = hex(16);
    const toolName = pick(TOOLS);
    const emittedAt = t + step * (summary.llm_calls + summary.tools.length + i + 1);
    intents.push({
      intent_id: intentId,
      tool_name: toolName,
      intent_kind: random() < 0.25 ? 'APPROVAL' : 'TOOL',
      step_index: i,
      expires_at_ms: emittedAt + 3_600_000,
      emitted_at_ms: emittedAt,
    });
    push(
      'INTENT_EMITTED',
      i,
      {
        'beam_agents.intent_id': intentId,
        'beam_agents.tool_name': toolName,
        'beam_agents.intent_kind': 'TOOL',
        'beam_agents.expires_at_ms': String(emittedAt + 3_600_000),
      },
      hex(8),
      rootSpan,
      emittedAt,
    );
  }

  if (summary.status === 'suspended') {
    push(
      'SUSPENDED',
      summary.intents,
      {
        'beam_agents.deadline_ms': String(t + 3_600_000),
        'beam_agents.adapter': 'approval',
        'beam_agents.pending_intent_ids': intents.map((i) => i.intent_id).join(','),
      },
      hex(8),
      rootSpan,
      t + (summary.wall_ms ?? 800),
    );
  }

  if (summary.status === 'error') {
    push(
      'ERROR',
      0,
      {
        'beam_agents.reason': summary.reasons[0] ?? 'activation_error',
        'error.type': 'ToolExecutionError',
        'beam_agents.failure.step': '3',
        'beam_agents.failure.last_event': 'TOOL_CALL',
        'beam_agents.failure.staged_intents': '2',
        'beam_agents.failure.llm_calls': String(summary.llm_calls),
      },
      hex(8),
      rootSpan,
      t + (summary.wall_ms ?? 900),
    );
  }

  if (summary.ended_ms !== null) {
    push(
      'ACTIVATION_END',
      summary.intents,
      {
        'beam_agents.activation.status': summary.status === 'suspended' ? 'suspended' : 'completed',
        'beam_agents.activation.kind': summary.kind,
      },
      rootSpan,
      '',
      summary.ended_ms,
    );
  }

  const attempts: AttemptSummary[] = [
    {
      span_id: rootSpan,
      kind: summary.kind === 'resume' ? 'start' : summary.kind,
      entry_step_index: 0,
      start_ms: summary.started_ms,
      end_ms: summary.ended_ms,
      status: summary.status,
    },
  ];
  if (summary.attempts > 1) {
    attempts.push({
      span_id: hex(8),
      kind: 'resume',
      entry_step_index: 2,
      start_ms: summary.started_ms + 240_000,
      end_ms: summary.ended_ms,
      status: summary.status,
    });
  }

  return {
    summary,
    attempts,
    spans,
    events,
    intents,
    // Was always empty, which made the failure-position panel unreachable from
    // an error record — the primary path both the Errors and Activations pages
    // render it from.
    errors: own,
    snapshot:
      summary.status === 'suspended'
        ? {
            entity_key: summary.entity_key,
            seq: summary.seq,
            snapshot_at_ms: summary.started_ms + 800,
            state_schema_version: 1,
            request_id: '',
            memory_entries: 4 + Math.floor(random() * 8),
            memory_bytes: 2048 + Math.floor(random() * 40000),
            llm_cache_entries: summary.llm_calls,
            pending_intent_ids: intents.map((i) => i.intent_id),
            continuation_step_index: summary.intents,
            continuation_deadline_ms: summary.started_ms + 3_600_000,
            continuation_adapter: 'approval',
          }
        : null,
    replay_command: `beam-agents-replay --snapshot snapshot.bin --traces traces.bin --agent my_pipeline:agent --seq ${summary.seq}`,
  };
}

const DATA = generate();

/* -- Derived aggregates ---------------------------------------------------- */

function series(values: number[], buckets: number, windowMs: number): BucketPoint[] {
  const width = windowMs / buckets;
  return Array.from({ length: buckets }, (_, i) => ({
    bucket_ms: Math.round(FIXTURE_NOW - windowMs + i * width),
    value: values[i] ?? 0,
  }));
}

function bucketize(items: { at: number }[], buckets: number, windowMs: number): number[] {
  const width = windowMs / buckets;
  const counts = new Array<number>(buckets).fill(0);
  for (const item of items) {
    const index = Math.floor((item.at - (FIXTURE_NOW - windowMs)) / width);
    if (index >= 0 && index < buckets) counts[index] = (counts[index] ?? 0) + 1;
  }
  return counts;
}

function models(since: number | null = null): ModelSummary[] {
  return MODELS.map((model) => {
    const rows = inWindow(DATA.activations, since).filter((a) => a.model === model);
    const calls = rows.reduce((sum, a) => sum + a.llm_calls, 0);
    const cacheHits = rows.reduce((sum, a) => sum + a.cache_hits, 0);
    const prompt = rows.reduce((sum, a) => sum + (a.prompt_tokens ?? 0), 0);
    const completion = rows.reduce((sum, a) => sum + (a.completion_tokens ?? 0), 0);
    return {
      model,
      calls,
      prompt_tokens: prompt || null,
      completion_tokens: completion || null,
      total_tokens: prompt + completion || null,
      cache_hits: cacheHits,
      cache_hit_ratio: calls ? cacheHits / calls : null,
      errors: rows.filter((a) => a.status === 'error').length,
      max_attempts: 3,
      circuit_states: { closed: rows.length, open: 0 },
    };
  }).sort((a, b) => b.calls - a.calls);
}

function tools(since: number | null = null): ToolSummary[] {
  return TOOLS.map((tool) => {
    const rows = inWindow(DATA.activations, since).filter((a) => a.tools.includes(tool));
    const calls = rows.length;
    const errors = rows.filter((a) => a.status === 'error').length;
    return {
      tool_name: tool,
      calls,
      intents: Math.floor(calls / 3),
      errors,
      failure_ratio: calls ? errors / calls : null,
      last_seen_ms: rows[0]?.started_ms ?? null,
    };
  })
    .filter((t) => t.calls > 0)
    .sort((a, b) => b.calls - a.calls);
}

function entities(): EntitySummary[] {
  const byKey = new Map<string, ActivationSummary[]>();
  for (const activation of DATA.activations) {
    const existing = byKey.get(activation.entity_key) ?? [];
    existing.push(activation);
    byKey.set(activation.entity_key, existing);
  }
  return Array.from(byKey.entries())
    .map(([entityKey, rows]) => {
      const sorted = [...rows].sort((a, b) => b.started_ms - a.started_ms);
      const latest = sorted[0];
      const tokens = rows.reduce((sum, a) => sum + (a.total_tokens ?? 0), 0);
      return {
        entity_key: entityKey,
        activations: rows.length,
        first_seen_ms: Math.min(...rows.map((a) => a.started_ms)),
        last_seen_ms: Math.max(...rows.map((a) => a.started_ms)),
        errors: rows.filter((a) => a.errors > 0).length,
        total_tokens: tokens || null,
        latest_seq: latest?.seq ?? null,
        latest_status: latest?.status ?? null,
      };
    })
    .sort((a, b) => b.last_seen_ms - a.last_seen_ms);
}

function errorGroups(since: number | null = null): ErrorGroup[] {
  const byReason = new Map<string, ErrorRecord[]>();
  for (const error of since === null
    ? DATA.errors
    : DATA.errors.filter((e) => e.event_time_ms >= since)) {
    const key = `${error.reason}\u0000${error.error_type ?? ''}`;
    const existing = byReason.get(key) ?? [];
    existing.push(error);
    byReason.set(key, existing);
  }
  return Array.from(byReason.entries())
    .map(([key, rows]) => {
      const [reason, errorType] = key.split('\u0000');
      const windowMs = 24 * 3_600_000;
      return {
        reason: reason ?? 'unknown',
        error_type: errorType || null,
        count: rows.length,
        entities: new Set(rows.map((r) => r.entity_key)).size,
        first_seen_ms: Math.min(...rows.map((r) => r.event_time_ms)),
        last_seen_ms: Math.max(...rows.map((r) => r.event_time_ms)),
        series: series(
          bucketize(
            rows.map((r) => ({ at: r.event_time_ms })),
            24,
            windowMs,
          ),
          24,
          windowMs,
        ),
        sample_detail: rows[0]?.detail ?? '',
      };
    })
    .sort((a, b) => b.count - a.count);
}

function storeStatus(): StoreStatus {
  return {
    row_counts: {
      events: DATA.activations.reduce((sum, a) => sum + a.llm_calls + a.tool_calls + 2, 0),
      activations: DATA.activations.length,
      errors: DATA.errors.length,
      snapshots: DATA.activations.filter((a) => a.status === 'suspended').length,
      entities: new Set(DATA.activations.map((a) => a.entity_key)).size,
    },
    retention_hours: 168,
    database_path: '/data/beam-agents-console.db',
    database_bytes: 18_452_480,
    oldest_record_ms: Math.min(...DATA.activations.map((a) => a.started_ms)),
    newest_record_ms: Math.max(...DATA.activations.map((a) => a.started_ms)),
    schema_version: 1,
  };
}

function overview(windowMs: number, buckets: number): Overview {
  const inWindow = DATA.activations.filter((a) => a.started_ms >= FIXTURE_NOW - windowMs);
  const walls = inWindow
    .map((a) => a.wall_ms)
    .filter((w): w is number => w !== null)
    .sort((a, b) => a - b);
  const at = (q: number) => walls[Math.floor(walls.length * q)] ?? null;
  const llmCalls = inWindow.reduce((sum, a) => sum + a.llm_calls, 0);
  const cacheHits = inWindow.reduce((sum, a) => sum + a.cache_hits, 0);
  const tokens = inWindow.reduce((sum, a) => sum + (a.total_tokens ?? 0), 0);

  return {
    window_ms: windowMs,
    activations: inWindow.length,
    completed: inWindow.filter((a) => a.status === 'completed').length,
    suspended: inWindow.filter((a) => a.status === 'suspended').length,
    in_flight: inWindow.filter((a) => a.status === 'in_flight').length,
    errors: DATA.errors.filter((e) => e.event_time_ms >= FIXTURE_NOW - windowMs).length,
    error_ratio: inWindow.length
      ? inWindow.filter((a) => a.errors > 0).length / inWindow.length
      : null,
    total_tokens: tokens || null,
    llm_calls: llmCalls,
    tool_calls: inWindow.reduce((sum, a) => sum + a.tool_calls, 0),
    cache_hit_ratio: llmCalls ? cacheHits / llmCalls : null,
    p50_wall_ms: at(0.5),
    p95_wall_ms: at(0.95),
    activation_series: series(
      bucketize(
        inWindow.map((a) => ({ at: a.started_ms })),
        buckets,
        windowMs,
      ),
      buckets,
      windowMs,
    ),
    error_series: series(
      bucketize(
        DATA.errors.map((e) => ({ at: e.event_time_ms })),
        buckets,
        windowMs,
      ),
      buckets,
      windowMs,
    ),
    token_series: series(
      bucketize(
        inWindow.flatMap((a) =>
          Array.from({ length: Math.ceil((a.total_tokens ?? 0) / 800) }, () => ({
            at: a.started_ms,
          })),
        ),
        buckets,
        windowMs,
      ).map((n) => n * 800),
      buckets,
      windowMs,
    ),
    top_models: models().slice(0, 4),
    top_tools: tools().slice(0, 5),
    recent_errors: DATA.errors.slice(0, 6),
    store: storeStatus(),
  };
}

function traces(): TraceSummary[] {
  return DATA.activations.map((a) => ({
    trace_id: a.trace_id,
    entity_key: a.entity_key,
    seq: a.seq,
    events: a.llm_calls + a.tool_calls + a.intents + 2,
    spans: a.llm_calls + a.tool_calls + a.intents + 1,
    started_ms: a.started_ms,
    ended_ms: a.ended_ms,
    status: a.status,
  }));
}

/**
 * The `since_ms` a request asked for, or null.
 *
 * The interceptor used to ignore this parameter entirely, so every window
 * selector in the UI looked inert against fixtures and "last 24 hours" listed
 * rows days old — a fixture that contradicts the control it is meant to
 * exercise teaches the wrong thing about the page.
 */
function sinceMs(params: URLSearchParams): number | null {
  const raw = params.get('since_ms');
  return raw === null || raw === '' ? null : Number(raw);
}

function inWindow(rows: ActivationSummary[], since: number | null): ActivationSummary[] {
  return since === null ? rows : rows.filter((a) => a.started_ms >= since);
}

function paginate<T>(items: T[], cursor: string | null, limit: number): Page<T> {
  const start = cursor ? Number(cursor) : 0;
  const slice = items.slice(start, start + limit);
  const next = start + limit < items.length ? String(start + limit) : null;
  return { items: slice, next_cursor: next, total: items.length };
}

/* -- The interceptor ------------------------------------------------------- */

/** Every fixture response, keyed by the path the client requests. */
function respond(path: string, params: URLSearchParams): unknown {
  const limit = Number(params.get('limit') ?? 50);
  const cursor = params.get('cursor');

  if (path === '/healthz') {
    return {
      status: 'ok',
      version: '1.0.0',
      schema_version: 1,
      ui_bundled: true,
      sources: ['fixtures'],
    } satisfies Health;
  }

  if (path === '/api/overview') {
    return overview(
      Number(params.get('window_ms') ?? 86_400_000),
      Number(params.get('buckets') ?? 48),
    );
  }

  if (path === '/api/activations') {
    let rows = DATA.activations;
    const status = params.get('status');
    const model = params.get('model');
    const tool = params.get('tool');
    const reason = params.get('reason');
    const entityKey = params.get('entity_key');
    const query = params.get('query');
    if (status) rows = rows.filter((a) => a.status === status);
    if (model) rows = rows.filter((a) => a.model === model);
    if (tool) rows = rows.filter((a) => a.tools.includes(tool));
    if (reason) rows = rows.filter((a) => a.reasons.includes(reason));
    if (entityKey) rows = rows.filter((a) => a.entity_key.includes(entityKey));
    if (query) {
      const q = query.toLowerCase();
      rows = rows.filter(
        (a) =>
          a.entity_key.includes(q) ||
          a.trace_id.includes(q) ||
          (a.model ?? '').toLowerCase().includes(q),
      );
    }
    return paginate(rows, cursor, limit);
  }

  const activationMatch = /^\/api\/activations\/([^/]+)\/(\d+)$/.exec(path);
  if (activationMatch) {
    const key = activationKey(
      decodeURIComponent(activationMatch[1] ?? ''),
      Number(activationMatch[2]),
    );
    return DATA.details.get(key) ?? null;
  }

  if (path === '/api/traces') {
    const query = params.get('query')?.toLowerCase();
    const rows = query
      ? traces().filter((t) => t.trace_id.includes(query) || t.entity_key.includes(query))
      : traces();
    return paginate(rows, cursor, limit);
  }

  const traceMatch = /^\/api\/traces\/([^/]+)$/.exec(path);
  if (traceMatch) {
    const traceId = decodeURIComponent(traceMatch[1] ?? '');
    const activation = DATA.activations.find((a) => a.trace_id === traceId);
    if (!activation) return null;
    const detail = DATA.details.get(activationKey(activation.entity_key, activation.seq));
    const summary = traces().find((t) => t.trace_id === traceId);
    if (!detail || !summary) return null;
    return {
      summary,
      roots: detail.spans.filter((s) => s.depth === 0),
      attempts: detail.attempts,
    } satisfies TraceDetail;
  }

  if (path === '/api/errors/groups') {
    return errorGroups(sinceMs(params));
  }

  if (path === '/api/errors') {
    const reason = params.get('reason');
    const since = sinceMs(params);
    let rows = since === null ? DATA.errors : DATA.errors.filter((e) => e.event_time_ms >= since);
    if (reason) rows = rows.filter((e) => e.reason === reason);
    return paginate(rows, cursor, limit);
  }

  if (path === '/api/models') return models(sinceMs(params));
  if (path === '/api/tools') return tools(sinceMs(params));
  if (path === '/api/approvals') {
    const pendingOnly = params.get('pending_only') === 'true';
    return pendingOnly ? DATA.approvals.filter((a) => a.decision === 'pending') : DATA.approvals;
  }
  if (path === '/api/entities') return paginate(entities(), cursor, limit);
  if (path === '/api/store') return storeStatus();

  if (path === '/api/search') {
    const q = (params.get('q') ?? '').toLowerCase();
    if (!q) return [];
    const hits: SearchHit[] = DATA.activations
      .filter((a) => a.entity_key.includes(q) || a.trace_id.includes(q))
      .slice(0, limit)
      .map((a) => ({
        kind: 'activation' as const,
        entity_key: a.entity_key,
        seq: a.seq,
        trace_id: a.trace_id,
        span_id: null,
        label: `${a.status} · seq ${a.seq}`,
        matched_field: a.entity_key.includes(q) ? 'entity_key' : 'trace_id',
        matched_value: a.entity_key.includes(q) ? a.entity_key : a.trace_id,
        at_ms: a.started_ms,
      }));
    return hits;
  }

  return null;
}

let installed = false;

/**
 * Whether a real console is answering behind the dev proxy.
 *
 * `installFixtures` has to ask before it takes over, not merely assume. The
 * banner it raises reads "no console is answering", and with a console up on
 * 8787 that sentence is false: the proxy in `vite.config.ts` forwards `/api`
 * and `/healthz` to it, so every page would quietly serve generated records
 * while asserting the backend was absent. A UI silently showing fake data is
 * the trap this module's own header warns about; claiming the backend is down
 * while it is up is that trap with a label on it.
 *
 * `/healthz` is the probe because it is the one endpoint that answers on an
 * empty store — the console being up is not the same question as the console
 * having data.
 */
async function consoleIsAnswering(fetcher: typeof globalThis.fetch): Promise<boolean> {
  try {
    const response = await fetcher('/healthz', {
      signal: AbortSignal.timeout(1500),
      cache: 'no-store',
    });
    return response.ok;
  } catch {
    // Anything at all — no proxy target, a refused connection, the timeout
    // above — means there is nothing to run against, which is what fixtures
    // exist for.
    return false;
  }
}

/**
 * Serve fixture data from `fetch`, in dev only and only with no console up.
 *
 * Idempotent. Returns whether the interceptor is active, so the shell can show
 * an unmistakable banner — a UI silently serving fake data is a trap.
 */
export async function installFixtures(): Promise<boolean> {
  if (installed) return true;
  if (!import.meta.env.DEV) return false;

  const original = globalThis.fetch.bind(globalThis);
  if (await consoleIsAnswering(original)) return false;

  installed = true;

  globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(
      typeof input === 'string' ? input : input instanceof URL ? input.href : input.url,
      globalThis.location.origin,
    );

    if (!url.pathname.startsWith('/api') && url.pathname !== '/healthz') {
      return original(input, init);
    }

    // A small delay so loading states are actually exercised while building.
    await new Promise((resolve) => setTimeout(resolve, 60));

    const body = respond(url.pathname, url.searchParams);
    if (body === null) {
      return new Response(JSON.stringify({ detail: 'not found' }), {
        status: 404,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };

  return true;
}

/** Whether fixture data is currently being served. */
export function usingFixtures(): boolean {
  return installed;
}

/** The generated dataset, for tests and for pages that need a shape to reason about. */
export const fixtures = DATA;
