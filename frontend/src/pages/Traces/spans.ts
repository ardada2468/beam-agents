/**
 * Assembling a trace's span forest, and attributing it to attempts.
 *
 * Three things about the runtime's spans drive every function here.
 *
 * **Spans are zero-width.** `start_ms === end_ms` on every event, because
 * `ActivationTrace._event` stamps both ends from the same injected activation
 * clock read (`observability/traces.py`, D7). Nothing in this file computes a
 * span duration, and `SpanNode` deliberately does not carry one. The only
 * ordering signal that exists is `start_ms` plus the API's `order`, and that is
 * what the tree is built on.
 *
 * **A span can hold more than one event.** `ACTIVATION_START` and
 * `ACTIVATION_END` share the attempt's activation span ID by construction, so
 * the API's flat `SpanNode` list contains two entries for it. They are merged
 * here, on the same `(trace_id, span_id, event_type)` identity the store dedups
 * on (`docs/traces.md`), so one span renders as one node holding both events.
 *
 * **A resume hangs under the initial attempt.** `ActivationTrace.__init__`
 * parents a resumed attempt's activation span on `span_id_for(…, "activation",
 * 0)`, which is why a suspend → effector → resume cycle is one trace whose tree
 * contains both attempts. Attributing a span to an attempt is therefore a walk
 * up to the nearest ancestor that is itself an attempt's activation span.
 */

import type { AttemptSummary, EventRecord, SpanNode } from '@/lib/api-types';

/** Attribute names, copied from `observability/traces.py` rather than guessed. */
export const ATTR = {
  requestModel: 'gen_ai.request.model',
  inputTokens: 'gen_ai.usage.input_tokens',
  outputTokens: 'gen_ai.usage.output_tokens',
  cacheHit: 'beam_agents.cache_hit',
  toolName: 'beam_agents.tool_name',
  reason: 'beam_agents.reason',
} as const;

/** The role `role_for_event_type` assigns to `ACTIVATION_START`/`ACTIVATION_END`. */
export const ROLE_ACTIVATION = 'activation';

/**
 * One span, with every event recorded against it.
 *
 * The API returns spans flat; this is the same shape with the duplicate
 * `span_id` entries folded together.
 */
export interface MergedSpan {
  span_id: string;
  parent_span_id: string;
  role: string;
  step_index: number;
  order: number;
  events: EventRecord[];
  /** Earliest event clock read on this span. Not a start time of anything wider. */
  at_ms: number | null;
}

/** A node in the assembled forest. `depth` is recomputed, never trusted from the wire. */
export interface SpanTreeNode {
  span: MergedSpan;
  depth: number;
  children: SpanTreeNode[];
}

function eventKey(event: EventRecord): string {
  return `${event.trace_id}|${event.span_id}|${event.event_type}`;
}

/** Order events the only way the records support: by clock read, then step. */
export function compareEvents(a: EventRecord, b: EventRecord): number {
  return (
    a.start_ms - b.start_ms ||
    a.step_index - b.step_index ||
    a.event_type.localeCompare(b.event_type)
  );
}

/**
 * Fold a flat span list into one node per `span_id`.
 *
 * `roles` disagreeing across entries is possible in principle (the same span ID
 * reached over two ingest paths); `activation` wins, because that is the role
 * that decides whether a span is an attempt root.
 */
export function mergeSpans(spans: readonly SpanNode[]): MergedSpan[] {
  const byId = new Map<string, MergedSpan>();
  const seenEvents = new Map<string, Set<string>>();

  for (const span of spans) {
    let merged = byId.get(span.span_id);
    if (!merged) {
      merged = {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        role: span.role,
        step_index: span.step_index,
        order: span.order,
        events: [],
        at_ms: null,
      };
      byId.set(span.span_id, merged);
      seenEvents.set(span.span_id, new Set());
    }
    if (!merged.parent_span_id) merged.parent_span_id = span.parent_span_id;
    if (span.role === ROLE_ACTIVATION) merged.role = ROLE_ACTIVATION;
    merged.step_index = Math.min(merged.step_index, span.step_index);
    merged.order = Math.min(merged.order, span.order);

    const seen = seenEvents.get(span.span_id);
    for (const event of span.events) {
      const key = eventKey(event);
      if (seen?.has(key)) continue;
      seen?.add(key);
      merged.events.push(event);
    }
  }

  for (const merged of byId.values()) {
    merged.events.sort(compareEvents);
    merged.at_ms = merged.events[0]?.start_ms ?? null;
  }
  return Array.from(byId.values());
}

function compareSpans(a: MergedSpan, b: MergedSpan): number {
  if (a.at_ms !== null && b.at_ms !== null && a.at_ms !== b.at_ms) return a.at_ms - b.at_ms;
  if (a.at_ms === null && b.at_ms !== null) return 1;
  if (a.at_ms !== null && b.at_ms === null) return -1;
  return a.order - b.order || a.span_id.localeCompare(b.span_id);
}

/**
 * Assemble the forest from `parent_span_id`.
 *
 * A span whose parent is absent from the set is a root rather than a dropped
 * node — which is the OTLP case, where the parent's events never arrive. The
 * visited set guards against a parent cycle rather than trusting the producer.
 */
export function buildForest(spans: readonly MergedSpan[]): SpanTreeNode[] {
  const byId = new Map(spans.map((span) => [span.span_id, span]));
  const childrenOf = new Map<string, MergedSpan[]>();
  const roots: MergedSpan[] = [];

  for (const span of spans) {
    const parent = span.parent_span_id;
    if (parent && parent !== span.span_id && byId.has(parent)) {
      const siblings = childrenOf.get(parent) ?? [];
      siblings.push(span);
      childrenOf.set(parent, siblings);
    } else {
      roots.push(span);
    }
  }

  const visited = new Set<string>();
  const build = (span: MergedSpan, depth: number): SpanTreeNode => {
    visited.add(span.span_id);
    const children = (childrenOf.get(span.span_id) ?? [])
      .filter((child) => !visited.has(child.span_id))
      .sort(compareSpans)
      .map((child) => build(child, depth + 1));
    return { span, depth, children };
  };

  const forest = roots.sort(compareSpans).map((root) => build(root, 0));
  // Anything a cycle kept out of the forest still has to be reachable.
  for (const span of spans) {
    if (!visited.has(span.span_id)) forest.push(build(span, 0));
  }
  return forest;
}

/** Every event in the forest, in recorded order. */
export function flattenEvents(spans: readonly MergedSpan[]): EventRecord[] {
  return spans.flatMap((span) => span.events).sort(compareEvents);
}

/**
 * Map each span to the attempt that produced it.
 *
 * Walks down from the roots carrying the nearest enclosing attempt span, so a
 * resume's children are attributed to the resume and not to the initial attempt
 * it hangs under.
 */
export function attributeToAttempts(
  roots: readonly SpanTreeNode[],
  attemptSpanIds: ReadonlySet<string>,
): Map<string, string> {
  const owner = new Map<string, string>();
  const walk = (node: SpanTreeNode, current: string | null) => {
    const next = attemptSpanIds.has(node.span.span_id) ? node.span.span_id : current;
    if (next !== null) owner.set(node.span.span_id, next);
    for (const child of node.children) walk(child, next);
  };
  for (const root of roots) walk(root, null);
  return owner;
}

/**
 * What one attempt did.
 *
 * Every count is derived from events; every one that has no recorded source is
 * `null` rather than `0`, because the runtime omits a token count it does not
 * know (`usage_attributes`) and a zero here would be summed as a real one.
 */
export interface AttemptFacts {
  attempt: AttemptSummary;
  /** False when no span in this trace carries the attempt's activation span ID. */
  present: boolean;
  spans: number;
  events: EventRecord[];
  llmCalls: number;
  toolCalls: number;
  intents: number;
  errors: number;
  suspends: number;
  models: string[];
  tools: string[];
  reasons: string[];
  inputTokens: number | null;
  outputTokens: number | null;
  cacheHits: number | null;
  /** ACTIVATION_START → ACTIVATION_END. Two clock reads, so a real measurement. */
  wallMs: number | null;
  /**
   * The attempt's end precedes its start.
   *
   * Not a duration, and not renderable as one. It happens when a resumed
   * attempt's `ACTIVATION_END` has not been ingested and the rollup is still
   * carrying the initial attempt's — the UI says so rather than printing a
   * negative number.
   */
  endBeforeStart: boolean;
}

function sumAttribute(events: readonly EventRecord[], name: string): number | null {
  let total: number | null = null;
  for (const event of events) {
    const raw = event.attributes[name];
    if (raw === undefined) continue;
    const value = Number(raw);
    if (!Number.isFinite(value)) continue;
    total = (total ?? 0) + value;
  }
  return total;
}

function distinct(values: readonly (string | undefined)[]): string[] {
  return Array.from(new Set(values.filter((value): value is string => Boolean(value)))).sort();
}

export function attemptFacts(
  attempt: AttemptSummary,
  spans: readonly MergedSpan[],
  owner: ReadonlyMap<string, string>,
): AttemptFacts {
  const owned = spans.filter((span) => owner.get(span.span_id) === attempt.span_id);
  const events = flattenEvents(owned);
  const ofType = (type: string) => events.filter((event) => event.event_type === type);
  const llm = ofType('LLM_CALL');

  const cacheEvents = events.filter((event) => event.attributes[ATTR.cacheHit] !== undefined);
  const endBeforeStart = attempt.end_ms !== null && attempt.end_ms < attempt.start_ms;

  return {
    attempt,
    present: owned.length > 0,
    spans: owned.length,
    events,
    llmCalls: llm.length,
    toolCalls: ofType('TOOL_CALL').length,
    intents: ofType('INTENT_EMITTED').length,
    errors: ofType('ERROR').length,
    suspends: ofType('SUSPENDED').length,
    models: distinct(llm.map((event) => event.attributes[ATTR.requestModel])),
    tools: distinct(events.map((event) => event.attributes[ATTR.toolName])),
    reasons: distinct(events.map((event) => event.attributes[ATTR.reason])),
    inputTokens: sumAttribute(events, ATTR.inputTokens),
    outputTokens: sumAttribute(events, ATTR.outputTokens),
    cacheHits:
      cacheEvents.length === 0
        ? null
        : cacheEvents.filter((event) => event.attributes[ATTR.cacheHit] === 'true').length,
    wallMs: attempt.end_ms === null || endBeforeStart ? null : attempt.end_ms - attempt.start_ms,
    endBeforeStart,
  };
}

/**
 * Read a role or an event type as a label.
 *
 * One function for both because they are one vocabulary: `role_for_event_type`
 * returns the event type's own enum name for everything except
 * `ACTIVATION_START`/`ACTIVATION_END`, which collapse to `activation`.
 */
export function roleLabel(role: string): string {
  switch (role) {
    case ROLE_ACTIVATION:
      return 'Activation';
    case 'timer':
      return 'Timer';
    case 'LLM_CALL':
      return 'LLM call';
    case 'TOOL_CALL':
      return 'Tool call';
    case 'INTENT_EMITTED':
      return 'Intent emitted';
    case 'SUSPENDED':
      return 'Suspended';
    case 'ERROR':
      return 'Error';
    case 'ACTIVATION_START':
      return 'Activation start';
    case 'ACTIVATION_END':
      return 'Activation end';
    default: {
      const lower = role.toLowerCase().replace(/_/g, ' ');
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    }
  }
}

/** A 1-based position, zero-padded so a column of them stays a column. */
export function ordinal(position: number): string {
  return String(position).padStart(2, '0');
}
