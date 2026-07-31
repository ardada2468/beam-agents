/**
 * The attribute vocabulary a trace event can carry, mirrored from
 * `observability/traces.py`, plus the readers that turn it into display values.
 *
 * Two rules hold everywhere in this file:
 *
 * - An absent attribute reads as `null`, never as `0` or `''`. The runtime omits
 *   a usage attribute it does not know rather than writing zero (`traces.py` D4),
 *   and coercing that to a number here would launder a missing measurement into
 *   a measured one.
 * - Nothing derives a duration. Every event in one attempt carries the same
 *   injected activation clock read, so a difference between two `start_ms`
 *   values inside an attempt is structurally zero and means nothing. The only
 *   real duration is `wall_ms`, which the API computes across the
 *   `ACTIVATION_START`/`ACTIVATION_END` pair.
 */

import type { ActivationKind, EventRecord } from '@/lib/api-types';
import {
  EM_DASH,
  formatCount,
  formatTimestamp,
  humanizeEventType,
  humanizeReason,
} from '@/lib/format';

/**
 * How an activation kind reads. Shared by the list and the detail so the two
 * cannot drift; `unknown` is a real value — it means the ingest path could not
 * tell a start from a resume — and never renders as a blank.
 */
export const KIND_LABEL: Record<ActivationKind, string> = {
  start: 'Start',
  resume: 'Resume',
  unknown: 'Unknown',
};

/** Attribute keys, in the same order as the constants they mirror. */
export const ATTR = {
  operationName: 'gen_ai.operation.name',
  requestModel: 'gen_ai.request.model',
  inputTokens: 'gen_ai.usage.input_tokens',
  outputTokens: 'gen_ai.usage.output_tokens',
  errorType: 'error.type',
  cacheHit: 'beam_agents.cache_hit',
  billed: 'beam_agents.billed',
  attempts: 'beam_agents.attempts',
  circuitState: 'beam_agents.circuit_state',
  reason: 'beam_agents.reason',
  failureStep: 'beam_agents.failure.step',
  failureLastEvent: 'beam_agents.failure.last_event',
  failureStagedIntents: 'beam_agents.failure.staged_intents',
  failureLlmCalls: 'beam_agents.failure.llm_calls',
  activationStatus: 'beam_agents.activation.status',
  activationKind: 'beam_agents.activation.kind',
  intentId: 'beam_agents.intent_id',
  intentKind: 'beam_agents.intent_kind',
  toolName: 'beam_agents.tool_name',
  expiresAtMs: 'beam_agents.expires_at_ms',
  deadlineMs: 'beam_agents.deadline_ms',
  adapter: 'beam_agents.adapter',
  pendingIntentIds: 'beam_agents.pending_intent_ids',
} as const;

/** An attribute's string value, or `null` when the event does not carry it. */
export function attrText(attributes: Record<string, string>, key: string): string | null {
  const value = attributes[key];
  return value === undefined || value === '' ? null : value;
}

/**
 * An attribute parsed as an integer, or `null`.
 *
 * A key that is absent, blank, or not a number yields `null` rather than `0` —
 * the whole point of the distinction the runtime is careful to preserve.
 */
export function attrNumber(attributes: Record<string, string>, key: string): number | null {
  const raw = attrText(attributes, key);
  if (raw === null) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

/** One recorded fact about a span, ready to render. `missing` values render faint. */
export interface SpanFact {
  key: string;
  label: string;
  value: string;
  missing?: boolean;
}

/**
 * The measurements one event actually recorded.
 *
 * Only attributes that are present become facts, except where an omission is
 * itself the finding — a billed LLM call with no usage attributes is worth
 * saying out loud, so it becomes an explicit "not recorded".
 */
export function spanFacts(event: EventRecord): SpanFact[] {
  const a = event.attributes;
  const facts: SpanFact[] = [];

  switch (event.event_type) {
    case 'LLM_CALL': {
      const model = attrText(a, ATTR.requestModel);
      if (model) facts.push({ key: 'model', label: 'model', value: model });

      const input = attrNumber(a, ATTR.inputTokens);
      const output = attrNumber(a, ATTR.outputTokens);
      if (input === null && output === null) {
        facts.push({ key: 'usage', label: 'tokens', value: 'not recorded', missing: true });
      } else {
        facts.push({
          key: 'usage',
          label: 'tokens',
          value: `${formatCount(input)} in · ${formatCount(output)} out`,
        });
      }

      if (attrText(a, ATTR.cacheHit) === 'true') {
        facts.push({ key: 'cache', label: '', value: 'cache hit' });
      }
      if (attrText(a, ATTR.billed) === 'false') {
        facts.push({ key: 'billed', label: '', value: 'not billed' });
      }
      const attempts = attrNumber(a, ATTR.attempts);
      if (attempts !== null && attempts > 1) {
        facts.push({ key: 'attempts', label: 'attempts', value: formatCount(attempts) });
      }
      const circuit = attrText(a, ATTR.circuitState);
      if (circuit) facts.push({ key: 'circuit', label: 'circuit', value: circuit });
      break;
    }

    case 'TOOL_CALL': {
      const tool = attrText(a, ATTR.toolName);
      facts.push({
        key: 'tool',
        label: 'tool',
        value: tool ?? 'not recorded',
        missing: tool === null,
      });
      break;
    }

    case 'INTENT_EMITTED': {
      const tool = attrText(a, ATTR.toolName);
      if (tool) facts.push({ key: 'tool', label: 'tool', value: tool });
      const kind = attrText(a, ATTR.intentKind);
      if (kind) facts.push({ key: 'kind', label: 'kind', value: kind });
      const expires = attrNumber(a, ATTR.expiresAtMs);
      if (expires !== null) {
        facts.push({ key: 'expires', label: 'expires', value: formatTimestamp(expires) });
      }
      break;
    }

    case 'SUSPENDED': {
      const adapter = attrText(a, ATTR.adapter);
      if (adapter) facts.push({ key: 'adapter', label: 'adapter', value: adapter });
      const deadline = attrNumber(a, ATTR.deadlineMs);
      if (deadline !== null) {
        facts.push({ key: 'deadline', label: 'deadline', value: formatTimestamp(deadline) });
      }
      const pending = attrText(a, ATTR.pendingIntentIds);
      if (pending) {
        facts.push({
          key: 'pending',
          label: 'pending intents',
          value: formatCount(pending.split(',').filter(Boolean).length),
        });
      }
      break;
    }

    case 'ERROR': {
      const reason = attrText(a, ATTR.reason);
      if (reason) facts.push({ key: 'reason', label: 'reason', value: humanizeReason(reason) });
      const type = attrText(a, ATTR.errorType);
      if (type) facts.push({ key: 'type', label: 'type', value: type });
      break;
    }

    case 'ACTIVATION_START':
    case 'ACTIVATION_END': {
      const kind = attrText(a, ATTR.activationKind);
      if (kind) facts.push({ key: 'kind', label: 'kind', value: kind });
      const status = attrText(a, ATTR.activationStatus);
      if (status) facts.push({ key: 'status', label: 'status', value: status });
      break;
    }

    default:
      break;
  }

  return facts;
}

/**
 * Where an activation was when it failed.
 *
 * Every field is nullable and a null is load-bearing: the runtime records these
 * only on the routes that can reach an activation context, so `null` means "the
 * route could not know", not "zero". `source` names where the values were read
 * from, because the same four scalars reach the console both as
 * `ActivationErrorRecord` columns and as `ERROR` event attributes.
 */
export interface FailurePosition {
  step: number | null;
  lastEvent: string | null;
  stagedIntents: number | null;
  llmCalls: number | null;
  source: string;
}

/** Read the failure position off an `ERROR` event's attributes. */
export function failurePositionFromEvent(event: EventRecord): FailurePosition {
  return {
    step: attrNumber(event.attributes, ATTR.failureStep),
    lastEvent: attrText(event.attributes, ATTR.failureLastEvent),
    stagedIntents: attrNumber(event.attributes, ATTR.failureStagedIntents),
    llmCalls: attrNumber(event.attributes, ATTR.failureLlmCalls),
    source: 'ERROR trace event attributes',
  };
}

/** True when the route recorded no position at all — all four scalars absent. */
export function positionIsEmpty(position: FailurePosition): boolean {
  return (
    position.step === null &&
    position.lastEvent === null &&
    position.stagedIntents === null &&
    position.llmCalls === null
  );
}

/**
 * An event type as a sentence-case label, with the initialisms kept.
 *
 * `humanizeEventType` alone gives "Llm call", which is the kind of small wrong
 * detail that makes a viewer look like it does not know the vocabulary it is
 * displaying.
 */
export function humanizeEventLabel(eventType: string): string {
  return humanizeEventType(eventType)
    .replace(/^Llm\b/, 'LLM')
    .replace(/^Hitl\b/, 'HITL')
    .replace(/^Ttl\b/, 'TTL');
}

/** A label for a span role: `activation` and the event-type enum names. */
export function humanizeRole(role: string): string {
  if (role === 'activation') return 'Activation';
  if (role === 'timer') return 'Timer';
  const lower = role.toLowerCase().replace(/_/g, ' ');
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

/** A clock stamp to the millisecond. Never a duration — see the module note. */
export function formatStamp(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return EM_DASH;
  const date = new Date(ms);
  const time = date.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  return `${time}.${String(date.getMilliseconds()).padStart(3, '0')}`;
}
