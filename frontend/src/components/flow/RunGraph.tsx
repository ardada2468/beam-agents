/**
 * One run, drawn as the sequence of steps the agent actually took.
 *
 * The span tree already on these pages answers "what is nested under what",
 * which is the right question when you suspect the *shape* of a run. It is the
 * wrong question when you want to know what the agent did: a reader has to
 * reconstruct the order by reading indentation, and the one thing every step
 * has — a position in the sequence — is the thing the tree does not draw.
 *
 * So this renders the same spans as a flow: admitted, then each model call,
 * tool call, staged intent, suspension and error in recorded order, laned by
 * attempt. A suspend and its resume are one activation with two attempts, so
 * they are two lanes joined by the effector — which is the picture that makes
 * a HITL cycle legible in one look.
 *
 * **An ordered list, not a drawing.** The steps are `<li>`s in an `<ol>` and
 * the arrows are borders. That is not a shortcut around SVG: the content *is*
 * an ordered sequence of variable length, so the list wraps at any width, reads
 * correctly to a screen reader in one pass, and needs no viewBox to stay legible
 * on a phone. The lifecycle diagram is SVG because its shape is fixed and its
 * edges loop backwards; this one is neither.
 *
 * **Nothing here is timed.** Spans are zero-width by construction
 * (`start_ms === end_ms`, traces D7), so no node carries a duration and no
 * connector is scaled by one. Position encodes recorded order and nothing else,
 * exactly as in the span tree beside it.
 */

import { StatusChip } from '@/components/ui';
import type { AttemptSummary } from '@/lib/api-types';
import {
  ATTR,
  ROLE_ACTIVATION,
  buildForest,
  ordinal,
  roleLabel,
  type MergedSpan,
  type SpanTreeNode,
} from '@/pages/Traces/spans';

import './run-graph.css';

/* -- Tone: the same vocabulary the lifecycle diagram uses ------------------- */

type Tone = 'neutral' | 'model' | 'tool' | 'intent' | 'suspended' | 'error';

function toneFor(role: string): Tone {
  switch (role) {
    case 'LLM_CALL':
      return 'model';
    case 'TOOL_CALL':
      return 'tool';
    case 'INTENT_EMITTED':
      return 'intent';
    case 'SUSPENDED':
      return 'suspended';
    case 'ERROR':
      return 'error';
    default:
      return 'neutral';
  }
}

/**
 * The one line of detail a step is worth, taken from its recorded attributes.
 *
 * Only attributes the runtime actually writes (`ATTR`, copied from
 * `observability/traces.py`) — a step with none returns nothing and renders
 * without a detail line rather than with a placeholder.
 */
function detailFor(span: MergedSpan): string | null {
  const attrs = span.events.reduce<Record<string, string>>(
    (all, event) => ({ ...all, ...event.attributes }),
    {},
  );

  const parts: string[] = [];
  const model = attrs[ATTR.requestModel];
  const tool = attrs[ATTR.toolName];
  const reason = attrs[ATTR.reason];
  const input = attrs[ATTR.inputTokens];
  const output = attrs[ATTR.outputTokens];

  if (model) parts.push(model);
  if (tool) parts.push(tool);
  if (reason) parts.push(reason);
  if (input !== undefined && output !== undefined) parts.push(`${input} in · ${output} out`);
  if (attrs[ATTR.cacheHit] === 'true') parts.push('cache hit');

  return parts.length > 0 ? parts.join(' · ') : null;
}

/* -- Laning ---------------------------------------------------------------- */

interface Lane {
  attempt: AttemptSummary | null;
  position: number;
  steps: MergedSpan[];
}

/**
 * Group the spans into one lane per attempt, in recorded order.
 *
 * The activation spans themselves are the lanes, so they are not also steps
 * inside one — an "Activation" node at the head of every lane would repeat the
 * lane's own heading. Anything the walk cannot attribute to an attempt lands in
 * a trailing lane with no heading rather than being dropped: on the OTLP path
 * there is no `ACTIVATION_START` at all, so *every* span is unattributed, and a
 * view that silently rendered nothing there would be the worst possible answer.
 */
function laneSpans(forest: SpanTreeNode[], attempts: readonly AttemptSummary[]): Lane[] {
  const attemptIds = new Set(attempts.map((attempt) => attempt.span_id));
  const owner = new Map<string, string>();

  const walk = (node: SpanTreeNode, current: string | null) => {
    const next = attemptIds.has(node.span.span_id) ? node.span.span_id : current;
    if (next !== null) owner.set(node.span.span_id, next);
    for (const child of node.children) walk(child, next);
  };
  for (const root of forest) walk(root, null);

  const flat: MergedSpan[] = [];
  const collect = (node: SpanTreeNode) => {
    flat.push(node.span);
    for (const child of node.children) collect(child);
  };
  for (const root of forest) collect(root);

  const lanes: Lane[] = attempts.map((attempt, index) => ({
    attempt,
    position: index + 1,
    steps: flat.filter(
      (span) =>
        owner.get(span.span_id) === attempt.span_id &&
        span.span_id !== attempt.span_id &&
        span.role !== ROLE_ACTIVATION,
    ),
  }));

  const orphans = flat.filter((span) => !owner.has(span.span_id) && span.role !== ROLE_ACTIVATION);
  if (orphans.length > 0) {
    lanes.push({ attempt: null, position: lanes.length + 1, steps: orphans });
  }

  /*
   * An attempt with no steps keeps its lane.
   *
   * A resume that recorded nothing but its own `ACTIVATION_START` and
   * `ACTIVATION_END` has an empty lane, and dropping it would erase the single
   * most interesting fact about the run — that it suspended, went to a human,
   * and came back. The lane renders with a line saying nothing was recorded,
   * which is the accurate statement. Only the orphan lane is conditional,
   * because it has no attempt to stand for.
   */
  return lanes;
}

/* -- Component ------------------------------------------------------------- */

export interface RunGraphProps {
  /**
   * Spans with their duplicate `span_id` entries already folded together.
   *
   * Merged rather than raw, because the trace page has merged them before it
   * can render anything else and re-merging here would do that work twice on
   * the page most likely to have a large trace open.
   */
  spans: readonly MergedSpan[];
  attempts: readonly AttemptSummary[];
}

export default function RunGraph({ spans, attempts }: RunGraphProps) {
  const forest = buildForest(spans);
  const lanes = laneSpans(forest, attempts);

  if (lanes.length === 0) {
    return (
      <p className="run-graph__empty">
        No steps were recorded for this run. An activation that was admitted and dead-lettered
        before its first model call has an error record and no steps.
      </p>
    );
  }

  return (
    <div className="run-graph">
      {lanes.map((lane) => (
        <section className="run-lane" key={lane.attempt?.span_id ?? `orphans-${lane.position}`}>
          <header className="run-lane__head">
            <span className="run-lane__ordinal">{ordinal(lane.position)}</span>
            <span className="run-lane__title">
              {lane.attempt === null
                ? 'Unattributed steps'
                : `Attempt ${lane.position} · ${lane.attempt.kind === 'resume' ? 'resume' : 'start'}`}
            </span>
            <span className="run-lane__meta">
              {lane.steps.length} {lane.steps.length === 1 ? 'step' : 'steps'}
            </span>
            {lane.attempt ? <StatusChip status={lane.attempt.status} /> : null}
          </header>

          {lane.steps.length === 0 ? (
            <p className="run-lane__empty">
              This attempt recorded its start and end and nothing between them — no model call, no
              tool, no intent staged.
            </p>
          ) : null}

          <ol className="run-steps">
            {lane.steps.map((span) => {
              const detail = detailFor(span);
              return (
                <li className={`run-step run-step--${toneFor(span.role)}`} key={span.span_id}>
                  <span className="run-step__role">{roleLabel(span.role)}</span>
                  <span className="run-step__step">step {span.step_index}</span>
                  {detail ? <span className="run-step__detail">{detail}</span> : null}
                </li>
              );
            })}
          </ol>
        </section>
      ))}
    </div>
  );
}
