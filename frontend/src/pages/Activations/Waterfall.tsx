/**
 * The activation waterfall — and the one component in this page that is easiest
 * to get wrong.
 *
 * The runtime's spans are zero-width: `start_ms === end_ms` on every event,
 * because measuring elapsed time would need a wall-clock read in the agent's hot
 * path (`add-trace-events` D7). There is deliberately no duration field on
 * `SpanNode`. So this draws **order and nesting only**: every rule is the same
 * length, and no visual extent anywhere is scaled by anything.
 *
 * Roles are told apart by their label and by one step of tonal weight, not by a
 * palette — a run of six differently-coloured bars would read as six magnitudes.
 *
 * Real measurements do appear here, as explicit numbers attributed to the
 * attribute that carried them: token counts, cache hits, retry attempts, tool
 * names, deadlines. Where a span recorded no measurement, the row says so.
 */

import type { SpanNode } from '@/lib/api-types';

import { humanizeEventLabel, humanizeRole, spanFacts } from './trace-attrs';

/** How one span row is labelled: its single event, or its role plus a count. */
function labelFor(span: SpanNode): string {
  const first = span.events[0];
  if (span.events.length === 1 && first) return humanizeEventLabel(first.event_type);
  return humanizeRole(span.role);
}

export function Waterfall({ spans }: { spans: SpanNode[] }) {
  const ordered = [...spans].sort((a, b) => a.order - b.order);

  return (
    <div className="scroll-x">
      <ol className="wf">
        {ordered.map((span, index) => {
          const facts = span.events.flatMap((event) => spanFacts(event));
          const types = span.events.map((event) => event.event_type);
          return (
            <li className="wf__row" key={`${span.span_id}-${span.order}-${index}`}>
              <span className="wf__index">{index + 1}</span>

              <span className="wf__node">
                {Array.from({ length: Math.max(0, span.depth) }, (_, level) => (
                  <span key={level} className="wf__guide" aria-hidden="true" />
                ))}
                <span
                  className={`wf__rule${span.depth === 0 ? ' wf__rule--root' : ''}`}
                  aria-hidden="true"
                />
                <span className="wf__label">{labelFor(span)}</span>
                <span className="wf__step">step {span.step_index}</span>
                {span.events.length > 1 ? (
                  <span className="wf__step">{types.join(' · ')}</span>
                ) : null}
              </span>

              <span className="wf__facts">
                {facts.length === 0 ? (
                  <span className="wf__fact faint">no measurement recorded</span>
                ) : (
                  facts.map((fact, factIndex) => (
                    <span
                      // Indexed: a span carrying two events can record the same
                      // fact key twice (an activation span holds both its start
                      // and its end).
                      key={`${span.order}-${factIndex}-${fact.key}`}
                      className={`wf__fact${fact.missing ? ' faint' : ''}`}
                    >
                      {fact.label ? <span className="wf__fact-label">{fact.label} </span> : null}
                      <span className="wf__fact-value">{fact.value}</span>
                    </span>
                  ))
                )}
              </span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}
