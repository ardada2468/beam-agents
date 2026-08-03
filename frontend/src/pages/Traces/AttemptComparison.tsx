/**
 * Attempt comparison across a suspend → resume.
 *
 * A trace is exactly one activation *scope* — `uuid5(entity_key, seq)` — so a
 * suspend, the effector round trip, and the resume are one trace with two
 * attempts (`observability/traces.py`, `trace_id_for`). The question someone
 * opens this for is "what did the second run do differently from the first",
 * which a single merged span tree cannot answer.
 *
 * So: one column per attempt, a row per figure, and an explicit `differs` mark
 * on every row whose values are not all equal — because scanning two columns of
 * near-identical numbers for the one that moved is exactly the task a UI should
 * be doing instead of the reader.
 *
 * Every figure is derived from the attempt's own events. Nothing is a duration
 * except the ACTIVATION_START → ACTIVATION_END wall delta, which is real: those
 * two events are stamped by two different clock reads.
 */

import type { ReactNode } from 'react';

import { Chip, CopyableId, EmptyState, StatusChip } from '@/components/ui';
import {
  EM_DASH,
  formatCount,
  formatDuration,
  formatTimestamp,
  humanizeEventType,
} from '@/lib/format';

import { ordinal, roleLabel, type AttemptFacts } from './spans';

interface MetricRow {
  key: string;
  label: string;
  compare: (facts: AttemptFacts) => string | number | null;
  render: (facts: AttemptFacts) => ReactNode;
  note?: string;
  /**
   * When to mark the row as differing.
   *
   * `identity` never marks: two attempts always have two span IDs, and saying
   * so on every trace is noise rather than a finding. `derived` marks only when
   * every attempt has events to derive from — otherwise the mark would be
   * reporting missing ingest as a behavioural difference.
   */
  mark?: 'identity' | 'derived';
}

/** A count that may not have been measured at all. `null` is never a `0`. */
function measurement(value: number | null): ReactNode {
  return value === null ? <span className="faint">{EM_DASH}</span> : formatCount(value);
}

function list(values: readonly string[]): ReactNode {
  if (values.length === 0) return <span className="faint">{EM_DASH}</span>;
  return (
    <span className="tr-cmp__chips">
      {values.map((value) => (
        <Chip key={value} plain>
          {value}
        </Chip>
      ))}
    </span>
  );
}

const ROWS: MetricRow[] = [
  {
    key: 'kind',
    label: 'Kind',
    compare: (f) => f.attempt.kind,
    render: (f) => (
      <Chip tone={f.attempt.kind === 'resume' ? 'suspended' : 'neutral'}>
        {humanizeEventType(f.attempt.kind)}
      </Chip>
    ),
  },
  {
    key: 'status',
    label: 'Status',
    compare: (f) => f.attempt.status,
    render: (f) => <StatusChip status={f.attempt.status} />,
  },
  {
    key: 'span',
    label: 'Activation span',
    compare: (f) => f.attempt.span_id,
    render: (f) => <CopyableId value={f.attempt.span_id} label="activation span id" />,
    mark: 'identity',
  },
  {
    key: 'entry',
    label: 'Entry step index',
    compare: (f) => f.attempt.entry_step_index,
    render: (f) => formatCount(f.attempt.entry_step_index),
    note: 'The step the attempt resumed at. This is what makes the two spans distinct.',
  },
  {
    key: 'started',
    label: 'Started',
    compare: (f) => f.attempt.start_ms,
    render: (f) => formatTimestamp(f.attempt.start_ms),
  },
  {
    key: 'ended',
    label: 'Ended',
    compare: (f) => f.attempt.end_ms,
    render: (f) => formatTimestamp(f.attempt.end_ms),
  },
  {
    key: 'wall',
    label: 'Wall time',
    compare: (f) => f.wallMs,
    render: (f) =>
      f.endBeforeStart ? (
        <>
          <span className="faint">{EM_DASH}</span>
          <span className="tr-cmp__cell-note">
            End precedes start — not a duration, so none is claimed.
          </span>
        </>
      ) : (
        formatDuration(f.wallMs)
      ),
    note: 'ACTIVATION_START → ACTIVATION_END. Two clock reads, so a real measurement.',
  },
  {
    key: 'spans',
    label: 'Spans',
    compare: (f) => f.spans,
    render: (f) => formatCount(f.spans),
    mark: 'derived',
  },
  {
    key: 'events',
    label: 'Events',
    compare: (f) => f.events.length,
    render: (f) => formatCount(f.events.length),
    mark: 'derived',
  },
  {
    key: 'llm',
    label: 'LLM calls',
    compare: (f) => f.llmCalls,
    render: (f) => formatCount(f.llmCalls),
    mark: 'derived',
  },
  {
    key: 'tool',
    label: 'Tool calls',
    compare: (f) => f.toolCalls,
    render: (f) => formatCount(f.toolCalls),
    mark: 'derived',
  },
  {
    key: 'intents',
    label: 'Intents emitted',
    compare: (f) => f.intents,
    render: (f) => formatCount(f.intents),
    mark: 'derived',
  },
  {
    key: 'suspends',
    label: 'Suspensions',
    compare: (f) => f.suspends,
    render: (f) => formatCount(f.suspends),
    mark: 'derived',
  },
  {
    key: 'errors',
    label: 'Errors',
    compare: (f) => f.errors,
    render: (f) => formatCount(f.errors),
    mark: 'derived',
  },
  {
    key: 'input',
    label: 'Input tokens',
    compare: (f) => f.inputTokens,
    render: (f) => measurement(f.inputTokens),
    note: 'Summed from gen_ai.usage.input_tokens. Absent when nothing decoded a response.',
    mark: 'derived',
  },
  {
    key: 'output',
    label: 'Output tokens',
    compare: (f) => f.outputTokens,
    render: (f) => measurement(f.outputTokens),
    mark: 'derived',
  },
  {
    key: 'cache',
    label: 'Cache hits',
    compare: (f) => f.cacheHits,
    render: (f) => measurement(f.cacheHits),
    note: 'Counted from beam_agents.cache_hit. Absent when no call recorded the attribute.',
    mark: 'derived',
  },
  {
    key: 'models',
    label: 'Models',
    compare: (f) => f.models.join(','),
    render: (f) => list(f.models),
    mark: 'derived',
  },
  {
    key: 'tools',
    label: 'Tools',
    compare: (f) => f.tools.join(','),
    render: (f) => list(f.tools),
    mark: 'derived',
  },
  {
    key: 'reasons',
    label: 'Error reasons',
    compare: (f) => f.reasons.join(','),
    render: (f) => list(f.reasons),
    mark: 'derived',
  },
];

export function AttemptComparison({ facts }: { facts: readonly AttemptFacts[] }) {
  if (facts.length === 0) {
    return (
      <EmptyState
        title="No attempts recorded"
        body="An attempt is an ACTIVATION_START event. A trace ingested over the OTLP path carries none, because OTLP has no representation for two events on one span — point the pipeline at console:// to record them."
      />
    );
  }

  const missing = facts.filter((f) => !f.present);

  return (
    <div className="stack">
      {facts.length === 1 ? (
        <p className="muted tr-note">
          This trace has one attempt. A second column appears when an activation suspends and
          resumes — the resume runs under the same <span className="mono">(entity_key, seq)</span>,
          so it lands in this same trace rather than starting a new one.
        </p>
      ) : (
        <p className="muted tr-note">
          {formatCount(facts.length)} attempts, one trace. Each column is one pass through the
          agent; a row marked <span className="tr-cmp__inline-chip">differs</span> is one where they
          did not do the same thing.
        </p>
      )}

      {missing.length > 0 ? (
        <p className="muted tr-note">
          {missing.length === 1
            ? 'One attempt has'
            : `${formatCount(missing.length)} attempts have`}{' '}
          no span in this trace carrying its activation span ID, so no events are attributed to it.
          That happens when the attempt&rsquo;s events have not been ingested yet, or arrived over a
          path that drops <span className="mono">ACTIVATION_START</span>.
        </p>
      ) : null}

      <div className="tr-cmp__scroll">
        <table className="tr-cmp">
          <caption className="visually-hidden">Attempt comparison</caption>
          <thead>
            <tr>
              <th scope="col" className="tr-cmp__corner">
                Figure
              </th>
              {facts.map((f, index) => (
                <th key={f.attempt.span_id} scope="col" className="tr-cmp__head">
                  <span className="tr-cmp__head-title">Attempt {index + 1}</span>
                  <span className="tr-cmp__head-sub muted">
                    {humanizeEventType(f.attempt.kind)} · entry step{' '}
                    {formatCount(f.attempt.entry_step_index)}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {ROWS.map((row) => {
              const comparable =
                facts.length > 1 &&
                row.mark !== 'identity' &&
                (row.mark !== 'derived' || facts.every((f) => f.present));
              const differs =
                comparable && new Set(facts.map((f) => String(row.compare(f)))).size > 1;
              return (
                <tr key={row.key} className={differs ? 'tr-cmp__row--differs' : ''}>
                  <th scope="row" className="tr-cmp__key">
                    <span className="tr-cmp__key-label">
                      {row.label}
                      {differs ? <Chip tone="info">differs</Chip> : null}
                    </span>
                    {row.note ? <span className="tr-cmp__note">{row.note}</span> : null}
                  </th>
                  {facts.map((f) => (
                    <td key={f.attempt.span_id} className="tr-cmp__cell">
                      {/* An attempt with no ingested events has not measured a
                          zero — it has measured nothing. Every derived figure
                          on such a column is an em dash, never a 0. */}
                      {row.mark === 'derived' && !f.present ? (
                        <span className="faint">{EM_DASH}</span>
                      ) : (
                        row.render(f)
                      )}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div>
        <p className="eyebrow tr-seq__heading">Recorded event sequence</p>
        <p className="muted tr-note">
          Order only. Nothing here is scaled by elapsed time, because no span carries one.
        </p>
        <div className="tr-seq">
          {facts.map((f, index) => (
            <div key={f.attempt.span_id} className="tr-seq__column">
              <p className="tr-seq__title">
                Attempt {index + 1}
                <span className="muted"> · {formatCount(f.events.length)} events</span>
              </p>
              {f.events.length === 0 ? (
                <p className="faint">No events attributed to this attempt.</p>
              ) : (
                <ol className="tr-seq__list">
                  {f.events.map((event, position) => (
                    <li key={`${event.span_id}:${event.event_type}`} className="tr-seq__item">
                      <span className="tr-seq__ordinal mono faint">{ordinal(position + 1)}</span>
                      <span className="tr-seq__label">{roleLabel(event.event_type)}</span>
                      <span className="muted">step {formatCount(event.step_index)}</span>
                    </li>
                  ))}
                </ol>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
