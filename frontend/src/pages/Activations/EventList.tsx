/**
 * Every trace event the activation produced, with its complete attribute map.
 *
 * Expandable rather than paged or truncated: the attribute map is the record.
 * A viewer that shows six "interesting" attributes and hides the rest is exactly
 * the tool you cannot use at the moment you need it, because the attribute you
 * came for is always one of the hidden ones.
 *
 * The clock stamp is shown as recorded and never differenced. Every event in one
 * attempt carries the same injected activation clock read, so the gap between
 * two of them inside an attempt is structurally zero — which is why this table
 * has no duration column and no elapsed column.
 */

import { useMemo, useState } from 'react';

import { Chip, CopyableId, KeyValueGrid, Select } from '@/components/ui';
import type { EventRecord, Provenance } from '@/lib/api-types';
import { formatCount, shortId } from '@/lib/format';

import { formatStamp, humanizeEventLabel } from './trace-attrs';

const PROVENANCE_TONE: Record<Provenance, 'neutral' | 'warn'> = {
  native: 'neutral',
  otlp: 'warn',
  kafka: 'neutral',
  bigquery: 'neutral',
  bundle: 'neutral',
};

function AttributeMap({ event }: { event: EventRecord }) {
  const keys = Object.keys(event.attributes).sort();

  return (
    <div className="ev__body">
      <KeyValueGrid
        compact
        entries={[
          {
            key: '__span',
            label: 'Span',
            value: <CopyableId value={event.span_id} display={event.span_id} label="span id" />,
          },
          {
            key: '__parent',
            label: 'Parent span',
            value: event.parent_span_id ? (
              <CopyableId
                value={event.parent_span_id}
                display={event.parent_span_id}
                label="parent span id"
              />
            ) : (
              <span className="faint">root</span>
            ),
          },
          {
            key: '__clock',
            label: 'Clock stamp',
            // The zero-width claim is read off the record rather than assumed:
            // this UI states what the bytes say, including on the day they stop
            // saying it.
            value: (
              <>
                <span className="mono">{formatStamp(event.start_ms)}</span>{' '}
                <span className="faint">
                  {event.start_ms === event.end_ms
                    ? `start_ms ${event.start_ms} = end_ms, zero-width by design`
                    : `start_ms ${event.start_ms}, end_ms ${event.end_ms}`}
                </span>
              </>
            ),
          },
          {
            key: '__provenance',
            label: 'Ingested via',
            value: <span className="mono">{event.provenance}</span>,
          },
        ]}
      />

      {keys.length === 0 ? (
        <p className="muted ev__none">This event carries no attributes.</p>
      ) : (
        <>
          <p className="eyebrow ev__attrs-title">{keys.length} attributes</p>
          <KeyValueGrid
            compact
            entries={keys.map((key) => ({
              key,
              label: <span className="mono">{key}</span>,
              value: <span className="mono">{event.attributes[key]}</span>,
            }))}
          />
        </>
      )}
    </div>
  );
}

export function EventList({ events }: { events: EventRecord[] }) {
  const [open, setOpen] = useState<ReadonlySet<number>>(new Set());
  const [type, setType] = useState('');

  const types = useMemo(
    () => Array.from(new Set(events.map((event) => event.event_type))).sort(),
    [events],
  );

  const rows = useMemo(
    () =>
      events
        .map((event, index) => ({ event, index }))
        .filter(({ event }) => type === '' || event.event_type === type),
    [events, type],
  );

  const toggle = (index: number) =>
    setOpen((current) => {
      const next = new Set(current);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });

  const allOpen = rows.length > 0 && rows.every(({ index }) => open.has(index));

  return (
    <>
      <div className="ev__toolbar">
        <Select
          aria-label="Filter by event type"
          value={type}
          onChange={(e) => setType(e.target.value)}
          placeholder="All event types"
          options={types.map((value) => ({ value, label: humanizeEventLabel(value) }))}
        />
        <button
          type="button"
          className="linkish"
          onClick={() => setOpen(allOpen ? new Set() : new Set(rows.map((row) => row.index)))}
        >
          {allOpen ? 'Collapse all' : 'Expand all attributes'}
        </button>
        <span className="muted ev__count">
          {formatCount(rows.length)} of {formatCount(events.length)} events
        </span>
      </div>

      <ol className="ev">
        {rows.map(({ event, index }) => {
          const expanded = open.has(index);
          return (
            <li key={`${event.span_id}-${event.event_type}-${index}`} className="ev__item">
              <button
                type="button"
                className="ev__head"
                aria-expanded={expanded}
                onClick={() => toggle(index)}
              >
                <span className="ev__index">{index + 1}</span>
                <span className="ev__type">
                  <span className="ev__type-name">{humanizeEventLabel(event.event_type)}</span>
                  <span className="mono faint">{event.event_type}</span>
                </span>
                <span className="ev__meta">
                  <span className="muted">step {event.step_index}</span>
                  <span className="mono muted">{shortId(event.span_id, 6, 4)}</span>
                  <span className="mono muted">{formatStamp(event.start_ms)}</span>
                  <Chip plain tone={PROVENANCE_TONE[event.provenance]}>
                    {event.provenance}
                  </Chip>
                  <span className="muted ev__attr-count">
                    {formatCount(Object.keys(event.attributes).length)}{' '}
                    {Object.keys(event.attributes).length === 1 ? 'attribute' : 'attributes'}
                  </span>
                  <span className="ev__caret" aria-hidden="true">
                    {expanded ? '−' : '+'}
                  </span>
                </span>
              </button>
              {expanded ? <AttributeMap event={event} /> : null}
            </li>
          );
        })}
      </ol>
    </>
  );
}
