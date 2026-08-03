/**
 * The span tree.
 *
 * This is the one view in the console that a reader is most likely to misread,
 * so it is built to make the misreading impossible rather than to look like the
 * waterfall they expect.
 *
 * The runtime's spans are zero-width: `start_ms === end_ms` on every event,
 * because measuring elapsed time would mean a wall-clock read in the agent hot
 * path (`observability/traces.py`, D7). There is therefore no duration to draw.
 * A bar scaled by span width would render every span as a tick; a bar scaled by
 * anything else would invent the one quantity the runtime declines to measure.
 *
 * So nothing here has a width that means something. What is encoded is exactly
 * what is recorded: **order** — the gutter number is an event's position in the
 * trace's recorded sequence — and **nesting** — indentation and a guide rule per
 * level. Roles are told apart by their label and their weight, never by a hue.
 * The only numbers on screen are ones the records actually carry.
 *
 * Keyboard: an ARIA flat tree. Up/Down move, Left collapses or ascends, Right
 * expands or descends, Home/End jump, Enter or Space toggles. A single roving
 * tabindex, so the tree is one tab stop rather than one per row.
 */

import type { KeyboardEvent } from 'react';
import { useCallback, useMemo, useRef, useState } from 'react';

import { Chip, CopyableId, KeyValueGrid } from '@/components/ui';
import type { EventRecord } from '@/lib/api-types';
import { EM_DASH, formatCount, formatTime, formatTimestamp, shortId } from '@/lib/format';

import { compareEvents, ordinal, roleLabel, ROLE_ACTIVATION, type SpanTreeNode } from './spans';

/**
 * One rendered line.
 *
 * `span` holds child spans and is expanded to reveal them. `event` is one
 * record on a span, expanded to reveal its attribute map. `leaf` is both at
 * once: most of the runtime's child spans carry exactly one event —
 * `span_id_for(entity_key, seq, role, index)` mints a span per LLM call, per
 * tool call, per staged intent — and drawing a "LLM call" span above an "LLM
 * call" event says the same thing twice. A childless span with one event is one
 * row.
 */
interface Row {
  key: string;
  kind: 'span' | 'event' | 'leaf';
  level: number;
  /** Only meaningful for rows that carry an event: position in recorded order. */
  ordinal: number | null;
  parentKey: string | null;
  childKeys: string[];
  span: SpanTreeNode | null;
  event: EventRecord | null;
}

/** Whether a row's disclosure reveals child rows or an attribute map. */
function revealsChildren(row: Row): boolean {
  return row.kind === 'span';
}

/** Clock read to the millisecond. Sub-second precision is what makes order legible. */
function clock(ms: number): string {
  return `${formatTime(ms)}.${String(Math.abs(ms) % 1000).padStart(3, '0')}`;
}

/**
 * Pre-order the forest into rows.
 *
 * A span's own events and its child spans are interleaved by clock read rather
 * than listed events-first, so `ACTIVATION_START`, the work between, and
 * `ACTIVATION_END` fall in the order they were recorded in.
 */
function buildRows(roots: readonly SpanTreeNode[], ordinals: ReadonlyMap<string, number>): Row[] {
  const rows: Row[] = [];

  const ordinalOf = (event: EventRecord) =>
    ordinals.get(`${event.span_id}|${event.event_type}`) ?? null;

  const visit = (node: SpanTreeNode, level: number, parentKey: string | null): string => {
    const key = `span:${node.span.span_id}`;
    const only =
      node.children.length === 0 && node.span.events.length === 1 ? node.span.events[0] : undefined;

    if (only) {
      rows.push({
        key,
        kind: 'leaf',
        level,
        ordinal: ordinalOf(only),
        parentKey,
        childKeys: [],
        span: node,
        event: only,
      });
      return key;
    }

    const row: Row = {
      key,
      kind: 'span',
      level,
      ordinal: null,
      parentKey,
      childKeys: [],
      span: node,
      event: null,
    };
    rows.push(row);

    // Sorted on the *same* comparator the gutter ordinals come from, so the
    // numbers ascend down the page instead of disagreeing with the rows they
    // are printed against.
    type Child = { at: EventRecord | undefined; tie: number; render: () => string };
    const children: Child[] = [
      ...node.span.events.map((event, index) => ({
        at: event,
        tie: index,
        render: () => {
          const eventKey = `event:${event.span_id}:${event.event_type}`;
          rows.push({
            key: eventKey,
            kind: 'event',
            level: level + 1,
            ordinal: ordinalOf(event),
            parentKey: key,
            childKeys: [],
            span: null,
            event,
          });
          return eventKey;
        },
      })),
      ...node.children.map((child, index) => ({
        at: child.span.events[0],
        tie: 1000 + index,
        render: () => visit(child, level + 1, key),
      })),
    ].sort((a, b) => {
      if (a.at && b.at) return compareEvents(a.at, b.at) || a.tie - b.tie;
      if (!a.at && b.at) return 1;
      if (a.at && !b.at) return -1;
      return a.tie - b.tie;
    });

    row.childKeys = children.map((child) => child.render());
    return key;
  };

  for (const root of roots) visit(root, 0, null);
  return rows;
}

export interface SpanTreeProps {
  roots: readonly SpanTreeNode[];
  /** Span IDs that are an attempt's activation span, marked as attempt entry points. */
  attemptSpanIds: ReadonlySet<string>;
  /** Which attempt each span belongs to, for the badge on a resumed trace. */
  attemptOwner: ReadonlyMap<string, string>;
  attemptLabels: ReadonlyMap<string, string>;
}

export function SpanTree({ roots, attemptSpanIds, attemptOwner, attemptLabels }: SpanTreeProps) {
  const ordinals = useMemo(() => {
    const events = roots
      .flatMap(function collect(node: SpanTreeNode): EventRecord[] {
        return [...node.span.events, ...node.children.flatMap(collect)];
      })
      .sort(compareEvents);
    return new Map(
      events.map((event, index) => [`${event.span_id}|${event.event_type}`, index + 1]),
    );
  }, [roots]);

  const rows = useMemo(() => buildRows(roots, ordinals), [roots, ordinals]);

  // Spans open, event detail closed: the shape of the trace is the first thing
  // to read, an attribute map is the second.
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(() => new Set());
  const [openEvents, setOpenEvents] = useState<ReadonlySet<string>>(() => new Set());
  const [focusKey, setFocusKey] = useState<string | null>(rows[0]?.key ?? null);
  const container = useRef<HTMLDivElement>(null);

  const byKey = useMemo(() => new Map(rows.map((row) => [row.key, row])), [rows]);

  const visible = useMemo(() => {
    const out: Row[] = [];
    const hidden = new Set<string>();
    for (const row of rows) {
      if (row.parentKey !== null && hidden.has(row.parentKey)) {
        hidden.add(row.key);
        continue;
      }
      out.push(row);
      const isCollapsed = revealsChildren(row) ? collapsed.has(row.key) : !openEvents.has(row.key);
      if (isCollapsed) hidden.add(row.key);
    }
    return out;
  }, [rows, collapsed, openEvents]);

  const expandable = useCallback(
    (row: Row) => (revealsChildren(row) ? row.childKeys.length > 0 : true),
    [],
  );

  const isExpanded = useCallback(
    (row: Row) => (revealsChildren(row) ? !collapsed.has(row.key) : openEvents.has(row.key)),
    [collapsed, openEvents],
  );

  const setExpanded = useCallback((row: Row, open: boolean) => {
    if (revealsChildren(row)) {
      setCollapsed((current) => {
        const next = new Set(current);
        if (open) next.delete(row.key);
        else next.add(row.key);
        return next;
      });
    } else {
      setOpenEvents((current) => {
        const next = new Set(current);
        if (open) next.add(row.key);
        else next.delete(row.key);
        return next;
      });
    }
  }, []);

  const focusRow = useCallback((key: string) => {
    setFocusKey(key);
    const node = container.current?.querySelector<HTMLElement>(`[data-row-key="${key}"]`);
    node?.focus();
  }, []);

  const onKeyDown = useCallback(
    (event: KeyboardEvent<HTMLDivElement>, row: Row) => {
      const index = visible.findIndex((candidate) => candidate.key === row.key);
      const move = (target: Row | undefined) => {
        if (!target) return;
        event.preventDefault();
        focusRow(target.key);
      };

      switch (event.key) {
        case 'ArrowDown':
          move(visible[index + 1]);
          break;
        case 'ArrowUp':
          move(visible[index - 1]);
          break;
        case 'Home':
          move(visible[0]);
          break;
        case 'End':
          move(visible[visible.length - 1]);
          break;
        case 'ArrowRight':
          if (expandable(row) && !isExpanded(row)) {
            event.preventDefault();
            setExpanded(row, true);
          } else {
            move(visible[index + 1]);
          }
          break;
        case 'ArrowLeft':
          if (expandable(row) && isExpanded(row)) {
            event.preventDefault();
            setExpanded(row, false);
          } else if (row.parentKey !== null) {
            move(byKey.get(row.parentKey));
          }
          break;
        case 'Enter':
        case ' ':
          if (expandable(row)) {
            event.preventDefault();
            setExpanded(row, !isExpanded(row));
          }
          break;
        default:
          break;
      }
    },
    [visible, byKey, expandable, isExpanded, setExpanded, focusRow],
  );

  const allSpanKeys = useMemo(
    () => rows.filter((row) => row.kind === 'span').map((row) => row.key),
    [rows],
  );

  // The roving tabindex has to sit on a row that is actually on screen. If the
  // last-focused row got collapsed away, falling back to `rows` rather than
  // `visible` would put the tree's only tab stop on a hidden element and take
  // the whole tree out of the tab order.
  const activeKey =
    focusKey !== null && visible.some((row) => row.key === focusKey)
      ? focusKey
      : (visible[0]?.key ?? null);

  return (
    <div className="tr-tree">
      <div className="tr-tree__actions">
        <button
          type="button"
          className="tr-tree__action"
          onClick={() => setCollapsed(new Set())}
          disabled={collapsed.size === 0}
        >
          Expand all spans
        </button>
        <button
          type="button"
          className="tr-tree__action"
          onClick={() => setCollapsed(new Set(allSpanKeys))}
          disabled={collapsed.size === allSpanKeys.length}
        >
          Collapse all spans
        </button>
        <button
          type="button"
          className="tr-tree__action"
          onClick={() => setOpenEvents(new Set())}
          disabled={openEvents.size === 0}
        >
          Close {openEvents.size === 1 ? 'attribute map' : 'attribute maps'}
        </button>
      </div>

      <div className="tr-tree__scroll">
        <div
          className="tr-tree__rows"
          role="tree"
          aria-label="Span tree"
          aria-multiselectable={false}
          ref={container}
        >
          {visible.map((row) => {
            const open = isExpanded(row);
            const canExpand = expandable(row);
            const spanId = row.span?.span.span_id;
            const attemptLabel =
              spanId === undefined ? undefined : attemptLabels.get(attemptOwner.get(spanId) ?? '');
            const isAttemptRoot = spanId !== undefined && attemptSpanIds.has(spanId);

            return (
              <div key={row.key} className="tr-tree__item">
                <div
                  role="treeitem"
                  data-row-key={row.key}
                  aria-level={row.level + 1}
                  aria-expanded={canExpand ? open : undefined}
                  aria-selected={row.key === activeKey}
                  tabIndex={row.key === activeKey ? 0 : -1}
                  className={[
                    'tr-row',
                    row.kind === 'event' ? 'tr-row--event' : 'tr-row--span',
                    row.span?.span.role === ROLE_ACTIVATION ? 'tr-row--activation' : '',
                    row.event?.event_type === 'ERROR' ? 'tr-row--error' : '',
                  ]
                    .filter(Boolean)
                    .join(' ')}
                  onClick={() => {
                    setFocusKey(row.key);
                    if (canExpand) setExpanded(row, !open);
                  }}
                  onKeyDown={(event) => onKeyDown(event, row)}
                >
                  <span className="tr-row__ordinal">
                    {row.ordinal === null ? '' : ordinal(row.ordinal)}
                  </span>

                  <span className="tr-row__rails" aria-hidden="true">
                    {Array.from({ length: row.level }, (_, level) => (
                      <span key={level} className="tr-row__rail" />
                    ))}
                  </span>

                  <span
                    className={`tr-row__twisty${canExpand ? '' : ' tr-row__twisty--leaf'}`}
                    aria-hidden="true"
                  >
                    {canExpand ? (open ? '▾' : '▸') : '·'}
                  </span>

                  {row.kind === 'span' && row.span ? (
                    <SpanRowBody
                      node={row.span}
                      isAttemptRoot={isAttemptRoot}
                      attemptLabel={attemptLabel}
                    />
                  ) : row.event ? (
                    <EventRowBody
                      event={row.event}
                      role={row.kind === 'leaf' ? (row.span?.span.role ?? null) : null}
                      isAttemptRoot={isAttemptRoot}
                      attemptLabel={attemptLabel}
                    />
                  ) : null}
                </div>

                {row.kind !== 'span' && row.event && open ? (
                  <div
                    className="tr-detail"
                    style={{ marginInlineStart: `calc(${row.level + 1} * var(--tr-indent))` }}
                  >
                    <EventDetail event={row.event} />
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function SpanRowBody({
  node,
  isAttemptRoot,
  attemptLabel,
}: {
  node: SpanTreeNode;
  isAttemptRoot: boolean;
  attemptLabel: string | undefined;
}) {
  const { span } = node;
  const childSpans = node.children.length;
  // The span ID is plain text here rather than a `CopyableId`: a row is a
  // `treeitem` whose whole surface toggles disclosure, and a button inside it
  // would be both a nested interactive control and a click target that does
  // something different from the row it sits in. The full ID is click-to-copy
  // one keystroke away, in the expanded detail.
  return (
    <span className="tr-row__body">
      <span className="tr-row__label">{roleLabel(span.role)}</span>
      <span className="tr-row__id mono muted">{shortId(span.span_id, 6, 4)}</span>
      {isAttemptRoot && attemptLabel ? <Chip tone="info">{attemptLabel}</Chip> : null}
      <span className="tr-row__meta muted">
        step {formatCount(span.step_index)} · {formatCount(span.events.length)}{' '}
        {span.events.length === 1 ? 'event' : 'events'}
        {childSpans > 0 ? ` · ${formatCount(childSpans)} child` : ''}
        {childSpans > 1 ? 'ren' : ''}
      </span>
    </span>
  );
}

/**
 * One event row.
 *
 * `role` is set when this row is also its span — the collapsed leaf case — in
 * which case the row carries the span's identity as well and reads at span
 * weight. An event that shares a span with others reads one step quieter.
 */
function EventRowBody({
  event,
  role,
  isAttemptRoot,
  attemptLabel,
}: {
  event: EventRecord;
  role: string | null;
  isAttemptRoot: boolean;
  attemptLabel: string | undefined;
}) {
  const attributeCount = Object.keys(event.attributes).length;
  return (
    <span className="tr-row__body">
      <span className={`tr-row__label${role === null ? ' tr-row__label--event' : ''}`}>
        {roleLabel(role ?? event.event_type)}
      </span>
      {role === null ? null : (
        <span className="tr-row__id mono muted">{shortId(event.span_id, 6, 4)}</span>
      )}
      {isAttemptRoot && attemptLabel ? <Chip tone="info">{attemptLabel}</Chip> : null}
      <span className="tr-row__clock mono muted">{clock(event.start_ms)}</span>
      <span className="tr-row__meta muted">
        step {formatCount(event.step_index)} · {formatCount(attributeCount)}{' '}
        {attributeCount === 1 ? 'attribute' : 'attributes'}
      </span>
    </span>
  );
}

function EventDetail({ event }: { event: EventRecord }) {
  const attributes = Object.entries(event.attributes).sort(([a], [b]) => a.localeCompare(b));
  return (
    <>
      <KeyValueGrid
        compact
        entries={[
          {
            key: 'type',
            label: 'Event type',
            value: <span className="mono">{event.event_type}</span>,
          },
          {
            key: 'span',
            label: 'Span ID',
            value: <CopyableId value={event.span_id} label="span id" />,
          },
          {
            key: 'parent',
            label: 'Parent span',
            value: event.parent_span_id ? (
              <CopyableId value={event.parent_span_id} label="parent span id" />
            ) : (
              <span className="faint">{EM_DASH} root</span>
            ),
          },
          { key: 'step', label: 'Step index', value: formatCount(event.step_index) },
          {
            key: 'clock',
            label: 'Clock read',
            value: (
              <>
                {formatTimestamp(event.start_ms)}{' '}
                {/* The raw integer, unseparated, because it is a record value
                    someone is about to paste into a query. */}
                <span className="faint mono">({event.start_ms} ms epoch)</span>
              </>
            ),
          },
          {
            key: 'width',
            label: 'Span width',
            value: (
              <span className="muted">
                Not measured. <span className="mono">start_ms</span> and{' '}
                <span className="mono">end_ms</span> are the same clock read.
              </span>
            ),
          },
          {
            key: 'provenance',
            label: 'Provenance',
            value: <Chip plain>{event.provenance}</Chip>,
          },
        ]}
      />

      <p className="eyebrow tr-detail__heading">Attributes ({formatCount(attributes.length)})</p>
      {attributes.length === 0 ? (
        <p className="muted">
          This event carries no attributes. The runtime omits what it does not know rather than
          writing a zero.
        </p>
      ) : (
        <table className="tr-attrs">
          <tbody>
            {attributes.map(([name, value]) => (
              <tr key={name}>
                <th scope="row" className="tr-attrs__key mono">
                  {name}
                </th>
                <td className="tr-attrs__value mono">{value === '' ? EM_DASH : value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
