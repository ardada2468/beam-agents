/**
 * One trace: its span tree, its attempts, and its literal records.
 *
 * The page is built around one fact and one consequence.
 *
 * The fact: a trace is `uuid5(entity_key, seq)` — an activation *scope*, not a
 * single pass through the agent. A resumed activation recomputes the same trace
 * ID with nothing carried on the wire (`traces.py`, `trace_id_for`), so a
 * suspend → effector → resume cycle is one trace with two attempts. The Attempts
 * tab exists so those two can be compared rather than merged into one blur.
 *
 * The consequence: the runtime's spans are zero-width, so there is no waterfall
 * to draw. See `SpanTree.tsx`, which is where that decision is enforced.
 *
 * `TraceDetail` gives the summary, the attempt list, and the trace's root spans;
 * the events recorded against the rest of the tree live on the activation
 * endpoint for the same `(entity_key, seq)` — the one identity a trace maps to.
 * Both are fetched and their span sets merged by `span_id`, so this page shows
 * the whole tree with whichever endpoint is richer winning, rather than showing
 * only the roots.
 */

import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { Link, useParams } from 'wouter';

import {
  Chip,
  CodeBlock,
  CopyableId,
  EmptyState,
  Select,
  Skeleton,
  SkeletonRows,
  StatTile,
  StatusChip,
  Tabs,
  TileRow,
} from '@/components/ui';
import RunGraph from '@/components/flow/RunGraph';
import { api, queryKeys } from '@/lib/api';
import type { SpanNode } from '@/lib/api-types';
import { formatCount, formatDuration, formatEntityKey, formatTimestamp } from '@/lib/format';

import { AttemptComparison } from './AttemptComparison';
import { SpanTree } from './SpanTree';
import { attemptFacts, attributeToAttempts, buildForest, flattenEvents, mergeSpans } from './spans';
import './traces.css';

type TabKey = 'spans' | 'flow' | 'attempts' | 'raw';
type RawView = 'events' | 'spans' | 'response';

const RAW_OPTIONS = [
  { value: 'events', label: 'Events' },
  { value: 'spans', label: 'Spans' },
  { value: 'response', label: 'Full API response' },
];

export default function TraceDetailPage() {
  const params = useParams<{ traceId: string }>();
  const traceId = params.traceId ?? '';
  const [tab, setTab] = useState<TabKey>('spans');
  const [rawView, setRawView] = useState<RawView>('events');

  const traceQuery = useQuery({
    queryKey: queryKeys.trace(traceId),
    queryFn: () => api.trace(traceId),
    enabled: traceId !== '',
  });

  const summary = traceQuery.data?.summary;
  const entityKey = summary?.entity_key;
  const seq = summary?.seq;

  // The activation for the same scope carries every span in the trace, not just
  // its roots. Fetched second because its identity comes out of the first.
  const activationQuery = useQuery({
    queryKey: queryKeys.activation(entityKey ?? '', seq ?? -1),
    queryFn: () => api.activation(entityKey ?? '', seq ?? -1),
    enabled: entityKey !== undefined && seq !== undefined,
  });

  const spans = useMemo(() => {
    const combined: SpanNode[] = [
      ...(traceQuery.data?.roots ?? []),
      ...(activationQuery.data?.spans ?? []),
    ];
    return mergeSpans(combined);
  }, [traceQuery.data, activationQuery.data]);

  const forest = useMemo(() => buildForest(spans), [spans]);
  const events = useMemo(() => flattenEvents(spans), [spans]);

  const attempts = useMemo(() => traceQuery.data?.attempts ?? [], [traceQuery.data]);
  const attemptSpanIds = useMemo(
    () => new Set(attempts.map((attempt) => attempt.span_id)),
    [attempts],
  );
  const attemptOwner = useMemo(
    () => attributeToAttempts(forest, attemptSpanIds),
    [forest, attemptSpanIds],
  );
  const attemptLabels = useMemo(
    () => new Map(attempts.map((attempt, index) => [attempt.span_id, `Attempt ${index + 1}`])),
    [attempts],
  );
  const facts = useMemo(
    () => attempts.map((attempt) => attemptFacts(attempt, spans, attemptOwner)),
    [attempts, spans, attemptOwner],
  );

  if (traceQuery.isPending) {
    return (
      <div className="page">
        <div className="page-title">
          <h1>Trace</h1>
        </div>
        <Skeleton width="320px" height="20px" />
        <div className="panel">
          <SkeletonRows rows={10} columns={4} />
        </div>
      </div>
    );
  }

  if (traceQuery.isError || !traceQuery.data || !summary) {
    return (
      <div className="page">
        <div className="page-title">
          <h1>Trace</h1>
        </div>
        <div className="panel">
          <EmptyState
            title="That trace is not in the store"
            body={
              <>
                <p>
                  Nothing is recorded for <span className="mono">{traceId || '(no id)'}</span>.
                  Trace IDs are derived, not random —{' '}
                  <span className="mono">uuid5(entity_key, seq)</span> — so an ID from a run the
                  console never ingested will simply not be here.
                </p>
                <p style={{ marginTop: 'var(--space-2)' }}>
                  Retention may also have dropped it: the store keeps a time window, not history.
                </p>
              </>
            }
            action={
              <Link href="/traces" className="btn">
                Back to all traces
              </Link>
            }
          />
        </div>
      </div>
    );
  }

  const wallMs = summary.ended_ms === null ? null : summary.ended_ms - summary.started_ms;
  const activationHref = `/activations/${encodeURIComponent(summary.entity_key)}/${summary.seq}`;
  const spansLoading = activationQuery.isPending;

  return (
    <div className="page">
      <div className="page-title">
        <h1>Trace</h1>
        <div className="row tr-head__actions">
          <StatusChip status={summary.status} />
          <Link href={activationHref} className="btn">
            Open activation
          </Link>
        </div>
      </div>

      <div className="tr-head__ids">
        <span className="muted">Trace ID</span>
        <CopyableId value={summary.trace_id} label="trace id" />
        <span className="tr-head__sep" aria-hidden="true" />
        <span className="muted">Entity key</span>
        <CopyableId
          value={summary.entity_key}
          display={formatEntityKey(summary.entity_key)}
          label="entity key"
        />
        <span className="tr-head__sep" aria-hidden="true" />
        <span className="muted">Seq</span>
        <span className="mono">{formatCount(summary.seq)}</span>
      </div>

      <TileRow>
        <StatTile label="Events" value={formatCount(summary.events)} meta="Recorded trace events" />
        <StatTile
          label="Spans"
          value={formatCount(summary.spans)}
          meta="Zero-width — order and nesting only"
        />
        <StatTile
          label="Attempts"
          value={formatCount(attempts.length)}
          meta={attempts.length > 1 ? 'Suspended and resumed' : 'One pass through the agent'}
        />
        <StatTile
          label="Wall time"
          value={formatDuration(wallMs)}
          meta="ACTIVATION_START → ACTIVATION_END"
        />
        <StatTile
          label="Started"
          value={<span className="tr-tile__time">{formatTimestamp(summary.started_ms)}</span>}
          meta={summary.ended_ms === null ? 'Still in flight' : formatTimestamp(summary.ended_ms)}
        />
      </TileRow>

      <div className="tr-note tr-note--framed">
        <p className="eyebrow">How to read the span view</p>
        <p>
          Every span in this runtime satisfies <span className="mono">start_ms === end_ms</span>:
          spans are zero-width by design, so the agent&rsquo;s hot path never reads a wall clock.
          Nothing below is scaled by elapsed time, and no per-span duration is claimed — the view
          encodes <strong>sequence</strong> and <strong>nesting</strong>, which are the two things
          the records actually contain. The only durations on this page are the wall-clock deltas
          between an attempt&rsquo;s <span className="mono">ACTIVATION_START</span> and{' '}
          <span className="mono">ACTIVATION_END</span>, which are two separate clock reads.
        </p>
      </div>

      <Tabs
        label="Trace sections"
        active={tab}
        onChange={(key) => setTab(key as TabKey)}
        items={[
          { key: 'spans', label: 'Span tree', count: spans.length },
          { key: 'flow', label: 'Flow', count: spans.length },
          { key: 'attempts', label: 'Attempts', count: attempts.length },
          { key: 'raw', label: 'Raw records', count: events.length },
        ]}
      />

      {tab === 'flow' ? (
        <div className="panel">
          <div className="panel-header">
            <div className="row">
              <h2 className="tr-section__title">What this run did</h2>
              <span className="muted">
                Every step in recorded order, laned by attempt. Two lanes mean a suspension — the
                gap between them is where the intent left the pipeline and a decision came back.
              </span>
            </div>
            {attempts.length > 1 ? <Chip tone="suspended">Resumed</Chip> : null}
          </div>
          <div className="panel-body">
            {spansLoading && spans.length === 0 ? (
              <SkeletonRows rows={4} columns={3} />
            ) : (
              <RunGraph spans={spans} attempts={attempts} />
            )}
          </div>
        </div>
      ) : null}

      {tab === 'spans' ? (
        <div className="panel">
          <div className="panel-header">
            <div className="row">
              <h2 className="tr-section__title">Span tree</h2>
              <span className="muted">
                {formatCount(spans.length)} {spans.length === 1 ? 'span' : 'spans'} ·{' '}
                {formatCount(events.length)} {events.length === 1 ? 'event' : 'events'}
              </span>
            </div>
            {attempts.length > 1 ? <Chip tone="suspended">Resumed</Chip> : null}
          </div>
          {spansLoading && spans.length === 0 ? (
            <SkeletonRows rows={8} columns={3} />
          ) : forest.length === 0 ? (
            <EmptyState
              title="No spans recorded for this trace"
              body="The trace summary counts spans the store has not assembled into a tree — that happens while an activation is still in flight, or when only some of its events have been ingested. Reload once the activation has ended."
            />
          ) : (
            <div className="panel-body tr-tree__panel">
              <SpanTree
                roots={forest}
                attemptSpanIds={attemptSpanIds}
                attemptOwner={attemptOwner}
                attemptLabels={attemptLabels}
              />
            </div>
          )}
        </div>
      ) : null}

      {tab === 'attempts' ? (
        <div className="panel">
          <div className="panel-header">
            <h2 className="tr-section__title">Attempts</h2>
            <span className="muted">{formatCount(attempts.length)} in this activation scope</span>
          </div>
          <div className="panel-body">
            <AttemptComparison facts={facts} />
          </div>
        </div>
      ) : null}

      {tab === 'raw' ? (
        <div className="panel">
          <div className="panel-header">
            <h2 className="tr-section__title">Raw records</h2>
            <Select
              options={RAW_OPTIONS}
              value={rawView}
              aria-label="Which records to show"
              onChange={(event) => setRawView(event.target.value as RawView)}
            />
          </div>
          <div className="panel-body stack">
            <p className="muted tr-note">
              The literal records, as the store holds them. Milliseconds are epoch integers, entity
              keys are hex, and the dedup identity is{' '}
              <span className="mono">(trace_id, span_id, event_type)</span> — the same tuple{' '}
              <span className="mono">docs/traces.md</span> publishes.
            </p>
            <div className="tr-raw">
              <CodeBlock
                label="records"
                code={JSON.stringify(
                  rawView === 'events'
                    ? events
                    : rawView === 'spans'
                      ? spans
                      : { summary, attempts, roots: traceQuery.data.roots },
                  null,
                  2,
                )}
              />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
