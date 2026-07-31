/**
 * Errors, grouped by the runtime's own vocabulary.
 *
 * The `reason` set is closed and small — nine values, defined in
 * `core/dofn.py` and `hitl.py` — which is what makes grouping by it a real
 * navigation axis rather than a string histogram over free text. A group is
 * `reason × error.type`: the reason says which route in the runtime produced the
 * record, the error type says which exception class got there, and the two
 * together are what an operator is actually triaging.
 *
 * The selected group lives in the query string rather than in component state,
 * so a drilled-in view is a link someone can paste into an incident channel.
 *
 * One deliberate restraint: this page is entirely about failures, and washing it
 * in `--error` would make every row look equally alarming. Severity is carried
 * by the counts, the sparkline, and one rule beside the failure-position panel.
 */

import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';
import { useSearchParams } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  DataTable,
  EmptyState,
  Select,
  Skeleton,
  SkeletonRows,
  Sparkline,
  StatTile,
  TileRow,
} from '@/components/ui';
import { DEFAULT_WINDOW_MS, api, queryKeys } from '@/lib/api';
import type { ErrorGroup, ErrorRecord } from '@/lib/api-types';
import { formatCount, formatRelative, formatTimestamp, humanizeReason } from '@/lib/format';
import { useWindowAnchor } from '@/lib/window-anchor';

import { GroupPanel } from './GroupPanel';
import { Occurrences, recordKey } from './Occurrences';
import { RecordPanel } from './RecordPanel';
import { REASON_INFO } from './reasons';

import './errors.css';

/* -- The time window ------------------------------------------------------- */

const HOUR_MS = 3_600_000;

const WINDOWS: { value: string; label: string; ms: number }[] = [
  { value: '1h', label: 'Last hour', ms: HOUR_MS },
  { value: '6h', label: 'Last 6 hours', ms: 6 * HOUR_MS },
  { value: '24h', label: 'Last 24 hours', ms: 24 * HOUR_MS },
  { value: '7d', label: 'Last 7 days', ms: 7 * 24 * HOUR_MS },
  { value: '30d', label: 'Last 30 days', ms: 30 * 24 * HOUR_MS },
];

const DEFAULT_WINDOW = '24h';

/** How many reasons the runtime can produce, so the tile never drifts from the list. */
const VOCABULARY_SIZE = Object.keys(REASON_INFO).length;

/** The chosen window's label, for prose that has to name it. */
function windowOfLabel(value: string): string | undefined {
  return WINDOWS.find((entry) => entry.value === value)?.label.replace(/^Last /, '');
}

/* -- Group identity -------------------------------------------------------- */

function groupKey(group: Pick<ErrorGroup, 'reason' | 'error_type'>): string {
  return `${group.reason}::${group.error_type ?? ''}`;
}

type SortKey = 'count' | 'entities' | 'last_seen';

export default function Page() {
  const [params, setParams] = useSearchParams();
  const [windowValue, setWindowValue] = useState(DEFAULT_WINDOW);
  // Anchored on the newest stored record rather than on the clock — see
  // `useWindowAnchor`. Read once per mount, so the property the frozen
  // `Date.now()` here was protecting (a `since_ms` that does not move under the
  // page and refetch forever) still holds.
  const anchorMs = useWindowAnchor();
  const [sortKey, setSortKey] = useState<SortKey>('count');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const [selectedRecordKey, setSelectedRecordKey] = useState<string | null>(null);

  const windowMs = WINDOWS.find((entry) => entry.value === windowValue)?.ms ?? DEFAULT_WINDOW_MS;
  const sinceMs = anchorMs - windowMs;
  // 48 buckets, matching the overview's default resolution, floored at a minute
  // so a one-hour window does not ask for sub-minute buckets.
  const bucketMs = Math.max(60_000, Math.round(windowMs / 48));

  const groupsQuery = useQuery({
    queryKey: queryKeys.errorGroups(sinceMs),
    queryFn: () => api.errorGroups(sinceMs, bucketMs),
  });

  const groups = useMemo(() => groupsQuery.data ?? [], [groupsQuery.data]);

  const selectedReason = params.get('reason');
  const selectedType = params.get('type');
  const selected = useMemo(
    () =>
      selectedReason === null
        ? null
        : (groups.find(
            (group) =>
              group.reason === selectedReason && (group.error_type ?? '') === (selectedType ?? ''),
          ) ?? null),
    [groups, selectedReason, selectedType],
  );

  const recordsQuery = useInfiniteQuery({
    queryKey: queryKeys.errors({ reason: selectedReason ?? '', since_ms: sinceMs }),
    queryFn: ({ pageParam }) =>
      api.errors(
        { reason: selectedReason ?? undefined, since_ms: sinceMs },
        pageParam ?? undefined,
        100,
      ),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor,
    // Gated on the resolved group, not just on the query string: a link to a
    // group that is not in this window should not fetch records for it.
    enabled: selected !== null,
  });

  // `/api/errors` narrows by reason only, so the last step to *exactly* this
  // group — its error type — happens here.
  const records: ErrorRecord[] = useMemo(() => {
    if (selected === null) return [];
    const pages = recordsQuery.data?.pages ?? [];
    return pages
      .flatMap((page) => page.items)
      .filter((record) => (record.error_type ?? '') === (selected.error_type ?? ''));
  }, [recordsQuery.data, selected]);

  // The first record stands selected until someone picks another, so the
  // failure-position panel is populated the moment a group opens rather than
  // waiting for a second click.
  const active = useMemo(() => {
    const keyed = records.map((record, index) => ({ key: recordKey(record, index), record }));
    return keyed.find((entry) => entry.key === selectedRecordKey) ?? keyed[0] ?? null;
  }, [records, selectedRecordKey]);

  const sorted = useMemo(() => {
    const rows = [...groups];
    const direction = sortDirection === 'asc' ? 1 : -1;
    rows.sort((a, b) => {
      if (sortKey === 'entities') return (a.entities - b.entities) * direction;
      if (sortKey === 'last_seen') return (a.last_seen_ms - b.last_seen_ms) * direction;
      return (a.count - b.count) * direction;
    });
    return rows;
  }, [groups, sortKey, sortDirection]);

  const totals = useMemo(
    () => ({
      errors: groups.reduce((sum, group) => sum + group.count, 0),
      reasons: new Set(groups.map((group) => group.reason)).size,
      lastSeenMs: groups.length ? Math.max(...groups.map((group) => group.last_seen_ms)) : null,
    }),
    [groups],
  );

  const selectGroup = (group: ErrorGroup | null) => {
    setSelectedRecordKey(null);
    if (group === null) {
      setParams(new URLSearchParams());
      return;
    }
    const next = new URLSearchParams({ reason: group.reason });
    if (group.error_type) next.set('type', group.error_type);
    setParams(next);
  };

  const onSort = (key: string) => {
    if (key !== 'count' && key !== 'entities' && key !== 'last_seen') return;
    if (key === sortKey) {
      setSortDirection((current) => (current === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(key);
    setSortDirection('desc');
  };

  const columns: Column<ErrorGroup>[] = [
    {
      key: 'reason',
      header: 'Reason',
      render: (group) => (
        <span className="errors-reason">
          <span className="errors-reason__label">{humanizeReason(group.reason)}</span>
          <span className="errors-reason__code">{group.reason}</span>
        </span>
      ),
    },
    {
      key: 'error_type',
      header: 'Error type',
      render: (group) =>
        group.error_type ? (
          <span className="mono">{group.error_type}</span>
        ) : (
          <span
            className="errors-unavailable"
            title="No error.type was recorded — this route has no exception to name."
          >
            Not recorded
          </span>
        ),
    },
    {
      key: 'count',
      header: 'Errors',
      numeric: true,
      sortable: true,
      width: '88px',
      render: (group) => formatCount(group.count),
    },
    {
      key: 'entities',
      header: 'Entity keys',
      numeric: true,
      sortable: true,
      width: '104px',
      render: (group) => formatCount(group.entities),
    },
    {
      key: 'first_seen',
      header: 'First seen',
      width: '110px',
      render: (group) => (
        <span title={formatTimestamp(group.first_seen_ms)}>
          {formatRelative(group.first_seen_ms)}
        </span>
      ),
    },
    {
      key: 'last_seen',
      header: 'Last seen',
      sortable: true,
      width: '110px',
      render: (group) => (
        <span title={formatTimestamp(group.last_seen_ms)}>
          {formatRelative(group.last_seen_ms)}
        </span>
      ),
    },
    {
      key: 'series',
      header: 'Occurrences',
      width: '140px',
      render: (group) => (
        <span className="errors-spark">
          <Sparkline
            values={group.series.map((point) => point.value)}
            label={`${humanizeReason(group.reason)} occurrences per bucket`}
          />
        </span>
      ),
    },
  ];

  // A tile is one of three things: still loading, unknown because the query
  // failed, or a figure. A failed aggregate must not render as `0` — that would
  // be the page asserting a measurement it does not have.
  const tile = (loading: ReactNode, value: ReactNode) =>
    groupsQuery.isPending ? loading : groupsQuery.isError ? null : value;

  return (
    <div className="page">
      <div className="page-title">
        <div>
          <h1>Errors</h1>
          <p className="muted" style={{ maxWidth: '68ch', marginTop: 'var(--space-1)' }}>
            Every element-level failure the runtime routes to <code>.errors</code>, grouped by its
            closed <code>reason</code> vocabulary and by <code>error.type</code>.
          </p>
        </div>
        <div className="errors-window">
          <Select
            label="Time window"
            value={windowValue}
            options={WINDOWS.map((entry) => ({ value: entry.value, label: entry.label }))}
            onChange={(event) => {
              setWindowValue(event.target.value);
              setSelectedRecordKey(null);
            }}
          />
        </div>
      </div>

      <TileRow>
        <StatTile
          label="Errors"
          value={tile(<Skeleton width="4ch" height="1em" />, formatCount(totals.errors))}
          meta={`since ${formatTimestamp(sinceMs)}`}
        />
        <StatTile
          label="Reasons"
          value={tile(<Skeleton width="3ch" height="1em" />, formatCount(totals.reasons))}
          meta={`of ${formatCount(VOCABULARY_SIZE)} in the vocabulary`}
        />
        <StatTile
          label="Groups"
          value={tile(<Skeleton width="3ch" height="1em" />, formatCount(groups.length))}
          meta="reason × error type"
        />
        <StatTile
          label="Most recent"
          value={tile(
            <Skeleton width="6ch" height="1em" />,
            totals.lastSeenMs === null ? null : formatRelative(totals.lastSeenMs),
          )}
          meta={
            groupsQuery.isPending
              ? 'reading the store'
              : groupsQuery.isError
                ? 'the aggregate query failed'
                : totals.lastSeenMs === null
                  ? 'nothing in this window'
                  : formatTimestamp(totals.lastSeenMs)
          }
        />
      </TileRow>

      <section className="panel" aria-labelledby="errors-groups-heading">
        <div className="panel-header">
          <h2 id="errors-groups-heading" style={{ fontSize: 'var(--text-md)' }}>
            Reason × error type
          </h2>
          {selected ? (
            <Button size="sm" variant="ghost" onClick={() => selectGroup(null)}>
              Clear selection
            </Button>
          ) : groups.length > 0 ? (
            <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
              Select a group to drill into its records
            </span>
          ) : null}
        </div>

        {groupsQuery.isPending ? (
          <SkeletonRows rows={5} columns={6} />
        ) : groupsQuery.isError ? (
          <EmptyState
            title="Could not load error groups"
            body={
              <>
                <p>
                  The console&rsquo;s <code>/api/errors/groups</code> endpoint did not answer:{' '}
                  <span className="mono">
                    {groupsQuery.error instanceof Error
                      ? groupsQuery.error.message.trim()
                      : 'unknown failure'}
                  </span>
                </p>
                <p style={{ marginTop: 'var(--space-2)' }}>
                  The records themselves are unaffected — this is the aggregate query, not the
                  store.
                </p>
              </>
            }
            action={<Button onClick={() => void groupsQuery.refetch()}>Try again</Button>}
          />
        ) : (
          <DataTable
            caption="Error groups by reason and error type"
            columns={columns}
            rows={sorted}
            rowKey={groupKey}
            selectedKey={selected ? groupKey(selected) : undefined}
            onRowClick={selectGroup}
            sortKey={sortKey}
            sortDirection={sortDirection}
            onSort={onSort}
            empty={
              <EmptyState
                title="No errors in this window — which is the outcome you want"
                body={
                  <>
                    <p>
                      A pipeline that never dead-letters an element produces nothing here. This page
                      fills in when <code>RunAgent</code> routes a failure to <code>.errors</code>:
                      an agent that raised, an activation past its timeout or its token budget, a
                      tool result that arrived with no continuation left to admit it, an approval
                      that never came, or working-memory GC reaching a key that was still waiting.
                    </p>
                    <p style={{ marginTop: 'var(--space-2)' }}>
                      If you expected records and see none, widen the time window, or check{' '}
                      <a href="/connect">Connect</a> to confirm an ingest path is carrying{' '}
                      <code>errors_to</code> into this console.
                    </p>
                  </>
                }
              />
            }
          />
        )}
      </section>

      {selected ? (
        <>
          <GroupPanel
            group={selected}
            records={records}
            loadedAll={!recordsQuery.hasNextPage && !recordsQuery.isFetching}
          />
          <div className="errors-split">
            <Occurrences
              reason={selected.reason}
              records={records}
              loading={recordsQuery.isPending}
              selectedKey={active?.key}
              onSelect={(_record, key) => setSelectedRecordKey(key)}
              hasMore={recordsQuery.hasNextPage}
              loadingMore={recordsQuery.isFetchingNextPage}
              onLoadMore={() => void recordsQuery.fetchNextPage()}
              total={recordsQuery.data?.pages[0]?.total ?? null}
            />
            <RecordPanel record={active?.record ?? null} />
          </div>
        </>
      ) : groups.length === 0 ? null : selectedReason !== null ? (
        // A shareable link is only useful if it says so when it no longer
        // resolves — the window may have moved past the group it named.
        <section className="panel">
          <EmptyState
            title="That group is not in this window"
            body={
              <p>
                Nothing in the last {(windowOfLabel(windowValue) ?? '').toLowerCase()} has{' '}
                <code>{selectedReason}</code>
                {selectedType ? (
                  <>
                    {' '}
                    with <code>{selectedType}</code>
                  </>
                ) : null}
                . Widen the time window, or pick a group from the table above.
              </p>
            }
            action={
              <Button onClick={() => selectGroup(null)}>Show all groups in this window</Button>
            }
          />
        </section>
      ) : (
        <section className="panel">
          <EmptyState
            title="No group selected"
            body={
              <>
                <p>
                  Selecting a reason drills into exactly that group&rsquo;s records: the failing
                  activations, the sub-reason its detail string carries, and — for the routes that
                  can reach an activation context — where in the activation it failed.
                </p>
                <p style={{ marginTop: 'var(--space-2)' }}>
                  <Chip tone="neutral" plain>
                    {formatCount(groups.length)} groups
                  </Chip>{' '}
                  are listed above, ordered by {sortKey === 'count' ? 'occurrence count' : sortKey}.
                </p>
              </>
            }
          />
        </section>
      )}
    </div>
  );
}
