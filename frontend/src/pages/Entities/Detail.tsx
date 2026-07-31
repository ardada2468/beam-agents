/**
 * One entity key's timeline across every sequence number it has run.
 *
 * The runtime's identity is `(entity_key, seq)`, so a key's history *is* its
 * activation list ordered by seq — which is why this page reuses
 * `api.activations({ entity_key })` rather than asking for a second, differently
 * derived view of the same rows.
 *
 * The headline figures come from the entity index when the key is on it, and
 * are otherwise computed from the activations actually loaded here and labelled
 * as such. A total that silently means "of what happens to be on screen" is the
 * kind of number that gets quoted in a meeting.
 */

import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import { useCallback, useMemo } from 'react';
import { Link, useLocation, useParams } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CopyableId,
  DataTable,
  EmptyState,
  SkeletonRows,
  StatTile,
  StatusChip,
  TileRow,
} from '@/components/ui';
import { api, queryKeys } from '@/lib/api';
import type { ActivationSummary, EntitySummary, Page } from '@/lib/api-types';
import {
  EM_DASH,
  formatCompact,
  formatCount,
  formatDuration,
  formatEntityKey,
  formatRelative,
  formatTimestamp,
  shortId,
} from '@/lib/format';
import { usePageSize } from '@/pages/Settings/preferences';

/** How far into the entity index to look for this key's rollup. */
const INDEX_LOOKUP_LIMIT = 200;

/** A link inside prose: the base stylesheet underlines on hover only. */
const LINK = { textDecoration: 'underline', textUnderlineOffset: '2px' } as const;

function stopRowClick(event: React.MouseEvent): void {
  event.stopPropagation();
}

export default function Page() {
  const params = useParams<{ entityKey: string }>();
  const entityKey = params.entityKey ?? '';
  const [, navigate] = useLocation();
  const [pageSize] = usePageSize();

  const filters = useMemo(() => ({ entity_key: entityKey }), [entityKey]);

  const timeline = useInfiniteQuery({
    queryKey: [...queryKeys.activations(filters), 'entity-timeline', pageSize],
    queryFn: ({ pageParam }) => api.activations(filters, pageParam, pageSize),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });

  // Keyed on the *request*, not on the key being looked up, so opening five
  // entities in a row reads one cached page rather than fetching the same 200
  // rows five times. `select` picks this page's row out of it.
  const index = useQuery({
    queryKey: [...queryKeys.entities, 'index', INDEX_LOOKUP_LIMIT],
    queryFn: () => api.entities(undefined, INDEX_LOOKUP_LIMIT),
    select: useCallback(
      (page: Page<EntitySummary>): EntitySummary | null =>
        page.items.find((item) => item.entity_key === entityKey) ?? null,
      [entityKey],
    ),
  });

  const rows = useMemo(
    () => timeline.data?.pages.flatMap((page) => page.items) ?? [],
    [timeline.data?.pages],
  );
  const total = timeline.data?.pages[0]?.total ?? null;

  const summary = index.data ?? null;
  /** Fallback figures, over exactly the rows on screen — never presented as totals. */
  const loaded = useMemo(() => {
    const tokens = rows.reduce<number | null>(
      (sum, row) => (row.total_tokens === null ? sum : (sum ?? 0) + row.total_tokens),
      null,
    );
    return {
      activations: rows.length,
      errors: rows.reduce((sum, row) => sum + row.errors, 0),
      tokens,
      first: rows.length ? Math.min(...rows.map((row) => row.started_ms)) : null,
      last: rows.length ? Math.max(...rows.map((row) => row.started_ms)) : null,
    };
  }, [rows]);

  const scope = summary ? 'All recorded activity' : `Across ${formatCount(rows.length)} loaded`;

  const columns: Column<ActivationSummary>[] = [
    {
      key: 'seq',
      header: 'Seq',
      numeric: true,
      width: '72px',
      render: (row) => formatCount(row.seq),
    },
    { key: 'status', header: 'Status', render: (row) => <StatusChip status={row.status} /> },
    {
      key: 'kind',
      header: 'Kind',
      render: (row) =>
        row.kind === 'unknown' ? (
          <Chip tone="warn" title="Assembled only from OTLP, which carries no ACTIVATION_START">
            Unknown
          </Chip>
        ) : (
          <Chip tone="neutral" plain>
            {row.kind === 'resume' ? 'Resume' : 'Start'}
          </Chip>
        ),
    },
    {
      key: 'started_ms',
      header: 'Started',
      render: (row) => (
        <span title={formatTimestamp(row.started_ms)}>{formatRelative(row.started_ms)}</span>
      ),
    },
    {
      key: 'wall_ms',
      header: 'Wall',
      numeric: true,
      render: (row) => (
        <span title="Between the ACTIVATION_START and ACTIVATION_END clock reads">
          {formatDuration(row.wall_ms)}
        </span>
      ),
    },
    {
      key: 'model',
      header: 'Model',
      render: (row) =>
        row.model ? (
          <span className="mono">{row.model}</span>
        ) : (
          <span className="faint">{EM_DASH}</span>
        ),
    },
    { key: 'llm_calls', header: 'LLM', numeric: true, render: (row) => formatCount(row.llm_calls) },
    {
      key: 'tool_calls',
      header: 'Tools',
      numeric: true,
      render: (row) => formatCount(row.tool_calls),
    },
    {
      key: 'total_tokens',
      header: 'Tokens',
      numeric: true,
      render: (row) => formatCount(row.total_tokens),
    },
    {
      key: 'errors',
      header: 'Errors',
      numeric: true,
      render: (row) =>
        row.errors > 0 ? (
          <span style={{ color: 'var(--error)' }}>{formatCount(row.errors)}</span>
        ) : (
          formatCount(row.errors)
        ),
    },
    {
      key: 'trace_id',
      header: 'Trace',
      render: (row) => (
        <span onClick={stopRowClick}>
          <CopyableId value={row.trace_id} display={shortId(row.trace_id)} label="trace id" />
        </span>
      ),
    },
  ];

  return (
    <div className="page">
      <div className="stack" style={{ gap: 'var(--space-2)' }}>
        <Link href="/entities" className="muted" style={{ fontSize: 'var(--text-xs)' }}>
          ← Entity keys
        </Link>
        <div className="page-title">
          <h1 style={{ overflowWrap: 'anywhere' }}>{formatEntityKey(entityKey)}</h1>
          <span className="row" style={{ gap: 'var(--space-3)' }}>
            {summary?.latest_status ? <StatusChip status={summary.latest_status} /> : null}
            <CopyableId value={entityKey} label="entity key hex" />
          </span>
        </div>
      </div>

      <TileRow>
        <StatTile
          label="Activations"
          value={formatCount(summary?.activations ?? loaded.activations)}
          meta={scope}
        />
        <StatTile
          label="Errors"
          value={formatCount(summary?.errors ?? loaded.errors)}
          meta={scope}
        />
        <StatTile
          label="Total tokens"
          value={formatCompact(summary ? summary.total_tokens : loaded.tokens)}
          meta="Sum of recorded usage"
        />
        <StatTile
          label="First seen"
          value={formatRelative(summary?.first_seen_ms ?? loaded.first)}
          meta={formatTimestamp(summary?.first_seen_ms ?? loaded.first)}
        />
        <StatTile
          label="Last seen"
          value={formatRelative(summary?.last_seen_ms ?? loaded.last)}
          meta={formatTimestamp(summary?.last_seen_ms ?? loaded.last)}
        />
      </TileRow>

      <div className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>Timeline</h2>
          <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            Every activation recorded for this key, newest first
          </span>
        </div>

        {timeline.isPending ? (
          <SkeletonRows rows={8} columns={9} />
        ) : timeline.isError ? (
          <EmptyState
            title="Could not load this key's activations"
            body={
              <div className="stack" style={{ gap: 'var(--space-2)' }}>
                <p>The console answered with an error: {(timeline.error as Error).message}.</p>
                <p>
                  <Link href="/connect" style={LINK}>
                    Connect
                  </Link>{' '}
                  shows whether any ingest path is reaching this console.
                </p>
              </div>
            }
            action={<Button onClick={() => void timeline.refetch()}>Retry</Button>}
          />
        ) : (
          <DataTable
            caption={`Activations for entity key ${formatEntityKey(entityKey)}`}
            columns={columns}
            rows={rows}
            rowKey={(row) => row.trace_id}
            onRowClick={(row) =>
              navigate(`/activations/${encodeURIComponent(row.entity_key)}/${row.seq}`)
            }
            empty={
              <EmptyState
                title="No activation recorded for this key"
                body={
                  <div className="stack" style={{ gap: 'var(--space-2)' }}>
                    <p>
                      The key is known to the store, but every activation for it has aged out of the
                      retention window — or its records have not arrived yet.
                    </p>
                    <p>
                      Check the ingest paths on{' '}
                      <Link href="/connect" style={LINK}>
                        Connect
                      </Link>
                      , or read the retention window on{' '}
                      <Link href="/settings" style={LINK}>
                        Settings
                      </Link>
                      .
                    </p>
                  </div>
                }
              />
            }
          />
        )}
      </div>

      {timeline.hasNextPage ? (
        <div className="row">
          <Button
            onClick={() => void timeline.fetchNextPage()}
            disabled={timeline.isFetchingNextPage}
          >
            {timeline.isFetchingNextPage ? 'Loading…' : 'Load more'}
          </Button>
          <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            {formatCount(rows.length)} of {formatCount(total)} loaded
          </span>
        </div>
      ) : null}
    </div>
  );
}
