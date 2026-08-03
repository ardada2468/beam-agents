/**
 * Entity keys — every key the runtime has activated.
 *
 * This is the "who is this running for" axis. An entity key is the runtime's
 * partition identity, so it is the one dimension along which a person asks a
 * question about a *subject* rather than about a run: what has this account
 * been doing, how much has this user cost, which key is failing.
 *
 * Keys are hex-encoded bytes and are very often UTF-8 text, so the readable
 * decoding is what the column shows and the hex is what copies — `formatEntityKey`
 * owns that decision so the list and the detail page cannot disagree.
 */

import { useInfiniteQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { Link, useLocation } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  CopyableId,
  DataTable,
  EmptyState,
  Input,
  SkeletonRows,
  StatusChip,
} from '@/components/ui';
import { api, queryKeys } from '@/lib/api';
import type { EntitySummary } from '@/lib/api-types';
import {
  EM_DASH,
  formatCount,
  formatEntityKey,
  formatRelative,
  formatTimestamp,
} from '@/lib/format';
import { usePageSize } from '@/pages/Settings/preferences';

/**
 * A link inside prose.
 *
 * The base stylesheet underlines links on hover only, which is right for the
 * navigation rail and wrong for a sentence: an empty state that points at
 * `/connect` has to *look* like it points somewhere.
 */
const LINK = { textDecoration: 'underline', textUnderlineOffset: '2px' } as const;

/** Stop a copy click inside a clickable row from also opening the row. */
function stopRowClick(event: React.MouseEvent): void {
  event.stopPropagation();
}

export default function Page() {
  const [, navigate] = useLocation();
  const [pageSize] = usePageSize();
  const [filter, setFilter] = useState('');

  const query = useInfiniteQuery({
    queryKey: [...queryKeys.entities, pageSize],
    queryFn: ({ pageParam }) => api.entities(pageParam, pageSize),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });

  const rows = useMemo(
    () => query.data?.pages.flatMap((page) => page.items) ?? [],
    [query.data?.pages],
  );
  const total = query.data?.pages[0]?.total ?? null;
  const known = total ?? rows.length;

  const visible = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter(
      (row) =>
        row.entity_key.toLowerCase().includes(needle) ||
        formatEntityKey(row.entity_key).toLowerCase().includes(needle),
    );
  }, [rows, filter]);

  const columns: Column<EntitySummary>[] = [
    {
      key: 'entity_key',
      header: 'Entity key',
      width: '26%',
      render: (row) => (
        <span onClick={stopRowClick}>
          <CopyableId
            value={row.entity_key}
            display={formatEntityKey(row.entity_key)}
            label="entity key"
          />
        </span>
      ),
    },
    {
      key: 'activations',
      header: 'Activations',
      numeric: true,
      render: (row) => formatCount(row.activations),
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
      key: 'total_tokens',
      header: 'Total tokens',
      numeric: true,
      render: (row) => formatCount(row.total_tokens),
    },
    {
      key: 'latest_seq',
      header: 'Latest seq',
      numeric: true,
      render: (row) => formatCount(row.latest_seq),
    },
    {
      key: 'latest_status',
      header: 'Latest status',
      render: (row) =>
        row.latest_status ? (
          <StatusChip status={row.latest_status} />
        ) : (
          <span className="faint">{EM_DASH}</span>
        ),
    },
    {
      key: 'first_seen_ms',
      header: 'First seen',
      render: (row) => (
        <span title={formatTimestamp(row.first_seen_ms)} className="muted">
          {formatRelative(row.first_seen_ms)}
        </span>
      ),
    },
    {
      key: 'last_seen_ms',
      header: 'Last seen',
      render: (row) => (
        <span title={formatTimestamp(row.last_seen_ms)}>{formatRelative(row.last_seen_ms)}</span>
      ),
    },
  ];

  return (
    <div className="page">
      <div className="page-title">
        <h1>Entity keys</h1>
        <p className="muted">
          {query.isSuccess
            ? `${formatCount(known)} key${known === 1 ? '' : 's'} activated`
            : 'Every key the runtime has activated'}
        </p>
      </div>

      <div className="panel">
        <div className="panel-header">
          <Input
            placeholder="Filter loaded keys"
            aria-label="Filter loaded entity keys"
            value={filter}
            mono
            onChange={(event) => setFilter(event.target.value)}
            style={{ width: 'min(280px, 100%)' }}
          />
          <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            {filter
              ? `${formatCount(visible.length)} of ${formatCount(rows.length)} loaded keys match`
              : `${formatCount(rows.length)} loaded`}
          </span>
        </div>

        {query.isPending ? (
          <SkeletonRows rows={8} columns={8} />
        ) : query.isError ? (
          <EmptyState
            title="Could not load entity keys"
            body={
              <div className="stack" style={{ gap: 'var(--space-2)' }}>
                <p>
                  The console answered with an error: {(query.error as Error).message}. The store
                  may not be reachable.
                </p>
                <p>
                  <Link href="/connect" style={LINK}>
                    Connect
                  </Link>{' '}
                  lists which ingest paths this console has configured.
                </p>
              </div>
            }
            action={<Button onClick={() => void query.refetch()}>Retry</Button>}
          />
        ) : (
          <DataTable
            caption="Entity keys the runtime has activated"
            columns={columns}
            rows={visible}
            rowKey={(row) => row.entity_key}
            onRowClick={(row) => navigate(`/entities/${encodeURIComponent(row.entity_key)}`)}
            empty={
              filter ? (
                <EmptyState
                  title="No loaded key matches that filter"
                  body="The filter runs over the keys already loaded on this page. Clear it, or load more pages, to widen the search."
                  action={<Button onClick={() => setFilter('')}>Clear filter</Button>}
                />
              ) : (
                <EmptyState
                  title="No entity key has been activated yet"
                  body={
                    <div className="stack" style={{ gap: 'var(--space-2)' }}>
                      <p>
                        A key appears here the first time an activation for it reaches the store —
                        so this fills in as soon as a pipeline exports to this console.
                      </p>
                      <p>
                        Point one at it from{' '}
                        <Link href="/connect" style={LINK}>
                          Connect
                        </Link>
                        : a <code>console://</code> sink, an existing <code>otlp://</code> exporter,
                        a Kafka topic, a BigQuery table, or a captured replay bundle.
                      </p>
                    </div>
                  }
                  action={
                    <Button variant="primary" onClick={() => navigate('/connect')}>
                      Connect a pipeline
                    </Button>
                  }
                />
              )
            }
          />
        )}
      </div>

      {query.hasNextPage ? (
        <div className="row">
          <Button onClick={() => void query.fetchNextPage()} disabled={query.isFetchingNextPage}>
            {query.isFetchingNextPage ? 'Loading…' : 'Load more'}
          </Button>
          <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            {formatCount(rows.length)} of {formatCount(total)} loaded
          </span>
        </div>
      ) : null}
    </div>
  );
}
