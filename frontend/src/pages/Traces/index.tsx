/**
 * The traces explorer.
 *
 * A trace here is not "a run". It is one activation *scope* —
 * `uuid5(entity_key, seq)` — so a suspend, the effector round trip, and the
 * resume all land in the same trace. The list says so out loud, because
 * everything downstream of that fact (why one row can hold two attempts, why the
 * event count is larger than a single pass would produce) is otherwise
 * surprising.
 *
 * Paging is keyset, not offset: the API hands back an opaque `next_cursor` and
 * the page keeps the stack of cursors it has walked, so "older" is a fetch and
 * "newer" is a pop rather than a re-scan.
 */

import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';
import { Link, useLocation } from 'wouter';

import {
  Button,
  CopyableId,
  DataTable,
  EmptyState,
  Input,
  SkeletonRows,
  StatusChip,
} from '@/components/ui';
import type { Column } from '@/components/ui';
import { api, queryKeys } from '@/lib/api';
import type { TraceSummary } from '@/lib/api-types';
import { formatCount, formatEntityKey, formatTimestamp, shortId } from '@/lib/format';

import './traces.css';

const PAGE_SIZE = 50;

export default function TracesPage() {
  const [, setLocation] = useLocation();
  const [rawQuery, setRawQuery] = useState('');
  const [query, setQuery] = useState('');
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const [pageIndex, setPageIndex] = useState(0);

  // Debounced, so typing a 32-character trace ID is one request rather than 32.
  useEffect(() => {
    const timer = setTimeout(() => setQuery(rawQuery.trim()), 250);
    return () => clearTimeout(timer);
  }, [rawQuery]);

  useEffect(() => {
    setCursors([undefined]);
    setPageIndex(0);
  }, [query]);

  const cursor = cursors[pageIndex];
  const result = useQuery({
    queryKey: [...queryKeys.traces(query), cursor ?? ''],
    queryFn: () => api.traces(query || undefined, cursor, PAGE_SIZE),
    placeholderData: (previous) => previous,
  });

  const rows = useMemo(() => result.data?.items ?? [], [result.data]);
  const total = result.data?.total ?? null;
  const nextCursor = result.data?.next_cursor ?? null;

  const columns: Column<TraceSummary>[] = [
    {
      key: 'trace_id',
      header: 'Trace ID',
      width: '20%',
      render: (row) => (
        <CopyableId value={row.trace_id} display={shortId(row.trace_id, 10, 6)} label="trace id" />
      ),
    },
    {
      key: 'entity_key',
      header: 'Entity key',
      width: '18%',
      render: (row) => (
        <CopyableId
          value={row.entity_key}
          display={formatEntityKey(row.entity_key)}
          label="entity key"
        />
      ),
    },
    { key: 'seq', header: 'Seq', numeric: true, render: (row) => formatCount(row.seq) },
    { key: 'events', header: 'Events', numeric: true, render: (row) => formatCount(row.events) },
    { key: 'spans', header: 'Spans', numeric: true, render: (row) => formatCount(row.spans) },
    { key: 'status', header: 'Status', render: (row) => <StatusChip status={row.status} /> },
    {
      key: 'started',
      header: 'Started',
      render: (row) => <span className="mono">{formatTimestamp(row.started_ms)}</span>,
    },
    {
      key: 'ended',
      header: 'Ended',
      render: (row) => <span className="mono">{formatTimestamp(row.ended_ms)}</span>,
    },
  ];

  return (
    <div className="page">
      <div className="page-title tr-title">
        <h1>Traces</h1>
        <p className="muted tr-lede">
          One trace is one activation scope — <span className="mono">uuid5(entity_key, seq)</span>.
          A suspend, its effector round trip, and its resume are all the same trace, so a row here
          can hold more than one attempt.
        </p>
      </div>

      <div className="panel">
        <div className="panel-header tr-toolbar">
          <Input
            mono
            className="tr-search"
            value={rawQuery}
            onChange={(event) => setRawQuery(event.target.value)}
            placeholder="Search by trace ID, entity key, or attribute value"
            aria-label="Search traces by trace ID, entity key, or attribute value"
            type="search"
          />
          <div className="row tr-toolbar__meta">
            {query ? (
              <Button size="sm" variant="ghost" onClick={() => setRawQuery('')}>
                Clear
              </Button>
            ) : null}
            <span className="muted">
              {total === null
                ? `${formatCount(rows.length)} shown`
                : `${formatCount(total)} ${total === 1 ? 'trace' : 'traces'}`}
            </span>
          </div>
        </div>

        {result.isPending ? (
          <SkeletonRows rows={10} columns={8} />
        ) : result.isError ? (
          <EmptyState
            title="Could not load traces"
            body={
              <>
                <p>{(result.error as Error).message}</p>
                <p style={{ marginTop: 'var(--space-2)' }}>
                  The console API answered <span className="mono">/api/traces</span> with an error.
                  Check that the server is running and that its store is readable.
                </p>
              </>
            }
            action={
              <Button onClick={() => void result.refetch()} variant="primary">
                Retry
              </Button>
            }
          />
        ) : (
          <DataTable
            caption="Traces"
            columns={columns}
            rows={rows}
            rowKey={(row) => row.trace_id}
            onRowClick={(row) => setLocation(`/traces/${encodeURIComponent(row.trace_id)}`)}
            empty={
              query ? (
                <EmptyState
                  title="No trace matches that search"
                  body={
                    <>
                      <p>
                        Nothing in the store matches <span className="mono">{query}</span>. The
                        search runs over trace IDs, entity keys, and event attribute values.
                      </p>
                      <p style={{ marginTop: 'var(--space-2)' }}>
                        Entity keys are stored hex-encoded, so searching for the decoded text will
                        only match if the key is printable ASCII.
                      </p>
                    </>
                  }
                  action={<Button onClick={() => setRawQuery('')}>Clear the search</Button>}
                />
              ) : (
                <EmptyState
                  title="No traces recorded yet"
                  body={
                    <>
                      <p>
                        A trace appears here as soon as an activation exports one. Point a pipeline
                        at the console by passing{' '}
                        <span className="mono">sink_resolver=ConsoleSinkResolver()</span> and{' '}
                        <span className="mono">traces_to=&quot;console://localhost:8787&quot;</span>{' '}
                        to <span className="mono">AgentConfig</span>.
                      </p>
                      <p style={{ marginTop: 'var(--space-2)' }}>
                        Already exporting somewhere else? Connect names the Kafka, BigQuery, OTLP,
                        and bundle-import paths that need no pipeline change.
                      </p>
                    </>
                  }
                  action={
                    <Link href="/connect" className="btn">
                      Open Connect
                    </Link>
                  }
                />
              )
            }
          />
        )}

        {rows.length > 0 ? (
          <div className="tr-pager">
            <span className="muted">
              Page {formatCount(pageIndex + 1)}
              {result.isFetching ? ' · loading' : ''}
            </span>
            <div className="row">
              <Button
                size="sm"
                disabled={pageIndex === 0}
                onClick={() => setPageIndex((index) => Math.max(0, index - 1))}
              >
                Newer
              </Button>
              <Button
                size="sm"
                disabled={nextCursor === null}
                onClick={() => {
                  if (nextCursor === null) return;
                  setCursors((current) => {
                    if (current.length > pageIndex + 1) return current;
                    return [...current, nextCursor];
                  });
                  setPageIndex((index) => index + 1);
                }}
              >
                Older
              </Button>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
