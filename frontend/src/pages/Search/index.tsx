/**
 * One input over every identifier and attribute value the store holds.
 *
 * The query lives in the URL, so a search is a link: the thing someone does
 * with a trace ID they found in a log is paste it here and then send the
 * address to whoever owns the pipeline.
 *
 * Results are grouped by what was hit — an activation, a single event, an
 * error, or an entity key — because those answer different questions and a flat
 * ranked list would bury the one row that identifies the run. Every hit says
 * which field matched and what the matched value was, so a result is never a
 * bare assertion that something somewhere contained the string.
 */

import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';
import { Link, useLocation, useSearchParams } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CopyableId,
  DataTable,
  EmptyState,
  Input,
  SkeletonRows,
} from '@/components/ui';
import { api, queryKeys } from '@/lib/api';
import type { SearchHit } from '@/lib/api-types';
import {
  formatCount,
  formatEntityKey,
  formatRelative,
  formatTimestamp,
  shortId,
} from '@/lib/format';
import { usePageSize } from '@/pages/Settings/preferences';

/** Reflecting every keystroke in the URL would put a history entry per letter. */
const DEBOUNCE_MS = 250;

/** A link inside prose: the base stylesheet underlines on hover only. */
const LINK = { textDecoration: 'underline', textUnderlineOffset: '2px' } as const;

const KIND_ORDER: SearchHit['kind'][] = ['activation', 'event', 'error', 'entity'];

const KIND_LABEL: Record<SearchHit['kind'], string> = {
  activation: 'Activations',
  event: 'Trace events',
  error: 'Errors',
  entity: 'Entity keys',
};

const KIND_BLURB: Record<SearchHit['kind'], string> = {
  activation: 'An activation whose identity or rollup matched.',
  event: 'A single trace event whose attribute map matched.',
  error: 'An activation error whose reason, type, or detail matched.',
  entity: 'An entity key that matched, as hex or as decoded text.',
};

/** Where a hit came from, which is where clicking it goes. */
function hitHref(hit: SearchHit): string {
  const key = encodeURIComponent(hit.entity_key);
  if (hit.kind === 'entity') return `/entities/${key}`;
  if (hit.kind === 'event' && hit.trace_id) return `/traces/${encodeURIComponent(hit.trace_id)}`;
  if (hit.seq !== null) return `/activations/${key}/${hit.seq}`;
  return `/entities/${key}`;
}

function hitTarget(hit: SearchHit): string {
  if (hit.kind === 'entity') return 'Entity key';
  if (hit.kind === 'event' && hit.trace_id) return `Trace ${shortId(hit.trace_id)}`;
  if (hit.seq !== null) return `Activation seq ${hit.seq}`;
  return 'Entity key';
}

/** Hex-encode printable text, so a readable key can be searched as stored bytes. */
function toHex(text: string): string {
  return Array.from(new TextEncoder().encode(text))
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('');
}

function looksHex(text: string): boolean {
  return text.length % 2 === 0 && /^[0-9a-f]+$/i.test(text);
}

export default function Page() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [, navigate] = useLocation();
  const [pageSize] = usePageSize();

  const urlQuery = searchParams.get('q') ?? '';
  const [draft, setDraft] = useState(urlQuery);

  // The URL is the source of truth; the input is a buffer in front of it. Both
  // directions are wired, so a pasted link fills the box and typing rewrites
  // the address.
  useEffect(() => setDraft(urlQuery), [urlQuery]);

  useEffect(() => {
    if (draft === urlQuery) return;
    const timer = setTimeout(() => {
      setSearchParams(draft ? { q: draft } : {}, { replace: true });
    }, DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [draft, urlQuery, setSearchParams]);

  const query = useQuery({
    queryKey: queryKeys.search(urlQuery),
    queryFn: () => api.search(urlQuery, pageSize),
    enabled: urlQuery.trim().length > 0,
  });

  const grouped = useMemo(() => {
    const groups = new Map<SearchHit['kind'], SearchHit[]>();
    for (const hit of query.data ?? []) {
      const existing = groups.get(hit.kind) ?? [];
      existing.push(hit);
      groups.set(hit.kind, existing);
    }
    return KIND_ORDER.filter((kind) => (groups.get(kind)?.length ?? 0) > 0).map((kind) => ({
      kind,
      hits: groups.get(kind) ?? [],
    }));
  }, [query.data]);

  const hitCount = query.data?.length ?? 0;
  const trimmed = urlQuery.trim();
  const hexAlternative =
    trimmed && !looksHex(trimmed) && /^[\x20-\x7e]+$/.test(trimmed) ? toHex(trimmed) : null;

  const columns: Column<SearchHit>[] = [
    {
      key: 'entity_key',
      header: 'Entity key',
      render: (hit) => (
        <span onClick={(event) => event.stopPropagation()}>
          <CopyableId
            value={hit.entity_key}
            display={formatEntityKey(hit.entity_key)}
            label="entity key"
          />
        </span>
      ),
    },
    {
      key: 'matched_field',
      header: 'Matched field',
      render: (hit) => (
        <Chip tone="neutral" plain>
          {hit.matched_field}
        </Chip>
      ),
    },
    {
      key: 'matched_value',
      header: 'Matched value',
      wrap: true,
      render: (hit) => <span className="mono">{hit.matched_value}</span>,
    },
    { key: 'label', header: 'Record', wrap: true, render: (hit) => hit.label },
    {
      key: 'target',
      header: 'Opens',
      render: (hit) => <span className="muted">{hitTarget(hit)}</span>,
    },
    {
      key: 'at_ms',
      header: 'When',
      render: (hit) => <span title={formatTimestamp(hit.at_ms)}>{formatRelative(hit.at_ms)}</span>,
    },
  ];

  return (
    <div className="page">
      <div className="page-title">
        <h1>Search</h1>
        {trimmed && query.isSuccess ? (
          <p className="muted">
            {formatCount(hitCount)} hit{hitCount === 1 ? '' : 's'} for{' '}
            <span className="mono">{urlQuery}</span>
          </p>
        ) : null}
      </div>

      <div className="panel">
        <div className="panel-body">
          <form
            className="row"
            style={{ flexWrap: 'wrap', gap: 'var(--space-2)' }}
            onSubmit={(event) => {
              event.preventDefault();
              setSearchParams(draft ? { q: draft } : {}, { replace: true });
            }}
          >
            <Input
              mono
              type="search"
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              placeholder="Entity key, trace or span ID, model, tool, reason, attribute value"
              aria-label="Search identifiers and attribute values"
              style={{ flex: '1 1 320px' }}
            />
            <Button type="submit" variant="primary">
              Search
            </Button>
            {trimmed ? (
              <Button
                onClick={() => {
                  setDraft('');
                  setSearchParams({}, { replace: true });
                }}
              >
                Clear
              </Button>
            ) : null}
          </form>

          {hexAlternative ? (
            <p className="muted" style={{ marginTop: 'var(--space-3)' }}>
              Entity keys are stored as hex bytes.{' '}
              <Button
                size="sm"
                onClick={() => setSearchParams({ q: hexAlternative }, { replace: true })}
              >
                Search as hex
              </Button>{' '}
              <span className="mono">{shortId(hexAlternative, 16, 4)}</span>
            </p>
          ) : null}
        </div>
      </div>

      {!trimmed ? (
        <div className="panel">
          <EmptyState
            title="Search the stored records"
            body={
              <div className="stack" style={{ gap: 'var(--space-2)' }}>
                <p>
                  One field over every identifier and attribute value the store holds: entity keys
                  (as hex or as decoded text), trace and span IDs, intent IDs, model and tool names,
                  the runtime&apos;s error reasons, and the values inside{' '}
                  <code>TraceEvent.attributes</code>.
                </p>
                <p>
                  Results group by what was hit, and the query is kept in the address bar so a
                  search can be pasted to someone else.
                </p>
                <p>
                  Nothing is stored until a pipeline exports to this console —{' '}
                  <Link href="/connect" style={LINK}>
                    Connect
                  </Link>{' '}
                  has a snippet for each way in.
                </p>
              </div>
            }
          />
        </div>
      ) : query.isPending || query.isFetching ? (
        <div className="panel">
          <SkeletonRows rows={5} columns={6} />
        </div>
      ) : query.isError ? (
        <div className="panel">
          <EmptyState
            title="The search could not run"
            body={
              <div className="stack" style={{ gap: 'var(--space-2)' }}>
                <p>The console answered with an error: {(query.error as Error).message}.</p>
                <p>
                  <Link href="/connect" style={LINK}>
                    Connect
                  </Link>{' '}
                  shows whether this console is reachable and what is reaching it.
                </p>
              </div>
            }
            action={<Button onClick={() => void query.refetch()}>Retry</Button>}
          />
        </div>
      ) : grouped.length === 0 ? (
        <div className="panel">
          <EmptyState
            title={`Nothing matched “${urlQuery}”`}
            body={
              <div className="stack" style={{ gap: 'var(--space-2)' }}>
                <p>
                  Search matches stored identifiers and attribute values as they were recorded. A
                  key typed as readable text will not match the hex bytes it is stored as, and a
                  record that has aged out of the retention window is gone.
                </p>
                <p>
                  If the store is empty,{' '}
                  <Link href="/connect" style={LINK}>
                    Connect
                  </Link>{' '}
                  lists the five ways to send it records — a <code>console://</code> sink, an
                  existing <code>otlp://</code> exporter, a Kafka topic, a BigQuery table, or a
                  captured replay bundle.
                </p>
              </div>
            }
            action={
              <Button onClick={() => navigate('/entities')}>Browse entity keys instead</Button>
            }
          />
        </div>
      ) : (
        grouped.map((group) => (
          <section key={group.kind} className="panel">
            <div className="panel-header">
              <h2 style={{ fontSize: 'var(--text-md)' }}>
                {KIND_LABEL[group.kind]}{' '}
                <span className="faint" style={{ fontWeight: 'var(--weight-regular)' }}>
                  {formatCount(group.hits.length)}
                </span>
              </h2>
              <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
                {KIND_BLURB[group.kind]}
              </span>
            </div>
            <DataTable
              caption={`${KIND_LABEL[group.kind]} matching ${urlQuery}`}
              columns={columns}
              rows={group.hits}
              rowKey={(hit, index) =>
                `${hit.kind}:${hit.entity_key}:${hit.seq ?? ''}:${hit.span_id ?? ''}:${index}`
              }
              onRowClick={(hit) => navigate(hitHref(hit))}
            />
          </section>
        ))
      )}
    </div>
  );
}
