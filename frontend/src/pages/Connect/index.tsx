/**
 * Connect — the five adoption paths from the design's migration plan, made
 * usable.
 *
 * The order is the design's, and it is not arbitrary: increasing intrusiveness,
 * so the first thing someone reads is the one that changes nothing about their
 * pipeline. A page that opened with "add this constructor argument" would ask
 * for a redeploy before establishing that the console is worth one.
 *
 * Every snippet is quoted from the real surface — `ConsoleSinkResolver` and the
 * `AgentConfig` fields it plugs into, the `beam-agents-console` flags, the URI
 * grammars the runtime's own resolver parses — because a snippet that does not
 * run is worse than no snippet.
 *
 * Each path carries live status from `/healthz` and `/api/store`, so the page
 * answers "is anything actually arriving?" rather than only "here is how you
 * would make it arrive".
 */

import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useMemo, useRef, useState } from 'react';
import { Link } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CodeBlock,
  DataTable,
  KeyValueGrid,
  Select,
  Skeleton,
  StatTile,
  TileRow,
} from '@/components/ui';
import { ApiError, api, queryKeys, request } from '@/lib/api';
import { EM_DASH, formatBytes, formatCount, formatRelative, formatTimestamp } from '@/lib/format';

import './connect.css';

/* -- The five paths -------------------------------------------------------- */

/** A link inside prose: the base stylesheet underlines on hover only. */
const LINK = { textDecoration: 'underline', textUnderlineOffset: '2px' } as const;

/** Which `/healthz` source names count as this path being wired up. */
type PathId = 'otlp' | 'pull' | 'bundle' | 'native' | 'demo';

const SOURCE_TOKENS: Record<PathId, string[]> = {
  otlp: ['otlp'],
  pull: ['kafka', 'bigquery'],
  bundle: ['bundle', 'import'],
  native: ['native', 'console'],
  demo: ['demo'],
};

/**
 * What the chip says when nothing matches.
 *
 * Not one string for all five: three of these paths are *sources the process
 * runs*, one is an action taken here, and one is how the process was started.
 * "Not configured" is only true of the first kind.
 */
const ABSENT_LABEL: Record<PathId, string> = {
  otlp: 'Not configured',
  pull: 'Not configured',
  bundle: 'Nothing imported yet',
  native: 'Not configured',
  demo: 'Demo not running',
};

function matchedSources(sources: string[] | undefined, id: PathId): string[] {
  if (!sources) return [];
  const tokens = SOURCE_TOKENS[id];
  return sources.filter((source) => tokens.some((token) => source.toLowerCase().includes(token)));
}

/**
 * The status chip for one path.
 *
 * Three states, and the third is not "no": until `/healthz` answers, the
 * console cannot say whether a path is live, and claiming it is not would be an
 * assertion it has not earned.
 */
function PathStatus({ sources, id }: { sources: string[] | undefined; id: PathId }) {
  if (sources === undefined) return <Chip tone="neutral">Status unknown</Chip>;
  const matched = matchedSources(sources, id);
  if (matched.length === 0) return <Chip tone="neutral">{ABSENT_LABEL[id]}</Chip>;
  return <Chip tone="ok" title={matched.join(', ')}>{`Receiving · ${matched.join(', ')}`}</Chip>;
}

const OTLP_SNIPPET = `# The console accepts OTLP/HTTP trace exports at the same /v1/traces path a
# collector does, so an existing exporter reaches it by changing only the host.
from beam_agents.core.transform import AgentConfig

config = AgentConfig(
    provider_factory=make_client,
    traces_to="otlp://localhost:8787",
)`;

const KAFKA_SNIPPET = `pip install 'beam-agents[console,console-ingest]'

# Reads the topic a pipeline already writes with traces_to="kafka://…".
# Starts from the end of the topic and commits no offsets, so starting a
# second console never disturbs the first.
beam-agents-console \\
    --db beam-agents-console.db \\
    --kafka-traces-from kafka://localhost:9092/beam-agents.traces \\
    --kafka-from-beginning`;

const BIGQUERY_SNIPPET = `pip install 'beam-agents[console,console-ingest]'

# Reads the table a pipeline already writes with traces_to="bigquery://…",
# reversing the published row encoding. Pulls are incremental by event_time,
# and re-reading an overlapping window is harmless: ingest is idempotent.
beam-agents-console \\
    --db beam-agents-console.db \\
    --bigquery-traces-from bigquery://my-project/agent_telemetry/trace_events`;

const BUNDLE_SNIPPET = `# The same files beam-agents-replay consumes: a varint-length-delimited
# TraceEvent stream and a serialized StateSnapshot.
beam-agents-console \\
    --db beam-agents-console.db \\
    --import-traces traces.bin \\
    --import-snapshot snapshot.bin`;

const CONSOLE_SNIPPET = `from beam_agents.console import ConsoleSinkResolver
from beam_agents.core.transform import AgentConfig

# ConsoleSinkResolver adds console:// and delegates every other scheme to the
# runtime's own resolver, so adopting it never removes an existing sink.
config = AgentConfig(
    provider_factory=make_client,
    traces_to="console://localhost:8787",
    errors_to="console://localhost:8787",
    snapshots_to="console://localhost:8787",
    sink_resolver=ConsoleSinkResolver(),
)`;

const COMPOSE_SNIPPET = `docker compose -f docker/compose.console.yaml up
# then open http://localhost:8787`;

/* -- The bundle importer --------------------------------------------------- */

type ImportPart = 'traces' | 'snapshot' | 'errors';

const PART_OPTIONS = [
  { value: 'traces', label: 'traces — TraceEvent stream' },
  { value: 'snapshot', label: 'snapshot — StateSnapshot' },
  { value: 'errors', label: 'errors — ActivationErrorRecord stream' },
];

interface StagedFile {
  id: string;
  file: File;
  part: ImportPart;
}

/** What `_sources/_bundle.BundleImportResult` reports back. Every field optional
 *  on the wire, because a console that predates a field should not blank the page. */
interface ImportResult {
  events?: number | null;
  snapshots?: number | null;
  errors?: number | null;
  activations?: number | null;
  truncated?: boolean;
  detail?: string;
}

/** Guess a part from a filename, which is right for the names the replay CLI writes. */
function guessPart(name: string): ImportPart {
  const lower = name.toLowerCase();
  if (lower.includes('snapshot')) return 'snapshot';
  if (lower.includes('error')) return 'errors';
  return 'traces';
}

function BundleImporter({ onImported }: { onImported: () => void }) {
  const [staged, setStaged] = useState<StagedFile[]>([]);
  const [dragging, setDragging] = useState(false);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [failure, setFailure] = useState<string | null>(null);
  const [unsupported, setUnsupported] = useState(false);
  const counter = useRef(0);

  const add = useCallback((files: FileList | null) => {
    if (!files) return;
    const next = Array.from(files).map((file) => ({
      id: `${(counter.current += 1)}`,
      file,
      part: guessPart(file.name),
    }));
    setStaged((current) => [...current, ...next]);
    setResult(null);
    setFailure(null);
    setUnsupported(false);
  }, []);

  const submit = useCallback(async () => {
    if (staged.length === 0) return;
    setBusy(true);
    setFailure(null);
    setUnsupported(false);
    setResult(null);
    try {
      const form = new FormData();
      for (const entry of staged) form.append(entry.part, entry.file, entry.file.name);
      const imported = await request<ImportResult>('/api/import', undefined, {
        method: 'POST',
        body: form,
      });
      setResult(imported ?? {});
      setStaged([]);
      onImported();
    } catch (error) {
      // The endpoint is the newest part of the console and may simply not be
      // there. That is a different message from "the import failed", and
      // collapsing the two would send someone hunting a bad file.
      if (error instanceof ApiError && [404, 405, 501].includes(error.status)) {
        setUnsupported(true);
      } else {
        setFailure(error instanceof Error ? error.message : String(error));
      }
    } finally {
      setBusy(false);
    }
  }, [staged, onImported]);

  return (
    <div className="stack">
      <div
        className={`connect-drop${dragging ? ' connect-drop--over' : ''}`}
        onDragOver={(event) => {
          event.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={(event) => {
          event.preventDefault();
          setDragging(false);
          add(event.dataTransfer.files);
        }}
      >
        <p>
          Drop a capture here — <span className="mono">traces.bin</span>,{' '}
          <span className="mono">snapshot.bin</span>, and an optional error stream.
        </p>
        <input
          className="connect-drop__input"
          type="file"
          multiple
          aria-label="Choose replay bundle files to import"
          onChange={(event) => {
            add(event.target.files);
            event.target.value = '';
          }}
        />
        <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
          Files are posted to <span className="mono">POST /api/import</span> and parsed with the
          runtime&apos;s own framing parser. Nothing leaves this machine.
        </p>
      </div>

      {staged.length > 0 ? (
        <>
          <ul className="connect-files">
            {staged.map((entry) => (
              <li key={entry.id} className="connect-files__row">
                <span className="connect-files__name mono">{entry.file.name}</span>
                <span className="muted" style={{ fontSize: 'var(--text-xs)' }}>
                  {formatBytes(entry.file.size)}
                </span>
                <Select
                  aria-label={`Which part ${entry.file.name} is imported as`}
                  options={PART_OPTIONS}
                  value={entry.part}
                  onChange={(event) =>
                    setStaged((current) =>
                      current.map((item) =>
                        item.id === entry.id
                          ? { ...item, part: event.target.value as ImportPart }
                          : item,
                      ),
                    )
                  }
                />
                <Button
                  size="sm"
                  onClick={() => setStaged((current) => current.filter((i) => i.id !== entry.id))}
                  aria-label={`Remove ${entry.file.name}`}
                >
                  Remove
                </Button>
              </li>
            ))}
          </ul>
          <div className="row">
            <Button variant="primary" disabled={busy} onClick={() => void submit()}>
              {busy
                ? 'Importing…'
                : `Import ${staged.length} file${staged.length === 1 ? '' : 's'}`}
            </Button>
            <Button disabled={busy} onClick={() => setStaged([])}>
              Clear
            </Button>
          </div>
        </>
      ) : null}

      {result ? (
        <div className="stack" style={{ gap: 'var(--space-2)' }}>
          <div className="row">
            <Chip tone={result.truncated ? 'warn' : 'ok'}>
              {result.truncated ? 'Imported, stream truncated' : 'Imported'}
            </Chip>
          </div>
          <KeyValueGrid
            compact
            entries={[
              { key: 'events', label: 'Trace events', value: formatCount(result.events ?? null) },
              {
                key: 'activations',
                label: 'Activations',
                value: formatCount(result.activations ?? null),
              },
              {
                key: 'snapshots',
                label: 'Snapshots',
                value: formatCount(result.snapshots ?? null),
              },
              { key: 'errors', label: 'Error records', value: formatCount(result.errors ?? null) },
              {
                key: 'truncated',
                label: 'Stream',
                value: result.truncated
                  ? 'Ended mid-record — everything read before the break was stored'
                  : 'Complete',
              },
              { key: 'detail', label: 'Detail', value: result.detail || EM_DASH },
            ]}
          />
          <p className="muted">
            The imported run is queryable now — start from{' '}
            <Link href="/entities" style={LINK}>
              Entity keys
            </Link>
            .
          </p>
        </div>
      ) : null}

      {unsupported ? (
        <div className="stack" style={{ gap: 'var(--space-2)' }}>
          <div className="row">
            <Chip tone="warn">Upload endpoint not available</Chip>
          </div>
          <p className="muted">
            This console did not accept <span className="mono">POST /api/import</span>. The files
            you staged were not sent anywhere. Use the command below instead — it writes to the same
            store this page is reading.
          </p>
        </div>
      ) : null}

      {failure ? (
        <div className="stack" style={{ gap: 'var(--space-2)' }}>
          <div className="row">
            <Chip tone="error">Import failed</Chip>
          </div>
          <p className="muted">{failure}</p>
        </div>
      ) : null}
    </div>
  );
}

/* -- The page -------------------------------------------------------------- */

/** How recent a record has to be for the console to claim records are arriving. */
const ARRIVING_WINDOW_MS = 5 * 60 * 1000;

/** The store's `row_counts` map, rendered as the table it is. */
const ROW_COUNT_COLUMNS: Column<[string, number]>[] = [
  { key: 'table', header: 'Table', render: ([name]) => name },
  { key: 'rows', header: 'Rows', numeric: true, render: ([, count]) => formatCount(count) },
];

export default function Page() {
  const queryClient = useQueryClient();

  const health = useQuery({ queryKey: queryKeys.health, queryFn: () => api.health() });
  const store = useQuery({ queryKey: queryKeys.store, queryFn: () => api.store() });

  const sources = health.data?.sources;
  const rowCounts = useMemo(
    () => Object.entries(store.data?.row_counts ?? {}).sort(([a], [b]) => a.localeCompare(b)),
    [store.data?.row_counts],
  );
  const newest = store.data?.newest_record_ms ?? null;
  const arriving = newest !== null && Date.now() - newest < ARRIVING_WINDOW_MS;
  const totalRows = rowCounts.reduce((sum, [, count]) => sum + count, 0);

  const refresh = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.store });
    void queryClient.invalidateQueries({ queryKey: queryKeys.health });
    void queryClient.invalidateQueries({ queryKey: queryKeys.entities });
  }, [queryClient]);

  return (
    <div className="page">
      <div className="page-title">
        <h1>Connect</h1>
        <p className="muted" style={{ maxWidth: '60ch' }}>
          Five ways to get records into this console, in increasing order of how much they ask of a
          pipeline. Nothing here changes how a deployed pipeline exports.
        </p>
      </div>

      <TileRow>
        <StatTile
          label="Ingest sources"
          value={sources === undefined ? null : formatCount(sources.length)}
          meta={sources && sources.length > 0 ? sources.join(', ') : 'None configured'}
        />
        <StatTile
          label="Records stored"
          value={store.isSuccess ? formatCount(totalRows) : null}
          meta={store.isSuccess ? `${rowCounts.length} tables` : 'Store not answering'}
        />
        <StatTile
          label="Newest record"
          value={store.isSuccess ? formatRelative(newest) : null}
          meta={store.isSuccess ? formatTimestamp(newest) : 'Store not answering'}
        />
        <StatTile
          label="Arriving"
          value={
            store.isSuccess ? (
              <Chip tone={arriving ? 'ok' : 'neutral'}>{arriving ? 'Yes' : 'Nothing recent'}</Chip>
            ) : null
          }
          meta={`Within the last ${ARRIVING_WINDOW_MS / 60_000} minutes`}
        />
      </TileRow>

      {store.isSuccess && totalRows === 0 ? (
        <div className="panel">
          <div className="panel-body stack" style={{ gap: 'var(--space-2)' }}>
            <div className="row">
              <Chip tone="warn">Store empty</Chip>
            </div>
            <p className="muted">
              This console is running and answering, and it holds no records yet. Any one of the
              paths below fills it; the last one needs nothing but Docker.
            </p>
          </div>
        </div>
      ) : null}

      {/* 1 */}
      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>1 · Already exporting to OTLP</h2>
          <PathStatus sources={sources} id="otlp" />
        </div>
        <div className="panel-body stack" style={{ gap: 'var(--space-3)', maxWidth: '92ch' }}>
          <p className="muted">
            The console accepts OTLP/HTTP trace exports at the path an exporter already posts to, so
            a pipeline configured with <span className="mono">otlp://</span> reaches it by changing
            only the host. No code change beyond the URI.
          </p>
          <CodeBlock code={OTLP_SNIPPET} label="OTLP sink configuration" />
          <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            Known loss, stated rather than hidden: OTLP carries no{' '}
            <span className="mono">ACTIVATION_START</span>, so activations that arrive only this way
            cannot distinguish a fresh attempt from a resume and are marked as having incomplete
            provenance. <span className="mono">otlp://</span> is valid for{' '}
            <span className="mono">traces_to</span> only — the encoding cannot represent an error
            record or a state snapshot.
          </p>
        </div>
      </section>

      {/* 2 */}
      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>2 · Already exporting to Kafka or BigQuery</h2>
          <PathStatus sources={sources} id="pull" />
        </div>
        <div className="panel-body stack" style={{ gap: 'var(--space-3)', maxWidth: '92ch' }}>
          <p className="muted">
            No pipeline change at all: the console reads what is already being written. Both URIs
            use the grammar the runtime&apos;s own sink resolver parses, so the value can be copied
            verbatim out of the pipeline&apos;s <span className="mono">traces_to</span>.
          </p>
          <CodeBlock code={KAFKA_SNIPPET} label="Kafka source command" />
          <CodeBlock code={BIGQUERY_SNIPPET} label="BigQuery source command" />
          <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            A message that does not decode is counted and skipped, never fatal to the consumer. Both
            clients are imported only when the flag is used, so neither is needed to run the
            console.
          </p>
        </div>
      </section>

      {/* 3 */}
      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>3 · Have a captured run</h2>
          <PathStatus sources={sources} id="bundle" />
        </div>
        <div className="panel-body stack" style={{ gap: 'var(--space-3)', maxWidth: '92ch' }}>
          <p className="muted">
            Import the files <span className="mono">beam-agents-replay</span> already consumes. No
            pipeline, no broker, and no network: a captured incident is inspectable offline.
          </p>
          <BundleImporter onImported={refresh} />
          <CodeBlock code={BUNDLE_SNIPPET} label="bundle import command" />
        </div>
      </section>

      {/* 4 */}
      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>4 · Want the full record</h2>
          <PathStatus sources={sources} id="native" />
        </div>
        <div className="panel-body stack" style={{ gap: 'var(--space-3)', maxWidth: '92ch' }}>
          <p className="muted">
            One constructor argument. The native path carries{' '}
            <span className="mono">event_type</span> as a first-class field, so it delivers{' '}
            <span className="mono">ACTIVATION_START</span> — which is what distinguishes a start
            from a resume — and it accepts errors and snapshots as well as traces.
          </p>
          <CodeBlock code={CONSOLE_SNIPPET} label="console:// sink configuration" />
          <p className="muted" style={{ fontSize: 'var(--text-xs)' }}>
            The sink batches, sends from a background thread, and drops-and-counts when the console
            is unreachable — it never raises, never retries indefinitely, and never applies
            backpressure to the pipeline. Reverting is removing the{' '}
            <span className="mono">sink_resolver</span> argument.
          </p>
        </div>
      </section>

      {/* 5 */}
      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>5 · Just want to see it work</h2>
          <PathStatus sources={sources} id="demo" />
        </div>
        <div className="panel-body stack" style={{ gap: 'var(--space-3)', maxWidth: '92ch' }}>
          <p className="muted">
            One command from a clean machine to a populated console: the compose stack boots this
            console alongside a demo pipeline that produces the full event vocabulary — completions,
            suspensions, approvals, tool errors, cache hits, and TTL wipes.
          </p>
          <CodeBlock code={COMPOSE_SNIPPET} label="docker compose command" />
        </div>
      </section>

      <section className="panel">
        <div className="panel-header">
          <h2 style={{ fontSize: 'var(--text-md)' }}>What this console holds</h2>
          <Button size="sm" onClick={refresh}>
            Refresh
          </Button>
        </div>
        {store.isPending ? (
          <div className="panel-body">
            <Skeleton height="4em" />
          </div>
        ) : store.isError ? (
          <div className="panel-body">
            <p className="muted">
              The store did not answer: {(store.error as Error).message}. The API may be up while
              the database is not readable — check the process&apos;{' '}
              <span className="mono">--db</span> path.
            </p>
          </div>
        ) : (
          <>
            <DataTable
              caption="Row counts by table"
              columns={ROW_COUNT_COLUMNS}
              rows={rowCounts}
              rowKey={([name]) => name}
            />
            <div className="panel-body">
              {/* The database path, size, retention window, and schema version are
                  the store's own report and live on Settings. Repeating them here
                  would be the same five facts, two clicks apart. */}
              <p className="muted">
                Written to <span className="mono">{store.data.database_path}</span>. The full store
                status — size, retention window, schema version — is on{' '}
                <Link href="/settings" style={LINK}>
                  Settings
                </Link>
                .
              </p>
            </div>
          </>
        )}
      </section>
    </div>
  );
}
