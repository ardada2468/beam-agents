/**
 * The typed API client. One function per endpoint, no ad-hoc `fetch` anywhere
 * else in the app.
 *
 * `request` is also the single seam the fixture interceptor hooks (see
 * `fixtures.ts`), which is what lets every page be built and screenshotted with
 * no backend running. Bypassing it with a raw `fetch` breaks that, so don't.
 */

import type {
  ActivationDetail,
  ActivationFilters,
  ActivationSummary,
  ApprovalSummary,
  EntitySummary,
  ErrorGroup,
  ErrorRecord,
  Health,
  ModelSummary,
  Overview,
  Page,
  SearchHit,
  StoreStatus,
  ToolSummary,
  TraceDetail,
  TraceSummary,
} from './api-types';

/** A non-2xx response, carrying the status and whatever detail the server gave. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly detail?: unknown,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

type Params = Record<string, string | number | boolean | undefined | null>;

function toQuery(params: Params = {}): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, String(value));
    }
  }
  const query = search.toString();
  return query ? `?${query}` : '';
}

/**
 * Fetch `path`, parse JSON, and raise `ApiError` on a non-2xx.
 *
 * Exported because the fixture interceptor replaces it wholesale in dev. It is
 * the only place in the app that talks to the network.
 */
export async function request<T>(path: string, params?: Params, init?: RequestInit): Promise<T> {
  const response = await fetch(`${path}${toQuery(params)}`, {
    headers: { accept: 'application/json' },
    ...init,
  });
  if (!response.ok) {
    let detail: unknown;
    try {
      detail = await response.json();
    } catch {
      detail = await response.text().catch(() => undefined);
    }
    throw new ApiError(response.status, `${response.status} ${response.statusText}`, detail);
  }
  return (await response.json()) as T;
}

/** Default lookback for views that take a window: 24 hours. */
export const DEFAULT_WINDOW_MS = 24 * 60 * 60 * 1000;

export const api = {
  health: () => request<Health>('/healthz'),

  overview: (windowMs: number = DEFAULT_WINDOW_MS, buckets = 48) =>
    request<Overview>('/api/overview', { window_ms: windowMs, buckets }),

  activations: (filters: ActivationFilters = {}, cursor?: string, limit = 50) =>
    request<Page<ActivationSummary>>('/api/activations', { ...filters, cursor, limit }),

  activation: (entityKey: string, seq: number) =>
    request<ActivationDetail>(
      `/api/activations/${encodeURIComponent(entityKey)}/${encodeURIComponent(seq)}`,
    ),

  traces: (query?: string, cursor?: string, limit = 50) =>
    request<Page<TraceSummary>>('/api/traces', { query, cursor, limit }),

  trace: (traceId: string) => request<TraceDetail>(`/api/traces/${encodeURIComponent(traceId)}`),

  errors: (
    params: { reason?: string; entity_key?: string; since_ms?: number } = {},
    cursor?: string,
    limit = 50,
  ) => request<Page<ErrorRecord>>('/api/errors', { ...params, cursor, limit }),

  errorGroups: (sinceMs?: number, bucketMs?: number) =>
    request<ErrorGroup[]>('/api/errors/groups', { since_ms: sinceMs, bucket_ms: bucketMs }),

  models: (sinceMs?: number) => request<ModelSummary[]>('/api/models', { since_ms: sinceMs }),

  tools: (sinceMs?: number) => request<ToolSummary[]>('/api/tools', { since_ms: sinceMs }),

  approvals: (pendingOnly = false, limit = 100) =>
    request<ApprovalSummary[]>('/api/approvals', { pending_only: pendingOnly, limit }),

  entities: (cursor?: string, limit = 50) =>
    request<Page<EntitySummary>>('/api/entities', { cursor, limit }),

  search: (query: string, limit = 50) => request<SearchHit[]>('/api/search', { q: query, limit }),

  store: () => request<StoreStatus>('/api/store'),
};

/**
 * Query keys for TanStack Query.
 *
 * Centralized so the live-stream hook can invalidate precisely what an ingest
 * event touched instead of blowing the whole cache away on every event — which,
 * with a pipeline running, would be a refetch storm.
 */
export const queryKeys = {
  health: ['health'] as const,
  overview: (windowMs: number) => ['overview', windowMs] as const,
  activations: (filters: ActivationFilters) => ['activations', filters] as const,
  activation: (entityKey: string, seq: number) => ['activation', entityKey, seq] as const,
  traces: (query?: string) => ['traces', query ?? ''] as const,
  trace: (traceId: string) => ['trace', traceId] as const,
  errors: (params: Record<string, unknown>) => ['errors', params] as const,
  errorGroups: (sinceMs?: number) => ['errorGroups', sinceMs ?? 0] as const,
  models: (sinceMs?: number) => ['models', sinceMs ?? 0] as const,
  tools: (sinceMs?: number) => ['tools', sinceMs ?? 0] as const,
  approvals: (pendingOnly: boolean) => ['approvals', pendingOnly] as const,
  entities: ['entities'] as const,
  search: (query: string) => ['search', query] as const,
  store: ['store'] as const,
};
