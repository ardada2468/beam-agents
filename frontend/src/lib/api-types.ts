/**
 * The TypeScript mirror of `src/beam_agents/console/_dto.py`.
 *
 * Hand-written rather than generated, because the generated output for a schema
 * with this many optional numeric fields is unreadable and the whole point of
 * these types is that a person reads them while building a page. The Python side
 * exposes `model_json_schemas()` so the two can be checked against each other
 * rather than trusted to be edited together.
 *
 * Two conventions carried over verbatim from the Python:
 *
 * - **Milliseconds, always, and always `*_ms`.** No `Date`, no ISO strings. The
 *   runtime's protos are int64 epoch millis throughout and a second
 *   representation would disagree with the first the moment a timezone got
 *   involved.
 * - **`null` is "not recorded"; `0` is a measured zero.** The runtime already
 *   omits token counts it does not know rather than writing zero, because
 *   anything summing them would read a real zero-token call. That distinction
 *   has to survive to the screen: render `null` as an em dash, never as `0`.
 */

export type ActivationStatus = 'completed' | 'suspended' | 'error' | 'in_flight';
export type ActivationKind = 'start' | 'resume' | 'unknown';
export type Provenance = 'native' | 'otlp' | 'kafka' | 'bigquery' | 'bundle';
export type ApprovalDecision = 'approved' | 'denied' | 'expired' | 'pending';

/** One page of a keyset-paginated list. `nextCursor` is null exactly when exhausted. */
export interface Page<T> {
  items: T[];
  next_cursor: string | null;
  /** Exact when cheap to compute, otherwise null. Never an estimate. */
  total: number | null;
}

/**
 * One trace event with its complete attribute map.
 *
 * `start_ms` and `end_ms` are equal for every event this runtime produces:
 * spans are zero-width by design so the hot path never reads a wall clock.
 * Their difference is not a duration and must never be rendered as one.
 */
export interface EventRecord {
  trace_id: string;
  span_id: string;
  parent_span_id: string;
  entity_key: string;
  seq: number;
  step_index: number;
  event_type: string;
  start_ms: number;
  end_ms: number;
  attributes: Record<string, string>;
  provenance: Provenance;
}

/**
 * A node in an activation's span tree.
 *
 * There is deliberately no width or duration field. The runtime does not
 * measure one, so the API does not offer one to render.
 */
export interface SpanNode {
  span_id: string;
  parent_span_id: string;
  role: string;
  step_index: number;
  depth: number;
  order: number;
  events: EventRecord[];
}

/** One attempt within an activation. A suspend and its resume are two attempts, one activation. */
export interface AttemptSummary {
  span_id: string;
  kind: ActivationKind;
  entry_step_index: number;
  start_ms: number;
  end_ms: number | null;
  status: ActivationStatus;
}

/** A tool intent staged by an activation. */
export interface IntentSummary {
  intent_id: string;
  tool_name: string;
  intent_kind: string;
  step_index: number;
  expires_at_ms: number | null;
  emitted_at_ms: number;
}

/**
 * One activation error.
 *
 * `failure_*` are the position scalars the runtime computes for the routes that
 * can reach a context. They are null on the routes that cannot — a real
 * distinction, not a default, so null renders as "not available" rather than 0.
 */
export interface ErrorRecord {
  entity_key: string;
  seq: number | null;
  reason: string;
  detail: string;
  error_type: string | null;
  event_time_ms: number;
  failure_step: number | null;
  failure_last_event: string | null;
  failure_staged_intents: number | null;
  failure_llm_calls: number | null;
}

/**
 * An activation's derived rollup — the primary list object.
 *
 * `wall_ms` is the only duration, and it is real: the gap between the
 * ACTIVATION_START and ACTIVATION_END clock reads. Null while in flight.
 */
export interface ActivationSummary {
  entity_key: string;
  seq: number;
  trace_id: string;
  status: ActivationStatus;
  kind: ActivationKind;
  attempts: number;
  started_ms: number;
  ended_ms: number | null;
  wall_ms: number | null;
  model: string | null;
  llm_calls: number;
  tool_calls: number;
  intents: number;
  errors: number;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  total_tokens: number | null;
  cache_hits: number;
  tools: string[];
  reasons: string[];
  provenance: Provenance[];
  /** False when assembled only from lossy sources, so start-vs-resume is unknown. */
  complete_provenance: boolean;
}

/** A state snapshot's countable metadata. The image itself stays opaque. */
export interface SnapshotSummary {
  entity_key: string;
  seq: number;
  snapshot_at_ms: number;
  state_schema_version: number;
  request_id: string;
  memory_entries: number;
  memory_bytes: number;
  llm_cache_entries: number;
  pending_intent_ids: string[];
  continuation_step_index: number | null;
  continuation_deadline_ms: number | null;
  continuation_adapter: string;
}

/** Everything recorded about one activation. */
export interface ActivationDetail {
  summary: ActivationSummary;
  attempts: AttemptSummary[];
  spans: SpanNode[];
  events: EventRecord[];
  intents: IntentSummary[];
  errors: ErrorRecord[];
  snapshot: SnapshotSummary | null;
  replay_command: string | null;
}

/** A trace, which is exactly one activation scope. */
export interface TraceSummary {
  trace_id: string;
  entity_key: string;
  seq: number;
  events: number;
  spans: number;
  started_ms: number;
  ended_ms: number | null;
  status: ActivationStatus;
}

/** A trace with its assembled span tree. */
export interface TraceDetail {
  summary: TraceSummary;
  roots: SpanNode[];
  attempts: AttemptSummary[];
}

/** One point in a time-bucketed series. `bucket_ms` is the bucket's inclusive start. */
export interface BucketPoint {
  bucket_ms: number;
  value: number;
}

/** Per-model usage. `cache_hit_ratio` is null when nothing recorded a cache attribute at all. */
export interface ModelSummary {
  model: string;
  calls: number;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  total_tokens: number | null;
  cache_hits: number;
  cache_hit_ratio: number | null;
  errors: number;
  max_attempts: number | null;
  circuit_states: Record<string, number>;
}

/** Per-tool activity, across both inline calls and staged intents. */
export interface ToolSummary {
  tool_name: string;
  calls: number;
  intents: number;
  errors: number;
  failure_ratio: number | null;
  last_seen_ms: number | null;
}

/** A human-approval intent and whatever is known about its resolution. */
export interface ApprovalSummary {
  intent_id: string;
  entity_key: string;
  seq: number;
  tool_name: string;
  step_index: number;
  requested_ms: number;
  deadline_ms: number | null;
  expires_at_ms: number | null;
  escalations: number;
  decision: ApprovalDecision;
  decided_ms: number | null;
}

/** One entity key's activity across every sequence number it has run. */
export interface EntitySummary {
  entity_key: string;
  activations: number;
  first_seen_ms: number;
  last_seen_ms: number;
  errors: number;
  total_tokens: number | null;
  latest_seq: number | null;
  latest_status: ActivationStatus | null;
}

/** Errors sharing a reason and error type. */
export interface ErrorGroup {
  reason: string;
  error_type: string | null;
  count: number;
  entities: number;
  first_seen_ms: number;
  last_seen_ms: number;
  series: BucketPoint[];
  sample_detail: string;
}

/** One attribute-search result, pointing at where it was found. */
export interface SearchHit {
  kind: 'activation' | 'event' | 'error' | 'entity';
  entity_key: string;
  seq: number | null;
  trace_id: string | null;
  span_id: string | null;
  label: string;
  matched_field: string;
  matched_value: string;
  at_ms: number;
}

/** What the store currently holds, and the window it holds it for. */
export interface StoreStatus {
  row_counts: Record<string, number>;
  retention_hours: number | null;
  database_path: string;
  database_bytes: number | null;
  oldest_record_ms: number | null;
  newest_record_ms: number | null;
  schema_version: number;
}

/** The headline figures and series the landing page renders. */
export interface Overview {
  window_ms: number;
  activations: number;
  completed: number;
  suspended: number;
  in_flight: number;
  errors: number;
  error_ratio: number | null;
  total_tokens: number | null;
  llm_calls: number;
  tool_calls: number;
  cache_hit_ratio: number | null;
  p50_wall_ms: number | null;
  p95_wall_ms: number | null;
  activation_series: BucketPoint[];
  error_series: BucketPoint[];
  token_series: BucketPoint[];
  top_models: ModelSummary[];
  top_tools: ToolSummary[];
  recent_errors: ErrorRecord[];
  store: StoreStatus | null;
}

/** Liveness, answered before any record has been ingested. */
export interface Health {
  status: 'ok';
  version: string;
  schema_version: number;
  ui_bundled: boolean;
  sources: string[];
}

/** A live-stream notification. Identity only — the client refetches. */
export interface LiveEvent {
  kind: string;
  entity_key: string;
  seq: number | null;
  trace_id: string;
  count: number;
}

/** The filters the activation list composes. Every field conjoins with the others. */
export interface ActivationFilters {
  entity_key?: string;
  status?: ActivationStatus;
  kind?: ActivationKind;
  model?: string;
  tool?: string;
  reason?: string;
  since_ms?: number;
  until_ms?: number;
  query?: string;
}
