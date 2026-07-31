/**
 * Formatting helpers. Every number on screen goes through one of these.
 *
 * The rule these exist to enforce: **`null` is not `0`.** The runtime omits a
 * token count it does not know rather than writing zero, because anything
 * summing them would read a real zero-token call. Every formatter here renders
 * `null`/`undefined` as an em dash, so a missing measurement is visibly missing
 * and cannot be mistaken for a measured zero anywhere in the UI.
 */

/** What a missing measurement looks like. Never `0`, never blank. */
export const EM_DASH = '—';

/** A thousands-separated integer, or an em dash when not recorded. */
export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined) return EM_DASH;
  return value.toLocaleString();
}

/** A compact figure for headline tiles: 1.2k, 3.4M. Exact below 1000. */
export function formatCompact(value: number | null | undefined): string {
  if (value === null || value === undefined) return EM_DASH;
  if (Math.abs(value) < 1000) return value.toLocaleString();
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(
    value,
  );
}

/** A ratio as a percentage. `null` is an em dash — "never cached" and "not measured" differ. */
export function formatRatio(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined) return EM_DASH;
  return `${(value * 100).toFixed(digits)}%`;
}

/**
 * A duration in milliseconds, scaled to a readable unit.
 *
 * Only ever called on a *real* measurement. Span `end_ms - start_ms` is not one:
 * the runtime's spans are zero-width by design, so that difference is always 0
 * and means nothing.
 */
export function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return EM_DASH;
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  if (minutes < 60) return `${minutes}m ${seconds}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

/** An absolute timestamp, to the second, in the viewer's locale. */
export function formatTimestamp(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return EM_DASH;
  return new Date(ms).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

/** Just the wall time, to the second — for dense table columns. */
export function formatTime(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return EM_DASH;
  return new Date(ms).toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

/** How long ago, coarsely: "12s ago", "4m ago", "3h ago". */
export function formatRelative(ms: number | null | undefined, now = Date.now()): string {
  if (ms === null || ms === undefined) return EM_DASH;
  const delta = Math.max(0, now - ms);
  if (delta < 1000) return 'just now';
  if (delta < 60_000) return `${Math.floor(delta / 1000)}s ago`;
  if (delta < 3_600_000) return `${Math.floor(delta / 60_000)}m ago`;
  if (delta < 86_400_000) return `${Math.floor(delta / 3_600_000)}h ago`;
  return `${Math.floor(delta / 86_400_000)}d ago`;
}

/** Bytes at three significant figures. */
export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return EM_DASH;
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? value : value.toFixed(1)} ${units[unit]}`;
}

/**
 * Shorten a hex identifier for a dense column.
 *
 * Head and tail, never head alone: trace and span IDs are `uuid5` derivatives,
 * so two IDs from the same entity key can share a long prefix and a
 * head-truncated pair would look identical.
 */
export function shortId(id: string | null | undefined, head = 8, tail = 4): string {
  if (!id) return EM_DASH;
  if (id.length <= head + tail + 1) return id;
  return `${id.slice(0, head)}…${id.slice(-tail)}`;
}

/**
 * Render an entity key for display.
 *
 * Entity keys are hex-encoded bytes and are very often UTF-8 text — a user ID,
 * an account number — so decoding when it is printable is the difference
 * between a readable list and a wall of hex. Falls back to the hex when it is
 * not, rather than showing replacement characters.
 */
export function formatEntityKey(hex: string | null | undefined): string {
  if (!hex) return EM_DASH;
  try {
    const bytes = Uint8Array.from(hex.match(/.{1,2}/g)?.map((b) => parseInt(b, 16)) ?? []);
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    // Printable ASCII plus common separators only. Anything else reads better
    // as hex than as a string with invisible characters in it.
    if (/^[\x20-\x7e]+$/.test(text)) return text;
  } catch {
    // Not UTF-8. Hex it is.
  }
  return shortId(hex, 12, 4);
}

/** Turn `ACTIVATION_START` into `Activation start` for a label. */
export function humanizeEventType(eventType: string): string {
  const lower = eventType.toLowerCase().replace(/_/g, ' ');
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

/** Turn `ttl_wiped_suspension` into `TTL wiped suspension`. */
export function humanizeReason(reason: string): string {
  const words = reason.split('_');
  const first = words[0] ?? '';
  const head = first === 'ttl' || first === 'hitl' || first === 'llm' ? first.toUpperCase() : first;
  const rest = words.slice(1).join(' ');
  const joined = rest ? `${head} ${rest}` : head;
  return joined.charAt(0).toUpperCase() + joined.slice(1);
}
