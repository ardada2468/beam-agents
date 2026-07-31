/**
 * The live-stream hook: keep the open page current while a pipeline runs.
 *
 * Two things this deliberately does *not* do.
 *
 * It does not poll. A list endpoint refetched fast enough to feel live
 * multiplies query cost by the number of open tabs to deliver mostly-unchanged
 * pages.
 *
 * And it does not put stream payloads into the cache. Events carry identity
 * only — which activation changed — and the hook invalidates precisely the
 * queries that identity touches. Merging pushed partial records into cached
 * lists would mean two code paths building the same object, and the pushed one
 * would be the one nobody tests.
 *
 * A dropped connection is surfaced, never hidden. Silently showing stale data
 * as if it were live is the specific failure this hook exists to prevent.
 */

import { useQueryClient } from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';

import type { LiveEvent } from './api-types';

export type LiveStatus = 'connecting' | 'live' | 'offline' | 'disabled';

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
/** Coalesce a burst of events into one invalidation pass. */
const FLUSH_INTERVAL_MS = 400;

export interface LiveState {
  status: LiveStatus;
  /** Events received since the connection opened. Shown in the status control. */
  received: number;
  lastEventMs: number | null;
}

/**
 * Subscribe to the console's event stream and invalidate what it touches.
 *
 * Pass `enabled: false` to hold the connection closed — the settings page uses
 * it to let someone freeze the view while they read a table.
 */
export function useLiveStream(enabled = true): LiveState {
  const queryClient = useQueryClient();
  const [state, setState] = useState<LiveState>({
    status: enabled ? 'connecting' : 'disabled',
    received: 0,
    lastEventMs: null,
  });
  const pending = useRef<LiveEvent[]>([]);

  useEffect(() => {
    if (!enabled) {
      setState((s) => ({ ...s, status: 'disabled' }));
      return;
    }

    let source: EventSource | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    let closed = false;

    const flush = () => {
      const batch = pending.current;
      if (batch.length === 0) return;
      pending.current = [];

      const activations = new Set<string>();
      const kinds = new Set<string>();
      for (const event of batch) {
        kinds.add(event.kind);
        if (event.entity_key && event.seq !== null) {
          activations.add(`${event.entity_key}/${event.seq}`);
        }
      }

      // Lists and aggregates always move; a detail view only if its own
      // activation was touched.
      void queryClient.invalidateQueries({ queryKey: ['activations'] });
      void queryClient.invalidateQueries({ queryKey: ['overview'] });
      void queryClient.invalidateQueries({ queryKey: ['entities'] });
      void queryClient.invalidateQueries({ queryKey: ['store'] });
      if (kinds.has('error')) {
        void queryClient.invalidateQueries({ queryKey: ['errors'] });
        void queryClient.invalidateQueries({ queryKey: ['errorGroups'] });
      }
      for (const key of activations) {
        const [entityKey, seq] = key.split('/');
        void queryClient.invalidateQueries({
          queryKey: ['activation', entityKey, Number(seq)],
        });
      }
    };

    const interval = setInterval(flush, FLUSH_INTERVAL_MS);

    const connect = () => {
      if (closed) return;
      setState((s) => ({ ...s, status: attempt === 0 ? 'connecting' : s.status }));
      source = new EventSource('/api/stream');

      source.onopen = () => {
        attempt = 0;
        setState((s) => ({ ...s, status: 'live' }));
      };

      source.onmessage = (message: MessageEvent<string>) => {
        try {
          pending.current.push(JSON.parse(message.data) as LiveEvent);
        } catch {
          // A frame we cannot parse is not a reason to tear down the stream.
          return;
        }
        setState((s) => ({
          status: 'live',
          received: s.received + 1,
          lastEventMs: Date.now(),
        }));
      };

      source.onerror = () => {
        source?.close();
        source = null;
        setState((s) => ({ ...s, status: 'offline' }));
        if (closed) return;
        attempt += 1;
        const delay = Math.min(RECONNECT_BASE_MS * 2 ** (attempt - 1), RECONNECT_MAX_MS);
        reconnectTimer = setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      closed = true;
      clearInterval(interval);
      if (reconnectTimer) clearTimeout(reconnectTimer);
      source?.close();
    };
  }, [enabled, queryClient]);

  return state;
}
