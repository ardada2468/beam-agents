/**
 * The instant a time window is measured back from.
 *
 * Every windowed page asks "the last N hours of what?", and the two possible
 * answers disagree badly. `_queries.py` already settled it for the overview and
 * wrote down why: the window ends at the newest stored record rather than at
 * `time.time()`, because a store full of records from a run that finished
 * yesterday should not render as an empty window.
 *
 * The other pages did not get that treatment — Errors, Models and Tools each
 * computed `Date.now() - span` in the browser — so one console showed, for the
 * same store and the same "Last 24 hours", an overview reporting hundreds of
 * activations beside an errors page reporting none. Both numbers were correct
 * about different questions, which is the worst way for a console to be wrong.
 *
 * So: anchor on the newest record the store holds, and fall back to the moment
 * the page opened only when the store is empty or has not answered yet, where
 * there is nothing for a window to be relative to anyway.
 *
 * **The anchor must not move under the page.** A `since_ms` that advanced with
 * each arriving record would change the query key on every tick and refetch
 * forever, which is exactly what the pages' own comments were protecting
 * against by freezing `Date.now()`. `staleTime: Infinity` keeps this to one
 * read per mount, so the property those comments wanted survives — it is now
 * just anchored to the data instead of to the clock.
 */

import { useQuery } from '@tanstack/react-query';
import { useRef } from 'react';

import { api, queryKeys } from './api';

export function useWindowAnchor(): number {
  const { data } = useQuery({
    queryKey: queryKeys.store,
    queryFn: () => api.store(),
    staleTime: Infinity,
  });

  // Captured once, so a store that never answers still gives a stable anchor
  // rather than a new one on every render.
  const openedAt = useRef(Date.now()).current;

  return data?.newest_record_ms ?? openedAt;
}
