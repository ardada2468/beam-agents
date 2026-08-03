/**
 * The live stream's state, readable by any page.
 *
 * `useLiveStream` opens an `EventSource`, so it must be called exactly once —
 * a page calling it again to find out whether the stream is connected would
 * open a second connection to answer a question about the first. `App` owns the
 * single call and publishes the result here.
 *
 * The default is `disabled` rather than `connecting`: a component rendered
 * outside the provider has no stream behind it, and reporting "connecting"
 * would promise one that is never going to arrive.
 */

import { createContext, useContext } from 'react';

import type { LiveState } from './live';

const FALLBACK: LiveState = { status: 'disabled', received: 0, lastEventMs: null };

export const LiveContext = createContext<LiveState>(FALLBACK);

export function useLive(): LiveState {
  return useContext(LiveContext);
}

/** Whether records are actually arriving, which is what motion should mean. */
export function useIsLive(): boolean {
  return useLive().status === 'live';
}
