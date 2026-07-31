/**
 * The two preferences the settings page owns, and the hooks that read them.
 *
 * They live next to the page that edits them rather than in `lib/`, because
 * they are settings-page state that two lists happen to read — not a runtime
 * concern. `localStorage` is the store: the console is a single-user local tool
 * with no account to hang a profile off, and a preference that does not survive
 * a reload is not a preference.
 *
 * `useSyncExternalStore` rather than `useState` plus an effect, so a change made
 * on the settings page is visible on a list rendered in the same tree without a
 * reload, and a change made in a second tab arrives through the `storage` event.
 */

import { useCallback, useSyncExternalStore } from 'react';

const PAGE_SIZE_KEY = 'beam-agents-console.page-size';
const LIVE_KEY = 'beam-agents-console.live';

/** How many rows a paged list asks for. Matches the API's own default. */
export const DEFAULT_PAGE_SIZE = 50;

/** The offered page sizes. The API refuses anything above 500. */
export const PAGE_SIZES = [25, 50, 100, 200] as const;

const listeners = new Set<() => void>();

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  window.addEventListener('storage', listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener('storage', listener);
  };
}

function emit(): void {
  for (const listener of listeners) listener();
}

function read(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    // Storage can be unavailable (private mode, embedded frame). Not fatal.
    return null;
  }
}

function write(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // See above: the preference just does not persist.
  }
  emit();
}

function readPageSize(): number {
  const stored = Number(read(PAGE_SIZE_KEY));
  return PAGE_SIZES.includes(stored as (typeof PAGE_SIZES)[number]) ? stored : DEFAULT_PAGE_SIZE;
}

function readLive(): boolean {
  return read(LIVE_KEY) !== 'off';
}

/** The stored page size, and a setter every reader of it sees immediately. */
export function usePageSize(): [number, (size: number) => void] {
  const value = useSyncExternalStore(subscribe, readPageSize, () => DEFAULT_PAGE_SIZE);
  const set = useCallback((size: number) => write(PAGE_SIZE_KEY, String(size)), []);
  return [value, set];
}

/** Whether the live stream should be connected. Defaults to on. */
export function useLivePreference(): [boolean, (enabled: boolean) => void] {
  const value = useSyncExternalStore(subscribe, readLive, () => true);
  const set = useCallback((enabled: boolean) => write(LIVE_KEY, enabled ? 'on' : 'off'), []);
  return [value, set];
}
