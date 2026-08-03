/**
 * Theme selection: system, light, or dark.
 *
 * The choice is stamped on `<html data-theme>` and the token file gives that
 * attribute higher precedence than the `prefers-color-scheme` media query, in
 * both directions — so an explicit choice always wins, including choosing light
 * on a machine set to dark.
 */

export type Theme = 'system' | 'light' | 'dark';

const STORAGE_KEY = 'beam-agents-console.theme';

/** Read the stored preference, defaulting to following the system. */
export function readTheme(): Theme {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === 'light' || stored === 'dark' || stored === 'system') return stored;
  } catch {
    // Storage can be unavailable (private mode, embedded frame). Not fatal.
  }
  return 'system';
}

/** Apply `theme` to the document and remember it. */
export function applyTheme(theme: Theme): void {
  const root = document.documentElement;
  if (theme === 'system') {
    root.removeAttribute('data-theme');
  } else {
    root.setAttribute('data-theme', theme);
  }
  try {
    localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // See above.
  }
}

/** Whether the page is currently rendering dark, whatever the preference is. */
export function isDark(): boolean {
  const explicit = document.documentElement.getAttribute('data-theme');
  if (explicit === 'dark') return true;
  if (explicit === 'light') return false;
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}
