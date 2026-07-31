'use client';

import { useEffect, useState } from 'react';

type Choice = 'system' | 'light' | 'dark';

const NEXT: Record<Choice, Choice> = { system: 'light', light: 'dark', dark: 'system' };
const LABEL: Record<Choice, string> = { system: 'System', light: 'Light', dark: 'Dark' };

/**
 * Theme control.
 *
 * Purely additive: with JavaScript disabled the button never renders and the
 * page still follows `prefers-color-scheme`. Nothing about readability depends
 * on this component running.
 */
export function ThemeToggle() {
  const [choice, setChoice] = useState<Choice>('system');
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const stored = window.localStorage.getItem('theme');
    if (stored === 'light' || stored === 'dark') setChoice(stored);
  }, []);

  useEffect(() => {
    if (!mounted) return;
    const root = document.documentElement;
    if (choice === 'system') {
      root.removeAttribute('data-theme');
      window.localStorage.removeItem('theme');
    } else {
      root.setAttribute('data-theme', choice);
      window.localStorage.setItem('theme', choice);
    }
  }, [choice, mounted]);

  if (!mounted) return null;

  return (
    <button
      type="button"
      onClick={() => setChoice(NEXT[choice])}
      className="rounded border px-2 py-1 text-xs"
      style={{ borderColor: 'var(--border)', color: 'var(--fg-muted)' }}
      aria-label={`Color theme: ${LABEL[choice]}. Activate to change.`}
    >
      {LABEL[choice]}
    </button>
  );
}
