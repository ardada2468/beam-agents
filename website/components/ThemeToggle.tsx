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
 *
 * `data-theme` on the root element is the contract: `globals.css` defines
 * `:root[data-theme='light']` and `:root[data-theme='dark']` blocks precisely
 * so an explicit choice beats the media query in both directions. Removing the
 * attribute — not setting it to some third value — is what hands control back.
 *
 * It is drawn as a `.chip`, the site's flat outlined token, so it sits in the
 * header as one of a set with the search field rather than as a lone pill.
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
      className="chip cursor-pointer"
      // `.chip` is sized for a label sitting in text; here it is a control
      // standing beside the search field, and a chip's height next to a field's
      // reads as two mismatched widgets rather than one set. `align-self`
      // overrides the header's `items-center` so this button takes the row's
      // full height — measured rather than hardcoded, so it stays right if
      // `.field` is ever re-padded. The stronger rule matches the field too.
      style={{ borderColor: 'var(--rule-2)', alignSelf: 'stretch', paddingInline: '0.6rem' }}
      aria-label={`Color theme: ${LABEL[choice]}. Activate to change.`}
    >
      {LABEL[choice]}
    </button>
  );
}
