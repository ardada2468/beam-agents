/**
 * The application shell: a quiet left rail, a thin top bar, and the page.
 *
 * The chrome is deliberately the least interesting thing on screen. A telemetry
 * viewer's navigation is used once per visit and then ignored for an hour; the
 * data is what should hold attention. So: no icons competing with labels, no
 * colored active state, no collapsed/expanded mode to remember. The active
 * route is marked with a rule and a weight change, and that is all.
 *
 * The one piece of chrome that earns its place is the live indicator. A UI that
 * silently shows stale data because its stream dropped is worse than one that is
 * obviously offline, so connection state is always visible.
 */

import type { ReactNode } from 'react';
import { useEffect, useState } from 'react';
import { Link, useLocation } from 'wouter';

import { Button, Chip } from '@/components/ui';
import { usingFixtures } from '@/lib/fixtures';
import type { LiveState } from '@/lib/live';
import { applyTheme, readTheme, type Theme } from '@/lib/theme';

import './shell.css';

interface NavItem {
  href: string;
  label: string;
  /** Match child routes too, so a detail page keeps its section marked. */
  prefix?: boolean;
}

/**
 * Grouped by the question each section answers, not by data type: "what is
 * happening", "what went wrong", "what is it costing", "who is it running for".
 */
const NAV_GROUPS: { label: string; items: NavItem[] }[] = [
  {
    label: 'Activity',
    items: [
      { href: '/', label: 'Overview' },
      { href: '/activations', label: 'Activations', prefix: true },
      { href: '/traces', label: 'Traces', prefix: true },
    ],
  },
  {
    label: 'Failures',
    items: [
      { href: '/errors', label: 'Errors', prefix: true },
      { href: '/approvals', label: 'Approvals' },
    ],
  },
  {
    label: 'Usage',
    items: [
      { href: '/models', label: 'Models' },
      { href: '/tools', label: 'Tools' },
    ],
  },
  {
    label: 'Data',
    items: [
      { href: '/entities', label: 'Entity keys', prefix: true },
      { href: '/search', label: 'Search' },
      { href: '/connect', label: 'Connect' },
      { href: '/settings', label: 'Settings' },
    ],
  },
];

function isActive(current: string, item: NavItem): boolean {
  if (item.href === '/') return current === '/';
  return item.prefix ? current.startsWith(item.href) : current === item.href;
}

const LIVE_TONE = {
  live: 'ok',
  connecting: 'pending',
  offline: 'error',
  disabled: 'neutral',
} as const;

const LIVE_LABEL = {
  live: 'Live',
  connecting: 'Connecting',
  offline: 'Not live',
  disabled: 'Paused',
} as const;

export function AppShell({ live, children }: { live: LiveState; children: ReactNode }) {
  const [location] = useLocation();
  const [theme, setTheme] = useState<Theme>(() => readTheme());
  const [railOpen, setRailOpen] = useState(false);

  useEffect(() => applyTheme(theme), [theme]);
  useEffect(() => setRailOpen(false), [location]);

  const cycleTheme = () => {
    setTheme((current) =>
      current === 'system' ? 'light' : current === 'light' ? 'dark' : 'system',
    );
  };

  return (
    <div className="shell">
      <a className="shell__skip" href="#main">
        Skip to content
      </a>

      <aside className={`rail${railOpen ? ' rail--open' : ''}`}>
        <div className="rail__brand">
          <Link href="/" className="rail__brand-link">
            <span className="rail__mark" aria-hidden="true" />
            <span>
              <span className="rail__title">Beam Agents</span>
              <span className="rail__subtitle">Console</span>
            </span>
          </Link>
        </div>

        <nav className="rail__nav" aria-label="Sections">
          {NAV_GROUPS.map((group) => (
            <div key={group.label} className="rail__group">
              <p className="rail__group-label eyebrow">{group.label}</p>
              {group.items.map((item) => (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`rail__link${isActive(location, item) ? ' rail__link--active' : ''}`}
                  aria-current={isActive(location, item) ? 'page' : undefined}
                >
                  {item.label}
                </Link>
              ))}
            </div>
          ))}
        </nav>

        {usingFixtures() ? (
          <div className="rail__notice">
            <Chip tone="warn">Fixture data</Chip>
            <p className="muted">
              No console is answering, so this page is showing generated sample records.
            </p>
          </div>
        ) : null}
      </aside>

      <div className="shell__main">
        <header className="topbar">
          <Button
            variant="ghost"
            iconOnly
            className="topbar__menu"
            onClick={() => setRailOpen((open) => !open)}
            aria-label={railOpen ? 'Hide navigation' : 'Show navigation'}
            aria-expanded={railOpen}
          >
            ☰
          </Button>

          <div className="topbar__spacer" />

          <span
            className="topbar__live"
            title={
              live.status === 'live'
                ? `${live.received} events received`
                : 'Records may be out of date'
            }
          >
            <Chip tone={LIVE_TONE[live.status]}>{LIVE_LABEL[live.status]}</Chip>
          </span>

          <Button
            variant="ghost"
            size="sm"
            onClick={cycleTheme}
            aria-label={`Theme: ${theme}. Change theme.`}
            title={`Theme: ${theme}`}
          >
            {theme === 'system' ? 'Auto' : theme === 'light' ? 'Light' : 'Dark'}
          </Button>
        </header>

        <main id="main" className="shell__content">
          {children}
        </main>
      </div>

      {railOpen ? (
        <div className="shell__scrim" onClick={() => setRailOpen(false)} aria-hidden="true" />
      ) : null}
    </div>
  );
}
