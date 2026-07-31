import Link from 'next/link';
import { SECTIONS } from '@/lib/sections';
import { REPO_URL, SITE_NAME } from '@/lib/site';
import { ThemeToggle } from './ThemeToggle';
import { SearchBox } from './SearchBox';

/**
 * The site header.
 *
 * It shares the landing page's `.shell`, so the wordmark sits on exactly the
 * same left edge as the hero headline beneath it, and it holds a deliberate
 * minimum height instead of collapsing to whatever the shortest control is.
 * The only thing separating it from the page is the same hairline every section
 * on the landing page is separated by — there is no shadow and no tinted bar,
 * because a header that floated over the content would be the one card on the
 * site.
 *
 * It wraps rather than collapsing into a menu below `sm`: every section stays
 * reachable with no JavaScript, which is the same constraint
 * `scripts/check_site_ssr.mjs` holds the rest of the site to.
 *
 * `<header>` and `<nav aria-label="Main">` are the page's banner and primary
 * navigation landmarks, and `app/layout.tsx`'s skip link jumps past them to
 * `#main` — so the landmark structure here is load-bearing, not decorative.
 */
export function Header() {
  return (
    <header
      className="sticky top-0 z-20 border-b"
      style={{ borderColor: 'var(--rule)', background: 'var(--paper)' }}
    >
      <div className="shell flex min-h-[3.5rem] flex-wrap items-center gap-x-7 gap-y-2.5 py-2.5">
        <Link
          href="/"
          className="mono text-[0.95rem] font-medium tracking-tight no-underline"
          style={{ color: 'var(--ink)' }}
        >
          {SITE_NAME}
        </Link>

        <nav
          aria-label="Main"
          className="flex flex-wrap items-center gap-x-5 gap-y-1 text-[0.9rem]"
        >
          {SECTIONS.filter((section) => section.inNav).map((section) => (
            <Link
              key={section.slug}
              href={`/${section.slug}`}
              className="no-underline"
              style={{ color: 'var(--ink-2)' }}
            >
              {section.title}
            </Link>
          ))}
          <Link href="/api" className="no-underline" style={{ color: 'var(--ink-2)' }}>
            API
          </Link>
        </nav>

        <div className="ml-auto flex items-center gap-4">
          <SearchBox />
          <a
            href={REPO_URL}
            className="text-[0.9rem] whitespace-nowrap no-underline"
            style={{ color: 'var(--ink-2)' }}
          >
            GitHub
          </a>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
