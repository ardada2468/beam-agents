import Link from 'next/link';
import { SECTIONS } from '@/lib/sections';
import { REPO_URL, SITE_NAME } from '@/lib/site';
import { ThemeToggle } from './ThemeToggle';
import { SearchBox } from './SearchBox';

export function Header() {
  return (
    <header
      className="sticky top-0 z-20 border-b"
      style={{ borderColor: 'var(--rule)', background: 'var(--paper)' }}
    >
      <div className="shell flex flex-wrap items-center gap-x-6 gap-y-2 py-3">
        <Link
          href="/"
          className="mono text-[0.9rem] font-medium tracking-tight no-underline"
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

        <div className="ml-auto flex items-center gap-3">
          <SearchBox />
          <a
            href={REPO_URL}
            className="text-[0.9rem] no-underline"
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
