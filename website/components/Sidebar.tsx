import Link from 'next/link';
import { pagesInSection } from '@/lib/content';
import { SECTION_BY_SLUG } from '@/lib/sections';
import { StatusBadge } from './StatusBadge';

/**
 * Section navigation.
 *
 * Every entry shows its page's maturity status. A reader scanning the sidebar
 * can see what is finished before clicking anything, which is the whole reason
 * the badge is here and not only on the page.
 */
export function Sidebar({ sectionSlug, current }: { sectionSlug: string; current?: string }) {
  const section = SECTION_BY_SLUG.get(sectionSlug);
  const pages = pagesInSection(sectionSlug);
  if (!section || pages.length === 0) return null;

  return (
    <nav aria-label={`${section.title} navigation`} className="text-sm">
      <p
        className="mb-2 text-[0.7rem] font-semibold tracking-wider uppercase"
        style={{ color: 'var(--fg-faint)' }}
      >
        {section.title}
      </p>
      <ul className="space-y-1">
        {pages.map((page) => {
          const active = page.slug === current;
          return (
            <li key={page.href}>
              <Link
                href={page.href}
                aria-current={active ? 'page' : undefined}
                className="flex items-baseline justify-between gap-2 rounded px-2 py-1 no-underline"
                style={{
                  background: active ? 'var(--accent-subtle)' : undefined,
                  color: active ? 'var(--fg)' : 'var(--fg-muted)',
                  fontWeight: active ? 600 : 400,
                }}
              >
                <span>{page.frontmatter.title}</span>
                <StatusBadge status={page.frontmatter.status} size="sm" />
              </Link>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
