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
 *
 * The current page used to be a filled rounded pill, which is the one shape
 * this site does not have. It is marked instead by the rule that runs down the
 * list thickening and going to full ink at that row, plus the weight and colour
 * of the label — three cues, none of which is colour on its own, so the current
 * page stays identifiable in greyscale.
 */
export function Sidebar({ sectionSlug, current }: { sectionSlug: string; current?: string }) {
  const section = SECTION_BY_SLUG.get(sectionSlug);
  const pages = pagesInSection(sectionSlug);
  if (!section || pages.length === 0) return null;

  return (
    <nav aria-label={`${section.title} navigation`} className="text-[0.9rem]">
      <p className="eyebrow">{section.title}</p>
      <ul className="mt-3.5">
        {pages.map((page) => {
          const active = page.slug === current;
          return (
            <li key={page.href}>
              <Link
                href={page.href}
                aria-current={active ? 'page' : undefined}
                className="flex flex-col gap-1 border-l-2 py-1.5 pl-3.5 no-underline"
                style={{
                  borderColor: active ? 'var(--ink)' : 'var(--rule)',
                  color: active ? 'var(--ink)' : 'var(--ink-2)',
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
