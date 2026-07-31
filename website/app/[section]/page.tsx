import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { pagesInSection, sectionFor } from '@/lib/content';
import { SECTIONS } from '@/lib/sections';
import { absoluteUrl, SITE_NAME } from '@/lib/site';
import { StatusBadge } from '@/components/StatusBadge';

interface Params {
  section: string;
}

export async function generateStaticParams(): Promise<Params[]> {
  return SECTIONS.map((section) => ({ section: section.slug }));
}

export async function generateMetadata({ params }: { params: Promise<Params> }): Promise<Metadata> {
  const { section } = await params;
  const def = sectionFor(section);
  if (!def) return {};
  const url = absoluteUrl(`/${def.slug}`);
  return {
    title: def.title,
    description: def.blurb,
    alternates: { canonical: url },
    // The roadmap index is reachable but not indexed: it is a list of things
    // that do not exist, and a search engine surfacing it as documentation
    // would be exactly the misreading it warns about.
    robots: def.inNav ? undefined : { index: false, follow: true },
    openGraph: { title: `${def.title} — ${SITE_NAME}`, description: def.blurb, url },
  };
}

/**
 * A section index.
 *
 * These used to be bordered cards, one per page, which said "these are separate
 * objects". A section index is not that — it is rows of one thing, so it is
 * built on the same hairline-and-eyebrow language as the landing page: a hero
 * band for the section's own header, a rule, then `.list-rule` rows. The status
 * of each page rides on the row, because deciding what to read next depends on
 * knowing what is finished.
 */
export default async function SectionIndex({ params }: { params: Promise<Params> }) {
  const { section } = await params;
  const def = sectionFor(section);
  if (!def) notFound();
  const pages = pagesInSection(section);

  return (
    <>
      <section className="shell pt-12 pb-10 sm:pt-16 sm:pb-12">
        <p className="eyebrow">
          {pages.length} {pages.length === 1 ? 'page' : 'pages'}
        </p>
        <h1 className="h-page mt-4">{def.title}</h1>
        <p className="lede mt-5">{def.blurb}</p>
      </section>

      <section className="rule-top">
        <div className="shell py-10 sm:py-12">
          {pages.length > 0 ? (
            <ul className="list-rule">
              {pages.map((page) => (
                <li key={page.href}>
                  <div className="flex flex-wrap items-baseline justify-between gap-x-5 gap-y-1.5">
                    <Link
                      href={page.href}
                      className="text-[1.05rem] font-semibold no-underline"
                      style={{ color: 'var(--ink)', letterSpacing: '-0.014em' }}
                    >
                      {page.frontmatter.title}
                    </Link>
                    <StatusBadge status={page.frontmatter.status} size="sm" />
                  </div>
                  <p
                    className="mt-1.5 max-w-[74ch] text-[0.93rem]"
                    style={{ color: 'var(--ink-2)' }}
                  >
                    {page.frontmatter.summary}
                  </p>
                </li>
              ))}
            </ul>
          ) : (
            <p style={{ color: 'var(--ink-2)' }}>No pages in this section yet.</p>
          )}
        </div>
      </section>
    </>
  );
}
