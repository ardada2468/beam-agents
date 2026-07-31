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

export default async function SectionIndex({ params }: { params: Promise<Params> }) {
  const { section } = await params;
  const def = sectionFor(section);
  if (!def) notFound();
  const pages = pagesInSection(section);

  return (
    <div className="mx-auto max-w-4xl px-5 py-10">
      <h1 className="text-[1.9rem] leading-tight font-bold tracking-tight">{def.title}</h1>
      <p className="mt-2 max-w-[70ch]" style={{ color: 'var(--fg-muted)' }}>
        {def.blurb}
      </p>

      <ul className="mt-8 space-y-3">
        {pages.map((page) => (
          <li
            key={page.href}
            className="rounded-md border p-4"
            style={{ borderColor: 'var(--border)' }}
          >
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <Link href={page.href} className="text-[1.05rem] font-semibold no-underline">
                {page.frontmatter.title}
              </Link>
              <StatusBadge status={page.frontmatter.status} size="sm" />
            </div>
            <p className="mt-1 text-sm" style={{ color: 'var(--fg-muted)' }}>
              {page.frontmatter.summary}
            </p>
          </li>
        ))}
      </ul>

      {pages.length === 0 ? (
        <p className="mt-8" style={{ color: 'var(--fg-muted)' }}>
          No pages in this section yet.
        </p>
      ) : null}
    </div>
  );
}
