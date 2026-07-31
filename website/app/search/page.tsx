import type { Metadata } from 'next';
import Link from 'next/link';
import { searchServerSide } from '@/lib/search';
import { absoluteUrl } from '@/lib/site';

/**
 * Server-rendered search.
 *
 * This route is the no-JavaScript path: the header's search box is a real GET
 * form, and this page answers it with results already in the HTML. It carries
 * `noindex` because a search-results URL is not content.
 */
export const metadata: Metadata = {
  title: 'Search',
  description: 'Search the beam-agents documentation.',
  alternates: { canonical: absoluteUrl('/search') },
  robots: { index: false, follow: true },
};

// Rendered per request, not pre-rendered: the results depend on `?q=`, and a
// statically-generated page would answer every query with an empty list. This
// is the one route on the site that is genuinely dynamic.
export const dynamic = 'force-dynamic';

export default async function SearchPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const raw = params.q;
  const query = (Array.isArray(raw) ? raw[0] : raw)?.trim() ?? '';
  const results = query ? searchServerSide(query) : [];

  return (
    <div className="mx-auto max-w-3xl px-5 py-10">
      <h1 className="text-[1.9rem] leading-tight font-bold tracking-tight">Search</h1>

      <form action="/search" method="get" role="search" className="mt-5 flex gap-2">
        <label htmlFor="q" className="sr-only">
          Search query
        </label>
        <input
          id="q"
          name="q"
          type="search"
          defaultValue={query}
          className="w-full rounded border px-3 py-2"
          style={{ borderColor: 'var(--border)', background: 'var(--bg)', color: 'var(--fg)' }}
        />
        <button
          type="submit"
          className="rounded border px-3 py-2"
          style={{ borderColor: 'var(--border)', color: 'var(--fg)' }}
        >
          Search
        </button>
      </form>

      {query ? (
        <p className="mt-4 text-sm" style={{ color: 'var(--fg-muted)' }}>
          {results.length} result{results.length === 1 ? '' : 's'} for &ldquo;{query}&rdquo;
        </p>
      ) : null}

      <ul className="mt-4 space-y-3">
        {results.map((result) => (
          <li
            key={result.href}
            className="rounded-md border p-3"
            style={{ borderColor: 'var(--border)' }}
          >
            <Link href={result.href} className="font-semibold no-underline">
              {result.title}
            </Link>
            <span className="ml-2 text-xs" style={{ color: 'var(--fg-faint)' }}>
              {result.section} · {result.status}
            </span>
            <p className="mt-1 text-sm" style={{ color: 'var(--fg-muted)' }}>
              {result.summary}
            </p>
          </li>
        ))}
      </ul>

      {query && results.length === 0 ? (
        <p className="mt-6" style={{ color: 'var(--fg-muted)' }}>
          Nothing matched. Try a single term — the fallback search requires every word to appear.
        </p>
      ) : null}
    </div>
  );
}
