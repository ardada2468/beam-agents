import type { Metadata } from 'next';
import Link from 'next/link';

/**
 * The 404 page, in the site's own idiom: an eyebrow, a display headline, and
 * the routes a lost reader most likely wanted. Dead-lettering vocabulary is
 * the one joke this site is allowed — a miss here really is an element no
 * route could process.
 */

export const metadata: Metadata = {
  title: 'Not found',
  robots: { index: false },
};

export default function NotFound() {
  return (
    <section className="shell band band--lead">
      <p className="eyebrow" style={{ color: 'var(--s-errors)' }}>
        404 · dead letter
      </p>
      <h1 className="display mt-5 max-w-[18ch]">No route processed this element.</h1>
      <p className="lede mt-6 max-w-[52ch]">
        The page you asked for does not exist — it may have moved when a roadmap item shipped and
        its page was reclassified.
      </p>
      <div className="mt-9 flex flex-wrap items-center gap-3">
        <Link href="/" className="btn btn--primary">
          Back to the start
        </Link>
        <Link href="/learn/what-is-beam-agents" className="btn btn--secondary">
          Read the concepts
        </Link>
        <Link href="/search" className="btn" style={{ color: 'var(--ink-2)' }}>
          Search the site
        </Link>
      </div>
    </section>
  );
}
