'use client';

import Link from 'next/link';
import { useEffect } from 'react';

/**
 * The client error boundary. It must be a client component (Next.js
 * requirement), so it keeps to what needs no data: name the failure honestly,
 * offer a retry, and offer the way home. No error details are rendered — a
 * digest in the console is for whoever debugs it, not for the page.
 */

export default function ErrorPage({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    // Surface the digest where a bug report can find it.
    console.error(error);
  }, [error]);

  return (
    <section className="shell band band--lead">
      <p className="eyebrow" style={{ color: 'var(--s-errors)' }}>
        Error · activation failed
      </p>
      <h1 className="display mt-5 max-w-[18ch]">This page raised.</h1>
      <p className="lede mt-6 max-w-[52ch]">
        Something went wrong rendering this page. Retrying is safe — nothing here commits state.
      </p>
      <div className="mt-9 flex flex-wrap items-center gap-3">
        <button type="button" onClick={reset} className="btn btn--primary">
          Retry
        </button>
        <Link href="/" className="btn btn--secondary">
          Back to the start
        </Link>
      </div>
    </section>
  );
}
