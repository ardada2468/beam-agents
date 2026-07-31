import { Fragment } from 'react';
import type { Assertion, Source } from '@/lib/schema';
import { repoFileUrl } from '@/lib/site';
import { sourceAnchor } from '@/lib/sources';

/**
 * What backs this page.
 *
 * Every assertion here is resolved against the repository by
 * `scripts/verify_docs_claims.py` before the site can build, so this block is
 * not a bibliography — it is the list of things that must remain true for the
 * page to be publishable at all. Showing it to readers is the point: it is how
 * someone checks the page instead of trusting it.
 *
 * That makes it the site's proudest feature, and it used to be rendered as a
 * grey rounded card, which is how a page marks the part nobody is meant to
 * read. It is now a hairline-topped block in the footer's idiom — a mono
 * definition list under an eyebrow — held to the prose measure so it reads as
 * the end of the article rather than as an attachment to it.
 */

const LABELS: Record<string, string> = {
  symbol: 'Symbol',
  module: 'Source',
  spec: 'Specification',
  test: 'Test',
  example: 'Example',
};

function assertionLink(kind: string, value: string): string {
  if (kind === 'symbol') return `/api/${value.split('.').pop() ?? value}`;
  if (kind === 'example') return repoFileUrl(`website/examples/${value}`);
  if (kind === 'test') return repoFileUrl(value.split('::')[0] ?? value);
  return repoFileUrl(value);
}

export function Provenance({
  verifies,
  sources,
}: {
  verifies: readonly Assertion[];
  sources: readonly Source[];
}) {
  if (verifies.length === 0 && sources.length === 0) return null;

  return (
    <section
      aria-labelledby="provenance"
      className="panel--flush mt-14"
      style={{ maxWidth: 'var(--measure)' }}
    >
      <h2 id="provenance" className="eyebrow">
        What backs this page
      </h2>

      {verifies.length > 0 ? (
        <dl className="mt-4 grid gap-x-6 gap-y-2 text-[0.85rem] sm:grid-cols-[7.5rem_minmax(0,1fr)]">
          {verifies.map((assertion) => {
            const [kind, value] = Object.entries(assertion)[0] ?? [];
            if (!kind || typeof value !== 'string') return null;
            return (
              <Fragment key={`${kind}:${value}`}>
                {/* Stacked on a phone, so each label needs more air above it
                    than below it or it groups with the wrong value. */}
                <dt className="eyebrow mt-1.5 sm:mt-0 sm:pt-0.5">{LABELS[kind] ?? kind}</dt>
                <dd className="min-w-0">
                  <a href={assertionLink(kind, value)} className="mono break-all">
                    {value}
                  </a>
                </dd>
              </Fragment>
            );
          })}
        </dl>
      ) : null}

      {sources.length > 0 ? (
        <>
          <h3 className="eyebrow mt-9">Cited sources</h3>
          <ol className="mt-4 space-y-3.5 text-[0.9rem]">
            {sources.map((source) => (
              <li key={source.url} id={sourceAnchor(source.url)}>
                <p>{source.claim}</p>
                <p className="mt-1 text-[0.8rem]" style={{ color: 'var(--ink-3)' }}>
                  <a href={source.url} className="mono break-all">
                    {source.url}
                  </a>{' '}
                  — retrieved <time dateTime={source.retrieved}>{source.retrieved}</time>
                </p>
              </li>
            ))}
          </ol>
        </>
      ) : null}
    </section>
  );
}
