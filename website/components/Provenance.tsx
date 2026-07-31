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
      className="mt-12 rounded-md border p-4 text-sm"
      style={{ borderColor: 'var(--border)', background: 'var(--bg-subtle)' }}
    >
      <h2
        id="provenance"
        className="mb-2 text-[0.7rem] font-semibold tracking-wider uppercase"
        style={{ color: 'var(--fg-faint)' }}
      >
        What backs this page
      </h2>

      {verifies.length > 0 ? (
        <ul className="space-y-1">
          {verifies.map((assertion) => {
            const [kind, value] = Object.entries(assertion)[0] ?? [];
            if (!kind || typeof value !== 'string') return null;
            return (
              <li key={`${kind}:${value}`} className="flex flex-wrap gap-2">
                <span className="shrink-0" style={{ color: 'var(--fg-faint)' }}>
                  {LABELS[kind] ?? kind}
                </span>
                <a href={assertionLink(kind, value)} className="font-mono text-[0.8rem] break-all">
                  {value}
                </a>
              </li>
            );
          })}
        </ul>
      ) : null}

      {sources.length > 0 ? (
        <>
          <h3
            className="mt-4 mb-2 text-[0.7rem] font-semibold tracking-wider uppercase"
            style={{ color: 'var(--fg-faint)' }}
          >
            Cited sources
          </h3>
          <ol className="space-y-2">
            {sources.map((source) => (
              <li key={source.url} id={sourceAnchor(source.url)}>
                <p>{source.claim}</p>
                <p className="text-[0.8rem]" style={{ color: 'var(--fg-muted)' }}>
                  <a href={source.url} className="break-all">
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
