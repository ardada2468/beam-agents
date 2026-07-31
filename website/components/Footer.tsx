import { Fragment, type ReactNode } from 'react';
import Link from 'next/link';
import { SECTIONS } from '@/lib/sections';
import {
  ASF_DISCLAIMER,
  LICENSE,
  PACKAGE_VERSION,
  REPO_URL,
  SITE_NAME,
  SITE_TAGLINE,
} from '@/lib/site';

/**
 * The footer carries the non-affiliation statement on every page.
 *
 * It is not tucked into an about page because the confusion it prevents — that
 * a project named after Apache Beam and licensed Apache-2.0 might be an ASF
 * project — is available on every page, so the correction should be too.
 * `scripts/check_docs_prose.py` enforces the same distinction in prose; this is
 * the standing half of it, and it may not be moved off a page.
 *
 * The three facts on the left are the ones a reader needs before deciding what
 * to do with the project — what version exists, under what licence, and where
 * the source is — so they are set as a definition list on a hairline grid
 * rather than run together as a byline. `0.0.0` reads as unreleased and says
 * so; `scripts/verify_docs_claims.py` checks that against the package.
 */

/**
 * The two destinations that are not sections. The roadmap is reachable here but
 * stays out of `SECTIONS`'s primary nav, because it documents nothing that
 * exists; the API reference is generated rather than authored, so it has no
 * content directory to be a section of.
 */
const EXTRA_LINKS = [
  { href: '/roadmap', label: 'Roadmap' },
  { href: '/api', label: 'API reference' },
] as const;

export function Footer() {
  const facts: readonly { term: string; value: ReactNode }[] = [
    { term: 'Version', value: `${PACKAGE_VERSION} · unreleased` },
    { term: 'License', value: LICENSE },
    { term: 'Source', value: <a href={REPO_URL}>github.com/ardada2468/beam-agents</a> },
  ];

  const links = [
    ...SECTIONS.filter((section) => section.inNav).map((section) => ({
      href: `/${section.slug}`,
      label: section.title,
    })),
    ...EXTRA_LINKS,
  ];

  return (
    <footer className="rule-top mt-20">
      <div className="shell py-12">
        <div className="grid gap-x-14 gap-y-10 md:grid-cols-[minmax(0,23rem)_1fr]">
          <div>
            <p className="mono text-[0.95rem] font-medium" style={{ color: 'var(--ink)' }}>
              {SITE_NAME}
            </p>
            <p className="mt-2 max-w-[42ch] text-[0.85rem]" style={{ color: 'var(--ink-3)' }}>
              {SITE_TAGLINE}
            </p>

            <dl className="mt-6 grid grid-cols-[5.5rem_minmax(0,1fr)] gap-x-6">
              {facts.map((fact) => (
                <Fragment key={fact.term}>
                  <dt
                    className="eyebrow border-t py-2.5 leading-5"
                    style={{ borderColor: 'var(--rule)' }}
                  >
                    {fact.term}
                  </dt>
                  <dd
                    className="border-t py-2.5 text-[0.85rem] leading-5"
                    style={{ borderColor: 'var(--rule)', color: 'var(--ink-2)' }}
                  >
                    {fact.value}
                  </dd>
                </Fragment>
              ))}
            </dl>
          </div>

          <nav aria-label="Footer">
            <ul className="grid grid-cols-2 gap-x-8 gap-y-2.5 text-[0.9rem] sm:grid-cols-3">
              {links.map((link) => (
                <li key={link.href}>
                  <Link href={link.href} className="no-underline" style={{ color: 'var(--ink-2)' }}>
                    {link.label}
                  </Link>
                </li>
              ))}
            </ul>
          </nav>
        </div>

        {/* The rule runs the full measure of the page, not the width of the
            sentence — it is the footer's last division, not an underline. */}
        <div className="mt-12 border-t pt-5" style={{ borderColor: 'var(--rule)' }}>
          <p className="max-w-[78ch] text-[0.82rem]" style={{ color: 'var(--ink-3)' }}>
            {ASF_DISCLAIMER}
          </p>
        </div>
      </div>
    </footer>
  );
}
