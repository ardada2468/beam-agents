import Link from 'next/link';
import { SECTIONS } from '@/lib/sections';
import { ASF_DISCLAIMER, LICENSE, PACKAGE_VERSION, REPO_URL, SITE_NAME } from '@/lib/site';

/**
 * The footer carries the non-affiliation statement on every page.
 *
 * It is not tucked into an about page because the confusion it prevents — that
 * a project named after Apache Beam and licensed Apache-2.0 might be an ASF
 * project — is available on every page, so the correction should be too.
 */
export function Footer() {
  return (
    <footer className="rule-top mt-20" style={{ background: 'var(--paper-2)' }}>
      <div className="shell py-10">
        <div className="flex flex-wrap justify-between gap-x-10 gap-y-8">
          <div>
            <p className="mono text-[0.9rem] font-medium" style={{ color: 'var(--ink)' }}>
              {SITE_NAME}
            </p>
            <dl className="mono mt-3 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-[0.75rem]">
              <dt style={{ color: 'var(--ink-3)' }}>VERSION</dt>
              <dd style={{ color: 'var(--ink-2)' }}>{PACKAGE_VERSION} · unreleased</dd>
              <dt style={{ color: 'var(--ink-3)' }}>LICENSE</dt>
              <dd style={{ color: 'var(--ink-2)' }}>{LICENSE}</dd>
              <dt style={{ color: 'var(--ink-3)' }}>SOURCE</dt>
              <dd>
                <a href={REPO_URL}>github.com/ardada2468/beam-agents</a>
              </dd>
            </dl>
          </div>

          <nav aria-label="Footer" className="flex flex-wrap gap-x-10 gap-y-2 text-[0.9rem]">
            <ul className="space-y-1.5">
              {SECTIONS.filter((section) => section.inNav)
                .slice(0, 3)
                .map((section) => (
                  <li key={section.slug}>
                    <Link
                      href={`/${section.slug}`}
                      className="no-underline"
                      style={{ color: 'var(--ink-2)' }}
                    >
                      {section.title}
                    </Link>
                  </li>
                ))}
            </ul>
            <ul className="space-y-1.5">
              {SECTIONS.filter((section) => section.inNav)
                .slice(3)
                .map((section) => (
                  <li key={section.slug}>
                    <Link
                      href={`/${section.slug}`}
                      className="no-underline"
                      style={{ color: 'var(--ink-2)' }}
                    >
                      {section.title}
                    </Link>
                  </li>
                ))}
              <li>
                <Link href="/roadmap" className="no-underline" style={{ color: 'var(--ink-2)' }}>
                  Roadmap
                </Link>
              </li>
            </ul>
          </nav>
        </div>

        <p
          className="mt-9 max-w-[76ch] border-t pt-5 text-[0.82rem]"
          style={{ borderColor: 'var(--rule)', color: 'var(--ink-3)' }}
        >
          {ASF_DISCLAIMER}
        </p>
      </div>
    </footer>
  );
}
