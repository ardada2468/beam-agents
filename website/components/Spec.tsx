import { readRepoFile, rewriteRepoLinks, stripTitle } from '@/lib/repodoc';
import { renderMarkdown } from '@/lib/mdx';
import { repoFileUrl } from '@/lib/site';

/**
 * Render a capability specification from `openspec/specs/`.
 *
 * The specs are the contract the implementation is written against —
 * Given/When/Then scenarios that tests are derived from and named after. They
 * are published verbatim rather than summarized, because a summarized spec is
 * a different document with the same title.
 *
 * The opening line says so, and counts the requirements and scenarios it found
 * in the file it just read — so the count cannot be stale either. It is drawn
 * as a provenance line rather than as an info card, matching `RepoDoc`: both
 * are saying the same thing about the page under them, and both are quoting.
 */
export async function Spec({ capability }: { capability: string }) {
  const path = `openspec/specs/${capability}/spec.md`;
  const raw = readRepoFile(path);
  const body = await renderMarkdown(
    rewriteRepoLinks(stripTitle(raw), `openspec/specs/${capability}`),
  );

  const requirements = [...raw.matchAll(/^### Requirement: (.+)$/gm)].map((match) => match[1]);
  const scenarios = [...raw.matchAll(/^#### Scenario: /gm)].length;

  return (
    <div data-spec={capability}>
      <div className="mb-8 pb-5" style={{ borderBottom: '1px solid var(--rule)' }}>
        <span
          aria-hidden="true"
          className="block h-[3px] w-9"
          style={{ background: 'var(--ink)' }}
        />
        <p className="mt-3.5 max-w-[68ch] text-[0.9rem]" style={{ color: 'var(--ink-2)' }}>
          Published verbatim from{' '}
          <a href={repoFileUrl(path)} className="mono text-[0.85rem]">
            {path}
          </a>{' '}
          — {requirements.length} requirement{requirements.length === 1 ? '' : 's'}, {scenarios}{' '}
          scenario{scenarios === 1 ? '' : 's'}. Each scenario is the source a test is derived from
          and named after.
        </p>
      </div>
      {body}
    </div>
  );
}
