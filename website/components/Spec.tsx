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
      <p
        className="mb-5 rounded-md border-l-4 px-4 py-2 text-sm"
        style={{
          background: 'var(--note-bg)',
          borderColor: 'var(--note-border)',
          color: 'var(--note-fg)',
        }}
      >
        Published verbatim from <a href={repoFileUrl(path)}>{path}</a> — {requirements.length}{' '}
        requirement{requirements.length === 1 ? '' : 's'}, {scenarios} scenario
        {scenarios === 1 ? '' : 's'}. Each scenario is the source a test is derived from and named
        after.
      </p>
      {body}
    </div>
  );
}
