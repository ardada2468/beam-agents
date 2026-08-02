import { readRepoFile, rewriteRepoLinks, stripTitle } from '@/lib/repodoc';
import { renderMarkdown } from '@/lib/mdx';
import { repoFileUrl } from '@/lib/site';

/**
 * Render a markdown file from the repository, in place.
 *
 * The page you are reading and the file in the checkout are the same text.
 * There is no second copy to fall out of date, which is why the operational
 * reference on this site cannot drift from the repository's own docs.
 *
 * That fact is the best thing about these pages, so it opens them — but as a
 * provenance line in the site's own idiom, not as a coloured info card. The
 * short rule above it is the same mark the landing page sets over each of the
 * four output names: it says "what follows is quoted", and `Spec` repeats it
 * for the same reason.
 */
export async function RepoDoc({ file }: { file: string }) {
  const raw = readRepoFile(file);
  const dir = file.split('/').slice(0, -1).join('/');
  const body = await renderMarkdown(rewriteRepoLinks(stripTitle(raw), dir));

  return (
    <div data-repo-doc={file}>
      <div className="mb-8 pb-5" style={{ borderBottom: '1px solid var(--rule)' }}>
        <span
          aria-hidden="true"
          className="block h-[3px] w-9"
          style={{ background: 'var(--ink)' }}
        />
        <p className="mt-3.5 max-w-[68ch] text-[0.9rem]" style={{ color: 'var(--ink-2)' }}>
          Rendered from{' '}
          <a href={repoFileUrl(file)} className="mono text-[0.85rem]">
            {file}
          </a>{' '}
          in the repository. This page and that file are the same text — there is no second copy to
          fall out of date.
        </p>
      </div>
      {body}
    </div>
  );
}
