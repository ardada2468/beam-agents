import { readRepoFile, rewriteRepoLinks, stripTitle } from '@/lib/repodoc';
import { renderMarkdown } from '@/lib/mdx';
import { repoFileUrl } from '@/lib/site';

/**
 * Render a markdown file from the repository, in place.
 *
 * The page you are reading and the file in the checkout are the same text.
 * There is no second copy to fall out of date, which is why the operational
 * reference on this site cannot drift from the repository's own docs.
 */
export async function RepoDoc({ file }: { file: string }) {
  const raw = readRepoFile(file);
  const dir = file.split('/').slice(0, -1).join('/');
  const body = await renderMarkdown(rewriteRepoLinks(stripTitle(raw), dir));

  return (
    <div data-repo-doc={file}>
      <p
        className="mb-5 rounded-md border-l-4 px-4 py-2 text-sm"
        style={{
          background: 'var(--note-bg)',
          borderColor: 'var(--note-border)',
          color: 'var(--note-fg)',
        }}
      >
        Rendered from <a href={repoFileUrl(file)}>{file}</a> in the repository. This page and that
        file are the same text — there is no second copy to fall out of date.
      </p>
      {body}
    </div>
  );
}
