import { codeToHtml } from 'shiki';
import { readExample, readExampleRegion } from '@/lib/examples';
import { repoFileUrl } from '@/lib/site';

/**
 * Embed example source read from `website/examples/` at build time.
 *
 * This component is the mechanism behind the rule that no code on this site is
 * transcribed. A missing file or region throws, which fails the build — a
 * broken embed can never render as an empty box.
 */
export async function Example({
  file,
  region,
  lines = false,
}: {
  file: string;
  region?: string;
  lines?: boolean;
}) {
  const source = region ? readExampleRegion(file, region) : readExample(file).source;
  const html = await codeToHtml(source, {
    lang: 'python',
    themes: { light: 'github-light', dark: 'github-dark' },
    defaultColor: false,
  });
  const href = repoFileUrl(`website/examples/${file}`);
  return (
    <figure className="my-5">
      <div
        className="overflow-x-auto rounded-t-md border px-4 py-3 text-[0.85rem] leading-relaxed [&_pre]:!bg-transparent"
        style={{ borderColor: 'var(--border)' }}
        data-example={file}
        data-lines={lines ? 'true' : undefined}
        dangerouslySetInnerHTML={{ __html: html }}
      />
      <figcaption
        className="rounded-b-md border border-t-0 px-3 py-1.5 text-xs"
        style={{
          borderColor: 'var(--border)',
          background: 'var(--bg-subtle)',
          color: 'var(--fg-muted)',
        }}
      >
        <a href={href}>
          website/examples/{file}
          {region ? ` (region: ${region})` : ''}
        </a>{' '}
        — executed by the repository&rsquo;s offline test tier.
      </figcaption>
    </figure>
  );
}
