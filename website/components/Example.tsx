import { codeToHtml } from 'shiki';
import { readExample, readExampleRegion } from '@/lib/examples';
import { repoFileUrl } from '@/lib/site';

/**
 * Embed example source read from `website/examples/` at build time.
 *
 * This component is the mechanism behind the rule that no code on this site is
 * transcribed. A missing file or region throws, which fails the build — a
 * broken embed can never render as an empty box.
 *
 * The frame is literally the one `Diagram` and `Figure` use — `.dg-figure`,
 * `.dg-scroll`, `.dg-caption` from `app/diagram.css` — a hairline box that
 * scrolls, with a caption under it. Code is the most common embedded object on
 * this site; it is on the landing page as well as on nearly every content page,
 * so if it rendered as a rounded, shadowed card, the whole site would read as
 * one.
 *
 * Two details below are load-bearing rather than cosmetic:
 *
 * 1. `defaultColor: false` makes Shiki emit `--shiki-light`/`--shiki-dark`
 *    custom properties instead of one baked-in palette. `globals.css` reads
 *    them under both the media query and the `[data-theme]` selectors, which is
 *    how the theme toggle recolours code with no second highlight pass.
 * 2. The `<pre>` Shiki emits is stripped back to nothing here. These blocks
 *    render inside `.prose`, which draws its own bordered box around a bare
 *    `<pre>`; without this the embed would be a box inside a box, and its
 *    appearance would drift with unrelated edits to the prose stylesheet.
 *
 * The caption is provenance, not a label: the file link and the claim that the
 * repository's offline test tier executes it are the reason the sample can be
 * trusted, so they stay attached to the code rather than living in a page's
 * preamble.
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
    <figure className="dg-figure">
      {/* Long lines scroll here, so the box is keyboard-reachable and named —
          without a tabindex a reader who does not use a pointer has no way to
          reach the end of a line that overflows on a phone. */}
      <div
        className="dg-scroll [&_pre]:border-0! [&_pre]:bg-transparent! [&_pre]:p-0! [&_pre]:text-[0.845rem]! [&_pre]:leading-[1.65]!"
        tabIndex={0}
        role="region"
        aria-label={`Example source, ${file}${region ? ` (region: ${region})` : ''}`}
        data-example={file}
        data-lines={lines ? 'true' : undefined}
        dangerouslySetInnerHTML={{ __html: html }}
      />
      <figcaption className="dg-caption">
        <a href={href} className="mono">
          website/examples/{file}
          {region ? ` (region: ${region})` : ''}
        </a>{' '}
        — executed by the repository&rsquo;s offline test tier.
      </figcaption>
    </figure>
  );
}
