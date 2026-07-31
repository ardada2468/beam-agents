import { beforeAll, describe, expect, it } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import { renderMarkdown, renderMdx } from '../mdx';

/**
 * These tests exist because of a bug that shipped: MDX does not enable
 * GitHub-flavored markdown by default, so for a while every pipe table on the
 * site rendered as literal `| Reason | Meaning |` and `|---|---|` paragraph
 * text. It was invisible to the type checker, to the link checker, and to the
 * prose checker — the only way to catch it is to compile a table and look at
 * the markup.
 *
 * Both pipelines are covered. `renderMarkdown` matters most: it is the one
 * that publishes repository files such as `docs/errors.md` through `<RepoDoc>`
 * and the capability specs through `<Spec>`, and those files are mostly tables.
 */

/** A document exercising the GFM features the site's content actually uses. */
const FIXTURE = [
  '| Reason | Meaning |',
  '| --- | --- |',
  '| `activation_error` | The activation raised before the effect ran. |',
  '| `timeout` | The step exceeded its deadline. |',
  '',
  'Superseded ~~guarantees~~ are struck through, and a bare URL such as',
  'https://example.com/spec becomes a link.',
  '',
].join('\n');

let mdxHtml = '';
let markdownHtml = '';

beforeAll(async () => {
  // Compiled once for the whole file: the first compile pays for loading the
  // Shiki themes, which is seconds of work that has nothing to do with tables.
  mdxHtml = renderToStaticMarkup(await renderMdx(FIXTURE));
  markdownHtml = renderToStaticMarkup(await renderMarkdown(FIXTURE));
}, 60_000);

describe.each([
  ['renderMdx', () => mdxHtml],
  ['renderMarkdown', () => markdownHtml],
])('%s', (_name, html) => {
  it('compiles a pipe table into a real table element', () => {
    expect(html()).toContain('<table>');
    expect(html()).toContain('<thead>');
    expect(html()).toContain('<th>Reason</th>');
    expect(html()).toContain('<td>The step exceeded its deadline.</td>');
  });

  it('leaves no raw table syntax in the output', () => {
    // The regression itself: without remark-gfm the delimiter row survives as
    // text, which is exactly what a reader reported seeing on /docs/errors.
    expect(html()).not.toContain('|---|');
    expect(html()).not.toContain('| --- |');
    expect(html()).not.toContain('| Reason | Meaning |');
  });

  it('wraps the table so a wide table scrolls instead of the page', () => {
    expect(html()).toMatch(/<div class="table-scroll" tabindex="0"><table>/);
  });

  it('supports the rest of GitHub-flavored markdown', () => {
    expect(html()).toContain('<del>guarantees</del>');
    expect(html()).toContain('href="https://example.com/spec"');
  });
});

describe('renderMarkdown', () => {
  it('renders a nested table once, not once per level of nesting', async () => {
    // The wrapper plugin rewrites a node's children while walking them; if it
    // recursed into what it had just created it would wrap forever.
    const html = renderToStaticMarkup(
      await renderMarkdown(['> | a | b |', '> | --- | --- |', '> | 1 | 2 |', ''].join('\n')),
    );
    expect(html.match(/table-scroll/g)).toHaveLength(1);
    expect(html).toContain('<blockquote>');
  });

  it('still renders ordinary markdown, and keeps HTML out of components', async () => {
    const html = renderToStaticMarkup(await renderMarkdown('## Failure modes\n\nA sentence.\n'));
    expect(html).toContain('<h2 id="failure-modes">Failure modes</h2>');
    expect(html).toContain('<p>A sentence.</p>');
  });
});
