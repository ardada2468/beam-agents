import type { ReactElement } from 'react';
import { compileMDX } from 'next-mdx-remote/rsc';
import rehypeShiki from '@shikijs/rehype';
import rehypeSlug from 'rehype-slug';
import { Example } from '@/components/Example';
import { Callout } from '@/components/Callout';
import { ClaimTable, Cell } from '@/components/ClaimTable';
import { Figure } from '@/components/Figure';
import { RepoDoc } from '@/components/RepoDoc';
import { Spec } from '@/components/Spec';

/**
 * Render an MDX body to a server-rendered React tree.
 *
 * Highlighting happens here, at build time, via `@shikijs/rehype`. Nothing
 * about code presentation reaches the browser as JavaScript — a reader with
 * scripting disabled sees the same highlighted code as everyone else, which is
 * part of what makes the pages indexable.
 */
export async function renderMdx(source: string): Promise<ReactElement> {
  const { content } = await compileMDX({
    source,
    options: {
      parseFrontmatter: false,
      mdxOptions: {
        rehypePlugins: [
          rehypeSlug,
          [
            rehypeShiki,
            {
              themes: { light: 'github-light', dark: 'github-dark' },
              defaultColor: false,
            },
          ],
        ],
      },
    },
    components: { Example, Callout, ClaimTable, Cell, Figure, RepoDoc, Spec },
  });
  return content;
}

/**
 * Render plain markdown lifted from the repository.
 *
 * Same pipeline, but with no components in scope: repository markdown is not
 * MDX, and a stray `<` in a doc must render as text rather than resolve to a
 * component that happens to share its name.
 */
export async function renderMarkdown(source: string): Promise<ReactElement> {
  const { content } = await compileMDX({
    source,
    options: {
      parseFrontmatter: false,
      mdxOptions: {
        format: 'md',
        rehypePlugins: [
          rehypeSlug,
          [
            rehypeShiki,
            {
              themes: { light: 'github-light', dark: 'github-dark' },
              defaultColor: false,
            },
          ],
        ],
      },
    },
  });
  return content;
}
