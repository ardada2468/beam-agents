import type { ReactElement } from 'react';
import { compileMDX } from 'next-mdx-remote/rsc';
import rehypeShiki from '@shikijs/rehype';
import rehypeSlug from 'rehype-slug';
import remarkGfm from 'remark-gfm';
import { Example } from '@/components/Example';
import { Callout } from '@/components/Callout';
import { ClaimTable, Cell } from '@/components/ClaimTable';
import { Diagram, DgEdge, DgNode, DgText } from '@/components/Diagram';
import { Figure } from '@/components/Figure';
import { RepoDoc } from '@/components/RepoDoc';
import { Spec } from '@/components/Spec';
import { StateCells, StateWriteLifecycle } from '@/components/diagrams/StateDiagrams';
import { HitlApprovalSequence } from '@/components/diagrams/HitlApprovalSequence';
import { HitlFailClosed } from '@/components/diagrams/HitlFailClosed';
import { HitlReinjection } from '@/components/diagrams/HitlReinjection';

/**
 * Shiki is configured once and shared by both pipelines, because a code block
 * lifted out of a repository doc has to look identical to one authored in MDX.
 * `defaultColor: false` emits both themes as CSS variables on the same markup,
 * so the theme toggle is a CSS concern rather than a second render.
 */
const SHIKI_OPTIONS = {
  themes: { light: 'github-light', dark: 'github-dark' },
  defaultColor: false,
};

/**
 * GitHub-flavored markdown, which MDX does *not* enable on its own.
 *
 * Without this, a pipe table is not a table at all: the parser sees ordinary
 * paragraph text and the reader gets literal `| Reason | Meaning |` and
 * `|---|---|` rows on the page. Every table on the site — the ones authored in
 * MDX content and the ones inside the repository markdown rendered through
 * `<RepoDoc>` and `<Spec>` — depends on this plugin, as do strikethrough and
 * bare-URL autolinks. It belongs in both pipelines; the `md` pipeline is the
 * one that publishes `docs/errors.md`, which is nothing but tables.
 */
const REMARK_PLUGINS = [remarkGfm];

/** The subset of hast this file walks. Kept local so the plugin needs no dependency of its own. */
type HastNode = {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  children?: HastNode[];
};

/**
 * Wrap every `<table>` in `<div class="table-scroll">`.
 *
 * A markdown table has no width budget: a reference table with five columns of
 * prose is far wider than the reading measure, and unwrapped it either forces
 * the whole page to scroll sideways or squeezes the body text. The wrapper
 * confines that overflow to the table itself, which matters most on a phone.
 * The class is styled in `app/globals.css`; this plugin only puts it there.
 *
 * The wrapper takes `tabindex="0"` because a region that scrolls has to be
 * reachable by keyboard: without it, a reader who does not use a mouse has no
 * way to see the columns that overflow. This is the `scrollable-region-
 * focusable` rule, which the jsdom accessibility check cannot evaluate because
 * jsdom has no layout and therefore never sees an element overflow.
 *
 * Written as a plain recursive walk rather than `unist-util-visit` so that the
 * site does not take a direct dependency on the unist tree utilities for the
 * sake of one transform.
 */
function rehypeScrollableTables() {
  return (tree: HastNode): void => {
    wrapTables(tree);
  };
}

function wrapTables(node: HastNode): void {
  const children = node.children;
  if (!children) return;

  for (let index = 0; index < children.length; index += 1) {
    const child = children[index];
    if (!child) continue;

    // Recurse first: a table nested inside a wrapper we just created would
    // otherwise be visited again and wrapped twice.
    wrapTables(child);

    if (child.type === 'element' && child.tagName === 'table') {
      children[index] = {
        type: 'element',
        tagName: 'div',
        properties: { className: ['table-scroll'], tabIndex: 0 },
        children: [child],
      };
    }
  }
}

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
        remarkPlugins: REMARK_PLUGINS,
        rehypePlugins: [rehypeSlug, rehypeScrollableTables, [rehypeShiki, SHIKI_OPTIONS]],
      },
    },
    components: {
      Example,
      Callout,
      ClaimTable,
      Cell,
      Diagram,
      DgNode,
      DgEdge,
      DgText,
      Figure,
      RepoDoc,
      Spec,
      // Page-specific diagrams. A diagram with real geometry is a component,
      // not markup a content author retypes; naming it here is what lets the
      // MDX call it by name without an import statement MDX cannot resolve.
      StateCells,
      StateWriteLifecycle,
      HitlApprovalSequence,
      HitlFailClosed,
      HitlReinjection,
    },
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
        remarkPlugins: REMARK_PLUGINS,
        rehypePlugins: [rehypeSlug, rehypeScrollableTables, [rehypeShiki, SHIKI_OPTIONS]],
      },
    },
  });
  return content;
}
