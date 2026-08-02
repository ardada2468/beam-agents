import { describe, expect, it } from 'vitest';
import { allPages, isIndexable, pagesInSection, slugifyHeading } from '../content';
import { readExample, readExampleRegion, ExampleError } from '../examples';
import { rewriteRepoLinks } from '../repodoc';
import { sourceAnchor } from '../sources';

describe('content loader', () => {
  it('loads every page in the tree with valid frontmatter', () => {
    const pages = allPages();
    expect(pages.length).toBeGreaterThan(0);
    for (const page of pages) {
      expect(page.frontmatter.title).toBeTruthy();
      expect(page.frontmatter.summary).toBeTruthy();
      expect(page.frontmatter.status).toBeTruthy();
    }
  });

  it('assigns every page a unique route', () => {
    const hrefs = allPages().map((page) => page.href);
    expect(new Set(hrefs).size).toBe(hrefs.length);
  });

  it('orders a section by explicit order, then title', () => {
    const learn = pagesInSection('learn');
    const ordered = learn.filter((page) => page.frontmatter.order !== undefined);
    const values = ordered.map((page) => page.frontmatter.order ?? 0);
    expect([...values].sort((a, b) => a - b)).toEqual(values);
  });

  it('treats planned pages as non-indexable and nothing else', () => {
    for (const page of allPages()) {
      expect(isIndexable(page)).toBe(page.frontmatter.status !== 'planned');
    }
  });

  it('skips headings inside fenced code blocks', () => {
    const withFence = allPages().find((page) => page.body.includes('```sh'));
    expect(withFence).toBeDefined();
    // A `# comment` on the first column of a shell block is not a heading.
    const texts = withFence?.headings.map((heading) => heading.text) ?? [];
    expect(texts.some((text) => text.startsWith('uv '))).toBe(false);
  });
});

describe('heading slugs', () => {
  it.each([
    ['The honest headline', 'the-honest-headline'],
    ['`RunAgent` outputs', 'runagent-outputs'],
    ['Not yet implemented', 'not-yet-implemented'],
    ['Timeouts fail closed, at both layers', 'timeouts-fail-closed-at-both-layers'],
  ])('slugifies %s', (input, expected) => {
    expect(slugifyHeading(input)).toBe(expected);
  });
});

describe('example embedding', () => {
  it('reads a whole example from disk', () => {
    const example = readExample('fast_path.py');
    expect(example.source).toContain('async def triage');
    expect(example.summary).toBeTruthy();
  });

  it('extracts a named region with its markers stripped', () => {
    const region = readExampleRegion('fast_path.py', 'agent');
    expect(region).toContain('async def triage');
    expect(region).not.toContain('# region:');
    expect(region).not.toContain('# endregion:');
  });

  it('throws for a missing file, so a broken embed fails the build', () => {
    expect(() => readExample('nope.py')).toThrow(ExampleError);
  });

  it('throws for a missing region rather than rendering nothing', () => {
    expect(() => readExampleRegion('fast_path.py', 'nope')).toThrow(ExampleError);
  });

  it('detects the required-extra marker', () => {
    expect(readExample('langgraph_adapter.py').requiresExtra).toBe('langgraph');
    expect(readExample('fast_path.py').requiresExtra).toBeNull();
  });
});

describe('repository link rewriting', () => {
  it('maps a sibling doc link to the site route that publishes it', () => {
    expect(rewriteRepoLinks('see [metrics](metrics.md)', 'docs')).toBe(
      'see [metrics](/docs/metrics)',
    );
  });

  it('maps a capability spec path to its site route', () => {
    const input = 'see [spec](../../openspec/specs/tool-registry/spec.md)';
    expect(rewriteRepoLinks(input, 'docs')).toContain('](/specs/tool-registry)');
  });

  it('points any other relative path at the repository', () => {
    const output = rewriteRepoLinks('see [dofn](../src/beam_agents/core/dofn.py)', 'docs');
    expect(output).toContain('https://github.com/');
    expect(output).toContain('src/beam_agents/core/dofn.py');
  });

  it('preserves anchors when rewriting', () => {
    expect(rewriteRepoLinks('[m](metrics.md#counters)', 'docs')).toBe(
      '[m](/docs/metrics#counters)',
    );
  });

  it('leaves absolute and in-page links alone', () => {
    const input = '[a](https://example.invalid) [b](#section)';
    expect(rewriteRepoLinks(input, 'docs')).toBe(input);
  });
});

describe('citation anchors', () => {
  it('is stable for the same URL', () => {
    expect(sourceAnchor('https://example.invalid/a')).toBe(
      sourceAnchor('https://example.invalid/a'),
    );
  });

  it('differs across URLs, so two footnotes do not collide', () => {
    expect(sourceAnchor('https://example.invalid/a')).not.toBe(
      sourceAnchor('https://example.invalid/b'),
    );
  });
});
