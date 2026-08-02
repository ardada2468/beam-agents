import { describe, expect, it } from 'vitest';
import { frontmatterSchema, assertionSchema, sourceSchema } from '../schema';

/**
 * The schema is the first gate a page passes. These tests pin the failures
 * that matter — the ones where a permissive default would let an unverified
 * page render as if it were checked.
 */

const valid = {
  title: 'A page',
  summary: 'What it says.',
  status: 'stable',
};

describe('frontmatter schema', () => {
  it('accepts a minimal valid page and defaults the claim lists', () => {
    const result = frontmatterSchema.parse(valid);
    expect(result.verifies).toEqual([]);
    expect(result.sources).toEqual([]);
  });

  it('rejects a page with no status rather than defaulting one', () => {
    const { status: _status, ...noStatus } = valid;
    const result = frontmatterSchema.safeParse(noStatus);
    expect(result.success).toBe(false);
  });

  it('rejects a status outside the closed set', () => {
    const result = frontmatterSchema.safeParse({ ...valid, status: 'production-ready' });
    expect(result.success).toBe(false);
  });

  it('rejects unknown top-level keys, so a typo cannot pass unnoticed', () => {
    const result = frontmatterSchema.safeParse({ ...valid, verifes: [] });
    expect(result.success).toBe(false);
  });

  it('requires a summary, which becomes the meta description', () => {
    const result = frontmatterSchema.safeParse({ ...valid, summary: '' });
    expect(result.success).toBe(false);
  });
});

describe('assertion schema', () => {
  it.each([
    { symbol: 'beam_agents.RunAgent' },
    { module: 'src/beam_agents/core/dofn.py' },
    { spec: 'openspec/specs/tool-registry/spec.md' },
    { test: 'tests/core/test_transform.py' },
    { example: 'fast_path.py' },
  ])('accepts a single %o assertion', (entry) => {
    expect(assertionSchema.safeParse(entry).success).toBe(true);
  });

  it('rejects an unrecognized assertion key', () => {
    expect(assertionSchema.safeParse({ symbl: 'beam_agents.RunAgent' }).success).toBe(false);
  });

  it('rejects an entry carrying two assertions at once', () => {
    const result = assertionSchema.safeParse({
      symbol: 'beam_agents.RunAgent',
      module: 'src/beam_agents/core/dofn.py',
    });
    expect(result.success).toBe(false);
  });

  it('rejects an empty entry', () => {
    expect(assertionSchema.safeParse({}).success).toBe(false);
  });
});

describe('source schema', () => {
  const source = {
    claim: 'Flink Agents is an Agentic AI framework based on Apache Flink.',
    url: 'https://github.com/apache/flink-agents',
    retrieved: '2026-07-30',
  };

  it('accepts a complete citation', () => {
    expect(sourceSchema.parse(source).retrieved).toBe('2026-07-30');
  });

  it('coerces a YAML-parsed Date, which is what an unquoted date becomes', () => {
    const result = sourceSchema.parse({ ...source, retrieved: new Date('2026-07-30T00:00:00Z') });
    expect(result.retrieved).toBe('2026-07-30');
  });

  it('rejects a citation with no retrieval date', () => {
    const { retrieved: _retrieved, ...undated } = source;
    expect(sourceSchema.safeParse(undated).success).toBe(false);
  });

  it('rejects a non-ISO retrieval date', () => {
    expect(sourceSchema.safeParse({ ...source, retrieved: 'July 2026' }).success).toBe(false);
  });

  it('rejects a citation with no URL', () => {
    const { url: _url, ...unsourced } = source;
    expect(sourceSchema.safeParse(unsourced).success).toBe(false);
  });
});
