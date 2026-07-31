import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Example source is read from disk at build time — never transcribed into
 * prose. An example is a real program that the repository's offline test tier
 * executes on every change (`tests/docs/test_website_examples.py`), so code on
 * this site cannot quietly stop working.
 */

export const EXAMPLES_ROOT = join(process.cwd(), 'examples');

export interface ExampleFile {
  readonly name: string;
  readonly file: string;
  readonly source: string;
  /** First docstring line, used as the one-line description on the index. */
  readonly summary: string;
  readonly requiresExtra: string | null;
}

const REGION_START = /^\s*#\s*region:\s*(\S+)\s*$/;
const REGION_END = /^\s*#\s*endregion:\s*(\S+)\s*$/;
/** Opt-in marker an example uses to declare an optional extra it needs. */
const EXTRA_MARKER = /^\s*#\s*requires-extra:\s*(\S+)\s*$/m;

export class ExampleError extends Error {}

export function exampleNames(): string[] {
  return readdirSync(EXAMPLES_ROOT)
    .filter((entry) => entry.endsWith('.py') && !entry.startsWith('_'))
    .sort();
}

export function readExample(name: string): ExampleFile {
  let source: string;
  try {
    source = readFileSync(join(EXAMPLES_ROOT, name), 'utf8');
  } catch {
    throw new ExampleError(
      `example file website/examples/${name} does not exist (referenced by <Example file="${name}" />)`,
    );
  }
  const extra = EXTRA_MARKER.exec(source);
  return {
    name,
    file: `website/examples/${name}`,
    source,
    summary: firstDocstringLine(source),
    requiresExtra: extra?.[1] ?? null,
  };
}

function firstDocstringLine(source: string): string {
  const match = /^"""(.*?)$/m.exec(source);
  return match?.[1]?.trim() ?? '';
}

/**
 * Extract a named region, with the region markers themselves stripped.
 *
 * Regions exist so that a three-line snippet in prose still comes from an
 * executed program. Dedenting keeps a region lifted out of a function body
 * from rendering with orphaned indentation.
 */
export function readExampleRegion(name: string, region: string): string {
  const { source } = readExample(name);
  const lines = source.split('\n');
  const collected: string[] = [];
  let inRegion = false;
  let found = false;
  for (const line of lines) {
    const start = REGION_START.exec(line);
    if (start && start[1] === region) {
      inRegion = true;
      found = true;
      continue;
    }
    const end = REGION_END.exec(line);
    if (end && end[1] === region) {
      inRegion = false;
      continue;
    }
    if (inRegion) collected.push(line);
  }
  if (!found) {
    throw new ExampleError(
      `example website/examples/${name} has no region "${region}" ` +
        `(expected "# region: ${region}" ... "# endregion: ${region}")`,
    );
  }
  return dedent(collected)
    .join('\n')
    .replace(/^\n+|\n+$/g, '');
}

function dedent(lines: string[]): string[] {
  const indents = lines
    .filter((line) => line.trim().length > 0)
    .map((line) => line.length - line.trimStart().length);
  const min = indents.length > 0 ? Math.min(...indents) : 0;
  return lines.map((line) => line.slice(min));
}
