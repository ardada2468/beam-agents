import { z } from 'zod';

/**
 * The frontmatter contract every content page must satisfy.
 *
 * Validation failure is a build error, never a default. A page that forgets
 * `status` must not silently render as if it were stable — that is precisely
 * the failure mode this site exists to make impossible.
 */

export const STATUSES = ['stable', 'experimental', 'partial', 'planned'] as const;
export type Status = (typeof STATUSES)[number];

export const STATUS_LABELS: Record<Status, string> = {
  stable: 'Stable',
  experimental: 'Experimental',
  partial: 'Partial',
  planned: 'Planned',
};

export const STATUS_DESCRIPTIONS: Record<Status, string> = {
  stable: 'Implemented, specified, and covered by tests in the repository.',
  experimental: 'Implemented and tested. The interface may still change.',
  partial: 'Partly implemented. The page states what is missing.',
  planned: 'Not implemented. Nothing described here exists in the current code.',
};

/**
 * A claim assertion. Exactly one key, drawn from the five verifiable kinds:
 *
 * - `symbol` — a dotted name the verifier resolves by importing the package
 * - `module` — a repository-relative source path
 * - `spec`   — a path under `openspec/specs/`
 * - `test`   — a pytest node id, checked against `pytest --collect-only`
 * - `example`— a file under `website/examples/`
 *
 * `strict()` is what rejects an unrecognized key: a typo like `symbl:` must
 * fail the build rather than pass unverified.
 */
export const assertionSchema = z
  .object({
    symbol: z.string().min(1).optional(),
    module: z.string().min(1).optional(),
    spec: z.string().min(1).optional(),
    test: z.string().min(1).optional(),
    example: z.string().min(1).optional(),
  })
  .strict()
  .refine((value) => Object.keys(value).length === 1, {
    message: 'each `verifies` entry must carry exactly one of: symbol, module, spec, test, example',
  });

export const sourceSchema = z
  .object({
    claim: z.string().min(1),
    url: z.string().url(),
    // ISO date the source was read. Rendered next to the citation so a reader
    // can judge how stale the claim is without chasing the link.
    //
    // Coerced from Date because YAML parses an unquoted `2026-07-30` into a
    // timestamp, not a string. Rejecting that would make the schema a trap
    // that punishes the natural way to write a date.
    retrieved: z
      .union([z.string(), z.date()])
      .transform((value) => (value instanceof Date ? value.toISOString().slice(0, 10) : value))
      .pipe(z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'retrieved must be an ISO date, YYYY-MM-DD')),
  })
  .strict();

export const frontmatterSchema = z
  .object({
    title: z.string().min(1),
    summary: z.string().min(1).max(300),
    status: z.enum(STATUSES),
    // Sort key inside a section. Pages without one sort last, alphabetically.
    order: z.number().int().optional(),
    verifies: z.array(assertionSchema).default([]),
    sources: z.array(sourceSchema).default([]),
  })
  .strict();

export type Assertion = z.infer<typeof assertionSchema>;
export type Source = z.infer<typeof sourceSchema>;
export type Frontmatter = z.infer<typeof frontmatterSchema>;

/** Format a Zod failure as a build error naming the file and every bad field. */
export function formatFrontmatterError(file: string, error: z.ZodError): string {
  const lines = error.issues.map((issue) => {
    const path = issue.path.length > 0 ? issue.path.join('.') : '(root)';
    return `  ${path}: ${issue.message}`;
  });
  return `${file}: invalid frontmatter\n${lines.join('\n')}`;
}
