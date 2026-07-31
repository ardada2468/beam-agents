import { readFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Reader for the generated API reference.
 *
 * `generated/api.json` is produced by `scripts/gen_api_reference.py`, which
 * imports the package and introspects it. Nothing here interprets or
 * embellishes: if a docstring is absent the page says so rather than inventing
 * a description. The file is committed and drift-checked, so what renders is
 * what the package actually declares.
 */

export interface SourceRef {
  readonly path: string;
  readonly line: number;
}

export interface ApiMember {
  readonly name: string;
  readonly kind: string;
  readonly signature: string | null;
  readonly doc: string | null;
  readonly source: SourceRef | null;
}

export interface ApiSymbol {
  readonly name: string;
  readonly qualname: string;
  readonly kind: string;
  readonly signature: string | null;
  readonly doc: string | null;
  readonly source: SourceRef | null;
  readonly requires_extra: string | null;
  readonly members: readonly ApiMember[];
}

export interface ApiModule {
  readonly module: string;
  readonly visibility: 'public' | 'documented-internal';
  readonly note: string;
}

export interface ApiReference {
  readonly generated_by: string;
  readonly package: string;
  readonly package_version: string;
  readonly public_surface: readonly string[];
  readonly symbols: readonly ApiSymbol[];
  readonly modules: readonly ApiModule[];
}

let cache: ApiReference | null = null;

export function apiReference(): ApiReference {
  if (cache) return cache;
  const path = join(process.cwd(), 'generated', 'api.json');
  cache = JSON.parse(readFileSync(path, 'utf8')) as ApiReference;
  return cache;
}

export function apiSymbols(): readonly ApiSymbol[] {
  return apiReference().symbols;
}

export function findSymbol(name: string): ApiSymbol | undefined {
  return apiSymbols().find((symbol) => symbol.name === name);
}
