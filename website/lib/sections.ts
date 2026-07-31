/**
 * The site's top-level information architecture.
 *
 * `inNav: false` keeps a section out of primary navigation without hiding it
 * from the router — the roadmap is reachable, but never presented as
 * documentation of something that exists.
 */

export interface SectionDef {
  readonly slug: string;
  readonly title: string;
  readonly blurb: string;
  readonly inNav: boolean;
}

export const SECTIONS: readonly SectionDef[] = [
  {
    slug: 'learn',
    title: 'Learn',
    blurb: 'What the runtime is, how an activation works, and the invariants it holds.',
    inNav: true,
  },
  {
    slug: 'docs',
    title: 'Docs',
    blurb: 'Operational reference: outputs, sinks, metrics, traces, the effector, CI.',
    inNav: true,
  },
  {
    slug: 'examples',
    title: 'Examples',
    blurb: 'Runnable programs, executed by the repository test suite on every change.',
    inNav: true,
  },
  {
    slug: 'specs',
    title: 'Specs',
    blurb: 'The capability specifications the implementation is written against.',
    inNav: true,
  },
  {
    slug: 'comparison',
    title: 'Comparison',
    blurb: 'How this compares to the alternatives, with every claim sourced.',
    inNav: true,
  },
  {
    slug: 'community',
    title: 'Community',
    blurb: 'License, repository, and how changes get made here.',
    inNav: true,
  },
  {
    slug: 'roadmap',
    title: 'Roadmap',
    blurb: 'Described but not implemented. Nothing in this section exists in the code.',
    inNav: false,
  },
];

export const SECTION_BY_SLUG = new Map(SECTIONS.map((s) => [s.slug, s]));

export function isSectionSlug(value: string): boolean {
  return SECTION_BY_SLUG.has(value);
}
