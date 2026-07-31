import type { Heading } from '@/lib/content';

export function TableOfContents({ headings }: { headings: readonly Heading[] }) {
  const shown = headings.filter((heading) => heading.depth <= 3);
  if (shown.length < 2) return null;
  return (
    <nav aria-label="On this page" className="text-sm">
      <p
        className="mb-2 text-[0.7rem] font-semibold tracking-wider uppercase"
        style={{ color: 'var(--fg-faint)' }}
      >
        On this page
      </p>
      <ul className="space-y-1">
        {shown.map((heading) => (
          <li key={heading.id} style={{ paddingLeft: heading.depth === 3 ? '0.75rem' : 0 }}>
            <a href={`#${heading.id}`} style={{ color: 'var(--fg-muted)' }}>
              {heading.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}
