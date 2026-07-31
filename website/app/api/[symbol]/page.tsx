import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { apiSymbols, findSymbol, type ApiMember } from '@/lib/api';
import { absoluteUrl, repoFileUrl, SITE_NAME } from '@/lib/site';

interface Params {
  symbol: string;
}

export async function generateStaticParams(): Promise<Params[]> {
  return apiSymbols().map((symbol) => ({ symbol: symbol.name }));
}

export async function generateMetadata({ params }: { params: Promise<Params> }): Promise<Metadata> {
  const { symbol: name } = await params;
  const symbol = findSymbol(name);
  if (!symbol) return {};
  const url = absoluteUrl(`/api/${symbol.name}`);
  const description = summarize(symbol.doc, symbol.name, symbol.kind);
  return {
    title: `${symbol.name} (API)`,
    description,
    alternates: { canonical: url },
    openGraph: { title: `${symbol.name} — ${SITE_NAME}`, description, url },
  };
}

export default async function SymbolPage({ params }: { params: Promise<Params> }) {
  const { symbol: name } = await params;
  const symbol = findSymbol(name);
  if (!symbol) notFound();

  return (
    <div className="mx-auto max-w-4xl px-5 py-10">
      <nav aria-label="Breadcrumb" className="mb-2 text-sm">
        <Link href="/api" style={{ color: 'var(--fg-muted)' }}>
          API reference
        </Link>
      </nav>

      <h1 className="font-mono text-[1.7rem] leading-tight font-bold tracking-tight">
        {symbol.name}
      </h1>
      <p className="mt-1 text-sm" style={{ color: 'var(--fg-muted)' }}>
        {symbol.kind} · <code>{symbol.qualname}</code>
      </p>

      {symbol.requires_extra ? (
        <p
          className="mt-4 rounded-md border-l-4 px-4 py-2 text-sm"
          style={{
            background: 'var(--warn-bg)',
            borderColor: 'var(--warn-border)',
            color: 'var(--warn-fg)',
          }}
        >
          Importing this name requires the <code>{symbol.requires_extra}</code> extra. Without it,
          the attribute access raises <code>ImportError</code> naming the extra.
        </p>
      ) : null}

      {symbol.signature ? (
        <pre
          className="mt-5 overflow-x-auto rounded-md border p-3 font-mono text-[0.85rem]"
          style={{ borderColor: 'var(--border)', background: 'var(--bg-subtle)' }}
        >
          <code>
            {symbol.name}
            {symbol.signature}
          </code>
        </pre>
      ) : null}

      <Doc doc={symbol.doc} />

      {symbol.source ? (
        <p className="mt-4 text-sm">
          <a href={repoFileUrl(symbol.source.path, symbol.source.line)}>
            {symbol.source.path}:{symbol.source.line}
          </a>
        </p>
      ) : null}

      {symbol.members.length > 0 ? (
        <>
          <h2
            className="mt-10 mb-3 border-t pt-4 text-xl font-semibold"
            style={{ borderColor: 'var(--border)' }}
          >
            Members
          </h2>
          <div className="space-y-6">
            {symbol.members.map((member) => (
              <Member key={`${member.kind}-${member.name}`} symbol={symbol.name} member={member} />
            ))}
          </div>
        </>
      ) : null}
    </div>
  );
}

function Member({ symbol, member }: { symbol: string; member: ApiMember }) {
  const id = `${member.name}`;
  return (
    <section id={id}>
      <h3 className="font-mono text-[1rem] font-semibold">
        <a href={`#${id}`} className="no-underline" style={{ color: 'var(--fg)' }}>
          {member.name}
        </a>
        <span className="ml-2 text-xs font-normal" style={{ color: 'var(--fg-faint)' }}>
          {member.kind} on {symbol}
        </span>
      </h3>
      {member.signature ? (
        <pre
          className="mt-2 overflow-x-auto rounded border p-2 font-mono text-[0.8rem]"
          style={{ borderColor: 'var(--border)', background: 'var(--bg-subtle)' }}
        >
          <code>{member.signature}</code>
        </pre>
      ) : null}
      <Doc doc={member.doc} small />
    </section>
  );
}

/**
 * Render a docstring as paragraphs, or say plainly that there is none.
 *
 * The "No docstring" marker is deliberate. A generated reference that fills
 * silence with invented description is worse than one that admits the gap.
 */
function Doc({ doc, small = false }: { doc: string | null; small?: boolean }) {
  if (!doc) {
    return (
      <p className={`mt-3 ${small ? 'text-sm' : ''}`} style={{ color: 'var(--fg-faint)' }}>
        No docstring.
      </p>
    );
  }
  const blocks = doc.split(/\n\s*\n/);
  return (
    <div className={`mt-3 max-w-[74ch] space-y-3 ${small ? 'text-sm' : ''}`}>
      {blocks.map((block, index) => (
        <p key={index} className="whitespace-pre-wrap">
          {block}
        </p>
      ))}
    </div>
  );
}

function summarize(doc: string | null, name: string, kind: string): string {
  const first = doc?.split('\n')[0]?.trim();
  return first && first.length > 0 ? first : `${name} — ${kind} in the beam_agents public API.`;
}
