import type { Metadata } from 'next';
import Link from 'next/link';
import { notFound } from 'next/navigation';
import { Literals } from '@/components/Literals';
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

/**
 * One symbol from the generated reference.
 *
 * The content is fixed by `generated/api.json`; only its presentation lives
 * here. Reference material is read by scanning rather than by reading through,
 * so the page is built as a sequence of hairline-separated bands — identity,
 * signature, description, members, provenance — in the landing page's `.shell`
 * rather than the narrow prose column. Members sit in a two-column grid so the
 * names line up in a scannable rail down the left.
 */
export default async function SymbolPage({ params }: { params: Promise<Params> }) {
  const { symbol: name } = await params;
  const symbol = findSymbol(name);
  if (!symbol) notFound();

  return (
    <>
      {/* ---------- identity and signature ---------- */}
      <section className="shell pt-10 pb-11 sm:pt-12 sm:pb-14">
        <nav aria-label="Breadcrumb" className="text-[0.9rem]">
          <Link href="/api" className="no-underline" style={{ color: 'var(--ink-2)' }}>
            ← API reference
          </Link>
        </nav>

        <p className="eyebrow mt-7">{symbol.kind}</p>

        {/* The heading is the name as you would type it, so it is set in mono.
            `.h-page`'s tracking is tuned for the sans face and reads cramped on
            a monospace, hence the relaxed letter-spacing. */}
        <h1 className="h-page mono mt-3" style={{ letterSpacing: '-0.01em' }}>
          {symbol.name}
        </h1>

        <p className="mono mt-3 text-[0.82rem]" style={{ color: 'var(--ink-3)' }}>
          {symbol.qualname}
        </p>

        {symbol.signature ? (
          // Wrapped, not scrolled. A constructor signature here runs to several
          // hundred characters, and a horizontally-scrolling block hides most
          // of it behind a gesture nobody makes. `overflow-x-auto` stays only
          // as the escape hatch for a single unbreakable token.
          <pre
            className="mono mt-7 overflow-x-auto border px-4 py-3.5 text-[0.82rem] leading-relaxed whitespace-pre-wrap"
            style={{
              borderColor: 'var(--rule)',
              background: 'var(--paper-2)',
              borderRadius: 2,
            }}
          >
            <code>
              {symbol.name}
              {symbol.signature}
            </code>
          </pre>
        ) : null}

        {symbol.requires_extra ? (
          <div className="panel mt-6 max-w-[64ch]">
            {/* The amber stream colour is this site's standing "attend to this"
                hue, and it is contrast-checked against `--paper-2`. */}
            <p className="eyebrow" style={{ color: 'var(--s-intents)' }}>
              Optional extra
            </p>
            <p className="mt-2 text-[0.93rem]" style={{ color: 'var(--ink-2)' }}>
              Importing this name requires the <code className="mono">{symbol.requires_extra}</code>{' '}
              extra. Without it, the attribute access raises{' '}
              <code className="mono">ImportError</code> naming the extra.
            </p>
          </div>
        ) : null}
      </section>

      {/* ---------- what the docstring says ---------- */}
      <section className="rule-top">
        <div className="shell py-11 sm:py-14">
          <p className="eyebrow">Docstring</p>
          <Doc doc={symbol.doc} />

          {symbol.source ? (
            <p className="mono mt-6 text-[0.78rem]">
              <a href={repoFileUrl(symbol.source.path, symbol.source.line)}>
                {symbol.source.path}:{symbol.source.line}
              </a>
            </p>
          ) : null}
        </div>
      </section>

      {/* ---------- members ---------- */}
      {symbol.members.length > 0 ? (
        <section className="rule-top">
          <div className="shell py-11 sm:py-14">
            <div className="flex flex-wrap items-end justify-between gap-x-8 gap-y-3">
              <div>
                <p className="eyebrow">Members</p>
                <h2 className="h-section mt-2">
                  {symbol.members.length} declared on {symbol.name}
                </h2>
              </div>
              <p className="max-w-[40ch] text-[0.93rem]" style={{ color: 'var(--ink-2)' }}>
                Introspected from the class itself. A member with no docstring is shown as having
                none rather than described from its name.
              </p>
            </div>

            <ul className="list-rule mt-9">
              {symbol.members.map((member) => (
                <Member key={`${member.kind}-${member.name}`} member={member} />
              ))}
            </ul>
          </div>
        </section>
      ) : null}
    </>
  );
}

function Member({ member }: { member: ApiMember }) {
  const signature = memberSignature(member);
  return (
    <li id={member.name} className="grid gap-x-10 gap-y-2 sm:grid-cols-[minmax(0,15rem)_1fr]">
      <div>
        <h3 className="mono text-[0.95rem] font-medium">
          <a href={`#${member.name}`} className="no-underline" style={{ color: 'var(--ink)' }}>
            {member.name}
          </a>
        </h3>
        <p className="eyebrow mt-1.5">{member.kind}</p>
      </div>
      <div>
        {signature ? (
          <p className="mono text-[0.82rem] break-words" style={{ color: 'var(--ink-2)' }}>
            {signature}
          </p>
        ) : null}
        <Doc doc={member.doc} small />
      </div>
    </li>
  );
}

/**
 * The signature as you would type it.
 *
 * `inspect.signature` reports a callable's parameters and nothing else, so a
 * method arrives as a bare `(self, ...)`; a dataclass field arrives already
 * qualified, as `ttl_ms: int`. Prefixing only the bare ones makes every row in
 * the list read the same way. This reshapes nothing — the generated text is
 * still rendered verbatim, with the member's own name in front of it.
 */
function memberSignature(member: ApiMember): string | null {
  if (!member.signature) return null;
  return member.signature.startsWith('(') ? `${member.name}${member.signature}` : member.signature;
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
      <p className={small ? 'mt-2 text-[0.9rem]' : 'mt-4'} style={{ color: 'var(--ink-3)' }}>
        No docstring.
      </p>
    );
  }
  const blocks = doc.split(/\n\s*\n/);
  return (
    <div
      className={`max-w-[70ch] space-y-3 ${small ? 'mt-2.5 text-[0.93rem]' : 'mt-4 text-[1.02rem]'}`}
      style={{ color: 'var(--ink-2)' }}
    >
      {blocks.map((block, index) => (
        <p key={index} className="whitespace-pre-wrap">
          <Literals text={block} />
        </p>
      ))}
    </div>
  );
}

function summarize(doc: string | null, name: string, kind: string): string {
  const first = doc?.split('\n')[0]?.trim();
  return first && first.length > 0 ? first : `${name} — ${kind} in the beam_agents public API.`;
}
