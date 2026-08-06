import type { Metadata } from 'next';
import type { CSSProperties } from 'react';
import Link from 'next/link';
import { Example } from '@/components/Example';
import { PipelineDiagram } from '@/components/PipelineDiagram';
import {
  absoluteUrl,
  IS_RELEASED,
  LICENSE,
  PACKAGE_VERSION,
  REPO_URL,
  SITE_NAME,
  SITE_TAGLINE,
} from '@/lib/site';

export const metadata: Metadata = {
  title: `${SITE_NAME} — an agent is a Beam transform`,
  description: SITE_TAGLINE,
  alternates: { canonical: absoluteUrl('/') },
  openGraph: { title: SITE_NAME, description: SITE_TAGLINE, url: absoluteUrl('/') },
};

/**
 * The landing page.
 *
 * The hero is the pipeline itself. There is no adopter wall, no benchmark
 * chart, and no gradient, because none of the three would be true — the
 * project is unreleased and has no published measurements. What it does have
 * is an unusual architecture, so the architecture leads.
 */

const OUTPUTS = [
  {
    name: '.output',
    key: 'output',
    what: 'Terminal agent output, as bytes.',
    href: '/learn/architecture',
  },
  {
    name: '.intents',
    key: 'intents',
    what: 'Side-effect requests, bound for the outbox. The only stream that comes back.',
    href: '/examples/intents-and-resume',
  },
  {
    name: '.traces',
    key: 'traces',
    what: 'Spans per activation, model call, and staged intent.',
    href: '/docs/traces',
  },
  {
    name: '.errors',
    key: 'errors',
    what: 'Dead letters. A record here means the activation committed nothing at all.',
    href: '/docs/errors',
  },
] as const;

const GUARANTEES = [
  {
    title: 'Atomic commit',
    body: 'Memory writes, intents, traces, and outputs are staged and applied only on success. A failed activation mutates nothing.',
    proof: 'tests/core/test_dofn_commit.py',
  },
  {
    title: 'Deterministic intent ids',
    body: 'uuid5 over key, seq, and step index — never a clock. A replayed bundle mints byte-identical intents, so the effector can deduplicate on them.',
    proof: 'tests/semantics/test_effectively_once_e2e.py',
  },
  {
    title: 'Replay-cached model calls',
    body: 'Every call is keyed on its content plus the activation position. A bundle retry costs zero additional provider calls and takes the same path.',
    proof: 'openspec/specs/llm-replay-cache/spec.md',
  },
  {
    title: 'Per-key serialization',
    body: 'Beam processes one element at a time per key, so memory is race-free by construction. Parallelism comes from having many keys.',
    proof: 'tests/core/test_dofn_streaming.py',
  },
] as const;

const BUILT = [
  'The RunAgent transform, keyed state, timers, and the async bridge',
  'Effectively-once side effects via the outbox and reference effector',
  'Human-in-the-loop approvals with fail-closed timeouts at both layers',
  'Anthropic, OpenAI-compatible, and vLLM providers, plus the replay cache',
  'LangGraph, Google ADK, and Pydantic AI adapters, on the conformance matrix',
  'Traces to OTLP or BigQuery; errors and intents to Kafka or Pub/Sub',
  'Long-term memory stores: Bigtable, Redis, Firestore, and SQL',
  'The YAML pipeline provider, and a pyperf benchmark harness with a gated baseline',
] as const;

const NOT_BUILT = [
  'Any published release — 1.0.0 is declared, but no v1.0.0 tag exists and nothing is on PyPI',
  'A Vertex AI provider',
  'Spark beyond the weekly conformance leg — no per-PR job exercises it',
] as const;

/*
 * A subgrid can only borrow rows its parent has declared, so `.ledger` has to
 * be told how many item rows to declare. Deriving it from the two lists means
 * adding a line to either one keeps the columns aligned without a CSS edit.
 */
const LEDGER_ROWS = {
  '--ledger-rows': String(Math.max(BUILT.length, NOT_BUILT.length)),
} as CSSProperties;

export default function Home() {
  const jsonLd = {
    '@context': 'https://schema.org',
    '@type': 'SoftwareSourceCode',
    name: SITE_NAME,
    description: SITE_TAGLINE,
    codeRepository: REPO_URL,
    programmingLanguage: 'Python',
    license: 'https://www.apache.org/licenses/LICENSE-2.0',
    url: absoluteUrl('/'),
  };

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />

      {/* ---------- hero ----------
          The vertical steps are a scale, not a ramp: the eyebrow, headline, and
          lede are one cluster and sit close together; the invocation and the
          call to action are a second, and the widest gap on the page separates
          the two. */}
      <section className="shell band band--lead">
        {/* `IS_RELEASED` is held to the real git-tag state by the verifier, so
            this badge cannot claim a release before the tag exists. */}
        <p className="eyebrow">
          v{PACKAGE_VERSION}
          {IS_RELEASED ? '' : ' · pre-release, not yet on PyPI'} · {LICENSE} · Python 3.11–3.12
        </p>

        <h1 className="display mt-5 max-w-[15ch]">An agent is a Beam transform.</h1>

        <p className="lede mt-6">
          <code className="mono">beam-agents</code> runs your agent as a keyed, stateful step inside
          an Apache Beam pipeline — durable per-key memory, effectively-once side effects, and
          event-time semantics wrapped around the agent you already wrote. It is a runtime, not a
          framework for authoring agents.
        </p>

        {/* `w-fit` rather than `inline-block`: an inline-block sits on a line
            box and collects the line's descender space underneath it, which put
            a few unaccounted-for pixels between this and the buttons. */}
        <p
          className="mono mt-9 w-fit border px-3.5 py-2.5 text-[0.9rem]"
          style={{ borderColor: 'var(--rule)', background: 'var(--paper-2)' }}
        >
          events <span style={{ color: 'var(--ink-3)' }}>|</span> RunAgent(my_agent)
        </p>

        {/* All three share `.btn`'s box, so the two buttons are the same height
            — the filled one used to be 2px shorter, having no border to the
            outlined one's 1px — and the text of all three sits on one line. */}
        <div className="mt-6 flex flex-wrap items-center gap-3">
          <Link href="/learn/what-is-beam-agents" className="btn btn--primary">
            Read the concepts
          </Link>
          <Link href="/learn/install" className="btn btn--secondary">
            Install from source
          </Link>
          <a
            href={REPO_URL}
            className="btn"
            style={{ color: 'var(--ink-2)', paddingInline: '0.35rem' }}
          >
            GitHub ↗
          </a>
        </div>
      </section>

      {/* ---------- the signature: the pipeline ---------- */}
      <section className="rule-top">
        <div className="shell band band--lead">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="eyebrow">The shape of an activation</p>
              <h2 className="h-section mt-2 max-w-[24ch]">
                The loop closes through the message bus, not the DAG.
              </h2>
            </div>
            <p className="max-w-[46ch] text-[0.93rem]" style={{ color: 'var(--ink-2)' }}>
              Beam graphs are acyclic, so an agent that calls a tool and acts on the result cannot
              loop inside the graph. It suspends, and the answer re-enters as a new element on the
              same key.
            </p>
          </div>

          <div className="mt-8">
            <PipelineDiagram />
          </div>
        </div>
      </section>

      {/* ---------- the four outputs ---------- */}
      <section className="rule-top">
        <div className="shell band">
          <p className="eyebrow">Four outputs</p>
          <h2 className="h-section mt-2 max-w-[30ch]">
            A complete pipeline consumes all four. Most forget the last one.
          </h2>

          <dl className="rule-grid mt-9">
            {OUTPUTS.map((output) => (
              <div key={output.key}>
                <dt>
                  <span
                    aria-hidden="true"
                    className="mb-3 block h-[3px] w-9"
                    style={{ background: `var(--s-${output.key})` }}
                  />
                  <Link
                    href={output.href}
                    className="mono text-[0.95rem] font-medium no-underline"
                    style={{ color: `var(--s-${output.key})` }}
                  >
                    {output.name}
                  </Link>
                </dt>
                <dd className="mt-2 text-[0.9rem]" style={{ color: 'var(--ink-2)' }}>
                  {output.what}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      </section>

      {/* ---------- guarantees ---------- */}
      <section className="rule-top">
        <div className="shell band">
          <p className="eyebrow">What the runtime holds</p>
          <h2 className="h-section mt-2 max-w-[30ch]">
            Four invariants, each traceable to the test that gates it.
          </h2>

          {/* A grid, so the four rules start on the same two verticals and each
              pair starts on the same horizontal. */}
          <ul className="mt-9 grid gap-x-12 gap-y-9 sm:grid-cols-2">
            {GUARANTEES.map((guarantee) => (
              <li
                key={guarantee.title}
                className="border-t pt-4"
                style={{ borderColor: 'var(--rule)' }}
              >
                <h3 className="text-[1.02rem] font-semibold" style={{ letterSpacing: '-0.012em' }}>
                  {guarantee.title}
                </h3>
                <p className="mt-1.5 max-w-[46ch] text-[0.93rem]" style={{ color: 'var(--ink-2)' }}>
                  {guarantee.body}
                </p>
                {/* A test path is one unbreakable word. Left alone it sets the
                    grid track's minimum and pushes the whole page wider than
                    the viewport — invisibly, because `body` hides the overflow
                    — at 320px and again at the two-column breakpoint. */}
                <p
                  className="mono mt-2.5 text-[0.72rem]"
                  style={{ color: 'var(--ink-3)', overflowWrap: 'anywhere' }}
                >
                  {guarantee.proof}
                </p>
              </li>
            ))}
          </ul>
        </div>
      </section>

      {/* ---------- real code ---------- */}
      <section className="rule-top">
        <div className="shell band">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="eyebrow">An activation, in full</p>
              <h2 className="h-section mt-2 max-w-[26ch]">
                Every line here is executed on every change.
              </h2>
            </div>
            <p className="max-w-[44ch] text-[0.93rem]" style={{ color: 'var(--ink-2)' }}>
              This is read from a real file at build time, not retyped. It runs on the DirectRunner
              with no credentials and no network, and the repository&rsquo;s required test tier runs
              it.
            </p>
          </div>

          <div className="prose mt-8 max-w-none">
            <Example file="fast_path.py" region="agent" />
          </div>

          <p className="mt-4 text-[0.93rem]">
            <Link href="/examples">All six examples →</Link>
          </p>
        </div>
      </section>

      {/* ---------- honest status ---------- */}
      <section className="rule-top">
        <div className="shell band">
          <p className="eyebrow">Where the project actually is</p>
          <h2 className="h-section mt-2 max-w-[28ch]">
            Read the right-hand column before you plan around this.
          </h2>

          {/* `.ledger` is a subgrid: the two columns share their rows, so an
              item that wraps in one of them keeps its opposite number level
              instead of shifting every rule below it. */}
          <div className="ledger mt-9" style={LEDGER_ROWS}>
            <div>
              <h3 className="eyebrow ledger__head" style={{ color: 'var(--ink)' }}>
                Built and tested
              </h3>
              <ul className="ledger__list">
                {BUILT.map((item) => (
                  <li
                    key={item}
                    className="border-t pt-2.5 text-[0.93rem]"
                    style={{ borderColor: 'var(--rule)' }}
                  >
                    {item}
                  </li>
                ))}
              </ul>
            </div>
            <div>
              <h3 className="eyebrow ledger__head" style={{ color: 'var(--s-errors)' }}>
                Not built
              </h3>
              <ul className="ledger__list">
                {NOT_BUILT.map((item) => (
                  <li
                    key={item}
                    className="border-t pt-2.5 text-[0.93rem]"
                    style={{ borderColor: 'var(--rule)', color: 'var(--ink-2)' }}
                  >
                    {item}
                  </li>
                ))}
              </ul>
              <p className="ledger__note text-[0.9rem]">
                <Link href="/roadmap">The roadmap, marked as roadmap →</Link>
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* ---------- how this site is kept honest ---------- */}
      <section className="rule-top">
        <div className="shell band">
          <p className="eyebrow">About this documentation</p>
          <h2 className="h-section mt-2 max-w-[32ch]">
            Every page declares what backs it, and the build fails when that stops being true.
          </h2>
          <p className="mt-4 max-w-[62ch] text-[0.95rem]" style={{ color: 'var(--ink-2)' }}>
            Pages carry typed claims — symbols, modules, specs, tests — and a verifier imports the
            package and resolves each one before this site can build. A page marked stable needs a
            spec and a test. A page marked planned fails the moment the code it describes starts to
            exist. Code samples are read from files the test suite executes, and the API reference
            is generated from the installed package, not written alongside it.
          </p>
          <p className="mt-4 text-[0.93rem]">
            <Link href="/specs/spec-driven-development">How changes get made here →</Link>
          </p>
        </div>
      </section>
    </>
  );
}
