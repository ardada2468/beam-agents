/**
 * The stand-in for a page that has not landed yet.
 *
 * Deliberately not a blank screen: it states which section this is, what it
 * will show, and — since the console may genuinely have no data — how to send
 * it some. A placeholder that says "coming soon" teaches nothing to someone who
 * opened the console to find out whether it works.
 */

import { EmptyState } from '@/components/ui';

export function Placeholder({ title, describes }: { title: string; describes: string }) {
  return (
    <div className="page">
      <div className="page-title">
        <h1>{title}</h1>
      </div>
      <div className="panel">
        <EmptyState
          title={`${title} is not built yet`}
          body={
            <>
              <p>This section will show {describes}.</p>
              <p style={{ marginTop: 'var(--space-2)' }}>
                The console is reading from its store already — check <a href="/connect">Connect</a>{' '}
                to see which ingest paths are configured.
              </p>
            </>
          }
        />
      </div>
    </div>
  );
}
