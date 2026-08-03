/**
 * Entry point.
 *
 * Fixtures are installed before anything renders, and only in dev with no
 * console answering: a production bundle can never serve generated data, and a
 * dev server proxying to a live console must show that console rather than
 * generated records. `installFixtures` returns whether it took over, and the
 * shell shows an unmistakable banner when it did.
 */

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import App from './App';
import { installFixtures } from './lib/fixtures';
import { applyTheme, readTheme } from './lib/theme';
import './styles/base.css';

applyTheme(readTheme());

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // The live stream is what keeps the page current, so nothing here needs
      // to poll. Refetching on window focus would fire a burst of queries every
      // time someone alt-tabs back to a console they left open.
      refetchOnWindowFocus: false,
      staleTime: 10_000,
      retry: 1,
    },
  },
});

// An async bootstrap rather than a top-level `await`: the build targets
// baseline browsers that predate top-level await (see `build.target`), and
// raising that target to satisfy a dev-only probe would change the shipped
// bundle. `installFixtures` decides whether the interceptor is installed at
// all, so it has to settle before the first component mounts and queries.
async function main(): Promise<void> {
  await installFixtures();

  const container = document.getElementById('root');
  if (!container) throw new Error('missing #root');

  createRoot(container).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </StrictMode>,
  );
}

void main();
