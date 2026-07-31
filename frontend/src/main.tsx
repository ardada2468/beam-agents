/**
 * Entry point.
 *
 * Fixtures are installed before anything renders, and only in dev: a production
 * bundle can never serve generated data. `installFixtures` returns whether it
 * took over, and the shell shows an unmistakable banner when it did.
 */

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import App from './App';
import { installFixtures } from './lib/fixtures';
import { applyTheme, readTheme } from './lib/theme';
import './styles/base.css';

applyTheme(readTheme());
installFixtures();

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

const container = document.getElementById('root');
if (!container) throw new Error('missing #root');

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
