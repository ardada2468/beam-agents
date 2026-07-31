/**
 * The route table.
 *
 * Every page is lazily imported, so the six page bundles are built and shipped
 * independently — which is also what lets six people build them in parallel
 * without touching each other's files. This module is the *only* shared file
 * between them, and adding a route is a one-line change here.
 *
 * `PagePlaceholder` stands in for a route whose page has not landed yet, so the
 * shell, the navigation, and the live stream are all exercisable from the first
 * commit rather than after the last one.
 */

import { lazy, Suspense } from 'react';
import { Route, Switch } from 'wouter';

import { AppShell } from '@/components/layout/AppShell';
import { EmptyState, SkeletonRows } from '@/components/ui';
import { useLiveStream } from '@/lib/live';

const Overview = lazy(() => import('@/pages/Overview'));
const Activations = lazy(() => import('@/pages/Activations'));
const ActivationDetailPage = lazy(() => import('@/pages/Activations/Detail'));
const Traces = lazy(() => import('@/pages/Traces'));
const TraceDetailPage = lazy(() => import('@/pages/Traces/Detail'));
const Errors = lazy(() => import('@/pages/Errors'));
const Approvals = lazy(() => import('@/pages/Approvals'));
const Models = lazy(() => import('@/pages/Models'));
const Tools = lazy(() => import('@/pages/Tools'));
const Entities = lazy(() => import('@/pages/Entities'));
const EntityDetailPage = lazy(() => import('@/pages/Entities/Detail'));
const Search = lazy(() => import('@/pages/Search'));
const Connect = lazy(() => import('@/pages/Connect'));
const Settings = lazy(() => import('@/pages/Settings'));

function NotFound() {
  return (
    <div className="page">
      <h1>Page not found</h1>
      <EmptyState
        title="Nothing here"
        body="That address does not match a section of the console. Pick one from the navigation."
      />
    </div>
  );
}

export default function App() {
  const live = useLiveStream();

  return (
    <AppShell live={live}>
      <Suspense
        fallback={
          <div className="page">
            <SkeletonRows rows={8} columns={6} />
          </div>
        }
      >
        <Switch>
          <Route path="/" component={Overview} />
          <Route path="/activations" component={Activations} />
          <Route path="/activations/:entityKey/:seq" component={ActivationDetailPage} />
          <Route path="/traces" component={Traces} />
          <Route path="/traces/:traceId" component={TraceDetailPage} />
          <Route path="/errors" component={Errors} />
          <Route path="/approvals" component={Approvals} />
          <Route path="/models" component={Models} />
          <Route path="/tools" component={Tools} />
          <Route path="/entities" component={Entities} />
          <Route path="/entities/:entityKey" component={EntityDetailPage} />
          <Route path="/search" component={Search} />
          <Route path="/connect" component={Connect} />
          <Route path="/settings" component={Settings} />
          <Route component={NotFound} />
        </Switch>
      </Suspense>
    </AppShell>
  );
}
