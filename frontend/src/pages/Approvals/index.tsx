/**
 * Approvals — the human-in-the-loop queue, read-only.
 *
 * There are deliberately no approve and deny buttons on this page. The console
 * is a reader over stored telemetry and never writes to a running pipeline; a
 * decision reaches a suspended activation as an `AgentEnvelope.Approval`
 * published to the pipeline's approvals topic under the same raw `entity_key`,
 * which is a channel the console has no part in. A button here would either lie
 * or quietly become a second, untested write path into a running job, so the
 * page names the real channel instead.
 *
 * The two time limits are shown separately because they are two different
 * fail-closed layers and they can differ:
 *
 * - `deadline_ms` is the **layer-1** HITL timer on the suspension. When it fires
 *   the policy's timeout route runs — deny, by default.
 * - `expires_at_ms` is the **layer-2** TTL on the staged intent. An effector
 *   refuses to execute an intent at or after it, and reads a non-positive value
 *   as already expired rather than as unbounded.
 *
 * Time-to-deadline is measured against the viewer's clock, which ticks, so a
 * queue left open on a screen does not silently go stale.
 */

import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'wouter';

import type { Column } from '@/components/ui';
import {
  Button,
  Chip,
  CopyableId,
  DataTable,
  EmptyState,
  KeyValueGrid,
  Select,
  SkeletonRows,
  StatTile,
  Tabs,
  TileRow,
} from '@/components/ui';
import { ApiError, api, queryKeys } from '@/lib/api';
import type { ApprovalDecision, ApprovalSummary } from '@/lib/api-types';
import {
  EM_DASH,
  formatCount,
  formatDuration,
  formatEntityKey,
  formatRelative,
  formatTimestamp,
  shortId,
} from '@/lib/format';

import './approvals.css';

/** How many approvals to pull. The queue is a working set, not an archive. */
const LIMIT = 200;

/** Re-read the clock often enough that "due in 3m" is not a lie on a left-open tab. */
const TICK_MS = 30_000;

/** A deadline inside this is imminent enough to read as a warning rather than a fact. */
const IMMINENT_MS = 15 * 60_000;

const DECISION_TONE: Record<ApprovalDecision, 'ok' | 'error' | 'warn' | 'pending'> = {
  approved: 'ok',
  denied: 'error',
  expired: 'warn',
  pending: 'pending',
};

const DECISION_LABEL: Record<ApprovalDecision, string> = {
  approved: 'Approved',
  denied: 'Denied',
  expired: 'Expired',
  pending: 'Pending',
};

const SORTS = [
  { value: 'urgency', label: 'Most urgent first' },
  { value: 'requested', label: 'Most recently requested' },
  { value: 'escalations', label: 'Most escalated' },
];

/** A value that is missing rather than zero, with the reason on hover. */
function Missing({ why }: { why: string }) {
  return (
    <span className="faint" title={why}>
      {EM_DASH}
    </span>
  );
}

/** True when a still-pending approval is past the deadline its suspension recorded. */
function isOverdue(row: ApprovalSummary, now: number): boolean {
  return row.decision === 'pending' && row.deadline_ms !== null && row.deadline_ms <= now;
}

/**
 * The deadline cell.
 *
 * A pending row reads as time remaining, because that is the only question
 * anyone opens this page to answer. A decided row reads as the plain instant,
 * since counting down to a deadline that no longer governs anything would be
 * theatre.
 */
function Deadline({ row, now }: { row: ApprovalSummary; now: number }) {
  if (row.deadline_ms === null) {
    return <Missing why="This suspension recorded no HITL deadline" />;
  }
  if (row.decision !== 'pending') {
    return (
      <span className="muted" title={formatTimestamp(row.deadline_ms)}>
        {formatRelative(row.deadline_ms, now)}
      </span>
    );
  }
  if (row.deadline_ms <= now) {
    // Coarse rather than exact: "passed 6d ago" is what an operator triages on,
    // and `formatDuration` would render the same fact as "153h 41m".
    return (
      <Chip tone="error" title={formatTimestamp(row.deadline_ms)}>
        Passed {formatRelative(row.deadline_ms, now)}
      </Chip>
    );
  }
  const remaining = row.deadline_ms - now;
  return (
    <Chip
      tone={remaining <= IMMINENT_MS ? 'warn' : 'info'}
      title={formatTimestamp(row.deadline_ms)}
    >
      Due in {formatDuration(remaining)}
    </Chip>
  );
}

function columns(now: number): Column<ApprovalSummary>[] {
  return [
    {
      key: 'decision',
      header: 'Decision',
      render: (row) => (
        <Chip tone={DECISION_TONE[row.decision]}>{DECISION_LABEL[row.decision]}</Chip>
      ),
    },
    {
      key: 'deadline',
      header: 'Deadline',
      width: '15%',
      render: (row) => <Deadline row={row} now={now} />,
    },
    {
      key: 'intent',
      header: 'Intent',
      render: (row) => (
        <CopyableId value={row.intent_id} display={shortId(row.intent_id)} label="intent id" />
      ),
    },
    {
      key: 'entity',
      header: 'Entity key',
      render: (row) => (
        <CopyableId
          value={row.entity_key}
          display={formatEntityKey(row.entity_key)}
          label="entity key"
        />
      ),
    },
    {
      key: 'seq',
      header: 'Seq',
      numeric: true,
      render: (row) => (
        <Link
          href={`/activations/${encodeURIComponent(row.entity_key)}/${row.seq}`}
          aria-label={`Open activation ${formatEntityKey(row.entity_key)} sequence ${row.seq}`}
        >
          {formatCount(row.seq)}
        </Link>
      ),
    },
    { key: 'channel', header: 'Channel', render: (row) => <span>{row.tool_name}</span> },
    {
      key: 'requested',
      header: 'Requested',
      numeric: true,
      render: (row) => (
        <span title={formatTimestamp(row.requested_ms)}>
          {formatRelative(row.requested_ms, now)}
        </span>
      ),
    },
    {
      key: 'expires',
      header: 'Expiry',
      numeric: true,
      render: (row) =>
        row.expires_at_ms === null ? (
          <Missing why="This intent recorded no expires_at_ms" />
        ) : (
          <span title={formatTimestamp(row.expires_at_ms)}>
            {formatRelative(row.expires_at_ms, now)}
          </span>
        ),
    },
    {
      key: 'escalations',
      header: 'Escalations',
      numeric: true,
      render: (row) => formatCount(row.escalations),
    },
    {
      key: 'decided',
      header: 'Decided',
      numeric: true,
      render: (row) =>
        row.decided_ms === null ? (
          <Missing why="Nothing has been recorded against this intent yet" />
        ) : (
          <span title={formatTimestamp(row.decided_ms)}>{formatRelative(row.decided_ms, now)}</span>
        ),
    },
  ];
}

function sortRows(rows: ApprovalSummary[], sort: string): ApprovalSummary[] {
  const sorted = [...rows];
  if (sort === 'requested') {
    return sorted.sort((a, b) => b.requested_ms - a.requested_ms);
  }
  if (sort === 'escalations') {
    return sorted.sort((a, b) => b.escalations - a.escalations || b.requested_ms - a.requested_ms);
  }
  // Urgency: everything still awaiting a decision first, then the earliest
  // deadline. A pending row with no recorded deadline sorts after the ones that
  // have one — it has no clock to be late against.
  return sorted.sort((a, b) => {
    const pending = Number(b.decision === 'pending') - Number(a.decision === 'pending');
    if (pending !== 0) return pending;
    if (a.deadline_ms === null && b.deadline_ms !== null) return 1;
    if (b.deadline_ms === null && a.deadline_ms !== null) return -1;
    if (a.deadline_ms !== null && b.deadline_ms !== null && a.deadline_ms !== b.deadline_ms) {
      return a.deadline_ms - b.deadline_ms;
    }
    return a.requested_ms - b.requested_ms;
  });
}

const DERIVATION = [
  {
    key: 'identity',
    label: 'Intent, entity key, seq',
    value: (
      <>
        The staged approval intent&rsquo;s own identity. <code>(entity_key, seq)</code> is the
        activation it suspended, so the sequence number links to that activation&rsquo;s full
        record.
      </>
    ),
  },
  {
    key: 'channel',
    label: 'Channel',
    value: (
      <>
        The <code>tool_name</code> the approval request carries — the channel an effector routes it
        to, not a registered tool. It defaults to <code>approval</code>, and the effector never
        executes it.
      </>
    ),
  },
  {
    key: 'deadline',
    label: 'Deadline (layer 1)',
    value: (
      <>
        The HITL timer on the suspension, from <code>HitlPolicy.timeout_ms</code> (24h by default).
        When it fires, the policy&rsquo;s timeout route runs against the live continuation —{' '}
        <code>Deny</code> unless the pipeline configured otherwise.
      </>
    ),
  },
  {
    key: 'expiry',
    label: 'Intent expiry (layer 2)',
    value: (
      <>
        <code>expires_at_ms</code> on the staged intent, from <code>HitlPolicy.intent_ttl_ms</code>{' '}
        (1h by default). An effector refuses to execute an intent at or after it, and reads a
        non-positive value as already expired rather than as unbounded.
      </>
    ),
  },
  {
    key: 'escalations',
    label: 'Escalations',
    value: (
      <>
        How many times a timeout re-staged the request on an escalation channel, bounded by{' '}
        <code>HitlPolicy.max_escalations</code>. The bound is what keeps an escalate loop from
        becoming a fail-open hole.
      </>
    ),
  },
  {
    key: 'decision',
    label: 'Decision',
    value: (
      <>
        What the runtime recorded: an approval or denial that resumed the activation, or an expiry.{' '}
        <strong>Pending</strong> means nothing has been recorded against this intent yet — not that
        nobody was asked.
      </>
    ),
  },
];

export default function Page() {
  const [tab, setTab] = useState<'pending' | 'all'>('pending');
  const [sort, setSort] = useState('urgency');
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), TICK_MS);
    return () => clearInterval(timer);
  }, []);

  const pending = useQuery({
    queryKey: queryKeys.approvals(true),
    queryFn: () => api.approvals(true, LIMIT),
  });
  const all = useQuery({
    queryKey: queryKeys.approvals(false),
    queryFn: () => api.approvals(false, LIMIT),
  });

  const active = tab === 'pending' ? pending : all;
  const rows = useMemo(() => sortRows(active.data ?? [], sort), [active.data, sort]);

  // The page is only truthful when both queries answered: the tiles count the
  // whole recorded queue while the table shows one tab of it, so a failure in
  // either one has to read as a failure rather than as a row of zeros.
  const failed = all.isError ? all : pending.isError ? pending : null;

  const everything = useMemo(() => all.data ?? [], [all.data]);
  const totals = useMemo(
    () => ({
      pending: everything.filter((row) => row.decision === 'pending').length,
      overdue: everything.filter((row) => isOverdue(row, now)).length,
      escalated: everything.filter((row) => row.escalations > 0).length,
      approved: everything.filter((row) => row.decision === 'approved').length,
      denied: everything.filter((row) => row.decision === 'denied').length,
      expired: everything.filter((row) => row.decision === 'expired').length,
    }),
    [everything, now],
  );

  const tableColumns = useMemo(() => columns(now), [now]);

  return (
    <div className="page">
      <div className="page-title">
        <h1>Approvals</h1>
        <Select
          label="Order"
          options={SORTS}
          value={sort}
          onChange={(event) => setSort(event.target.value)}
        />
      </div>

      <p className="muted" style={{ maxWidth: '84ch' }}>
        Approval intents the store has recorded, with the deadline each suspension is waiting
        against and whatever decision the runtime recorded. The page reads up to {LIMIT} records, so
        a queue longer than that is not counted in full.
      </p>

      {/*
        A disclosure, not a panel.

        The claim that matters — this page cannot approve anything — is one
        line, and it was spending the entire top of the screen to say it:
        somebody arriving to look at the queue read a paragraph about
        `AgentEnvelope.Approval` before reaching a single row of one. The
        summary carries the whole point, and the detail stays one click away for
        the reader who has just noticed there is no Approve button and wants to
        know where the button actually is.
      */}
      <details className="ap-readonly">
        <summary>
          <Chip tone="neutral" plain>
            Read-only
          </Chip>
          <span>This page cannot approve or deny — where to do it instead</span>
        </summary>
        <div className="ap-readonly__body">
          <p>
            The console reads stored telemetry and never writes to a running pipeline. A decision
            reaches a suspended activation as an{' '}
            <code>AgentEnvelope.Approval(intent_id, approved, approver, decided_at_ms)</code>{' '}
            published to the pipeline&rsquo;s approvals topic under the same raw{' '}
            <code>entity_key</code> — the channel the effector forwarded the{' '}
            <code>kind = APPROVAL</code> intent to. Approve or deny in that surface; the outcome
            appears here once the resume is recorded.
          </p>
          <p className="muted">
            If nothing answers before the deadline, the runtime resolves the suspension itself: the
            HITL timer fires and the policy&rsquo;s timeout route runs, which denies by default.
            Waiting is a decision.
          </p>
        </div>
      </details>

      {all.isPending || pending.isPending ? (
        <div className="panel">
          <SkeletonRows rows={6} columns={8} />
        </div>
      ) : failed !== null ? (
        <div className="panel">
          <EmptyState
            title="Could not load the approval queue"
            body={
              <p>
                {failed.error instanceof ApiError
                  ? `The console API answered ${failed.error.status}.`
                  : 'The console API could not be reached.'}{' '}
                Check that <code>beam-agents-console</code> is running and serving this page.
              </p>
            }
            action={
              <Button
                onClick={() => {
                  void all.refetch();
                  void pending.refetch();
                }}
                disabled={failed.isFetching}
              >
                {failed.isFetching ? 'Retrying…' : 'Retry'}
              </Button>
            }
          />
        </div>
      ) : (
        <>
          <TileRow>
            <StatTile
              label="Pending"
              value={formatCount(totals.pending)}
              meta="awaiting a decision"
            />
            <StatTile
              label="Overdue"
              value={formatCount(totals.overdue)}
              meta="pending past the deadline"
            />
            <StatTile
              label="Escalated"
              value={formatCount(totals.escalated)}
              meta="re-staged at least once"
            />
            <StatTile label="Approved" value={formatCount(totals.approved)} meta="resumed" />
            <StatTile label="Denied" value={formatCount(totals.denied)} meta="resumed" />
            <StatTile
              label="Expired"
              value={formatCount(totals.expired)}
              meta="never answered in time"
            />
          </TileRow>

          <section className="panel">
            <div className="panel-header" style={{ paddingBottom: 0 }}>
              <Tabs
                label="Approval queue"
                active={tab}
                onChange={(key) => setTab(key === 'all' ? 'all' : 'pending')}
                items={[
                  { key: 'pending', label: 'Pending', count: pending.data?.length ?? null },
                  { key: 'all', label: 'All recorded', count: all.data?.length ?? null },
                ]}
              />
            </div>
            <DataTable
              columns={tableColumns}
              rows={rows}
              rowKey={(row) => row.intent_id}
              caption="Approval intents with their deadlines, expiries, escalations, and recorded decisions"
              empty={
                tab === 'pending' ? (
                  <EmptyState
                    title="Nothing is waiting on a human"
                    body={
                      <p>
                        Every approval the store holds has a recorded decision. An intent appears
                        here when an activation calls <code>ctx.request_approval(...)</code>,
                        suspends, and stages a <code>kind = APPROVAL</code> intent that no decision
                        has answered yet.
                      </p>
                    }
                    action={
                      <Button onClick={() => setTab('all')}>See all recorded approvals</Button>
                    }
                  />
                ) : (
                  <EmptyState
                    title="No approvals recorded"
                    body={
                      <>
                        <p>
                          Approvals appear once an activation calls{' '}
                          <code>ctx.request_approval(...)</code> and its suspension reaches the
                          console.
                        </p>
                        <p style={{ marginTop: 'var(--space-2)' }}>
                          Point the pipeline at the console with{' '}
                          <code>traces_to=&quot;console://localhost:8787&quot;</code> and{' '}
                          <code>sink_resolver=ConsoleSinkResolver()</code>, and route the
                          effector&rsquo;s approval intents to a surface that can answer them.
                        </p>
                      </>
                    }
                    action={<a href="/connect">Configure an ingest path</a>}
                  />
                )
              }
            />
          </section>

          <section className="panel">
            <div className="panel-header">
              <h2 style={{ fontSize: 'var(--text-md)' }}>What each column means</h2>
            </div>
            <div className="panel-body">
              <KeyValueGrid entries={DERIVATION} />
            </div>
          </section>
        </>
      )}
    </div>
  );
}
