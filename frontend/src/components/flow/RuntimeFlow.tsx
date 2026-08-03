/**
 * The runtime, drawn as the state machine it actually is.
 *
 * Every other screen in this console is a list. Lists answer "which one", and
 * they are the right shape for that — but none of them answers "what is the
 * system doing", which is the question somebody opening a console at 3am has
 * first. This view is that answer: one activation's whole possible life, with
 * the live count standing on each state.
 *
 * **Why a fixed lifecycle and not a discovered graph.** A force-directed blob
 * of entity keys would look like a graph and mean nothing: entity keys are
 * independent by construction — the runtime keys by them precisely so they do
 * not interact — so edges between them would be invented. What genuinely has
 * edges is the lifecycle every activation moves through, and that shape is
 * fixed by the runtime, not discovered from data. Drawing the known shape and
 * putting measured counts on it is the honest version of "show me the graph".
 *
 * **The four states add up, and the count that does not is kept off the
 * diagram.** `ActivationStatus` is closed — completed, suspended, in_flight,
 * error — so those four sum to the admitted total and a reader can check the
 * picture against itself. Error *records* cannot join them: an activation that
 * failed twice wrote two, which is why `_queries.py` warns that the two must
 * never be divided into each other. That figure is reported in the caption
 * under the diagram instead of standing on a node, because a fifth box would
 * read as a fifth slice of a four-slice whole.
 *
 * **The resume edge carries no number.** Nothing in the overview payload counts
 * resumes, and this console does not print figures it did not measure. The edge
 * is drawn because the path is real; it is unlabelled because the volume is
 * unknown.
 */

import { useLocation } from 'wouter';

import { formatCompact } from '@/lib/format';

import './flow.css';

/* -- Geometry -------------------------------------------------------------- */

/*
 * A fixed viewBox, scaled by CSS. The lifecycle has a known number of nodes in
 * a known arrangement, so laying it out by hand beats a layout engine: the
 * result is identical on every render and every screen, and the feedback edge
 * can be routed around the nodes deliberately rather than through them.
 */
const VIEW_W = 1120;
/*
 * Tall enough to clear the resume edge *and* its label below the bottom row of
 * nodes. At the earlier height the label's baseline fell inside the failed
 * node's box and, since edges paint before nodes, it was drawn and then
 * covered — present in the DOM, invisible on screen.
 */
const VIEW_H = 396;

const NODE_W = 188;
const NODE_H = 84;

/*
 * The node ids are a closed set, and the type says so. `Object.fromEntries`
 * over an array would have typed the lookup as possibly-undefined and pushed a
 * non-null assertion onto every edge coordinate below — for a table whose keys
 * are written twenty lines above it.
 */
type NodeId = 'admitted' | 'in_flight' | 'completed' | 'suspended' | 'failed' | 'approvals';

interface NodeSpec {
  id: NodeId;
  label: string;
  /** What the number counts. Never the same word twice across two nodes. */
  unit: string;
  x: number;
  y: number;
  tone: 'neutral' | 'run' | 'ok' | 'suspended' | 'error' | 'info';
  href: string;
  /** Read out after the label, so the link is unambiguous out of context. */
  hint: string;
}

const COL_1 = 16;
const COL_2 = 276;
const COL_3 = 552;
const COL_4 = 856;

const ROW_MID = 138;
const ROW_TOP = 20;
const ROW_BOTTOM = 256;

const NODE: Record<NodeId, NodeSpec> = {
  admitted: {
    id: 'admitted',
    label: 'Admitted',
    unit: 'activations',
    x: COL_1,
    y: ROW_MID,
    tone: 'neutral',
    href: '/activations',
    hint: 'every activation in this window',
  },
  in_flight: {
    id: 'in_flight',
    label: 'In flight',
    unit: 'running now',
    x: COL_2,
    y: ROW_MID,
    tone: 'run',
    href: '/activations?status=in_flight',
    hint: 'activations still running',
  },
  completed: {
    id: 'completed',
    label: 'Completed',
    unit: 'activations',
    x: COL_3,
    y: ROW_TOP,
    tone: 'ok',
    href: '/activations?status=completed',
    hint: 'activations that finished',
  },
  suspended: {
    id: 'suspended',
    label: 'Suspended',
    unit: 'activations',
    x: COL_3,
    y: ROW_MID,
    tone: 'suspended',
    href: '/activations?status=suspended',
    hint: 'activations checkpointed and waiting',
  },
  failed: {
    id: 'failed',
    label: 'Failed',
    unit: 'activations',
    x: COL_3,
    y: ROW_BOTTOM,
    tone: 'error',
    href: '/activations?status=error',
    hint: 'activations that ended in an error state',
  },
  approvals: {
    id: 'approvals',
    label: 'At the effector',
    unit: 'awaiting a decision',
    x: COL_4,
    y: ROW_MID,
    tone: 'info',
    href: '/approvals',
    hint: 'intents waiting on an approval',
  },
};

/** Draw order. The record above is the lookup; this is the paint list. */
const NODES: readonly NodeSpec[] = Object.values(NODE);

/** Right edge midpoint of a node box. */
function rightOf(node: { x: number; y: number }) {
  return { x: node.x + NODE_W, y: node.y + NODE_H / 2 };
}

/** Left edge midpoint of a node box. */
function leftOf(node: { x: number; y: number }) {
  return { x: node.x, y: node.y + NODE_H / 2 };
}

/**
 * A cubic with horizontal control handles, so every edge leaves and enters a
 * node on the horizontal — the branch fan reads as one gesture instead of
 * three unrelated diagonals.
 */
function curve(from: { x: number; y: number }, to: { x: number; y: number }): string {
  const dx = Math.max(48, (to.x - from.x) * 0.5);
  return `M ${from.x} ${from.y} C ${from.x + dx} ${from.y}, ${to.x - dx} ${to.y}, ${to.x} ${to.y}`;
}

/* -- Edge weight ----------------------------------------------------------- */

/**
 * Stroke width carries share of outflow — unlike the span rules in the trace
 * view, where width deliberately encodes nothing because the runtime never
 * measured a duration. Here the quantity is a count the store really holds, so
 * letting the line carry it is reporting, not decoration. The floor keeps a
 * real-but-tiny branch visible instead of hairline-invisible.
 */
const STROKE_MIN = 1.25;
const STROKE_MAX = 7;

function strokeFor(value: number, total: number): number {
  if (total <= 0 || value <= 0) return STROKE_MIN;
  return STROKE_MIN + (STROKE_MAX - STROKE_MIN) * Math.min(1, value / total);
}

/* -- Component ------------------------------------------------------------- */

export interface RuntimeFlowProps {
  activations: number;
  inFlight: number;
  completed: number;
  suspended: number;
  /**
   * Error *records* in the window. Not a state — an activation that failed
   * twice contributes two — so it is reported under the diagram rather than
   * standing on a node.
   */
  errorRecords: number;
  /** Intents parked at the effector awaiting a decision, or null if unknown. */
  awaitingApproval: number | null;
  /** Drives the one animated element. False stops it dead. */
  live: boolean;
}

export default function RuntimeFlow({
  activations,
  inFlight,
  completed,
  suspended,
  errorRecords,
  awaitingApproval,
  live,
}: RuntimeFlowProps) {
  const [, navigate] = useLocation();

  /*
   * The failed count is the remainder, not a payload field.
   *
   * `ActivationStatus` is a closed set of four — completed, suspended,
   * in_flight, error — and the overview reports the first three plus the total,
   * so the fourth is exactly what is left. Subtracting integers is also the
   * only derivation that cannot drift from the total: multiplying
   * `error_ratio` by `activations` reproduces the same figure but reintroduces
   * a rounding error, and the whole point of putting these five numbers on one
   * picture is that a reader can add them up and get the sixth.
   *
   * Clamped at zero because the three counts and the total are read in one
   * query but the store is being written to continuously; a negative remainder
   * would be a torn read, and drawing "-2 failed" is worse than drawing zero.
   */
  const failed = Math.max(0, activations - completed - suspended - inFlight);

  const counts: Record<NodeId, number | null> = {
    admitted: activations,
    in_flight: inFlight,
    completed,
    suspended,
    failed,
    approvals: awaitingApproval,
  };

  const branchTotal = Math.max(1, completed + suspended + failed);

  const edges = [
    {
      id: 'admit',
      d: curve(rightOf(NODE.admitted), leftOf(NODE.in_flight)),
      width: strokeFor(activations, Math.max(1, activations)),
    },
    {
      id: 'to-completed',
      d: curve(rightOf(NODE.in_flight), leftOf(NODE.completed)),
      width: strokeFor(completed, branchTotal),
    },
    {
      id: 'to-suspended',
      d: curve(rightOf(NODE.in_flight), leftOf(NODE.suspended)),
      width: strokeFor(suspended, branchTotal),
    },
    {
      id: 'to-failed',
      d: curve(rightOf(NODE.in_flight), leftOf(NODE.failed)),
      width: strokeFor(failed, branchTotal),
    },
    {
      id: 'to-effector',
      d: curve(rightOf(NODE.suspended), leftOf(NODE.approvals)),
      width: strokeFor(suspended, branchTotal),
    },
  ];

  /*
   * The resume path, routed under every node rather than between them: a
   * suspension that is approved re-enters the same activation as a second
   * attempt under the same seq, so this edge closes the loop back to "in
   * flight". Drawn with a dashed stroke because, unlike the edges above, no
   * figure in the payload says how much travels along it.
   */
  const resumeY = VIEW_H - 24;
  const resumeFrom = { x: COL_4 + NODE_W / 2, y: ROW_MID + NODE_H };
  const resumeTo = { x: COL_2 + NODE_W / 2, y: ROW_MID + NODE_H };
  const resumePath =
    `M ${resumeFrom.x} ${resumeFrom.y} L ${resumeFrom.x} ${resumeY - 16} ` +
    `Q ${resumeFrom.x} ${resumeY} ${resumeFrom.x - 16} ${resumeY} ` +
    `L ${resumeTo.x + 16} ${resumeY} ` +
    `Q ${resumeTo.x} ${resumeY} ${resumeTo.x} ${resumeY - 16} ` +
    `L ${resumeTo.x} ${resumeTo.y}`;

  return (
    <figure className="flow">
      <svg
        className={live ? 'flow__svg is-live' : 'flow__svg'}
        viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
        role="img"
        aria-labelledby="flow-title flow-desc"
      >
        <title id="flow-title">The activation lifecycle, with current counts</title>
        <desc id="flow-desc">
          {`${formatCompact(activations)} activations admitted: ${formatCompact(completed)} ` +
            `completed, ${formatCompact(suspended)} suspended, ${formatCompact(inFlight)} still ` +
            `in flight, ${formatCompact(failed)} failed. Approved suspensions resume as a second ` +
            `attempt on the same activation.`}
        </desc>

        <defs>
          <marker
            id="flow-arrow"
            viewBox="0 0 8 8"
            refX="7"
            refY="4"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 1 L 7 4 L 0 7 z" className="flow__arrowhead" />
          </marker>
        </defs>

        <g className="flow__edges">
          {edges.map((edge) => (
            <g key={edge.id}>
              <path d={edge.d} className="flow__edge" strokeWidth={edge.width} />
              <path d={edge.d} className="flow__pulse" strokeWidth={edge.width} />
            </g>
          ))}
          <path
            d={resumePath}
            className="flow__edge flow__edge--resume"
            markerEnd="url(#flow-arrow)"
          />
          <text x={(resumeFrom.x + resumeTo.x) / 2} y={resumeY - 9} className="flow__edge-label">
            approved — resumes as the next attempt
          </text>
        </g>

        <g className="flow__nodes">
          {NODES.map((node) => {
            const value = counts[node.id];
            return (
              <a
                key={node.id}
                href={node.href}
                className={`flow__node flow__node--${node.tone}`}
                onClick={(event) => {
                  event.preventDefault();
                  navigate(node.href);
                }}
                aria-label={`${node.label}: ${
                  value === null ? 'not counted' : formatCompact(value)
                } ${node.unit} — ${node.hint}`}
              >
                <rect
                  x={node.x}
                  y={node.y}
                  width={NODE_W}
                  height={NODE_H}
                  rx={10}
                  className="flow__box"
                />
                <rect
                  x={node.x}
                  y={node.y}
                  width={3}
                  height={NODE_H}
                  className="flow__spine"
                  rx={1.5}
                />
                <text x={node.x + 20} y={node.y + 26} className="flow__label">
                  {node.label}
                </text>
                <text x={node.x + 20} y={node.y + 60} className="flow__value">
                  {value === null ? '—' : formatCompact(value)}
                </text>
                <text x={node.x + 20} y={node.y + 76} className="flow__unit">
                  {node.unit}
                </text>
              </a>
            );
          })}
        </g>
      </svg>

      <figcaption className="flow__note">
        <span>
          Completed, suspended, in flight, and failed are the four states an activation can be in,
          so they add up to admitted. Failed is the remainder — it is not reported directly.
        </span>
        <span>
          Those {formatCompact(activations)} activations produced{' '}
          <strong>{formatCompact(errorRecords)} error records</strong>, which is a different count:
          an activation that failed twice wrote two. <strong>At the effector</strong> is a queue,
          not a window — an intent that has been waiting since before this window still blocks a
          resume today. The resume edge carries no figure because nothing recorded counts one.
        </span>
      </figcaption>
    </figure>
  );
}
