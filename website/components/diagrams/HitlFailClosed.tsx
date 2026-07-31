import type { ReactNode } from 'react';
import { Diagram, DgEdge, DgNode, DgText } from '@/components/Diagram';

/**
 * The two fail-closed layers, drawn as two guards with two exits each.
 *
 * Correctness invariant 6 says a timeout fails closed at *both* layers, and the
 * sentence is easy to read as one mechanism described twice. It is not. The two
 * guards run in different processes, read different fields, and produce
 * different records:
 *
 * - **Layer 1** runs inside the pipeline, in `_admission_failure`. It refuses a
 *   resume whose continuation is gone, whose intent id was never pended, whose
 *   deadline has passed, or whose pending intent has expired — four distinct
 *   details on one `orphaned_result` record, so triage never has to re-derive
 *   which one it was.
 * - **Layer 2** runs in the effector, in `refuse_expired`, *before* the dedup
 *   store is touched. It publishes an `EXPIRED` result instead of executing, so
 *   an outage in the store can never make a deadline fail open.
 *
 * The two rows are one component rendered twice rather than two hand-placed
 * rows, because "the same shape twice" is the claim the drawing is making: if
 * the geometry of the layers could drift apart, the diagram would stop saying
 * it.
 *
 * A note on colour, because the four stream colours are a key rather than
 * decoration: only the layer-1 refusal is tinted `errors`, because only it is
 * literally a record on `.errors`. The layer-2 refusal is an ordinary
 * `ToolResult` on the results topic — it re-enters and can resume a
 * still-live continuation so the agent takes its own degraded path — so
 * tinting it would say something untrue about where it lands.
 */

// Column geometry. One row is: what arrives -> the guard -> two exits.
const ARRIVE_X = 20;
const ARRIVE_W = 150;
const GUARD_X = 208;
const GUARD_W = 182;
const EXIT_X = 428;
const EXIT_W = 192;

const ROW_H = 44;
const EXIT_H = 40;

// The two exits sit above and below the guard they belong to.
const PASS_DY = -22;
const FAIL_DY = 28;

// Where the two branches of a fork split. Kept as a separate stub from the
// guard so the trunk is drawn once, in the neutral rule colour: if each branch
// carried its own trunk the tinted one would be painted last and the *admitted*
// branch would leave the guard looking like an error path.
const SPLIT_X = 408;

const LAYER1_Y = 56;
const LAYER2_Y = 246;

interface LayerProps {
  /** Top of the guard row; both exits are positioned from it. */
  y: number;
  /** Which layer this is and where it runs, set above the row. */
  name: string;
  arrive: string;
  arriveSub: string;
  guard: string;
  guardSub: string;
  pass: string;
  passSub: string;
  fail: string;
  failSub: string;
  /**
   * Tint for the refusal, and only when the refusal is genuinely a record on
   * that stream. Layer 2's refusal is not, so it is left untinted.
   */
  failStream?: 'errors';
  /** Free annotation under the row — the four details, or the conclusion. */
  children?: ReactNode;
}

function Layer({
  y,
  name,
  arrive,
  arriveSub,
  guard,
  guardSub,
  pass,
  passSub,
  fail,
  failSub,
  failStream,
  children,
}: LayerProps) {
  const mid = y + ROW_H / 2;
  const passY = y + PASS_DY;
  const failY = y + FAIL_DY;
  const toExit = (exitY: number) => `M${SPLIT_X},${mid} V${exitY + EXIT_H / 2} H${EXIT_X - 4}`;

  return (
    <g>
      <DgText x={ARRIVE_X} y={y - 16} anchor="start" variant="faint">
        {name}
      </DgText>

      <DgNode x={ARRIVE_X} y={y} w={ARRIVE_W} h={ROW_H} label={arrive} sub={arriveSub} soft />
      <DgEdge d={`M${ARRIVE_X + ARRIVE_W},${mid} H${GUARD_X - 4}`} />
      <DgNode x={GUARD_X} y={y} w={GUARD_W} h={ROW_H} label={guard} sub={guardSub} />

      <DgEdge d={`M${GUARD_X + GUARD_W},${mid} H${SPLIT_X}`} arrow={false} />
      <DgEdge d={toExit(passY)} />
      <DgNode x={EXIT_X} y={passY} w={EXIT_W} h={EXIT_H} label={pass} sub={passSub} soft />

      <DgEdge d={toExit(failY)} stream={failStream} />
      <DgNode
        x={EXIT_X}
        y={failY}
        w={EXIT_W}
        h={EXIT_H}
        label={fail}
        sub={failSub}
        stream={failStream}
        soft
      />

      {children}
    </g>
  );
}

export function HitlFailClosed() {
  return (
    <Diagram
      title="Failing closed at both layers"
      desc={
        'Two rows, one per layer, each a guard with two exits. Layer 1 runs in the pipeline: ' +
        'an answer arriving on the entity key meets an admission check that can fail four ' +
        'ways, and either resumes the activation with ctx.is_resume true or is recorded on ' +
        'the errors stream as an orphaned_result whose detail is one of no_continuation, ' +
        'unknown_intent, deadline_passed, or intent_expired. Layer 2 runs in the effector, ' +
        'outside the pipeline: an intent arriving from the outbox is checked against its ' +
        'expires_at_ms before the dedup store is touched, and either the tool runs exactly ' +
        'once and its result re-enters the pipeline, or an EXPIRED result is published and ' +
        'the tool never runs at all. A non-positive expires_at_ms reads as expired, never as ' +
        'unbounded. Because both guards refuse independently, a late approval can neither ' +
        'resume an activation the runtime has given up on nor cause an effect.'
      }
      caption="Two guards in two processes, not one mechanism described twice. Layer 1 decides whether a suspension may resume; layer 2 decides whether an effect may happen at all. A late answer has to get past both, and gets past neither."
      viewBox="0 0 640 360"
      minWidth={620}
    >
      <Layer
        y={LAYER1_Y}
        name="LAYER 1 · IN THE PIPELINE"
        arrive="answer arrives"
        arriveSub="ON THE SAME KEY"
        guard="admission check"
        guardSub="FOUR WAYS TO FAIL"
        pass="resume"
        passSub="CTX.IS_RESUME TRUE"
        fail="orphaned_result"
        failSub="ON .errors"
        failStream="errors"
      >
        {/* The four details, spelled out: they are what makes the record triageable. */}
        <DgText x={EXIT_X} y={140} anchor="start" variant="faint">
          no_continuation · unknown_intent
        </DgText>
        <DgText x={EXIT_X} y={153} anchor="start" variant="faint">
          deadline_passed · intent_expired
        </DgText>
      </Layer>

      {/* A hairline, so the two layers read as two rows rather than one flow. */}
      <g opacity={0.45}>
        <DgEdge d="M20,190 H620" arrow={false} />
      </g>

      {/*
        The expiry test comes first in the effector's phase order — refuse
        -expired, then claim, then execute — precisely so a dedup-store outage
        cannot turn a deadline into an unbounded wait.
      */}
      <Layer
        y={LAYER2_Y}
        name="LAYER 2 · IN THE EFFECTOR"
        arrive="intent arrives"
        arriveSub="FROM THE OUTBOX"
        guard="expires_at_ms"
        guardSub="ZERO READS AS EXPIRED"
        pass="tool runs once"
        passSub="RESULT RE-ENTERS"
        fail="EXPIRED result"
        failSub="NOTHING RAN"
      >
        <DgText x={EXIT_X} y={330} anchor="start" variant="faint">
          A LATE APPROVAL CANNOT ACT
        </DgText>
      </Layer>
    </Diagram>
  );
}
