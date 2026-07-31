import { Diagram, DgEdge, DgNode, DgText } from '@/components/Diagram';

/**
 * The approval round trip, drawn as a sequence over time.
 *
 * The human-in-the-loop page describes an ordering across two systems — an
 * activation that ends, a person who answers minutes or hours later, and a
 * second activation that picks up where the first stopped. Prose has to say
 * "meanwhile" and "later" a lot to carry that; a sequence diagram carries it in
 * the geometry. Time runs down the page, the four participants run across it.
 *
 * Three things about this drawing are deliberate and easy to get wrong:
 *
 * 1. **`keyed state` is a participant, not an annotation.** The whole point of
 *    suspension is that nothing is blocked and nothing is held in memory: what
 *    survives the wait is a `Continuation` row in Beam keyed state. Giving it
 *    its own lifeline is what makes "the activation ended, the wait did not"
 *    visible.
 * 2. **Lifelines are solid, messages that cross the bus are dashed.** UML draws
 *    lifelines dashed, but on this site dashed means exactly one thing — "this
 *    leaves the pipeline through the message bus and comes back" (see
 *    `app/diagram.css`). The site's key wins over UML's; the lifelines are
 *    drawn solid and faded instead.
 * 3. **The timeout is a second band, not a relabelled first one.** Correctness
 *    invariant 6 makes an unanswered request a distinct outcome with its own
 *    routing, its own state mutation, and its own outputs. Drawing it as an
 *    `alt` fragment says that; drawing one path with a "(or times out)" note
 *    would not.
 *
 * Geometry is authored in a ~640-wide user space so the figure renders about
 * 1:1 in the article column and the 11px labels stay 11px. Widening it would
 * scale the type down instead of showing more.
 */

// -- participants --------------------------------------------------------------

const LANE_W = 118;
const LANE_TOP = 20;
const LANE_H = 40;
const NOTE_H = 42;
const LIFE_TOP = LANE_TOP + LANE_H;
const BOTTOM = 756;

// Lifeline x-centres. Even 144px spacing: a message label is drawn between two
// lifelines at 9.5px mono, which is ~22 characters of room. Every label below
// is written to that budget rather than truncated by it.
const AGENT = 111;
const STATE = 255;
const EFFECTOR = 399;
const APPROVER = 543;

// The left margin, where elements enter the pipeline and outputs leave it.
const MARGIN = 28;
// The two `alt` bands span every lifeline, so they start left of the outgoing
// arrows and end right of the last one.
const BAND_L = 22;
const BAND_R = 624;

const LANES = [
  { c: AGENT, label: 'RunAgent', sub: 'ONE KEY' },
  { c: STATE, label: 'keyed state', sub: 'PER ENTITY KEY' },
  { c: EFFECTOR, label: 'effector', sub: 'OUTSIDE BEAM' },
  { c: APPROVER, label: 'approver', sub: 'A PERSON' },
] as const;

/** The left edge of anything pinned to a lifeline: a lane header or a note. */
function laneX(centre: number): number {
  return centre - LANE_W / 2;
}

/** A message between two lifelines, stopping short so the arrowhead is visible. */
function msg(from: number, to: number, y: number): string {
  const dir = to > from ? 1 : -1;
  return `M${from + 6 * dir},${y} H${to - 4 * dir}`;
}

/** An element arriving from outside the diagram onto the RunAgent lifeline. */
function enters(y: number): string {
  return `M${MARGIN},${y} H${AGENT - 4}`;
}

/** An output leaving the pipeline from the RunAgent lifeline. */
function leaves(y: number): string {
  return `M${AGENT - 4},${y} H${MARGIN}`;
}

/**
 * An `alt` fragment: the outline around one of the two mutually exclusive
 * outcomes. Derived from `BOTTOM` rather than written out, so the lower band
 * and the lifelines cannot drift apart when the diagram grows.
 */
function band(top: number, bottom: number): string {
  return `M${BAND_L},${top} H${BAND_R} V${bottom} H${BAND_L} Z`;
}

/** A note pinned to a lifeline: what that participant holds at that moment. */
function Note({ on, y, label, sub }: { on: number; y: number; label: string; sub: string }) {
  return <DgNode x={laneX(on)} y={y} w={LANE_W} h={NOTE_H} label={label} sub={sub} soft />;
}

export function HitlApprovalSequence() {
  return (
    <Diagram
      title="The approval round trip"
      desc={
        'A sequence diagram with four participants across the top — RunAgent, keyed state, ' +
        'the effector, and a human approver — and time running downwards. An external event ' +
        'activates RunAgent, which calls ctx.request_approval to stage an intent and returns ' +
        'Suspend. The commit writes a continuation holding the activation seq and snapshot, ' +
        'records the pending intent id, and arms HITL_TIMER at the deadline; the activation ' +
        'then ends. The approval intent leaves on the intents output, through the outbox, to ' +
        'the effector, which publishes it to the approver without executing it. The diagram ' +
        'then forks into two outcomes. If the approval arrives before the deadline, it ' +
        're-enters as an ordinary element on the same entity key, the continuation is read ' +
        'back, and the agent is invoked again with ctx.is_resume true, completing on the ' +
        'output stream. If nobody answers, HITL_TIMER fires instead, the pure on_timeout ' +
        'policy runs, and its route either denies with deterministic bytes on the output ' +
        'stream, drops with a hitl_timeout record on the errors stream, or escalates a fresh ' +
        'intent on another channel; Deny and Drop clear the continuation, ending the wait.'
      }
      caption="Time runs down, participants run across. Nothing is blocked during the wait — the activation ends and a Continuation in keyed state is all that survives it. The lower band is the fail-closed outcome, not a variation on the upper one."
      viewBox={`0 0 640 ${BOTTOM + 24}`}
      minWidth={620}
    >
      {/* --- the time axis, so "down" is not left to inference --- */}
      <DgText x={2} y={LIFE_TOP + 8} anchor="start" variant="faint">
        TIME
      </DgText>
      <DgEdge d={`M12,${LIFE_TOP + 16} V${BOTTOM - 6}`} />

      {/* --- participants and their lifelines --- */}
      {LANES.map((lane) => (
        <DgNode
          key={lane.label}
          x={laneX(lane.c)}
          y={LANE_TOP}
          w={LANE_W}
          h={LANE_H}
          label={lane.label}
          sub={lane.sub}
        />
      ))}
      {/*
        Faded so the messages read as the foreground. There is no lighter line
        token, and inventing a colour here would break the rule that every value
        on this site comes from `app/globals.css`; opacity is the honest way to
        get a second weight out of one token.
      */}
      <g opacity={0.45}>
        {LANES.map((lane) => (
          <DgEdge key={lane.label} d={`M${lane.c},${LIFE_TOP} V${BOTTOM}`} arrow={false} />
        ))}
      </g>

      {/* --- 1. the request --------------------------------------------------- */}

      <DgEdge d={enters(88)} />
      <DgText x={118} y={92} anchor="start" variant="faint">
        external event, keyed
      </DgText>

      <Note on={AGENT} y={102} label="request_approval" sub="STAGES AN INTENT" />

      {/*
        The write happens at commit, not at the call: `_commit` runs only on
        activation success and writes MEMORY, LLM_CACHE, CONTINUATION, PENDING,
        SEQ and the timers in a fixed order. Drawing one message for the commit
        rather than one per state cell keeps the ordering claim honest without
        implying five separate round trips.
      */}
      <DgEdge d={msg(AGENT, STATE, 168)} label="Suspend commits" labelX={184} labelY={160} />
      <Note on={STATE} y={176} label="continuation" sub="SEQ · SNAPSHOT" />
      <Note on={STATE} y={224} label="pending ids" sub="HITL_TIMER ARMED" />

      {/* Dashed: it leaves the pipeline here. Tinted because it is `.intents`. */}
      <DgEdge
        d={msg(AGENT, EFFECTOR, 286)}
        dashed
        stream="intents"
        label="ToolIntent · APPROVAL"
        labelX={327}
        labelY={278}
      />
      <DgText x={327} y={302} anchor="middle" variant="faint" stream="intents">
        ON .intents
      </DgText>

      {/*
        An APPROVAL intent is published verbatim and never executed — the
        effector routes it to a channel and publishes no ToolResult, because the
        answer comes back as an Approval, not as a tool return value.
      */}
      <DgEdge
        d={msg(EFFECTOR, APPROVER, 330)}
        dashed
        stream="intents"
        label="published, never run"
        labelX={471}
        labelY={322}
      />

      {/* --- 2a. the answer arrives ------------------------------------------- */}

      <DgEdge d={band(354, 512)} arrow={false} />
      <DgText x={32} y={372} anchor="start" variant="faint">
        IF THE APPROVAL ARRIVES IN TIME
      </DgText>

      <DgEdge
        d={msg(APPROVER, AGENT, 396)}
        dashed
        label="Approval, same key"
        labelX={471}
        labelY={388}
      />
      <DgEdge d={msg(STATE, AGENT, 428)} label="continuation read" labelX={184} labelY={420} />
      <Note on={AGENT} y={436} label="same activation" sub="CTX.IS_RESUME" />
      <DgEdge d={leaves(498)} stream="output" />
      <DgText x={114} y={502} anchor="start" variant="faint" stream="output">
        Complete on .output
      </DgText>

      {/* --- 2b. nobody answers ----------------------------------------------- */}

      <DgEdge d={band(532, BOTTOM)} arrow={false} />
      <DgText x={32} y={550} anchor="start" variant="faint">
        IF NOBODY ANSWERS BY deadline_ms
      </DgText>

      <DgEdge d={msg(STATE, AGENT, 574)} label="HITL_TIMER fires" labelX={184} labelY={566} />
      {/*
        The policy is user code running inside a timer callback, and a timer
        callback re-executes when its bundle is retried. `PURE · RETRY-SAFE` is
        the contract that makes the retry reach the same route as the attempt it
        replaced — not a description of good taste.
      */}
      <Note on={AGENT} y={582} label="on_timeout" sub="PURE · RETRY-SAFE" />

      <DgEdge d={leaves(648)} stream="output" />
      <DgText x={114} y={652} anchor="start" variant="faint" stream="output">
        Deny: on .output
      </DgText>

      <DgEdge d={leaves(676)} stream="errors" />
      <DgText x={114} y={680} anchor="start" variant="faint" stream="errors">
        Drop: on .errors
      </DgText>

      <DgEdge
        d={msg(AGENT, EFFECTOR, 704)}
        dashed
        stream="intents"
        label="Escalate: ask again"
        labelX={327}
        labelY={696}
      />

      {/*
        Only Deny and Drop end the suspension. Escalate leaves the continuation
        in place with a later deadline, which is why the bound on escalations is
        what stops the wait from being unbounded.
      */}
      <DgEdge d={msg(AGENT, STATE, 732)} label="cleared on Deny/Drop" labelX={184} labelY={724} />
    </Diagram>
  );
}
