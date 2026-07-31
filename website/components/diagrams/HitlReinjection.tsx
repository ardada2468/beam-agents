import { Diagram, DgEdge, DgNode, DgText } from '@/components/Diagram';

/**
 * The re-injection path, drawn around the one value that holds it together.
 *
 * The loop itself is the easy half: an intent goes out on `.intents`, an
 * effector runs it, and the result comes back as an ordinary keyed element
 * because a Beam DAG is acyclic and cannot carry a cycle. The half that costs a
 * paragraph in prose is *why the two halves find each other*.
 *
 * `intent_id` is `uuid5(ns, key|seq|step_index)` — a pure function of the
 * activation's position, never a clock and never a counter. So one value shows
 * up in three places that never talk to each other: the intent the activation
 * stages, the key the outbox deduplicates on, and the id a resume is matched
 * against. Putting that box in the middle of the loop with three leaders out of
 * it is the whole argument, and it is also why the example can name the id it
 * is about to receive a result for before the pipeline has run at all.
 *
 * The leaders are drawn faded and **without arrowheads**. That is the load
 * -bearing detail: an arrowhead would make them read as flow, and there is no
 * flow here — nothing travels from that box, the same number is simply already
 * present at all three points. Flow in this diagram is only ever between
 * adjacent nodes.
 *
 * Drawn as an out-row and a back-row rather than one line, mirroring the
 * landing page's animated diagram: the return is not the next stage of a
 * pipeline, it is the same pipeline being entered a second time.
 */

// Three 150-wide nodes with 75px gutters across a ~640-wide user space — about
// the width of the article column, so the 11px labels render at 11px instead of
// being scaled down into illegibility. The gutters are sized to hold a stream
// name (`.intents`) with clear air on both sides: at 54px it collided with the
// node borders either side of it.
const NODE_W = 150;
const NODE_H = 50;

const LEFT = 20;
const MID = 245;
const RIGHT = 470;

const LEFT_C = LEFT + NODE_W / 2;
const MID_C = MID + NODE_W / 2;
const RIGHT_C = RIGHT + NODE_W / 2;

const OUT_Y = 32;
const BACK_Y = 244;
const OUT_MID = OUT_Y + NODE_H / 2;
const BACK_MID = BACK_Y + NODE_H / 2;

// The identity box, parked in the gap between the two rows so its leaders reach
// both of them without crossing anything.
const ID_X = 150;
const ID_Y = 132;
const ID_W = 340;
const ID_H = 54;
// Where the two elbowed leaders leave the box. Inset from its left edge so they
// run clear of the straight leader that drops onto the outbox from `MID_C`.
const ID_TAP = 200;
// Rows the elbowed leaders turn on, between the box and the row they reach.
const TAP_UP = 112;
const TAP_DOWN = 206;

export function HitlReinjection() {
  return (
    <Diagram
      title="Where the intent id is used"
      desc={
        'A box in the middle states that intent_id is uuid5 of a fixed namespace with the ' +
        'entity key, the activation seq, and the step index — a pure function of the ' +
        'position, so no clock and no counter is involved. Three faded leader lines run from ' +
        'that box to the three places the same value appears. The upper row runs left to ' +
        'right: ctx.act stages a ToolIntent at seq 0 step 0, the intent leaves on the intents ' +
        'output to an outbox topic that deduplicates on the intent id, and the effector runs ' +
        'the tool once. The lower row runs right to left: the effector publishes a ToolResult ' +
        'carrying the same intent id, it arrives on the results topic as an ordinary element ' +
        'keyed by entity key, and RunAgent matches the id against the suspended continuation ' +
        'and resumes the activation with ctx.is_resume true. The turn between the two rows ' +
        'travels through the message bus rather than through the pipeline graph, because a ' +
        'Beam DAG is acyclic.'
      }
      caption="One value, derived once from the activation's position, is what lets the outbox deduplicate and the resume find its continuation. Nothing in that derivation reads a clock, which is also why a caller can compute the id before the pipeline runs."
      viewBox="0 0 640 320"
      minWidth={620}
    >
      {/* --- out: the intent leaves ------------------------------------------- */}

      <DgText x={LEFT} y={18} anchor="start" variant="faint">
        THE SAME VALUE IN ALL THREE PLACES
      </DgText>

      <DgNode x={LEFT} y={OUT_Y} w={NODE_W} h={NODE_H} label="ctx.act(...)" sub="SEQ 0 · STEP 0" />
      <DgEdge
        d={`M${LEFT + NODE_W},${OUT_MID} H${MID - 4}`}
        dashed
        stream="intents"
        label=".intents"
        labelX={(LEFT + NODE_W + MID) / 2}
        labelY={OUT_MID - 9}
      />
      <DgNode
        x={MID}
        y={OUT_Y}
        w={NODE_W}
        h={NODE_H}
        label="outbox topic"
        sub="DEDUP BY INTENT_ID"
      />
      <DgEdge d={`M${MID + NODE_W},${OUT_MID} H${RIGHT - 4}`} />
      <DgNode x={RIGHT} y={OUT_Y} w={NODE_W} h={NODE_H} label="effector" sub="RUNS IT ONCE" />

      {/* --- the identity, and the three points it reaches -------------------- */}

      <DgNode
        x={ID_X}
        y={ID_Y}
        w={ID_W}
        h={ID_H}
        label="intent_id = uuid5(ns, key|seq|step_index)"
        sub="A PURE FUNCTION OF THE POSITION"
        stream="intents"
      />
      <g opacity={0.6}>
        <DgEdge d={`M${ID_TAP},${ID_Y} V${TAP_UP} H${LEFT_C} V${OUT_Y + NODE_H}`} arrow={false} />
        <DgEdge d={`M${MID_C},${ID_Y} V${OUT_Y + NODE_H}`} arrow={false} />
        <DgEdge d={`M${ID_TAP},${ID_Y + ID_H} V${TAP_DOWN} H${LEFT_C} V${BACK_Y}`} arrow={false} />
      </g>

      {/*
        The turn. Beam DAGs are acyclic, so this is not an edge in the graph —
        it is the pipeline being entered again on the same key. Set beside the
        line rather than across the middle, where the identity box lives.
      */}
      <DgEdge d={`M${RIGHT_C},${OUT_Y + NODE_H} V${BACK_Y - 4}`} />
      <DgText x={556} y={152} anchor="start" variant="faint">
        THROUGH THE
      </DgText>
      <DgText x={556} y={166} anchor="start" variant="faint">
        MESSAGE BUS
      </DgText>
      <DgText x={556} y={180} anchor="start" variant="faint">
        NOT THE DAG
      </DgText>

      {/* --- back: the result re-enters --------------------------------------- */}

      <DgNode x={RIGHT} y={BACK_Y} w={NODE_W} h={NODE_H} label="ToolResult" sub="SAME INTENT_ID" />
      <DgEdge d={`M${RIGHT},${BACK_MID} H${MID + NODE_W + 4}`} dashed />
      <DgNode
        x={MID}
        y={BACK_Y}
        w={NODE_W}
        h={NODE_H}
        label="results topic"
        sub="KEYED BY ENTITY_KEY"
      />
      <DgEdge d={`M${MID},${BACK_MID} H${LEFT + NODE_W + 4}`} />
      <DgNode x={LEFT} y={BACK_Y} w={NODE_W} h={NODE_H} label="resume" sub="CTX.IS_RESUME TRUE" />
    </Diagram>
  );
}
