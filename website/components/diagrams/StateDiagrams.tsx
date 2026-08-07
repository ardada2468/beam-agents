import { Diagram, DgEdge, DgNode, DgText } from '@/components/Diagram';

/**
 * The two diagrams on `/learn/state-and-memory`.
 *
 * That page describes something entirely spatial — five state cells and two
 * timers hanging off one entity key, and a write that exists in three places
 * over its life (a process, then durable state, then nothing) — and it did it
 * in prose and two tables. Prose is the wrong medium for both: a reader who
 * has just learned that `TTL_TIMER` is event-time and `HITL_TIMER` is real
 * time has no way to *see* that those are two different clocks, and the
 * consequence of them being two different clocks is the one failure the page
 * has to warn about.
 *
 * Both diagrams are drawn from `src/beam_agents/core/dofn.py` — the state and
 * timer specs at the top of `_AgentDoFn`, `_commit`, and `on_ttl` — not from
 * the page's prose. If the runtime grows a sixth cell or a third timer, this
 * file is wrong and has to change with it.
 *
 * Both live in one file, and one file only, so `lib/mdx.tsx` gains a single
 * import: seven of these landed in the same batch, and every extra line in the
 * shared component map is a merge conflict someone has to resolve by hand.
 *
 * Geometry note that governs every number below: a figure gets the full article
 * column, which is about 660px on a laptop and wider still on a tablet, where
 * the two side gutters are gone. So both diagrams are authored 600 units wide
 * and grow downward. A wider `viewBox` would have scrolled sideways on a
 * 27-inch display, which reads as a mistake rather than as a deliberately large
 * drawing. They still scroll on a phone, and `Diagram` holds the floor at this
 * width so the type never scales below the size it is drawn at.
 */

const W = 600;
const MARGIN = 14;
const RIGHT = W - MARGIN;

/* -- diagram 1: what one key holds ----------------------------------------- */

/**
 * The five cells, in declaration order from `_AgentDoFn`.
 *
 * `spec` is the Beam state spec, because the spec is what decides how the cell
 * behaves: read-modify-write is a whole-value swap, a bag accumulates, and a
 * combining value merges. `note` is what the cell is *for*, which is the part
 * a reader is actually looking up.
 */
const CELLS = [
  { name: 'MEMORY', spec: 'READ-MODIFY-WRITE', note: 'working memory, one MemoryBlob' },
  { name: 'CONTINUATION', spec: 'READ-MODIFY-WRITE', note: 'where a suspended activation resumes' },
  { name: 'LLM_CACHE', spec: 'READ-MODIFY-WRITE', note: 'the bounded replay cache' },
  { name: 'PENDING', spec: 'BAG', note: 'tool intents waiting for an answer' },
  { name: 'SEQ', spec: 'COMBINING SUM', note: 'activations committed on this key' },
] as const;

const CELL_W = 196;
const CELL_H = 42;
const CELL_Y0 = 32;
const CELL_PITCH = 52;
// Where each cell's plain-language note starts. Far enough right that the
// longest spec line ("READ-MODIFY-WRITE") cannot reach it.
const NOTE_X = 224;
// Where each timer's fire point is ticked on its axis. Far enough past the
// timer box (which ends at 214) that the run of axis between them reads as
// elapsed time rather than as a gap.
const MARK_X = 430;

/**
 * One timer, drawn sitting on its own clock.
 *
 * The two are stacked rather than placed side by side, and their axes run the
 * full width, because that is the only way to make "these are two independent
 * clocks" a thing the eye reads rather than a thing the caption asserts. Both
 * rows are laid out identically so the domain name is the only difference
 * between them.
 */
function TimerRow({
  y,
  domain,
  qualifier,
  name,
  armed,
  mark,
  fires,
  effect,
}: {
  /** Baseline of the domain label; everything else is measured off it. */
  y: number;
  /** "EVENT TIME" / "REAL TIME" — the thing the two rows differ by. */
  domain: string;
  /** Beam's own name for the domain, so the runtime stays greppable. */
  qualifier: string;
  name: string;
  armed: string;
  /** The point on the axis the timer is set to, e.g. "TTL MARK". */
  mark: string;
  fires: string;
  effect: string;
}) {
  const axisY = y + 36;
  return (
    <g>
      <DgText x={MARGIN} y={y}>
        {domain}
      </DgText>
      <DgText x={MARGIN + 82} y={y} variant="faint">
        {qualifier}
      </DgText>

      {/*
        The axis is drawn before the timer box so the box's opaque fill cuts
        it, which is what makes the timer read as sitting *on* that clock
        rather than merely next to a line.
      */}
      <DgEdge d={`M${MARGIN},${axisY} H${RIGHT}`} />
      <DgNode x={MARGIN} y={axisY - 22} w={200} h={44} label={name} sub={armed} />

      {/*
        The tick is what turns the axis from decoration into a claim: the timer
        is armed where the box is and fires where the tick is, and the run of
        axis between them is time passing on *that* clock and no other.
      */}
      <DgEdge d={`M${MARK_X},${axisY - 9} V${axisY + 9}`} arrow={false} />
      <DgText x={MARK_X} y={axisY - 15} anchor="middle" variant="faint">
        {mark}
      </DgText>

      <DgText x={MARGIN} y={axisY + 40} variant="faint">
        {fires}
      </DgText>
      <DgText x={MARGIN} y={axisY + 52} variant="faint">
        {effect}
      </DgText>
    </g>
  );
}

export function StateCells() {
  return (
    <Diagram
      title="What one entity key holds"
      desc={
        'The runtime is a single Beam stateful DoFn. Each entity key owns five state cells: ' +
        'MEMORY, a read-modify-write cell holding working memory as one MemoryBlob; CONTINUATION, ' +
        'a read-modify-write cell holding where a suspended activation resumes; LLM_CACHE, a ' +
        'read-modify-write cell holding the bounded replay cache; PENDING, a bag of tool intents ' +
        'waiting for an answer; and SEQ, a combining sum counting activations committed on that ' +
        'key. Two timers hang off the same key, and they run on different clocks. TTL_TIMER is in ' +
        'the event-time (watermark) domain, is re-armed on every commit, fires when the watermark ' +
        'passes its mark, and wipes all five cells for that key. HITL_TIMER is in the real-time ' +
        '(processing-time) domain, is set when a key suspends, fires when real time passes the ' +
        'suspension deadline, and hands the wait to the HITL policy.'
      }
      caption="Everything here is scoped to one entity key; Beam serializes activations per key, so no two of them contend for it. The two timers are the part worth staring at: both are decided in the same commit, and then measured against different clocks that nothing keeps in step."
      viewBox={`0 0 ${W} 578`}
    >
      <DgText x={MARGIN} y={18} variant="faint">
        FIVE STATE CELLS · ONE SET PER ENTITY KEY
      </DgText>

      {CELLS.map((cell, index) => {
        const y = CELL_Y0 + index * CELL_PITCH;
        return (
          <g key={cell.name}>
            <DgNode x={MARGIN} y={y} w={CELL_W} h={CELL_H} label={cell.name} sub={cell.spec} />
            <DgText x={NOTE_X} y={y + CELL_H / 2 + 4}>
              {cell.note}
            </DgText>
          </g>
        );
      })}

      {/* A rule, not a connector: the timers are not downstream of the cells. */}
      <DgEdge d={`M${MARGIN},302 H${RIGHT}`} arrow={false} />

      <DgText x={MARGIN} y={326} variant="faint">
        TWO TIMERS · SET FROM THE COMMIT, FIRED OUTSIDE ANY ACTIVATION
      </DgText>

      <TimerRow
        y={348}
        domain="EVENT TIME"
        qualifier="WATERMARK DOMAIN"
        name="TTL_TIMER"
        armed="RE-ARMED ON EVERY COMMIT"
        mark="TTL MARK"
        fires="fires when the watermark passes the mark"
        effect="wipes every cell above, for that key"
      />

      <TimerRow
        y={470}
        domain="REAL TIME"
        qualifier="PROCESSING-TIME DOMAIN"
        name="HITL_TIMER"
        armed="SET WHEN A KEY SUSPENDS"
        mark="SUSPENSION DEADLINE"
        fires="fires when real time passes the deadline"
        effect="hands the wait to the HITL policy"
      />
    </Diagram>
  );
}

/* -- diagram 2: the life of a write ---------------------------------------- */

/**
 * Two bands, because a write has two endings and they are separated in time.
 *
 * The upper band is one activation: the agent's writes are staged in the
 * memory facade — a plain object in the worker process — and only `_commit`
 * turns them into Beam state. The branch is the whole point. An activation
 * that raises never reaches `_commit`, so the question "how do I undo the
 * writes a failed agent made?" has no answer because it has no subject.
 *
 * The lower band is `on_ttl`, which happens later and to a key rather than to
 * an activation. It is drawn as a straight line with a branch hanging off it,
 * not as a fork, because the wipe is unconditional: the dead letter is an
 * *extra* record emitted on the way past, not an alternative to the wipe.
 */
export function StateWriteLifecycle() {
  return (
    <Diagram
      title="The life of a staged write"
      desc={
        "An agent's memory.set and memory.append calls are staged in the memory facade inside the " +
        'worker process; nothing is in Beam state yet. If the activation returns, the runtime ' +
        'commits: MEMORY, LLM_CACHE, CONTINUATION, PENDING and SEQ are written and the timers are ' +
        'set or cleared, atomically with the bundle. If it raised or timed out, the staged writes ' +
        'vanish and a dead letter is emitted on the errors output with reason activation_error or ' +
        'activation_timeout; nothing reached Beam state, so there is nothing to roll back. Later, ' +
        'when the watermark passes the TTL mark, TTL_TIMER wipes every cell for the key ' +
        'unconditionally and the key holds nothing again. If a continuation was still live at ' +
        'that moment, the same firing also emits a record on the errors output with reason ' +
        'ttl_wiped_suspension: the continuation is gone, so nothing can ever answer that ' +
        'suspension.'
      }
      caption="Staged writes are not state. They become state at the commit, all at once, and a failed activation simply never gets there. The TTL wipe is the one path that destroys committed state, and it is unconditional — which is why it reports the suspension it destroyed on the way past."
      viewBox={`0 0 ${W} 598`}
    >
      <DgText x={MARGIN} y={18} variant="faint">
        WHILE ONE ACTIVATION RUNS
      </DgText>

      <DgNode x={MARGIN} y={32} w={200} h={46} label="activation" sub="AGENT CODE RUNS" />
      <DgEdge d="M114,78 V104" label="set / append" labelX={176} labelY={96} />

      {/*
        Soft, unlike every other box here: the staged blob is the only thing in
        this diagram that is not durable. It lives in the facade, in the worker
        process, and a lost worker takes it with it.
      */}
      <DgNode
        x={MARGIN}
        y={106}
        w={200}
        h={46}
        label="staged writes"
        sub="NOT IN BEAM STATE YET"
        soft
      />

      {/* The branch: the activation returned, or it did not. No third case. */}
      <DgEdge d="M114,152 V186" />
      <DgEdge
        d="M214,129 H442 Q458,129 458,145 V186"
        label="RAISED OR TIMED OUT"
        labelX={330}
        labelY={122}
      />

      <DgNode x={MARGIN} y={188} w={200} h={46} label="commit" sub="ACTIVATION RETURNED" />
      <DgNode x={330} y={188} w={256} h={46} label="discard" sub="STAGED WRITES VANISH" />

      {/*
        The qualifier belongs on this edge rather than beside the commit box:
        what is atomic is the crossing from staged to durable, together with
        the outputs of the same activation.
      */}
      <DgEdge d="M114,234 V264" label="atomic with the bundle" labelX={196} labelY={253} />
      <DgNode x={MARGIN} y={266} w={200} h={46} label="keyed state" sub="DURABLE, PER KEY" />
      <DgText x={114} y={330} anchor="middle" variant="faint">
        MEMORY · LLM_CACHE
      </DgText>
      <DgText x={114} y={342} anchor="middle" variant="faint">
        CONTINUATION · PENDING · SEQ
      </DgText>
      {/*
        "SET OR CLEARED", not "re-armed": `_commit` always re-arms TTL_TIMER,
        but it only sets HITL_TIMER when the activation suspended and clears it
        otherwise. Saying both are armed would invent a timer on every key that
        ever committed.
      */}
      <DgText x={114} y={354} anchor="middle" variant="faint">
        + TIMERS SET OR CLEARED
      </DgText>

      {/*
        The errors colour is a key across the whole site, so it is used on these
        two edges and their nodes and nowhere else in this figure: they are
        literally the `.errors` output, not merely the unhappy path.
      */}
      <DgEdge d="M458,234 V264" stream="errors" />
      <DgNode x={330} y={266} w={256} h={46} label=".errors" sub="DEAD LETTER" stream="errors" />
      <DgText x={458} y={330} anchor="middle" variant="faint">
        reason=activation_error
      </DgText>
      <DgText x={458} y={342} anchor="middle" variant="faint">
        or activation_timeout
      </DgText>
      <DgText x={458} y={360} anchor="middle" variant="faint">
        nothing reached Beam state,
      </DgText>
      <DgText x={458} y={372} anchor="middle" variant="faint">
        so there is nothing to roll back
      </DgText>

      <DgEdge d={`M${MARGIN},396 H${RIGHT}`} arrow={false} />

      <DgText x={MARGIN} y={420} variant="faint">
        LATER · WHEN THE TTL MARK PASSES
      </DgText>

      <DgNode x={MARGIN} y={436} w={150} h={46} label="TTL_TIMER" sub="EVENT-TIME MARK" />
      <DgEdge d="M164,459 H210" />
      <DgNode x={212} y={436} w={200} h={46} label="wipe" sub="EVERY CELL, UNCONDITIONALLY" />
      <DgEdge d="M412,459 H446" />
      <DgText x={454} y={454} variant="faint">
        the key holds
      </DgText>
      <DgText x={454} y={466} variant="faint">
        nothing again
      </DgText>

      {/*
        Hangs off the main line rather than forking it. `on_ttl` clears every
        cell whether or not a continuation was there; the dead letter is what it
        emits on the way past, and drawing it as a fork would say the wipe is
        skipped for a suspended key — exactly the wrong lesson, and exactly why
        this failure surprises people.
      */}
      <DgEdge d="M188,459 V501 Q188,517 204,517 H336" stream="errors" />
      <DgText x={196} y={492} variant="faint">
        IF STILL SUSPENDED
      </DgText>
      {/*
        The reason string stays lowercase, in a note rather than in the node's
        uppercase qualifier line: `ttl_wiped_suspension` is a literal a reader
        will grep for, and `TTL_WIPED_SUSPENSION` is a different string.
      */}
      <DgNode x={338} y={494} w={248} h={46} label=".errors" sub="DEAD LETTER" stream="errors" />
      <DgText x={462} y={552} anchor="middle" variant="faint">
        reason=ttl_wiped_suspension
      </DgText>
      <DgText x={462} y={570} anchor="middle" variant="faint">
        the continuation is gone;
      </DgText>
      <DgText x={462} y={582} anchor="middle" variant="faint">
        nothing can answer it now
      </DgText>
    </Diagram>
  );
}
