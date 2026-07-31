/**
 * The pipeline, animated.
 *
 * The geometry is the topology from `openspec/project.md`, not an
 * illustration of it: three keyed inputs flatten into `RunAgent`, four named
 * outputs leave it, and `.intents` is the only one that comes back — out to
 * the outbox, through the effector, and in again on the same key.
 *
 * That return path is the thing worth animating. Beam DAGs are acyclic, so an
 * agent that calls a tool and acts on the result cannot loop inside the graph;
 * it loops through the message bus. Every other way of explaining that costs a
 * paragraph.
 *
 * Server-rendered SVG with CSS motion (see `app/pipeline.css`) — no client
 * JavaScript, so it works in the same conditions the rest of the site does.
 */

/*
 * Six steps, timed to the six windows in `app/pipeline.css`. The narration has
 * to track the motion exactly — a caption describing the activation while the
 * diagram already shows it suspended teaches the wrong thing.
 */
const STEPS = [
  'An event arrives on the events topic and is keyed by entity id.',
  'RunAgent activates for that key, reading working memory and the replay cache from keyed state.',
  'The agent needs an external write. It stages a ToolIntent, suspends, and the intent leaves for the outbox.',
  'The effector executes it — once per intent id — and publishes the result, which re-enters on the same key.',
  'The suspended activation resumes where it stopped and runs to completion.',
  'State and outputs commit atomically with the bundle, or not at all.',
];

/*
 * `.intents` is drawn last, at the bottom rail, even though the docs list it
 * second: it is the one stream that leaves the frame downward, and routing it
 * from the middle made its return path cross the two rails below it.
 */
const OUTPUTS = [
  { name: '.output', y: 114, key: 'output' },
  { name: '.traces', y: 138, key: 'traces' },
  { name: '.errors', y: 162, key: 'errors' },
  { name: '.intents', y: 186, key: 'intents' },
] as const;

export function PipelineDiagram() {
  return (
    <div className="pipe-pause-scope">
      <input
        type="checkbox"
        id="pipeline-pause"
        className="pipe-pause-input"
        // Rendered unchecked, so the animation runs by default and pausing is
        // the reader's choice.
      />
      <label htmlFor="pipeline-pause" className="pipe-pause pipe-pause-wrap">
        <span className="when-running">Pause</span>
        <span className="when-paused">Play</span>
        <span aria-hidden="true">·</span>
        <span>12s loop</span>
      </label>

      <figure className="pipeline mt-4">
        <div className="pipe-scroll">
          <svg
            viewBox="0 0 1000 400"
            role="img"
            aria-labelledby="pipeline-title pipeline-desc"
            preserveAspectRatio="xMidYMid meet"
          >
            <title id="pipeline-title">
              How an activation flows through a beam-agents pipeline
            </title>
            <desc id="pipeline-desc">
              Events, tool results, and approvals are keyed by entity and flattened into the
              RunAgent transform. RunAgent emits four outputs: output, intents, traces, and errors.
              The intents output leaves the pipeline to an outbox topic, is executed by an external
              effector, and the result re-enters the pipeline on the same key, resuming the
              suspended activation.
            </desc>

            <defs>
              <marker
                id="arrow"
                viewBox="0 0 8 8"
                refX="7"
                refY="4"
                markerWidth="7"
                markerHeight="7"
                orient="auto-start-reverse"
              >
                <path d="M0,1 L7,4 L0,7 z" className="pipe-arrow" />
              </marker>
              <marker
                id="arrow-loop"
                viewBox="0 0 8 8"
                refX="7"
                refY="4"
                markerWidth="7"
                markerHeight="7"
                orient="auto-start-reverse"
              >
                <path d="M0,1 L7,4 L0,7 z" className="pipe-arrow--loop" />
              </marker>
            </defs>

            {/* ---- inputs ---- */}
            <text x="132" y="82" textAnchor="end" className="pipe-label">
              events
            </text>
            <text x="132" y="154" textAnchor="end" className="pipe-label">
              tool results
            </text>
            <text x="132" y="226" textAnchor="end" className="pipe-label">
              approvals
            </text>
            <text x="132" y="252" textAnchor="end" className="pipe-label--faint">
              KAFKA / PUB-SUB
            </text>

            <path
              d="M140,78 H198 Q214,78 214,94 V134 Q214,150 230,150"
              className="pipe-line"
              markerEnd="url(#arrow)"
            />
            <path d="M140,150 H230" className="pipe-line" markerEnd="url(#arrow)" />
            <path
              d="M140,222 H198 Q214,222 214,206 V166 Q214,150 230,150"
              className="pipe-line"
              markerEnd="url(#arrow)"
            />

            {/* ---- keying / flatten ---- */}
            <rect x="232" y="128" width="126" height="44" rx="2" className="pipe-node" />
            <text x="295" y="147" textAnchor="middle" className="pipe-label">
              WithKeys
            </text>
            <text x="295" y="162" textAnchor="middle" className="pipe-label--faint">
              FLATTEN
            </text>

            <path d="M358,150 H428" className="pipe-line" markerEnd="url(#arrow)" />

            {/* ---- RunAgent ---- */}
            <rect x="430" y="96" width="190" height="108" rx="2" className="pipe-node" />
            <text x="446" y="122" className="pipe-label--title">
              RunAgent
            </text>

            {/* Lit while an activation is running. */}
            <rect x="592" y="110" width="10" height="10" className="pipe-active" />

            <rect x="446" y="136" width="158" height="22" rx="2" className="pipe-node--soft" />
            <text x="456" y="151" className="pipe-label--faint">
              KEYED STATE
            </text>

            <rect x="446" y="166" width="158" height="22" rx="2" className="pipe-node--soft" />
            <text x="456" y="181" className="pipe-label--faint">
              CONTINUATION
            </text>
            {/* Lit only while the activation is suspended awaiting a result. */}
            <rect x="586" y="171" width="10" height="10" className="pipe-suspended" />

            {/* ---- outputs ---- */}
            {OUTPUTS.map((output) => (
              <g key={output.key}>
                <path
                  d={`M620,${output.y} H872`}
                  className={`pipe-line pipe-rail--${output.key}`}
                  markerEnd="url(#arrow)"
                />
                <text x="880" y={output.y + 4} className={`pipe-label pipe-name--${output.key}`}>
                  {output.name}
                </text>
              </g>
            ))}

            {/* ---- the loop: intents leave, results come back ---- */}
            <path
              d="M620,186 H684 Q700,186 700,202 V330 Q700,346 684,346 H662"
              className="pipe-line pipe-line--loop"
              markerEnd="url(#arrow-loop)"
            />
            <rect x="505" y="324" width="140" height="44" rx="2" className="pipe-node" />
            <text x="575" y="343" textAnchor="middle" className="pipe-label">
              outbox topic
            </text>
            <text x="575" y="357" textAnchor="middle" className="pipe-label--faint">
              DEDUP BY INTENT_ID
            </text>

            <path
              d="M505,346 H472"
              className="pipe-line pipe-line--loop"
              markerEnd="url(#arrow-loop)"
            />
            <rect x="330" y="324" width="140" height="44" rx="2" className="pipe-node" />
            <text x="400" y="343" textAnchor="middle" className="pipe-label">
              effector
            </text>
            <text x="400" y="357" textAnchor="middle" className="pipe-label--faint">
              SEPARATE SERVICE
            </text>

            {/*
            The return rises at x=170 and merges into the tool-results rail
            rather than running to the left margin: results re-enter as an
            ordinary input on that topic, and routing it past the input labels
            would collide with them for no gain in accuracy.
          */}
            <path
              d="M330,346 H186 Q170,346 170,330 V166 Q170,150 186,150"
              className="pipe-line pipe-line--loop"
              markerEnd="url(#arrow-loop)"
            />
            <rect x="186" y="146" width="8" height="8" rx="1" className="pipe-junction" />

            {/* ---- moving packets ---- */}
            <rect className="packet packet--event" />
            <rect className="packet packet--output" />
            <rect className="packet packet--output-2" />
            <rect className="packet packet--intent" />
            <rect className="packet packet--outbox" />
            <rect className="packet packet--return" />
            <rect className="packet packet--trace" />
            <rect className="packet packet--error" />
          </svg>
        </div>
      </figure>

      <ol className="pipe-steps" aria-label="What happens on each pass">
        {STEPS.map((step, index) => (
          <li key={step} className="pipe-step">
            <span className="pipe-step__n" aria-hidden="true">
              {String(index + 1).padStart(2, '0')}
            </span>
            <span className="pipe-step__t">{step}</span>
          </li>
        ))}
      </ol>
    </div>
  );
}
