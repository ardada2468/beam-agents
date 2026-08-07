# beam-agents

An agent is a Beam transform.
{ .lede }

`beam-agents` makes AI agents first-class citizens of Apache Beam streaming
pipelines. An agent becomes a keyed, stateful, fault-tolerant transform —
`events | RunAgent(my_agent)` — for **system-triggered** workloads: fraud
triage, anomaly response, personalization, IoT reaction, ops automation.
Events invoke the agent; its decisions are durable, replayable, and
horizontally scalable. It is not built for sub-second interactive chat.

[Read the quickstart](quickstart.md){ .md-button .md-button--primary }
[Browse the examples](examples/hello-world.md){ .md-button }

`v1.0.0a1` · pre-release, not yet on PyPI · Apache-2.0 · Python 3.11–3.12
{ .eyebrow }

## A runtime, not a framework

beam-agents deliberately owns only what agent frameworks lack: durable keyed
memory, event/processing-time semantics, effectively-once side effects,
backpressure-aware scale-out, and runner portability (DirectRunner, Dataflow,
Flink). Agent *authoring* belongs to LangGraph, Google ADK, Pydantic AI, or a
plain async function — integrated via adapters. There is no prompt templating,
no orchestration DSL, and no agent-authoring abstraction here, on purpose.

## The shape of a pipeline

<figure class="diagram" markdown="1">
<div class="diagram-scroll">
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 720 248" role="img" aria-labelledby="shape-t shape-d">
<title id="shape-t">The shape of a beam-agents pipeline</title>
<desc id="shape-d">Three topics — events, tool results and approvals — are flattened onto one stream, keyed by entity id, and fed to RunAgent. RunAgent emits four tagged outputs: dot output, dot traces and dot errors leave the graph to the right, while dot intents leaves downward to an outbox topic, is executed by an external effector, and re-enters the pipeline as tool results on the same key.</desc>
<defs>
<marker id="shape-a" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--rule-2, #8e8e87)"/></marker>
<marker id="shape-a-out" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--s-output, #0b0c0e)"/></marker>
<marker id="shape-a-tra" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--s-traces, #3c5c78)"/></marker>
<marker id="shape-a-err" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--s-errors, #9e2a18)"/></marker>
<marker id="shape-a-int" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,1 L7,4 L0,7 z" fill="var(--s-intents, #8a5205)"/></marker>
</defs>
<g fill="none" stroke="var(--rule-2, #8e8e87)" stroke-width="1.25">
<path d="M150,52 H166"/>
<path d="M150,128 H166"/>
<path d="M150,90 H166"/>
<path d="M166,52 V128"/>
<path d="M166,90 H182" marker-end="url(#shape-a)"/>
<path d="M260,90 H284" marker-end="url(#shape-a)"/>
<path d="M430,90 H446" marker-end="url(#shape-a)"/>
</g>
<g fill="var(--paper, #ffffff)" stroke="var(--ink, #0b0c0e)" stroke-width="1.25">
<rect x="38" y="38" width="112" height="28" rx="2"/>
<rect x="38" y="76" width="112" height="28" rx="2"/>
<rect x="38" y="114" width="112" height="28" rx="2"/>
<rect x="182" y="76" width="78" height="28" rx="2"/>
<rect x="284" y="76" width="146" height="28" rx="2"/>
<rect x="446" y="42" width="132" height="96" rx="2"/>
</g>
<g fill="var(--paper-2, #f6f6f4)" stroke="var(--rule-2, #8e8e87)" stroke-width="1">
<rect x="446" y="196" width="132" height="30" rx="2"/>
<rect x="256" y="196" width="132" height="30" rx="2"/>
</g>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="var(--ink-2, #4a4e54)" text-anchor="middle">
<text x="94" y="56">events</text>
<text x="94" y="94">tool-results</text>
<text x="94" y="132">approvals</text>
<text x="221" y="94">Flatten</text>
<text x="357" y="94">WithKeys(entity_id)</text>
<text x="512" y="215">outbox topic</text>
<text x="322" y="215">effector</text>
</g>
<text x="512" y="86" text-anchor="middle" font-family="'Instrument Sans', ui-sans-serif, system-ui, sans-serif" font-size="14" font-weight="600" fill="var(--ink, #0b0c0e)">RunAgent</text>
<text x="512" y="104" text-anchor="middle" font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">keyed · stateful</text>
<g fill="none" stroke-width="1.25">
<path d="M578,62 H620" stroke="var(--s-output, #0b0c0e)" marker-end="url(#shape-a-out)"/>
<path d="M578,90 H620" stroke="var(--s-traces, #3c5c78)" marker-end="url(#shape-a-tra)"/>
<path d="M578,118 H620" stroke="var(--s-errors, #9e2a18)" marker-end="url(#shape-a-err)"/>
<path d="M512,138 V190" stroke="var(--s-intents, #8a5205)" stroke-dasharray="4 3" marker-end="url(#shape-a-int)"/>
<path d="M446,211 H394" stroke="var(--s-intents, #8a5205)" stroke-dasharray="4 3" marker-end="url(#shape-a-int)"/>
<path d="M256,211 H16 V90 H32" stroke="var(--s-intents, #8a5205)" stroke-dasharray="4 3" marker-end="url(#shape-a-int)"/>
</g>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11">
<text x="626" y="66" fill="var(--s-output, #0b0c0e)">.output</text>
<text x="626" y="94" fill="var(--s-traces, #3c5c78)">.traces</text>
<text x="626" y="122" fill="var(--s-errors, #9e2a18)">.errors</text>
<text x="520" y="170" fill="var(--s-intents, #8a5205)">.intents</text>
</g>
<g font-family="'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace" font-size="9.5" letter-spacing="0.06" fill="var(--ink-3, #63676d)">
<text x="132" y="204" text-anchor="middle">results, re-injected on the same key</text>
<text x="417" y="242" text-anchor="middle">outside the pipeline</text>
</g>
</svg>
</div>
<figcaption markdown="1">
Solid lines are the Beam graph; dashed lines leave it. Beam graphs are acyclic,
so an agent that stages a side effect cannot loop inside the graph — it
suspends, and the result re-enters as a new element on the same key.
</figcaption>
</figure>

Side effects never execute inside the pipeline: the agent stages declarative
`ToolIntent`s, an external [effector](effector.md) executes them exactly once
per deterministic `intent_id`, and results re-enter on the same key.

## Guarantees, and the gates that enforce them

Every guarantee below is a machine-verified release gate, not an aspiration.
A claim on this site that nothing enforces is a defect.

| Guarantee | Enforced by |
|---|---|
| Side effects execute effectively once under real worker kills, duplicate sink writes, and full pipeline replay | the [effectively-once e2e gate](ci.md#the-effectively-once-end-to-end-gate) ([`tests/semantics/test_effectively_once_e2e.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_effectively_once_e2e.py)) |
| A retried bundle adds zero provider calls and commits byte-identical intents | the retry-determinism gate ([`tests/semantics/test_retry_determinism.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_retry_determinism.py)) |
| Every adapter exhibits identical lifecycle semantics on DirectRunner and Flink | the adapter conformance matrix ([`tests/conformance/`](https://github.com/ardada2468/beam-agents/tree/main/tests/conformance)) |
| Human-in-the-loop timeouts fail closed at both layers | the HITL semantics gates ([`tests/semantics/test_hitl_fail_closed.py`](https://github.com/ardada2468/beam-agents/blob/main/tests/semantics/test_hitl_fail_closed.py)) |
| A failed activation commits nothing, and coverage/mutation scores never regress | the [`ci` and `quality` workflows](ci.md) |

## Install

Once `v1.0.0` is published to PyPI, the install is the ordinary one:

```sh
pip install beam-agents
```

Until then — and always, for working on the runtime itself — install from
source, exactly as the
[repository README](https://github.com/ardada2468/beam-agents#readme)
describes:

```sh
git clone https://github.com/ardada2468/beam-agents
cd beam-agents
uv sync --all-groups
```

Requires Python `>=3.11,<3.13`; the source install additionally needs
[`uv`](https://docs.astral.sh/uv/). The adapters, the effector, and the other
optional pieces are extras (`beam-agents[langgraph]`, `[pydantic-ai]`, `[adk]`,
`[effector]`, …) — each page below names the extra it needs.

## Start here

The seven example programs are real, runnable Beam pipelines on the real
runtime. The code each page shows is included verbatim from `examples/` — the
same file CI executes in [`tests/examples/`](https://github.com/ardada2468/beam-agents/tree/main/tests/examples).

### Hermetic — no API keys, no network

Four are driven by a scripted `FakeLLM`, so they run offline with no API keys
and (except the console demo's viewer) no docker.

<div class="grid cards" markdown>

-   **[Hello, world](examples/hello-world.md)**

    The minimal fast path: one event, one model call, one output.

-   **[Fraud triage](examples/fraud-triage.md)**

    Suspension, human approval, and the fail-closed timeout.

-   **[IoT reaction](examples/iot-reaction.md)**

    Keyed rolling memory on a stream, with no model calls for quiet readings.

-   **[Console demo](examples/console-demo.md)**

    One command that exercises the whole error-and-approval vocabulary and
    feeds the [console](console.md).

</div>

### Against the real thing

Three deliberately touch the world outside, because "does it work against the
real thing" is a question too.

<div class="grid cards" markdown>

-   **[Quickstart](examples/quickstart.md)**

    A real provider over the network, streaming into a running console —
    alongside [the task-oriented guide](quickstart.md).

-   **[Slack approval](examples/slack-approval.md)**

    A worked approval surface closing the HITL loop through Slack.

-   **[Fraud triage on Dataflow](examples/fraud-triage-dataflow.md)**

    The same fraud agent packaged as a Dataflow Flex Template.

</div>

### Then, operating it

<div class="grid cards" markdown>

-   **[CI workflow map](ci.md)**

    Six workflows mapped to the testing tiers they gate.

-   **[Running the effector](effector.md)**

    The service that executes staged intents exactly once.

-   **[Errors and dead letters](errors.md)**

    The `.errors` output, and the closed vocabulary of reasons.

-   **[Runtime metrics](metrics.md)**

    What the runtime publishes under `beam_agents.runtime`.

-   **[Trace delivery](traces.md)**

    The `.traces` output and its OTLP, BigQuery, and broker sinks.

-   **[Deploying to Dataflow](deploying.md)**

    Building the container image and wiring a real provider credential.

</div>
