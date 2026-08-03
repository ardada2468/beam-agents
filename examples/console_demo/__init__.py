"""Fill a beam-agents console with the runtime's full event vocabulary.

The console renders what the runtime already records — activations, span trees,
errors grouped by the closed `reason` vocabulary, per-model token spend, the HITL
approval queue — and every one of those views is empty until something produces
the record it renders. A pipeline that only ever completes successfully leaves
most of the screen looking broken.

This example is the fix: one command that produces a suspension awaiting
approval, a denial, an elapsed deadline, a failing tool, a raising agent, an
exhausted token budget, a cache hit, an orphaned result, a dead-lettered intent,
and a shed batch — offline, on the `DirectRunner`, over a scripted `FakeLLM`,
with no API key, no broker, and no network.

Sample code, not a supported runtime surface: nothing here enters the wheel or
`beam_agents.__init__`. See `docs/examples/console-demo.md`.
"""
