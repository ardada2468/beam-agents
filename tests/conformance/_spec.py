"""The canonical conformance scenarios: one ``ScenarioSpec`` per lifecycle behavior.

A ``ScenarioSpec`` is the single source of truth a matrix cell is built from
(design D1): the scripted conversation (one ``Turn`` per FakeLLM rule), the
tool definitions, the expected terminal outputs, the expected deterministic
intent coordinates ``(seq, step_index)``, the deadlines/TTLs, and the per-leg
run/skip declarations. Every adapter factory translates the *same* spec into
its framework's shape, so an assertion difference between matrix cells is
attributable to the adapter, never to the fixture.

The scripted model *responses* are shared bytes across adapters — a small JSON
directive vocabulary (``answer`` / ``run_tool`` / ``act`` /
``request_approval``) that each factory's agent interprets in its framework's
idiom. Only the request *matchers* differ per adapter (the reference agent
sends a flat transcript; the LangGraph factory sends provider-shaped JSON
through the transport hook). ``model_id`` is unique per scenario so the Flink
leg can multiplex every scenario's rules into one provider script.

Importing this module has no side effects.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace

from beam_agents._protos import ToolIntent
from beam_agents.hitl import DEFAULT_APPROVAL_CHANNEL, HITL_TIMEOUT_OUTPUT
from beam_agents.tools import Tool, ToolRegistry, tool

# Leg names: the runners every scenario must declare itself for. `spark` is the
# weekly (never per-PR) leg of the `promote-spark-runner` change — it is a leg
# like any other here, so the meta-test's registry x scenario x leg accounting
# counts its cells whether they run or are declared skips.
DIRECT = "direct"
FLINK = "flink"
SPARK = "spark"
LEGS = (DIRECT, FLINK, SPARK)

#: Legs whose HITL deadline waits on a real wall clock (portable runner on a
#: container stack under CI load), as opposed to the DirectRunner leg's
#: scripted processing time. Both portable legs share one override field
#: (design D1): a spark-specific budget is added only if measurements demand it.
_REAL_TIME_LEGS = frozenset({FLINK, SPARK})

_HOUR_MS = 3_600_000
# Large event-time TTL so working-memory GC never fires unless the scenario is
# about it (same convention as tests/core/test_dofn_streaming.py).
BIG_TTL_MS = 1_000_000_000


@dataclass(frozen=True)
class Run:
    """The scenario runs on this leg."""


@dataclass(frozen=True)
class Skip:
    """The scenario is declared not runnable on this leg, with the reason.

    A declared skip is still a matrix cell: it is collected, reported as a
    skip carrying ``reason``, and counted by the meta-test.
    """

    reason: str


LegDecl = Run | Skip


@dataclass(frozen=True)
class ToolDef:
    """One tool the scenario's conversation uses, by name and effect class."""

    name: str
    side_effect: bool


@dataclass(frozen=True)
class Turn:
    """One scripted model behavior — exactly one FakeLLM rule per turn."""

    directive: str  # "answer" | "run_tool" | "act" | "request_approval"
    text: str = ""
    tool: str = ""
    args: tuple[tuple[str, str], ...] = ()


def answer(text: str) -> Turn:
    return Turn("answer", text=text)


def run_tool(tool_name: str, **args: str) -> Turn:
    return Turn("run_tool", tool=tool_name, args=tuple(sorted(args.items())))


def act(tool_name: str, **args: str) -> Turn:
    return Turn("act", tool=tool_name, args=tuple(sorted(args.items())))


def request_approval(**args: str) -> Turn:
    return Turn("request_approval", args=tuple(sorted(args.items())))


def turn_response(turn: Turn) -> bytes:
    """The scripted response bytes for one turn — identical for every adapter."""
    payload: dict[str, object]
    if turn.directive == "answer":
        payload = {"answer": turn.text}
    else:
        payload = {turn.directive: {"name": turn.tool, "args": dict(turn.args)}}
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True)
class IntentExpectation:
    """The deterministic coordinates and identity of one expected intent."""

    seq: int
    step_index: int
    tool_name: str
    kind: int  # a ToolIntent.Kind value


@dataclass(frozen=True)
class ScenarioSpec:
    """Everything a conformance cell needs, declared once per scenario."""

    name: str
    #: Unique per scenario; scopes every FakeLLM matcher so the Flink leg can
    #: merge all scenarios' rules into one provider script per adapter job.
    model_id: str
    turns: tuple[Turn, ...]
    tools: tuple[ToolDef, ...]
    expected_outputs: tuple[bytes, ...]
    expected_intents: tuple[IntentExpectation, ...] = ()
    #: Real provider invocations for the whole scenario (cache hits excluded).
    expected_provider_calls: int = 1
    #: ``Suspend.timeout_ms`` a suspending scenario uses on the DirectRunner
    #: leg (scripted processing time); None for scenarios that never suspend.
    hitl_timeout_ms: int | None = None
    #: Real-time override for the portable-runner legs (Flink and Spark),
    #: where the HITL timer actually waits on a wall clock under CI load.
    #: Named for Flink because Flink is where it was measured; ``variant_for``
    #: applies it to every real-time leg.
    flink_hitl_timeout_ms: int | None = None
    #: Working-memory TTL for the cell's AgentConfig.
    memory_ttl_ms: int = BIG_TTL_MS
    #: True when the scenario reads/writes working memory across activations.
    uses_memory: bool = False
    #: External events the scenario feeds (per key).
    events: int = 1
    legs: dict[str, LegDecl] = field(default_factory=dict)
    #: Adapters for which this scenario's *construction* is not expressible,
    #: by adapter name -> reason. Reserved for framework semantics that make the
    #: scenario's premise unreachable (never for an adapter that merely fails
    #: it): a declared adapter skip is still a collected, counted matrix cell,
    #: reported as a skip carrying the reason, exactly like a per-leg `Skip`.
    adapter_skips: dict[str, str] = field(default_factory=dict)

    def variant_for(self, leg: str) -> ScenarioSpec:
        """The spec as built for ``leg``.

        Only the real-time legs differ from the declaration: on Flink and
        Spark the HITL timer waits on a wall clock, so the scenario's
        real-time override (if any) replaces the scripted deadline. The
        DirectRunner leg is always the spec as declared.
        """
        if leg not in _REAL_TIME_LEGS or self.flink_hitl_timeout_ms is None:
            return self
        return replace(self, hitl_timeout_ms=self.flink_hitl_timeout_ms)


# -- the scenario tools (module-level so they pickle by reference) ---------------

#: Side-effect executions observed *inside the test process*. The DirectRunner
#: is in-process, so a side-effect tool body running anywhere in the pipeline
#: lands here and fails the scenario's zero-execution assertion.
EXECUTED_SIDE_EFFECTS: list[str] = []


@tool
def lookup_a(customer_id: str) -> str:
    """Read-only conformance tool A: uppercases its argument."""
    return customer_id.upper()


@tool
def lookup_b(customer_id: str) -> str:
    """Read-only conformance tool B: uppercases its argument."""
    return customer_id.upper()


@tool(side_effect=True)
def charge(amount: str) -> str:
    """Side-effect conformance tool: must never execute inside the pipeline."""
    EXECUTED_SIDE_EFFECTS.append(amount)
    return "charged"


_TOOLS: dict[str, Tool] = {t.name: t for t in (lookup_a, lookup_b, charge)}


def tool_for(name: str) -> Tool:
    return _TOOLS[name]


def registry_for(spec: ScenarioSpec) -> ToolRegistry:
    """A fresh registry holding exactly the spec's tools (no module-global
    registry instance, per the no-global-mutable-state convention)."""
    registry = ToolRegistry()
    for tool_def in spec.tools:
        registry.register(_TOOLS[tool_def.name])
    return registry


# -- the seven canonical scenarios -----------------------------------------------

SINGLE_SHOT = ScenarioSpec(
    name="single_shot",
    model_id="conf-single-shot",
    turns=(answer("done-single-shot"),),
    tools=(),
    expected_outputs=(b"done-single-shot",),
    expected_provider_calls=1,
    legs={
        DIRECT: Run(),
        FLINK: Run(),
        SPARK: Skip(
            "the Spark portable runner registers no bundle checkpoint handler, and this "
            "leg's ingest requires one: every spark cell reads its events through the "
            "unbounded-per-element spool SDF in tests/semantics/_e2e/spool.py, whose "
            "tail poll calls tracker.defer_remainder to self-checkpoint while waiting "
            "for the next sealed segment, and the residual that produces fails the job "
            "invocation with 'The ActiveBundle does not have a registered bundle "
            "checkpoint handler'. Even the simplest cell in the matrix — one scripted "
            "answer, no tools, no timers, no suspension — dies in the source before any "
            "agent code runs, which is why the failure was byte-identical for all four "
            "adapters on the first real job-server run (2026-07-31)"
        ),
    },
)

MULTI_TOOL_INLINE = ScenarioSpec(
    name="multi_tool_inline",
    model_id="conf-multi-tool",
    turns=(
        run_tool("lookup_a", customer_id="aa"),
        run_tool("lookup_b", customer_id="bb"),
        answer("done-multi-tool"),
    ),
    tools=(ToolDef("lookup_a", False), ToolDef("lookup_b", False)),
    # The terminal embeds both tool results: inline execution is proven by the
    # output, uniformly for every adapter. (TOOL_CALL trace events are a
    # reference-agent-only signal: BeamToolNode executes read-only tools
    # without staging traces — a surfaced finding, see design.md.)
    expected_outputs=(b"AA,BB|done-multi-tool",),
    expected_provider_calls=3,
    legs={
        DIRECT: Run(),
        FLINK: Run(),
        SPARK: Skip(
            "the same runner-level ingest gap that skips single_shot, reached before "
            "the first model request: the spool SDF in tests/semantics/_e2e/spool.py "
            "tails its input by calling tracker.defer_remainder, and the Spark portable "
            "runner has no registered bundle checkpoint handler to accept that "
            "residual, so the invocation fails with 'The ActiveBundle does not have a "
            "registered bundle checkpoint handler' and no tool is ever dispatched. "
            "Nothing about inline tool execution is implicated — the job server starts "
            "its SparkContext, builds the DStream graph and checkpoints batches, then "
            "fails on the source's residual, identically for every adapter (first real "
            "job-server run, 2026-07-31)"
        ),
    },
)

SUSPENSION_RESUME = ScenarioSpec(
    name="suspension_resume",
    model_id="conf-suspension-resume",
    turns=(act("charge", amount="5"), answer("done-suspension-resume")),
    tools=(ToolDef("charge", True),),
    expected_outputs=(b"resumed:ack|done-suspension-resume",),
    expected_intents=(IntentExpectation(0, 1, "charge", ToolIntent.TOOL),),
    expected_provider_calls=2,
    hitl_timeout_ms=_HOUR_MS,
    legs={
        DIRECT: Run(),
        FLINK: Run(),
        SPARK: Skip(
            "blocked by the same missing runner capability as every other spark cell, "
            "so this scenario's actual subject — staging an intent, suspending, and "
            "resuming from the injected result — is never exercised on Spark at all: "
            "the leg's ingest is the unbounded-per-element spool SDF in "
            "tests/semantics/_e2e/spool.py, whose tracker.defer_remainder tail poll "
            "needs a bundle checkpoint handler the Spark portable runner does not "
            "register, and the invocation dies with 'The ActiveBundle does not have a "
            "registered bundle checkpoint handler' before the first activation. The "
            "gap is runner-level and adapter-independent: all four adapters failed "
            "identically on the first real job-server run (2026-07-31)"
        ),
    },
)

APPROVAL_TIMEOUT_FALLBACK = ScenarioSpec(
    name="approval_timeout_fallback",
    model_id="conf-approval-timeout",
    turns=(request_approval(amount="5"),),
    tools=(),
    expected_outputs=(HITL_TIMEOUT_OUTPUT,),
    expected_intents=(IntentExpectation(0, 1, DEFAULT_APPROVAL_CHANNEL, ToolIntent.APPROVAL),),
    expected_provider_calls=1,
    hitl_timeout_ms=1_000,
    # Real-time on Flink: long enough that suspend-commit and intent routing
    # never race it, short enough to demonstrably fire mid-run (the e2e gate's
    # late-population value).
    flink_hitl_timeout_ms=30_000,
    legs={
        DIRECT: Run(),
        FLINK: Run(),
        SPARK: Skip(
            "the spark leg never gets far enough to arm the REAL_TIME approval timer: "
            "its ingest is the unbounded-per-element spool SDF in "
            "tests/semantics/_e2e/spool.py, which self-checkpoints by calling "
            "tracker.defer_remainder while tailing the spool, and the Spark portable "
            "runner registers no bundle checkpoint handler for that residual, failing "
            "the invocation with 'The ActiveBundle does not have a registered bundle "
            "checkpoint handler'. This cell therefore says nothing about Spark's "
            "REAL_TIME timer support, which stays untested; the failure is in the "
            "source, is runner-level rather than adapter-level, and was identical for "
            "all four adapters on the first real job-server run (2026-07-31)"
        ),
    },
)

RESTART_MID_SUSPENSION = ScenarioSpec(
    name="restart_mid_suspension",
    model_id="conf-restart",
    turns=(act("charge", amount="7"), answer("done-restart")),
    tools=(ToolDef("charge", True),),
    expected_outputs=(b"resumed:ack|done-restart",),
    expected_intents=(IntentExpectation(0, 1, "charge", ToolIntent.TOOL),),
    expected_provider_calls=2,
    hitl_timeout_ms=_HOUR_MS,
    legs={
        DIRECT: Run(),
        FLINK: Run(),
        SPARK: Skip(
            "the spark overlay runs the job server's embedded local[4] master, so "
            "executors live inside the job-server container and there is no separate "
            "worker container to restart between the suspend commit and the result "
            "delivery; restarting the job server itself tears down the driver, which "
            "is a job resubmission, not a mid-suspension executor restart. Expressing "
            "this cell needs dedicated Spark master/worker containers in the overlay "
            "(design D2 / open question), and its absence is enumerated as a bound on "
            "any future supported claim"
        ),
    },
)

BUNDLE_RETRY_CACHE = ScenarioSpec(
    name="bundle_retry_cache",
    model_id="conf-bundle-retry",
    # No post-resume turn: the resume completes from the injected result with
    # no novel model request, so a chaos-retried resume is fully covered by the
    # suspend-committed replay cache / checkpoint and adds zero provider calls.
    turns=(act("charge", amount="9"),),
    tools=(ToolDef("charge", True),),
    expected_outputs=(b"resumed:ack",),
    expected_intents=(IntentExpectation(0, 1, "charge", ToolIntent.TOOL),),
    expected_provider_calls=1,
    hitl_timeout_ms=_HOUR_MS,
    legs={
        DIRECT: Run(),
        FLINK: Skip(
            "the chaos commit-failure monkeypatch is in-process and cannot reach "
            "the beam-sdk-harness container; real replay on Flink is exercised by "
            "the restart-mid-suspension cell"
        ),
        SPARK: Skip(
            "the chaos commit-failure monkeypatch is in-process and cannot reach the "
            "spark-scoped beam-sdk-harness container — the identical harness "
            "constraint that skips this cell on Flink, and one no runner feature can "
            "lift; on Spark there is not even a restart-mid-suspension cell to carry "
            "the replay evidence instead (see that scenario's spark skip)"
        ),
    },
    adapter_skips={
        "adk": (
            "this scenario's premise is a resume that issues NO novel model request "
            "(so the retried resume is served entirely from the suspend-committed "
            "replay cache). ADK's resume semantics make that unreachable: delivering "
            "a function response always drives one summarization turn, which is a "
            "novel request, so a discarded attempt legitimately repeats it. The "
            "adapter's replay-cache guarantee is covered by its own "
            "recognized-client-replay-cached test and by the restart-mid-suspension "
            "cell; its intent-byte determinism by bundle-replay tests in "
            "tests/adapters/adk/test_shim_suspension.py"
        )
    },
)

TTL_EXPIRY = ScenarioSpec(
    name="ttl_expiry",
    model_id="conf-ttl",
    turns=(answer("ok-ttl"),),
    tools=(),
    expected_outputs=(b"seen=1|ok-ttl", b"seen=2|ok-ttl", b"seen=1|ok-ttl"),
    expected_provider_calls=3,
    memory_ttl_ms=100,
    uses_memory=True,
    events=3,
    legs={
        DIRECT: Run(),
        FLINK: Skip(
            "advancing an unbounded Kafka source's watermark deterministically past "
            "a TTL requires idle-partition watermark control the harness does not "
            "have; TTL GC is runtime (not adapter-visible) behavior, so the "
            "DirectRunner leg keeps full adapter coverage"
        ),
        SPARK: Skip(
            "the same missing idle-partition watermark control as on Flink: the spool "
            "SDF source's watermark cannot be advanced deterministically past the TTL "
            "from the host side, and the Spark portable runner exposes no watermark "
            "hold the harness could drive instead; TTL GC is runtime (not "
            "adapter-visible) behavior, so the DirectRunner leg keeps full adapter "
            "coverage"
        ),
    },
)

SCENARIOS: tuple[ScenarioSpec, ...] = (
    SINGLE_SHOT,
    MULTI_TOOL_INLINE,
    SUSPENSION_RESUME,
    APPROVAL_TIMEOUT_FALLBACK,
    RESTART_MID_SUSPENSION,
    BUNDLE_RETRY_CACHE,
    TTL_EXPIRY,
)

SCENARIOS_BY_NAME: dict[str, ScenarioSpec] = {spec.name: spec for spec in SCENARIOS}

#: The scenarios each leg actually runs (declared skips excluded).
FLINK_SCENARIOS: tuple[ScenarioSpec, ...] = tuple(
    spec for spec in SCENARIOS if isinstance(spec.legs[FLINK], Run)
)

# Every spark declaration above is a `Skip`, so this tuple is EMPTY: the spark
# leg has zero executing cells. All of its matrix cells are still collected and
# counted — they report their declared reason — but no pipeline is submitted and
# nothing about the Spark portable runner is verified. Three of the seven skips
# are structural (harness/overlay constraints, see their reasons); the other
# four are the 2026-07-31 finding that the leg's SDF ingest needs a bundle
# checkpoint handler the Spark portable runner does not register.
#
# The consequence is deliberate and should not surprise the next reader:
# `scripts/spark_weekly_status.py` treats a spark `Skip` added inside the
# promotion window as coverage shrinkage and resets the streak, so no number of
# green weekly runs can promote an all-skip leg. A leg that executes nothing has
# earned nothing. The leg stays wired (declarations, harness, overlay, weekly
# workflow) so that closing the gap is a declaration flip: either a Beam Spark
# runner that registers a bundle checkpoint handler, or a non-SDF ingest path
# for this leg (see openspec/changes/record-spark-sdf-checkpoint-gap).
SPARK_SCENARIOS: tuple[ScenarioSpec, ...] = tuple(
    spec for spec in SCENARIOS if isinstance(spec.legs[SPARK], Run)
)


def skip_inventory(leg: str, scenarios: tuple[ScenarioSpec, ...] = SCENARIOS) -> dict[str, str]:
    """``scenario name -> declared reason`` for every skip on ``leg``.

    Read by ``scripts/spark_weekly_status.py`` so each weekly summary prints
    the current spark skip inventory: the diff-based drift scan is a
    heuristic, and a printed inventory makes a refactor-shaped evasion visible
    week over week (design D5).
    """
    inventory: dict[str, str] = {}
    for spec in scenarios:
        declaration = spec.legs[leg]
        if isinstance(declaration, Skip):
            inventory[spec.name] = declaration.reason
    return inventory
