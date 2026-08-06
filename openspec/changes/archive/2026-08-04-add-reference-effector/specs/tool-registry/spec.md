## MODIFIED Requirements

### Requirement: Direct invocation of a side-effecting tool raises

The system SHALL enforce correctness invariant 5: a `side_effect=True` tool MUST NOT execute inside the pipeline. Any attempt to run a side-effecting tool from an in-pipeline path — via the `ToolRunner` or by calling the decorated tool as a function — SHALL raise a `SideEffectToolError` and MUST NOT invoke the underlying callable. Side-effecting tools are requested only through the intents path (`ctx.act(...)`) and are executed only outside the pipeline, by the effector's `EffectorToolRunner`, which reaches the callable through `Tool.unwrap()` and which conversely refuses `side_effect=False` tools. These two runners are therefore disjoint: neither can execute the other's class of tool, so "side effects only via intents" remains a closed statement.

#### Scenario: ToolRunner refuses a side-effecting tool

- **WHEN** the `ToolRunner` is asked to run a `side_effect=True` tool
- **THEN** a `SideEffectToolError` is raised naming the tool and the underlying callable is never invoked

#### Scenario: Calling a side-effecting tool directly raises

- **WHEN** a `side_effect=True` decorated tool is invoked directly as a callable
- **THEN** a `SideEffectToolError` is raised and no external write occurs

#### Scenario: The effector's runner is the one sanctioned executor

- **WHEN** a `side_effect=True` tool is run through the effector's `EffectorToolRunner`
- **THEN** the callable is invoked, and this is the only execution path in the codebase that does not raise `SideEffectToolError` for such a tool

#### Scenario: The sanctioned executor refuses read-only tools

- **WHEN** a `side_effect=False` tool is run through the effector's `EffectorToolRunner`
- **THEN** it raises and the callable is never invoked, since a read-only tool belongs to the in-pipeline fast path

## ADDED Requirements

### Requirement: Tool exposes a named accessor for its wrapped callable

The `Tool` type SHALL expose a public `unwrap()` accessor returning the callable it wraps, bypassing the `side_effect` guard on `Tool.__call__`. Its docstring SHALL state that the effector's execution path is its only sanctioned caller. This exists so that the single permitted bypass of correctness invariant 5 is a named, greppable, testable call rather than access to a private attribute.

#### Scenario: unwrap returns the original callable

- **WHEN** `unwrap()` is called on a `Tool` wrapping a function `f`
- **THEN** the returned object is `f` itself, and calling it invokes `f` without a `side_effect` check

#### Scenario: unwrap is available for side-effecting and read-only tools alike

- **WHEN** `unwrap()` is called on a `side_effect=True` tool and on a `side_effect=False` tool
- **THEN** both return their wrapped callable, since the guard that decides who may run what lives on the runners, not on the accessor
