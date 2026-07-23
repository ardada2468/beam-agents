# tool-registry Specification

## Purpose
`beam_agents.tools` lets agent code declare tools via the `@tool` decorator, deriving each tool's provider-facing JSON schema from its Python signature through a generated Pydantic v2 model, and collects them in a `ToolRegistry` for name resolution and aggregate `tools_schema` lookup. The `side_effect` flag on every tool separates the fast path — `side_effect=False` tools run inline through `ToolRunner` with argument validation — from the effects path: a `side_effect=True` tool raises `SideEffectToolError` on any direct invocation, enforcing correctness invariant 5 that side effects only ever execute through `ctx.act(...)` and the intents/effector pipeline.

## Requirements

### Requirement: The @tool decorator registers a callable as a Tool

The system SHALL provide a `@tool` decorator that wraps a Python callable into a `Tool`. The decorator SHALL accept an optional `name` (defaulting to the callable's `__name__`), an optional `description` (defaulting to the callable's docstring), and a `side_effect: bool` flag (defaulting to `False`). Applying `@tool` SHALL return an object that exposes the tool's `name`, `description`, `side_effect`, the derived argument model, and the derived JSON `schema`, and SHALL remain callable with the original function's semantics for read-only tools. The decorator SHALL support both bare (`@tool`) and parameterized (`@tool(side_effect=True)`) usage.

#### Scenario: Bare decorator derives name and description from the function

- **WHEN** `@tool` is applied without arguments to a function `lookup_customer` with a docstring
- **THEN** the resulting `Tool` has `name == "lookup_customer"`, `description` equal to the function's docstring, and `side_effect == False`

#### Scenario: Parameterized decorator overrides name and declares a side effect

- **WHEN** `@tool(name="charge", side_effect=True)` is applied to a function
- **THEN** the resulting `Tool` has `name == "charge"` and `side_effect == True`

### Requirement: Tool schema is generated from the function signature via Pydantic v2

The system SHALL derive each tool's argument schema from the wrapped callable's parameters and type hints using a generated Pydantic v2 model. The `Tool` SHALL expose a provider-facing JSON schema (`schema`) containing the tool `name`, `description`, and a JSON Schema `parameters` object describing the arguments, their types, and which are required. Parameters without defaults SHALL be required; parameters with defaults SHALL be optional with the default reflected. A callable whose parameters lack type annotations SHALL raise a `ToolDefinitionError` at decoration time.

#### Scenario: Schema reflects parameter types and required-ness

- **WHEN** a tool wraps `def f(customer_id: str, limit: int = 10) -> ...`
- **THEN** the tool's JSON schema `parameters` marks `customer_id` and `limit` as `string` and `integer` respectively, lists only `customer_id` as required, and records `limit`'s default of `10`

#### Scenario: Missing type annotations are rejected at decoration time

- **WHEN** `@tool` is applied to a function with an un-annotated parameter
- **THEN** decoration raises `ToolDefinitionError` naming the offending parameter

### Requirement: The ToolRegistry collects and resolves tools

The system SHALL provide a `ToolRegistry` that registers `Tool` instances by name, rejects duplicate names, resolves a name to its `Tool`, and exposes the aggregate `tools_schema` (the list of every registered tool's JSON schema) for passing to LLM providers. Resolving an unregistered name SHALL raise a `ToolNotFoundError`.

#### Scenario: A registered tool is resolvable and appears in tools_schema

- **WHEN** a `Tool` named `lookup_customer` is registered and `tools_schema` is read
- **THEN** `registry.get("lookup_customer")` returns that `Tool` and `tools_schema` contains that tool's schema

#### Scenario: Duplicate registration is rejected

- **WHEN** two tools with the same `name` are registered into one `ToolRegistry`
- **THEN** the second registration raises an error identifying the conflicting name

#### Scenario: Resolving an unknown tool raises

- **WHEN** `registry.get("does_not_exist")` is called
- **THEN** a `ToolNotFoundError` is raised naming the requested tool

### Requirement: The ToolRunner executes read-only tools inline with argument validation

The system SHALL provide a `ToolRunner` that executes `side_effect=False` tools inline for the fast path. Before invoking the callable, the runner SHALL validate the supplied arguments against the tool's Pydantic argument model, coercing and rejecting per the model. Invalid arguments SHALL raise a `ToolArgumentError` without invoking the underlying callable. On valid arguments the runner SHALL call the tool and return its result.

#### Scenario: Valid arguments are validated and the tool runs

- **WHEN** the `ToolRunner` runs a `side_effect=False` tool with arguments satisfying its schema
- **THEN** the arguments are validated against the tool's Pydantic model and the underlying callable is invoked with the coerced values, returning its result

#### Scenario: Invalid arguments are rejected before the callable runs

- **WHEN** the `ToolRunner` runs a read-only tool with arguments that fail its Pydantic model (missing required field or wrong type)
- **THEN** a `ToolArgumentError` is raised and the underlying callable is never invoked

### Requirement: Direct invocation of a side-effecting tool raises

The system SHALL enforce correctness invariant 5: a `side_effect=True` tool MUST NOT execute inside the pipeline. Any attempt to run a side-effecting tool directly — via the `ToolRunner` or by calling the decorated tool as a function — SHALL raise a `SideEffectToolError` and MUST NOT invoke the underlying callable. Side-effecting tools are requested only through the intents path (`ctx.act(...)`, implemented by a later change); this requirement establishes the guard.

#### Scenario: ToolRunner refuses a side-effecting tool

- **WHEN** the `ToolRunner` is asked to run a `side_effect=True` tool
- **THEN** a `SideEffectToolError` is raised naming the tool and the underlying callable is never invoked

#### Scenario: Calling a side-effecting tool directly raises

- **WHEN** a `side_effect=True` decorated tool is invoked directly as a callable
- **THEN** a `SideEffectToolError` is raised and no external write occurs
