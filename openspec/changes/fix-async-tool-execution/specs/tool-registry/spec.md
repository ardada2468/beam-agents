## MODIFIED Requirements

### Requirement: The ToolRunner executes read-only tools inline with argument validation

The system SHALL provide a `ToolRunner` that executes `side_effect=False` tools inline for the fast path. `ToolRunner.run` SHALL be an async method. Before invoking the callable, the runner SHALL validate the supplied arguments against the tool's Pydantic argument model, coercing and rejecting per the model. Invalid arguments SHALL raise a `ToolArgumentError` without invoking the underlying callable. On valid arguments the runner SHALL call the tool and, if the call returns an awaitable (an `async def` tool, or any sync tool returning an awaitable), SHALL `await` it and return the awaited result; otherwise it SHALL return the result directly.

#### Scenario: Valid arguments are validated and a sync tool runs

- **WHEN** the `ToolRunner` runs a `side_effect=False` sync tool with arguments satisfying its schema
- **THEN** the arguments are validated against the tool's Pydantic model, the underlying callable is invoked with the coerced values, and its result is returned directly

#### Scenario: Invalid arguments are rejected before the callable runs

- **WHEN** the `ToolRunner` runs a read-only tool with arguments that fail its Pydantic model (missing required field or wrong type)
- **THEN** a `ToolArgumentError` is raised and the underlying callable is never invoked

#### Scenario: An async tool is awaited and its result returned

- **WHEN** the `ToolRunner` runs a `side_effect=False` tool defined with `async def` and arguments satisfying its schema
- **THEN** the coroutine is awaited to completion and `ToolRunner.run` returns the tool's actual result, not a coroutine object
