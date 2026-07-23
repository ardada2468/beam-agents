## 1. Module scaffolding

- [x] 1.1 Create `src/beam_agents/tools/__init__.py` and stub `registry.py`, `runner.py`, `errors.py`
- [x] 1.2 Define the error taxonomy in `errors.py`: `ToolError` base plus `ToolDefinitionError`, `ToolNotFoundError`, `ToolArgumentError`, `SideEffectToolError`

## 2. Tests first (scenario → test)

- [x] 2.1 `tests/tools/test_tool_decorator.py`: bare-decorator name/description/`side_effect=False` defaults; parameterized name override + `side_effect=True`
- [x] 2.2 `tests/tools/test_tool_schema.py`: schema reflects param types, required-ness, and defaults; un-annotated param raises `ToolDefinitionError` at decoration
- [x] 2.3 `tests/tools/test_registry.py`: register/get, `tools_schema` aggregation, duplicate-name rejection, `ToolNotFoundError` on unknown
- [x] 2.4 `tests/tools/test_runner.py`: valid args validated + callable invoked with coerced values; invalid args raise `ToolArgumentError` without invoking callable
- [x] 2.5 `tests/tools/test_side_effect_guard.py`: `ToolRunner` refuses `side_effect=True` with `SideEffectToolError`; direct call of a side-effecting tool raises and never runs the callable
- [x] 2.6 Confirm the new tests fail for the right reason before implementing

## 3. @tool decorator and Tool

- [x] 3.1 Implement `Tool` in `registry.py`: holds `name`, `description`, `side_effect`, argument model, and derived `schema`; read-only tools stay transparently callable
- [x] 3.2 Generate the Pydantic v2 argument model from the signature via `create_model` (no-default → required); raise `ToolDefinitionError` on un-annotated params
- [x] 3.3 Build provider-neutral JSON `schema` (`name`/`description`/`parameters`) from `model_json_schema()`
- [x] 3.4 Implement `@tool` supporting bare and parameterized (`name`, `description`, `side_effect`) forms
- [x] 3.5 Enforce the side-effect guard on `Tool.__call__`: `side_effect=True` raises `SideEffectToolError` before invoking the callable

## 4. ToolRegistry

- [x] 4.1 Implement `ToolRegistry.register`/`get` keyed by name with duplicate rejection and `ToolNotFoundError` on miss
- [x] 4.2 Implement aggregate `tools_schema` over registered tools

## 5. ToolRunner

- [x] 5.1 Implement `ToolRunner.run`: refuse `side_effect=True` with `SideEffectToolError` before validation
- [x] 5.2 Validate arguments against the tool's Pydantic model, mapping validation failures to `ToolArgumentError` without invoking the callable; invoke with coerced values and return the result

## 6. Public API and quality gates

- [x] 6.1 Export `tool`, `Tool`, `ToolRegistry`, `ToolRunner`, and error types from `tools/__init__.py` (per repo convention, `src/beam_agents/__init__.py` stays empty — see `tests/test_import.py::test_public_surface_is_empty` — so this is the capability's public surface, not the root package)
- [x] 6.2 Underscore-prefix any internal-only helpers; keep `Any` out of public signatures
- [x] 6.3 `ruff` clean, `mypy --strict` clean on `src/`; full test suite green; coverage does not decrease
- [x] 6.4 PR description links each new test to its spec scenario
