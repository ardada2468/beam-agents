## ADDED Requirements

### Requirement: Intent identity is injected into tools that declare it

When executing a `ToolIntent`, the effector SHALL build an `IntentInfo` from the intent's own wire fields — `intent_id`, `entity_key`, `seq`, `step_index`, `attempt` — and pass it as the keyword argument `intent` to the tool callable if and only if the tool's `accepts_intent` is true. The injected value SHALL NOT participate in argument validation: the arguments validated against the tool's Pydantic model remain exactly the parsed `args_json`, and an `intent` key arriving inside `args_json` of a declaring tool SHALL be rejected as an unknown argument, never silently shadowed by or merged with the injected identity. Tools with `accepts_intent` false SHALL be invoked exactly as before this capability existed, with no additional keyword.

Because `intent_id` is deterministic (uuid5 over `entity_key + seq + step_index`) and the effector redelivers rather than re-mints, every invocation of the same logical effect — across pipeline replays, sink duplicates, and lease-expiry re-executions — SHALL receive the identical `intent_id`, making it usable as a downstream idempotency key.

#### Scenario: A declaring tool receives the executing intent's identity

- **WHEN** `execute_intent` runs a `ToolIntent` for a tool declaring `*, intent: IntentInfo`
- **THEN** the callable receives an `IntentInfo` whose `intent_id`, `entity_key`, `seq`, `step_index`, and `attempt` equal the executing intent's wire fields, alongside its validated arguments

#### Scenario: A non-declaring tool is invoked unchanged

- **WHEN** `execute_intent` runs a `ToolIntent` for a tool with `accepts_intent` false
- **THEN** the callable is invoked with only its validated arguments and no `intent` keyword

#### Scenario: Injection does not alter argument validation

- **WHEN** a declaring tool's intent carries `args_json` that is valid against the tool's argument model
- **THEN** validation passes without an `intent` value being present in the arguments, and invalid `args_json` is still `REJECTED` before the callable runs

#### Scenario: An intent key inside args_json is rejected, not shadowed

- **WHEN** a declaring tool's `args_json` contains an `"intent"` key
- **THEN** argument validation fails and the intent is `REJECTED` without invoking the callable

#### Scenario: A re-executed intent carries identical identity

- **WHEN** the same `intent_id` is executed again after a lease expiry or duplicate delivery (the dedup store permitting a second invocation)
- **THEN** the injected `IntentInfo.intent_id` is byte-identical to the first invocation's, so a tool keying its downstream effect on it performs one effective effect

#### Scenario: Async declaring tools are injected identically

- **WHEN** the declaring tool is an `async def` callable
- **THEN** the `intent` keyword is injected the same way and the coroutine is awaited as before
