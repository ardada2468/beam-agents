# docs-examples Specification

## Purpose
TBD - created by archiving change add-docs-website. Update Purpose after archive.
## Requirements
### Requirement: Examples are standalone runnable programs

Every example the site publishes SHALL be a real Python file under `website/examples/`, runnable as `python website/examples/<name>.py` with no arguments, using `DirectRunner` and `FakeLLM` only. Each file MUST open with a module docstring stating what it demonstrates. Examples MUST NOT require docker, network access, provider credentials, or any optional extra beyond those declared in their own header.

#### Scenario: Example runs offline with no arguments

- **WHEN** a contributor runs `python website/examples/<name>.py` on a machine with docker down and no network access
- **THEN** the program exits `0`

#### Scenario: Example imports only from the disclosed module surface

- **WHEN** the test suite inspects an example's imports
- **THEN** every `beam_agents` import resolves to `beam_agents` itself or to a module on the disclosed allowlist, and an import outside that list fails

#### Scenario: The disclosed allowlist is published, not hidden

- **WHEN** a reader loads the API index
- **THEN** it lists every module outside `beam_agents.__all__` that using the runtime currently requires — including the underscore-prefixed `beam_agents._protos`, needed to construct the `AgentEnvelope` input `RunAgent` demands — with the reason each is needed and the statement that they carry no stability promise

### Requirement: Examples are executed by the repository's offline test tier

`tests/docs/test_website_examples.py` SHALL discover every file in `website/examples/` and execute it, asserting a successful exit and any documented expected output. These tests MUST run in the default `pytest` tier — no marker, no docker, offline — so that a change under `src/` that breaks a published example fails the required `ci` check.

#### Scenario: Every example is covered without per-file registration

- **WHEN** a contributor adds a new file to `website/examples/`
- **THEN** `make test-unit` executes it with no edit to the test module

#### Scenario: Breaking the runtime fails the example tests

- **WHEN** a change under `src/beam_agents/` breaks the API an example uses
- **THEN** `make test-unit` exits non-zero identifying the failing example file

#### Scenario: Example tests need no docker

- **WHEN** `make test-unit` runs with the compose stack down
- **THEN** the example tests execute rather than skip

### Requirement: Pages embed example source by reference, never by transcription

Content pages SHALL embed example code with a build-time component that reads the file from `website/examples/` at build time. Fragments smaller than a whole program MUST be embedded as named regions delimited in the source file by `# region: <name>` / `# endregion: <name>` comments. Content MUST NOT contain a fenced Python block that reproduces example code inline.

#### Scenario: Embedded code matches the file on disk

- **WHEN** an example file is edited and the site is rebuilt
- **THEN** every page embedding it renders the updated source with no content edit

#### Scenario: Reference to a missing file or region fails the build

- **WHEN** a page embeds `<Example file="gone.py" />` or names a region that does not exist
- **THEN** `make site-build` exits non-zero naming the page, the file, and the region

#### Scenario: Transcribed example code is rejected

- **WHEN** a content page contains a fenced `python` block whose non-trivial content also appears in a file under `website/examples/`
- **THEN** `make site-check` exits non-zero directing the author to embed by reference

### Requirement: Examples cover the runtime's documented paths

The example set SHALL include, at minimum: a fast-path activation, an activation emitting a `ToolIntent` and resuming on a re-injected result, a human-in-the-loop suspension with a timeout fallback, consumption of the four `RunAgent` outputs including `.errors`, and a LangGraph adapter example marked with the extra it requires. Each example MUST be reachable from the Examples index with a one-line description of what it demonstrates.

#### Scenario: Required example topics are present

- **WHEN** `make site-check` audits the example set against the required topic list
- **THEN** every required topic maps to at least one example file, and a missing topic fails the check

#### Scenario: Extra-dependent examples declare their extra

- **WHEN** an example imports an adapter requiring an optional extra
- **THEN** its page states the extra required to run it, and the test module skips it with a stated reason when the extra is absent
