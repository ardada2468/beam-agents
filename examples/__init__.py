"""Runnable beam-agents examples, rendered verbatim by the docs site.

Each module is a single, self-contained Beam pipeline on the real runtime,
driven by a scripted `FakeLLM` so it runs offline with no credentials and no
docker: `uv run python -m examples.<name>`. The pages under `docs/examples/`
include these files by path, and the tests under `tests/examples/` execute
them — the code the site shows is the code CI runs.

Sample code, not part of the `beam-agents` wheel.
"""
