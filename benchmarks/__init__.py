"""Offline pyperf benchmark suite for the beam-agents runtime.

One module per measured dimension, each a pyperf script writing JSON into
``bench-results/`` (gitignored); ``make bench`` runs them all and
``make bench-gate`` (``scripts/bench_gate.py``) renders the verdict and the
report. Fully offline: no docker, no network, ``FakeLLM`` only. See
``docs/benchmarks.md`` and ``openspec/changes/add-benchmark-harness/``.
"""
