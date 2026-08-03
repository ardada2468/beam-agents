"""Pull-based ingest sources: places the console reads records *from*.

The ``console://`` sink pushes; these pull. That distinction is the whole point
of the group — a pipeline already exporting to Kafka or BigQuery should not have
to be reconfigured, redeployed, or even restarted for its telemetry to become
visible, and a run captured for the replay CLI should be inspectable with no
pipeline running at all.

Every source in this package obeys the same three rules:

- It decodes to protos and hands them to ``_ingest.normalize``. No source builds
  store rows itself, so none of them can drift from the store's understanding of
  a record (design D7).
- Its client library is imported **inside** the constructor that needs it,
  matching ``memory/stores/`` and ``effector/``. Importing this package must
  work with no extras installed, and a missing client must produce an error
  naming the extra rather than a transitive ``ImportError``.
- A record it cannot decode is counted and skipped. One malformed message on a
  topic is not a reason for a viewer to stop viewing.

Importing this module has no side effects.
"""

from __future__ import annotations

__all__: list[str] = []
