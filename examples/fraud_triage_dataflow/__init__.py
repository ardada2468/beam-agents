"""Dataflow Flex Template packaging for the fraud-triage example.

`launch.py` is the template's entrypoint logic, `main.py` the file the Flex
Template launcher executes, `metadata.json` the parameter surface the console
and `gcloud` validate against, and `Dockerfile` the image that serves as both
template launcher and Beam SDK harness. See `README.md` for the parameter table
and the one-command launch.

Importing this package has no side effects.
"""

from __future__ import annotations
