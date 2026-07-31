"""The file `FLEX_TEMPLATE_PYTHON_PY_FILE` names — a shim, on purpose.

The Flex Template launcher executes this file as `__main__`, and Beam pickles
agents and DoFns by module reference: anything a pipeline touches that was
defined in `__main__` cannot be unpickled on a worker, where no `__main__` of
that shape exists. So all of the launcher's logic — including the agent wrapper
the pipeline holds — lives in the importable `launch` module, and this file only
calls into it.
"""

from __future__ import annotations

import sys

from examples.fraud_triage_dataflow.launch import main

if __name__ == "__main__":
    sys.exit(main())
