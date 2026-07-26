"""Repo-wide fixtures."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from apache_beam.coders.typecoders import registry as coder_registry


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    """Snapshot and restore the global coder registry so tests are isolated.

    ``CoderRegistry.register_coder`` (called by ``register_coders()``, which
    ``RunAgent.expand``/``WriteIntents.expand`` invoke) mutates two structures:
    ``_coders`` and ``custom_types``. Restoring only ``_coders`` leaves
    ``custom_types`` holding types no longer in ``_coders``, which later makes
    ``get_custom_type_coder_tuples()`` (called during cloudpickle pickling)
    raise ``KeyError`` in an unrelated, later test. Both must be restored
    together.
    """
    saved_coders = dict(coder_registry._coders)
    saved_custom_types = list(coder_registry.custom_types)
    try:
        yield
    finally:
        coder_registry._coders = saved_coders
        coder_registry.custom_types = saved_custom_types
