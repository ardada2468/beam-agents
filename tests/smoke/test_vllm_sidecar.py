"""Nightly-only, hardware-gated smoke test for the vLLM GPU-worker sidecar.

Covers the `model-providers` scenario "Real-engine tests are smoke-marked and
hardware-gated": carries `-m smoke` (excluded from `make test-unit`) and skips
unless both the `vllm` extra and a visible GPU are present, so the nightly
smoke lane stays green on GPU-less runners. Everything else about the sidecar
is verified offline through the engine seam in `tests/model/test_vllm.py`.
"""

from __future__ import annotations

import importlib
import importlib.util

import pytest

from beam_agents.model.client import LlmRequest
from beam_agents.model.openai_compat import decode as openai_decode
from beam_agents.model.vllm import vllm_sidecar_factory

pytestmark = pytest.mark.smoke

_SMALL_MODEL = "Qwen/Qwen2.5-0.5B-Instruct"


def _vllm_and_gpu_available() -> bool:
    if importlib.util.find_spec("vllm") is None:
        return False
    if importlib.util.find_spec("torch") is None:
        return False
    torch = importlib.import_module("torch")
    return bool(torch.cuda.is_available())


@pytest.mark.skipif(
    not _vllm_and_gpu_available(), reason="requires the vllm extra and a visible GPU"
)
@pytest.mark.timeout(900)
async def test_sidecar_loads_a_small_model_and_returns_a_decodable_response() -> None:
    factory = vllm_sidecar_factory(
        {"model": _SMALL_MODEL, "gpu_memory_utilization": 0.4},
        health_deadline_s=600.0,
        request_timeout_s=120.0,
    )
    provider = factory()

    request = LlmRequest(
        model_id=_SMALL_MODEL,
        messages=[{"role": "user", "content": "Say the single word: pong"}],
        tools_schema=None,
        sampling_params={"max_tokens": 8},
    )

    response = await provider.complete(request)
    decoded = openai_decode(response.response)

    assert decoded.text
    assert decoded.usage.total_tokens > 0
