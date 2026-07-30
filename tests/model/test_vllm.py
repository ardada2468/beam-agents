"""Tests for the vLLM providers (`beam_agents.model.vllm`).

Offline only: endpoint mode is exercised over `httpx.MockTransport`, and the
GPU-worker sidecar is exercised entirely through the injectable engine seam
(`FakeEngine` below) — no GPU, no `vllm` installation, no network. This file
is itself the "Sidecar behavior is verified offline with a fake engine"
scenario of the `model-providers` capability.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import importlib
import importlib.util
import json
import sys
import threading
from collections.abc import Callable

import httpx
import pytest
from apache_beam.utils import shared as beam_shared

from beam_agents.core.bridge import AsyncBridge
from beam_agents.model.client import (
    LlmRequest,
    ProviderError,
    ProviderRequestError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)
from beam_agents.model.openai_compat import decode as openai_decode
from beam_agents.model.vllm import (
    EngineGeneration,
    EngineInvalidRequestError,
    EngineSaturatedError,
    VllmEndpointProvider,
    VllmSidecarProvider,
    engine_config_tag,
    vllm_sidecar_factory,
)

from ._facade_helpers import make_facade

_MESSAGES = [{"role": "user", "content": "hi"}]

_SUCCESS_BODY: dict[str, object] = {
    "id": "chatcmpl_1",
    "object": "chat.completion",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "hello from vllm"},
            "finish_reason": "stop",
        }
    ],
    "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17},
}

_Handler = Callable[[httpx.Request], httpx.Response]


def _request(model_id: str = "local-model") -> LlmRequest:
    return LlmRequest(model_id=model_id, messages=_MESSAGES, tools_schema=None, sampling_params={})


# --- The fake engine: the offline seam the sidecar design (D7) specifies ----


class FakeEngine:
    """Scripted implementation of the sidecar's internal engine seam.

    Records every readiness/generation call plus the loop and thread each ran
    on, so tests can assert singleton sharing, cross-loop submission, and
    graceful shutdown without a real engine.
    """

    def __init__(
        self,
        *,
        text: str = "fake generation",
        prompt_tokens: int = 7,
        completion_tokens: int = 3,
        ready_error: Exception | None = None,
        ready_delay_s: float = 0.0,
        generate_error: Exception | None = None,
        generate_delay_s: float = 0.0,
        generate_gate: threading.Event | None = None,
    ) -> None:
        self.text = text
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.ready_error = ready_error
        self.ready_delay_s = ready_delay_s
        self.generate_error = generate_error
        self.generate_delay_s = generate_delay_s
        self.generate_gate = generate_gate

        self.ready_calls = 0
        self.ready_threads: list[threading.Thread] = []
        self.generate_calls: list[LlmRequest] = []
        self.generate_loops: list[asyncio.AbstractEventLoop] = []
        self.aclose_calls = 0

    async def check_ready(self) -> None:
        self.ready_calls += 1
        self.ready_threads.append(threading.current_thread())
        if self.ready_delay_s > 0:
            await asyncio.sleep(self.ready_delay_s)
        if self.ready_error is not None:
            raise self.ready_error

    async def generate(self, request: LlmRequest) -> EngineGeneration:
        self.generate_calls.append(request)
        self.generate_loops.append(asyncio.get_running_loop())
        if self.generate_gate is not None:
            await asyncio.get_running_loop().run_in_executor(None, self.generate_gate.wait)
        if self.generate_delay_s > 0:
            await asyncio.sleep(self.generate_delay_s)
        if self.generate_error is not None:
            raise self.generate_error
        return EngineGeneration(
            text=self.text,
            prompt_tokens=self.prompt_tokens,
            completion_tokens=self.completion_tokens,
        )

    async def aclose(self) -> None:
        self.aclose_calls += 1


def _sidecar(
    engine: FakeEngine,
    *,
    health_deadline_s: float = 5.0,
    request_timeout_s: float = 5.0,
) -> VllmSidecarProvider:
    """One provider over one fake engine via the public factory helper."""
    factory = vllm_sidecar_factory(
        {"model": "fake-model"},
        health_deadline_s=health_deadline_s,
        request_timeout_s=request_timeout_s,
        engine_factory=lambda: engine,
    )
    return factory()


class _KeepaliveDisplacer:
    """Weakref-able placeholder acquired to displace Beam's Shared keepalive."""


def _displace_shared_keepalive() -> None:
    """Drop Beam's process-global keepalive reference to the last-acquired
    shared object. `apache_beam.utils.shared._SharedMap` deliberately holds a
    strong reference to the most recently acquired object; acquiring any other
    `Shared` handle displaces it — the same event that, in a real worker, lets
    a fully-released engine reach zero strong references and finalize.
    """
    beam_shared.Shared().acquire(_KeepaliveDisplacer)


# --- Requirement: vLLM endpoint mode is a preset over the OpenAI-compatible -


def _endpoint(
    handler: _Handler, *, api_key: str | None = None, base_url: str = "http://vllm.local/v1"
) -> VllmEndpointProvider:
    transport = httpx.MockTransport(handler)
    return VllmEndpointProvider(base_url=base_url, api_key=api_key, transport=transport)


async def test_endpoint_call_is_a_non_streaming_chat_completions_post_returning_raw_bytes() -> None:
    # Scenario: Endpoint call is a non-streaming chat-completions POST
    # returning raw bytes.
    raw_body = json.dumps(_SUCCESS_BODY).encode("utf-8")
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, content=raw_body, headers={"content-type": "application/json"})

    provider = _endpoint(handler)

    response = await provider.complete(_request())

    assert len(calls) == 1
    assert str(calls[0].url) == "http://vllm.local/v1/chat/completions"
    assert json.loads(calls[0].content)["stream"] is False
    assert response.response == raw_body  # raw body bytes, unchanged


async def test_no_api_key_means_no_authorization_header() -> None:
    # Scenario: No API key means no Authorization header.
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_SUCCESS_BODY)

    provider = _endpoint(handler, api_key=None)

    await provider.complete(_request())

    assert "authorization" not in calls[0].headers

    # base_url is required at construction: there is no default endpoint.
    with pytest.raises(TypeError):
        VllmEndpointProvider()  # type: ignore[call-arg]


async def test_configured_api_key_is_sent_as_a_bearer_token() -> None:
    # Scenario: Configured API key is sent as a bearer token.
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_SUCCESS_BODY)

    provider = _endpoint(handler, api_key="sk-vllm")

    await provider.complete(_request())

    assert calls[0].headers["authorization"] == "Bearer sk-vllm"
    # Credentials stay provider state: no credential field exists on LlmRequest.
    assert {f.name for f in dataclasses.fields(LlmRequest)} == {
        "model_id",
        "messages",
        "tools_schema",
        "sampling_params",
    }


async def test_taxonomy_mapping_is_inherited_from_the_shared_mapper() -> None:
    # Scenario: Taxonomy mapping is inherited from the shared mapper.
    with pytest.raises(RateLimitError):
        await _endpoint(lambda _r: httpx.Response(429)).complete(_request())

    with pytest.raises(ServerError) as server_exc:
        await _endpoint(lambda _r: httpx.Response(503)).complete(_request())
    assert server_exc.value.status == 503

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("no route to engine")

    with pytest.raises(ProviderTimeout):
        await _endpoint(timeout_handler).complete(_request())


# --- Requirement: vLLM sidecar engine is a per-worker-process singleton -----


async def test_two_dofn_instances_share_one_engine() -> None:
    # Scenario: Two DoFn instances share one engine.
    built: list[FakeEngine] = []

    def make_engine() -> FakeEngine:
        engine = FakeEngine()
        built.append(engine)
        return engine

    factory = vllm_sidecar_factory(
        {"model": "shared-model"}, health_deadline_s=5.0, engine_factory=make_engine
    )
    provider_one = factory()
    provider_two = factory()

    assert len(built) == 1  # the engine constructor ran exactly once

    await provider_one.complete(_request())
    await provider_two.complete(_request())

    assert len(built) == 1
    assert len(built[0].generate_calls) == 2  # both providers use the same engine


def test_a_changed_engine_configuration_yields_a_distinct_engine() -> None:
    # Scenario: A changed engine configuration yields a distinct engine.
    assert engine_config_tag({"model": "a"}) != engine_config_tag({"model": "b"})

    built: list[FakeEngine] = []

    def make_engine() -> FakeEngine:
        engine = FakeEngine()
        built.append(engine)
        return engine

    handle = beam_shared.Shared()
    VllmSidecarProvider(
        shared_handle=handle,
        tag=engine_config_tag({"model": "a"}),
        engine_config={"model": "a"},
        health_deadline_s=5.0,
        engine_factory=make_engine,
    )
    VllmSidecarProvider(
        shared_handle=handle,
        tag=engine_config_tag({"model": "b"}),
        engine_config={"model": "b"},
        health_deadline_s=5.0,
        engine_factory=make_engine,
    )

    assert len(built) == 2  # the changed tag constructed a new engine


def test_generation_is_submitted_cross_loop_without_blocking_the_bridge() -> None:
    # Scenario: Generation is submitted cross-loop without blocking the bridge.
    gate = threading.Event()
    engine = FakeEngine(generate_gate=gate)
    provider = _sidecar(engine)
    bridge = AsyncBridge()
    bridge.start()
    caller_loops: list[asyncio.AbstractEventLoop] = []
    try:

        async def call() -> bytes:
            caller_loops.append(asyncio.get_running_loop())
            task = asyncio.ensure_future(provider.complete(_request()))
            # The canary only runs if the bridge loop is not blocked by a
            # synchronous engine call while generation is gated.
            await asyncio.sleep(0.05)
            assert not task.done()
            gate.set()
            response = await task
            return response.response

        result = bridge.run(call, timeout_s=10.0)
        assert result
    finally:
        gate.set()
        bridge.stop()

    assert engine.generate_loops[0] is not caller_loops[0]  # engine ran on its own loop


# --- Requirement: Sidecar engine construction is health-checked during setup


def test_an_unhealthy_engine_fails_setup_before_any_element() -> None:
    # Scenario: An unhealthy engine fails setup before any element.
    def broken_factory() -> FakeEngine:
        raise RuntimeError("engine cannot load")

    factory = vllm_sidecar_factory(
        {"model": "broken"}, health_deadline_s=5.0, engine_factory=broken_factory
    )
    with pytest.raises(RuntimeError, match="engine cannot load"):
        factory()

    probe_failing = FakeEngine(ready_error=RuntimeError("probe failed"))
    with pytest.raises(RuntimeError, match="probe failed"):
        _sidecar(probe_failing)

    assert probe_failing.generate_calls == []  # no element reached the broken engine


def test_a_probe_deadline_overrun_raises_instead_of_hanging() -> None:
    # Scenario: A probe deadline overrun raises instead of hanging.
    slow = FakeEngine(ready_delay_s=3600.0)

    with pytest.raises(RuntimeError, match=r"health_deadline_s=0\.05"):
        _sidecar(slow, health_deadline_s=0.05)


def test_a_second_instances_probe_reuses_the_live_engine() -> None:
    # Scenario: A second instance's probe reuses the live engine.
    built: list[FakeEngine] = []

    def make_engine() -> FakeEngine:
        engine = FakeEngine()
        built.append(engine)
        return engine

    factory = vllm_sidecar_factory(
        {"model": "reused"}, health_deadline_s=5.0, engine_factory=make_engine
    )
    factory()
    factory()

    assert len(built) == 1  # no new engine, no re-load
    assert built[0].ready_calls == 2  # but each instance's probe verified readiness


# --- Requirement: Sidecar responses are cacheable chat-completions-shaped ---


async def test_a_sidecar_response_decodes_with_the_shared_decode() -> None:
    # Scenario: A sidecar response decodes with the shared Decode.
    engine = FakeEngine(text="hello from engine", prompt_tokens=11, completion_tokens=4)
    provider = _sidecar(engine)

    response = await provider.complete(_request())

    decoded = openai_decode(response.response)
    assert decoded.usage.prompt_tokens == 11
    assert decoded.usage.completion_tokens == 4
    assert decoded.usage.total_tokens == 15
    assert decoded.text == "hello from engine"

    # Canonical serialization: sorted keys, compact separators, UTF-8.
    expected = json.dumps(
        {
            "choices": [
                {
                    "finish_reason": "stop",
                    "index": 0,
                    "message": {"content": "hello from engine", "role": "assistant"},
                }
            ],
            "object": "chat.completion",
            "usage": {"completion_tokens": 4, "prompt_tokens": 11, "total_tokens": 15},
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert response.response == expected


async def test_cached_sidecar_bytes_replay_without_engine_calls() -> None:
    # Scenario: Cached sidecar bytes replay without engine calls.
    engine = FakeEngine(text="cache me", prompt_tokens=2, completion_tokens=1)
    provider = _sidecar(engine)
    facade, _ = make_facade(provider, decode=openai_decode)
    request = _request()

    first = await facade.complete(request, entity_key=b"key-1", seq=0, step_index=0)
    assert len(engine.generate_calls) == 1
    assert first.cache_hit is False

    second = await facade.complete(request, entity_key=b"key-1", seq=0, step_index=0)

    assert len(engine.generate_calls) == 1  # zero additional engine calls
    assert second.cache_hit is True
    assert second.response.response == first.response.response
    assert openai_decode(second.response.response) == openai_decode(first.response.response)


# --- Requirement: Sidecar engine failures map onto the provider taxonomy ----


async def test_engine_saturation_is_retryable_as_a_rate_limit() -> None:
    # Scenario: Engine saturation is retryable as a rate limit.
    provider = _sidecar(FakeEngine(generate_error=EngineSaturatedError("queue full")))

    with pytest.raises(RateLimitError) as excinfo:
        await provider.complete(_request())

    assert isinstance(excinfo.value, ProviderError)  # retryable by type
    assert excinfo.value.retry_after_ms is None


async def test_engine_internal_failure_is_a_retryable_server_error() -> None:
    # Scenario: Engine internal failure is a retryable server error.
    provider = _sidecar(FakeEngine(generate_error=RuntimeError("generation exploded")))

    with pytest.raises(ServerError) as excinfo:
        await provider.complete(_request())

    assert isinstance(excinfo.value, ProviderError)
    assert excinfo.value.status == 500


async def test_generation_deadline_maps_to_provider_timeout() -> None:
    # Scenario: Generation deadline maps to ProviderTimeout.
    provider = _sidecar(FakeEngine(generate_delay_s=3600.0), request_timeout_s=0.05)

    with pytest.raises(ProviderTimeout):
        await provider.complete(_request())


async def test_invalid_request_material_is_non_retryable() -> None:
    # Scenario: Invalid request material is non-retryable.
    provider = _sidecar(FakeEngine(generate_error=EngineInvalidRequestError("prompt too long")))

    with pytest.raises(ProviderRequestError) as excinfo:
        await provider.complete(_request())

    assert not isinstance(excinfo.value, ProviderError)  # propagates without retry
    assert excinfo.value.status == 400


# --- Requirement: The sidecar engine shuts down gracefully on last release --


async def test_the_engine_survives_a_siblings_teardown() -> None:
    # Scenario: The engine survives a sibling's teardown.
    built: list[FakeEngine] = []

    def make_engine() -> FakeEngine:
        engine = FakeEngine()
        built.append(engine)
        return engine

    factory = vllm_sidecar_factory(
        {"model": "survivor"}, health_deadline_s=5.0, engine_factory=make_engine
    )
    provider_one = factory()
    provider_two = factory()
    engine = built[0]

    del provider_one  # one of two holders releases (what teardown does)
    _displace_shared_keepalive()
    gc.collect()

    assert engine.aclose_calls == 0  # the engine kept running
    response = await provider_two.complete(_request())
    assert openai_decode(response.response).text == engine.text


def test_last_release_shuts_the_engine_down_exactly_once() -> None:
    # Scenario: Last release shuts the engine down exactly once.
    built: list[FakeEngine] = []

    def make_engine() -> FakeEngine:
        engine = FakeEngine()
        built.append(engine)
        return engine

    factory = vllm_sidecar_factory(
        {"model": "final"}, health_deadline_s=5.0, engine_factory=make_engine
    )
    provider_one = factory()
    provider_two = factory()
    engine = built[0]
    engine_thread = engine.ready_threads[0]
    assert engine_thread.is_alive()

    del provider_one
    del provider_two  # the last holder releases
    _displace_shared_keepalive()
    gc.collect()

    assert engine.aclose_calls == 1  # the finalizer released the engine
    engine_thread.join(timeout=5.0)
    assert not engine_thread.is_alive()  # engine loop stopped, thread joined

    gc.collect()
    assert engine.aclose_calls == 1  # exactly once


# --- Requirement: The vllm extra gates the sidecar; verification is offline -


def test_the_model_package_imports_without_the_extra() -> None:
    # Scenario: The model package imports without the extra.
    module = importlib.import_module("beam_agents.model")

    assert "vllm" not in sys.modules  # the lazy import never ran
    # Endpoint mode is fully usable without the extra (exercised above); both
    # providers surface through the package's re-exports. Compared against the
    # live submodule (not this file's imports): a sibling test legitimately
    # purges and re-imports `beam_agents.model.*` to prove clean importability.
    submodule = sys.modules["beam_agents.model.vllm"]
    for name in ("VllmEndpointProvider", "VllmSidecarProvider", "vllm_sidecar_factory"):
        assert name in module.__all__
        assert getattr(module, name) is getattr(submodule, name)


@pytest.mark.skipif(
    importlib.util.find_spec("vllm") is not None, reason="the vllm extra is installed"
)
def test_sidecar_construction_without_the_extra_fails_actionably() -> None:
    # Scenario: Sidecar construction without the extra fails actionably.
    factory = vllm_sidecar_factory({"model": "tiny"}, health_deadline_s=30.0)

    with pytest.raises(RuntimeError, match=r"beam-agents\[vllm\]"):
        factory()
