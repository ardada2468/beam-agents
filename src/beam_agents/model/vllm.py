"""vLLM `LLMClient`s: an endpoint preset and a GPU-worker sidecar.

Two ways to reach vLLM, both structural subtypes of the async `LLMClient`
protocol and both constructed through `AgentConfig.provider_factory` with zero
`core/` changes:

- :class:`VllmEndpointProvider` — a preset over the OpenAI-compatible provider
  for a separately served vLLM OpenAI endpoint (design D1): `base_url` is
  required (no OpenAI default to mis-hit) and the API key is optional (no
  `Authorization` header is sent when absent, matching unauthenticated vLLM
  deployments). The POST/raw-bytes/taxonomy path is delegated unchanged.
- :class:`VllmSidecarProvider` — an in-process engine held as one worker-local
  singleton via `apache_beam.utils.shared.Shared` (design D2), the documented
  exception to the no-global-mutable-state rule. The shared engine handle owns
  a dedicated engine thread with its own asyncio loop (design D3); provider
  construction runs a bounded readiness probe so a broken engine fails
  `_AgentDoFn.setup` before any element (design D4); and a `weakref.finalize`
  shuts the engine down when the last holder releases it (design D5).

Both modes pair with the existing OpenAI-compatible decoder — pass
``beam_agents.model.openai_compat.decode`` as ``AgentConfig.decode``; there is
no vLLM-specific decoder. The sidecar serializes engine output into canonical
chat-completions-shaped bytes (design D6) so that one `Decode` serves both
modes and the replay-cache opaque-bytes contract holds.

`vllm` itself is an optional extra imported lazily inside the real engine
constructor only (design D7): importing this module — and every endpoint-mode
code path — works without it. Importing this module has no side effects.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import functools
import hashlib
import importlib
import itertools
import json
import threading
import weakref
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import Protocol, TypeVar, runtime_checkable

import httpx
from apache_beam.utils import shared as beam_shared

from beam_agents.model.client import (
    LlmRequest,
    LlmResponse,
    ProviderRequestError,
    ProviderTimeout,
    RateLimitError,
    ServerError,
)
from beam_agents.model.openai_compat import OpenAICompatProvider

__all__ = [
    "VllmEndpointProvider",
    "VllmSidecarProvider",
    "vllm_sidecar_factory",
]

_T = TypeVar("_T")

_DEFAULT_TIMEOUT_S = 60.0
# Generous by default: weight loading legitimately takes minutes on large
# models. Raise it further (and pre-bake weights into the container image) for
# multi-GB models; an overrun fails `setup` loudly rather than hanging a bundle.
_DEFAULT_HEALTH_DEADLINE_S = 600.0
_SHUTDOWN_GRACE_S = 10.0

# Conventional stand-in statuses (design D8): in-process engine failures carry
# no HTTP status, but the taxonomy's constructors require one. 500 marks an
# engine internal failure (retryable `ServerError`); 400 marks request material
# the engine rejected as invalid (non-retryable `ProviderRequestError`).
_ENGINE_INTERNAL_FAILURE_STATUS = 500
_ENGINE_INVALID_REQUEST_STATUS = 400


# --- Endpoint mode (design D1) ----------------------------------------------


class _OmitAuthTransport(httpx.AsyncBaseTransport):
    """Strips the `Authorization` header the delegate unconditionally attaches.

    The one genuinely vLLM-shaped endpoint behavior — key-less deployments —
    lives here, so the POST, raw-bytes passthrough, and taxonomy mapping stay
    delegated to `OpenAICompatProvider` instead of being forked.
    """

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        request.headers.pop("authorization", None)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:  # pragma: no cover - close passthrough
        await self._inner.aclose()


class VllmEndpointProvider:
    """`LLMClient` preset for a served vLLM OpenAI-compatible endpoint.

    Differs from the general `OpenAICompatProvider` in exactly two knobs:
    `base_url` is required (there is no default endpoint) and `api_key` is
    optional (no `Authorization` header is sent when absent). Everything else —
    one non-streaming POST to `<base_url>/chat/completions`, raw response bytes,
    the shared HTTP-outcome taxonomy mapping — is delegated. Pair with
    ``openai_compat.decode`` as ``AgentConfig.decode``. Never needs the `vllm`
    extra.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str | None = None,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        delegate_transport = transport
        if api_key is None:
            inner = transport if transport is not None else httpx.AsyncHTTPTransport()
            delegate_transport = _OmitAuthTransport(inner)
        self._delegate = OpenAICompatProvider(
            api_key=api_key if api_key is not None else "",
            base_url=base_url,
            timeout_s=timeout_s,
            transport=delegate_transport,
        )

    async def complete(self, request: LlmRequest) -> LlmResponse:
        """Delegate to the OpenAI-compatible client pointed at the vLLM endpoint."""
        return await self._delegate.complete(request)


# --- The engine seam (design D7) --------------------------------------------


@dataclass(frozen=True, slots=True)
class _EngineGeneration:
    """One completed generation at the engine seam: text plus the engine's
    token accounting, exactly what the canonical response body needs.
    """

    text: str
    prompt_tokens: int
    completion_tokens: int


class _EngineSaturatedError(Exception):
    """The engine refused admission (queue full / KV-cache exhaustion); the
    provider maps it to the retryable `RateLimitError` (design D8).
    """


class _EngineInvalidRequestError(Exception):
    """The engine rejected the request material as invalid (bad sampling
    params, over-length prompt); mapped to the non-retryable
    `ProviderRequestError` (design D8).
    """


@runtime_checkable
class _VllmEngine(Protocol):
    """Narrow internal seam the sidecar drives the engine through.

    The real implementation adapts vLLM's async engine; unit tests inject a
    fake, so every sidecar behavior is verifiable offline — no GPU, no `vllm`
    install (design D7). All three coroutines run on the engine handle's own
    loop, never on a DoFn bridge loop.
    """

    async def check_ready(self) -> None: ...

    async def generate(self, request: LlmRequest) -> _EngineGeneration: ...

    async def aclose(self) -> None: ...


class _RealVllmEngine:
    """Seam adapter over an in-process vLLM async engine.

    Imports `vllm` lazily — here and only here — so the module, endpoint mode,
    and every offline test path work without the extra. Real-engine behavior is
    exercised only by the hardware-gated nightly smoke tier
    (`tests/smoke/test_vllm_sidecar.py`); the pinned version floor in the
    `vllm` extra bounds seam/reality drift.
    """

    def __init__(self, engine_config: dict[str, object]) -> None:
        try:
            self._vllm = importlib.import_module("vllm")
        except ImportError as exc:
            raise RuntimeError(
                "VllmSidecarProvider requires the optional vllm extra, which is not "
                "installed; install it with: pip install 'beam-agents[vllm]'"
            ) from exc
        # From here on down is smoke-only (needs the extra plus a GPU), hence
        # outside offline coverage; the missing-extra path above stays counted.
        engine_args = self._vllm.AsyncEngineArgs(**engine_config)  # pragma: no cover
        self._engine = self._vllm.AsyncLLMEngine.from_engine_args(  # pragma: no cover
            engine_args
        )
        self._request_ids = itertools.count()  # pragma: no cover

    async def check_ready(self) -> None:  # pragma: no cover - smoke-only
        # A trivial engine round-trip: succeeds iff the engine loaded and
        # answers. Never re-loads weights on an already-live engine.
        await self._engine.get_model_config()

    async def generate(self, request: LlmRequest) -> _EngineGeneration:  # pragma: no cover
        try:
            sampling_params = self._vllm.SamplingParams(
                **(request.sampling_params if isinstance(request.sampling_params, dict) else {})
            )
            tokenizer = await self._engine.get_tokenizer()
            prompt = str(
                tokenizer.apply_chat_template(
                    request.messages, tokenize=False, add_generation_prompt=True
                )
            )
        except ValueError as exc:
            raise _EngineInvalidRequestError(str(exc)) from exc

        final = None
        try:
            async for output in self._engine.generate(
                prompt, sampling_params, str(next(self._request_ids))
            ):
                final = output
        except ValueError as exc:
            # vLLM signals over-length prompts / invalid params as ValueError.
            raise _EngineInvalidRequestError(str(exc)) from exc
        if final is None:
            raise RuntimeError("vLLM engine produced no output")
        first = final.outputs[0]
        return _EngineGeneration(
            text=str(first.text),
            prompt_tokens=len(final.prompt_token_ids),
            completion_tokens=len(first.token_ids),
        )

    async def aclose(self) -> None:  # pragma: no cover - smoke-only
        shutdown = getattr(self._engine, "shutdown_background_loop", None)
        if callable(shutdown):
            shutdown()


# --- The shared engine handle (designs D3/D5) -------------------------------


def _drive_loop(loop: asyncio.AbstractEventLoop) -> None:
    # Module-level on purpose: a bound-method thread target would hold a strong
    # reference to the handle from its own thread, so the last-holder finalizer
    # (design D5) could never fire.
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _shutdown_engine(
    loop: asyncio.AbstractEventLoop,
    thread: threading.Thread,
    cell: dict[str, _VllmEngine],
) -> None:
    """Finalizer body: release the engine, stop the engine loop, join the
    engine thread. Best-effort by design — on hard worker death no finalizer
    runs and the OS reclaims the GPU with the process; correctness never
    depends on this path.
    """
    engine = cell.pop("engine", None)
    if engine is not None:
        with contextlib.suppress(BaseException):
            future = asyncio.run_coroutine_threadsafe(engine.aclose(), loop)
            future.result(timeout=_SHUTDOWN_GRACE_S)
    with contextlib.suppress(BaseException):
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=_SHUTDOWN_GRACE_S)
        loop.close()


class _EngineHandle:
    """The worker-local singleton `Shared` stores: owns the dedicated engine
    thread and asyncio loop (design D3), constructs the engine lazily on that
    loop (so the readiness probe's deadline bounds weight loading), and
    registers the last-reference shutdown finalizer (design D5).
    """

    def __init__(self, engine_factory: Callable[[], _VllmEngine]) -> None:
        self._engine_factory = engine_factory
        # The engine lives in a cell (not an attribute) so the finalizer can
        # release it without referencing the handle it is finalizing.
        self._cell: dict[str, _VllmEngine] = {}
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=_drive_loop, args=(self._loop,), name="beam-agents-vllm-engine", daemon=True
        )
        self._thread.start()
        self._finalizer = weakref.finalize(
            self, _shutdown_engine, self._loop, self._thread, self._cell
        )

    def submit(self, coro: Coroutine[object, object, _T]) -> concurrent.futures.Future[_T]:
        """Submit a coroutine to the engine loop from any thread/loop."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    async def _ensure_engine(self) -> _VllmEngine:
        # Runs on the engine loop; the check-construct-store sequence has no
        # await, so concurrent probes cannot double-build the engine.
        engine = self._cell.get("engine")
        if engine is None:
            engine = self._engine_factory()
            self._cell["engine"] = engine
        return engine

    async def check_ready(self) -> None:
        engine = await self._ensure_engine()
        await engine.check_ready()

    async def generate(self, request: LlmRequest) -> _EngineGeneration:
        engine = await self._ensure_engine()
        return await engine.generate(request)


# --- Sidecar provider (designs D2/D4/D6/D8) ---------------------------------


def _engine_config_tag(engine_config: Mapping[str, object]) -> str:
    """Digest of the engine configuration, used as the `Shared` acquisition
    tag: a changed configuration yields a fresh engine on pipeline update
    instead of silently reusing a stale one (design D2).
    """
    canonical = json.dumps(dict(engine_config), sort_keys=True, separators=(",", ":"), default=repr)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class VllmSidecarProvider:
    """`LLMClient` over the in-process vLLM engine singleton.

    Construction acquires the shared engine handle and runs the bounded
    readiness probe (design D4): `provider_factory()` runs inside
    `_AgentDoFn.setup`, so a broken or wedged engine fails the worker before
    any element is processed. The provider holds the handle strongly for the
    DoFn's lifetime — `Shared` retains it only weakly — and `teardown`'s
    provider-nulling is the release that lets the last-holder finalizer run.
    Pair with ``openai_compat.decode`` as ``AgentConfig.decode``.
    """

    def __init__(
        self,
        *,
        shared_handle: beam_shared.Shared,
        tag: str,
        engine_config: Mapping[str, object],
        health_deadline_s: float = _DEFAULT_HEALTH_DEADLINE_S,
        request_timeout_s: float = _DEFAULT_TIMEOUT_S,
        engine_factory: Callable[[], _VllmEngine] | None = None,
    ) -> None:
        if engine_factory is None:
            engine_factory = functools.partial(_RealVllmEngine, dict(engine_config))
        self._request_timeout_s = request_timeout_s
        # The strong reference that keeps the weakly-held singleton alive for
        # this DoFn instance's lifetime.
        self._handle: _EngineHandle = shared_handle.acquire(
            functools.partial(_EngineHandle, engine_factory), tag=tag
        )
        probe = self._handle.submit(self._handle.check_ready())
        try:
            probe.result(timeout=health_deadline_s)
        except concurrent.futures.TimeoutError:
            probe.cancel()
            raise RuntimeError(
                "vLLM sidecar readiness probe did not complete within "
                f"health_deadline_s={health_deadline_s}; large models need a raised "
                "deadline and weights pre-baked into the worker image"
            ) from None

    async def complete(self, request: LlmRequest) -> LlmResponse:
        """Generate on the worker-local engine, bridged onto the engine's own loop.

        Raises :class:`ProviderTimeout` when the request outlives
        ``request_timeout_s`` and the mapped :class:`ProviderError` for an
        engine rejection, so sidecar failures classify exactly like remote
        provider failures.
        """
        future = self._handle.submit(self._handle.generate(request))
        try:
            generation = await asyncio.wait_for(
                asyncio.wrap_future(future), timeout=self._request_timeout_s
            )
        except TimeoutError as exc:
            # Per-request generation deadline (design D8).
            raise ProviderTimeout() from exc
        except _EngineSaturatedError as exc:
            raise RateLimitError(retry_after_ms=None) from exc
        except _EngineInvalidRequestError as exc:
            raise ProviderRequestError(status=_ENGINE_INVALID_REQUEST_STATUS) from exc
        except Exception as exc:
            raise ServerError(status=_ENGINE_INTERNAL_FAILURE_STATUS) from exc
        return LlmResponse(_serialize_chat_completion(generation))


def _serialize_chat_completion(generation: _EngineGeneration) -> bytes:
    """Canonical chat-completions-shaped body (design D6): sorted keys, compact
    separators, UTF-8 — decodable by `openai_compat.decode`, stable for digests.
    """
    body = {
        "choices": [
            {
                "finish_reason": "stop",
                "index": 0,
                "message": {"content": generation.text, "role": "assistant"},
            }
        ],
        "object": "chat.completion",
        "usage": {
            "completion_tokens": generation.completion_tokens,
            "prompt_tokens": generation.prompt_tokens,
            "total_tokens": generation.prompt_tokens + generation.completion_tokens,
        },
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def vllm_sidecar_factory(
    engine_config: Mapping[str, object],
    *,
    health_deadline_s: float = _DEFAULT_HEALTH_DEADLINE_S,
    request_timeout_s: float = _DEFAULT_TIMEOUT_S,
    engine_factory: Callable[[], _VllmEngine] | None = None,
) -> Callable[[], VllmSidecarProvider]:
    """Build the `provider_factory` for sidecar mode.

    The `Shared` handle is created here, once, at pipeline-construction time
    (design D2): every DoFn instance deserialized in a worker process carries
    the same handle identity, so all of them resolve to one engine — one copy
    of model weights per process. `engine_config` is an opaque mapping passed
    through to vLLM's engine args and digested into the acquisition tag;
    `engine_factory` is the offline test seam (design D7).
    """
    handle = beam_shared.Shared()
    tag = _engine_config_tag(engine_config)
    config = dict(engine_config)

    def provider_factory() -> VllmSidecarProvider:
        return VllmSidecarProvider(
            shared_handle=handle,
            tag=tag,
            engine_config=config,
            health_deadline_s=health_deadline_s,
            request_timeout_s=request_timeout_s,
            engine_factory=engine_factory,
        )

    return provider_factory
