"""Cache-key derivation tests for the `llm-replay-cache` capability.

Covers the content-hash cache key: order-independent canonicalization, that
every component perturbs the key, and loud rejection of non-canonical input.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from beam_agents.model import compute_cache_key

# A representative provider-request shape reused across tests.
_MESSAGES = [
    {"role": "system", "content": "you are a fraud triage agent"},
    {"role": "user", "content": "score this transaction"},
]
_TOOLS = [{"name": "lookup", "parameters": {"type": "object", "properties": {}}}]
_PARAMS = {"temperature": 0.0, "max_tokens": 512, "top_p": 1.0}
_KEY = b"entity-1"
_SEQ = 7


def _baseline() -> str:
    return compute_cache_key("claude-opus-4-8", _MESSAGES, _TOOLS, _PARAMS, _KEY, _SEQ)


# --- Requirement: Deterministic content-hash cache key -----------------------


def test_logically_equal_requests_hash_identically() -> None:
    # Scenario: Logically equal requests hash identically.
    # Same content, different dict insertion order in every dict component.
    params_permuted = {"top_p": 1.0, "max_tokens": 512, "temperature": 0.0}
    messages_permuted = [
        {"content": "you are a fraud triage agent", "role": "system"},
        {"content": "score this transaction", "role": "user"},
    ]
    tools_permuted = [{"parameters": {"properties": {}, "type": "object"}, "name": "lookup"}]

    first = compute_cache_key("claude-opus-4-8", _MESSAGES, _TOOLS, _PARAMS, _KEY, _SEQ)
    second = compute_cache_key(
        "claude-opus-4-8", messages_permuted, tools_permuted, params_permuted, _KEY, _SEQ
    )

    assert first == second
    assert len(first) == 64
    assert first == first.lower()


def test_every_component_perturbs_the_key() -> None:
    # Scenario: Every component perturbs the key.
    baseline = _baseline()
    perturbed = [
        compute_cache_key("claude-sonnet-5", _MESSAGES, _TOOLS, _PARAMS, _KEY, _SEQ),
        compute_cache_key(
            "claude-opus-4-8",
            [*_MESSAGES, {"role": "user", "content": "extra"}],
            _TOOLS,
            _PARAMS,
            _KEY,
            _SEQ,
        ),
        compute_cache_key("claude-opus-4-8", _MESSAGES, [], _PARAMS, _KEY, _SEQ),
        compute_cache_key("claude-opus-4-8", _MESSAGES, _TOOLS, {"temperature": 0.7}, _KEY, _SEQ),
        compute_cache_key("claude-opus-4-8", _MESSAGES, _TOOLS, _PARAMS, b"entity-2", _SEQ),
        compute_cache_key("claude-opus-4-8", _MESSAGES, _TOOLS, _PARAMS, _KEY, _SEQ + 1),
    ]

    for key in perturbed:
        assert key != baseline
    # All six perturbations are distinct from one another too.
    assert len(set(perturbed)) == len(perturbed)


def test_key_is_sha256_of_canonical_utf8_json() -> None:
    # The key is the sha256 hex digest of the canonical JSON document. Rebuild
    # that document independently and confirm the digest matches, pinning the
    # canonical form (sorted keys, compact separators, hex-encoded entity_key).
    canonical = json.dumps(
        {
            "model": "claude-opus-4-8",
            "messages": _MESSAGES,
            "tools": _TOOLS,
            "params": _PARAMS,
            "key": _KEY.hex(),
            "seq": _SEQ,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert _baseline() == expected


def test_non_serializable_input_is_rejected() -> None:
    # Scenario: Non-canonical input is rejected loudly (non-serializable object).
    with pytest.raises(TypeError):
        compute_cache_key("claude-opus-4-8", {"obj": object()}, _TOOLS, _PARAMS, _KEY, _SEQ)


def test_nan_sampling_param_is_rejected() -> None:
    # Scenario: Non-canonical input is rejected loudly (NaN has no canonical form).
    with pytest.raises(ValueError):
        compute_cache_key(
            "claude-opus-4-8", _MESSAGES, _TOOLS, {"temperature": float("nan")}, _KEY, _SEQ
        )
