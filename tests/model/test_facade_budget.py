"""`LlmFacade` budget enforcement (`token-budgets` capability).

The facade is one of the two model-call surfaces the budget has to hold on. The
load-bearing behaviors pinned here: the charge lands after decode (so the
crossing call is the one that raises), a replay-cache hit charges exactly what
the miss that stored it charged (while billing nothing), the entry check
precedes both the cache lookup and the breaker, and the transport retry loop
never sees a trip.

Named ``test_facade_budget`` rather than folded into ``test_facade.py``: the
model-facade suite is split one file per concern (`test_facade_cache`,
`test_facade_retry`, ...) and this is one more.
"""

from __future__ import annotations

import pytest

from beam_agents._protos import TraceEvent
from beam_agents.model import (
    BudgetExceeded,
    DecodedResponse,
    ReplayCache,
    ReplayEntry,
    TokenBudget,
    TokenUsage,
)
from beam_agents.model.client import LlmRequest, ProviderError
from beam_agents.model.facade import CircuitBreaker, CircuitOpenError, LlmFacade, RetryPolicy
from beam_agents.model.fake import FakeLLM, match_any, raise_error, respond_with
from tests.model._facade_helpers import MaxJitterRandom, RecordingSleep, RecordingStaging

_KEY = b"k"
_SEQ = 0
_NOW_MS = 1_000_000


def decode_counted(response: bytes) -> DecodedResponse:
    """Decode whose token counts are the integer the response payload spells.

    Lets a scenario say "a call that decodes to 250 tokens" literally, instead
    of deriving it from a payload length. The split is deterministic so the
    prompt/completion cost metrics can be asserted on the same responses.
    """
    total = int(response)
    prompt = total // 2
    return DecodedResponse(
        usage=TokenUsage(
            prompt_tokens=prompt, completion_tokens=total - prompt, total_tokens=total
        ),
        text=response.decode("utf-8"),
    )


def _facade(
    provider: FakeLLM,
    *,
    budget: TokenBudget | None,
    replay_cache: ReplayCache | None = None,
    breaker: CircuitBreaker | None = None,
    sleep: RecordingSleep | None = None,
    retry_policy: RetryPolicy | None = None,
) -> tuple[LlmFacade, RecordingStaging]:
    staging = RecordingStaging()
    facade = LlmFacade(
        provider,
        replay_cache if replay_cache is not None else ReplayCache(now_ms=_NOW_MS),
        now_ms=_NOW_MS,
        rng=MaxJitterRandom(0),
        sleep=sleep if sleep is not None else RecordingSleep(),
        breaker=breaker
        if breaker is not None
        else CircuitBreaker(endpoint="test", threshold=1_000, cooldown_ms=1_000),
        retry_policy=retry_policy
        if retry_policy is not None
        else RetryPolicy(max_attempts=3, base_ms=100, max_ms=1_000),
        decode=decode_counted,
        staging=staging,
        budget=budget,
    )
    return facade, staging


def _request(message: str) -> LlmRequest:
    return LlmRequest(model_id="m", messages=[message], tools_schema=None, sampling_params=None)


async def _complete(facade: LlmFacade, message: str, step_index: int = 0) -> object:
    return await facade.complete(
        _request(message), entity_key=_KEY, seq=_SEQ, step_index=step_index
    )


def _provider() -> FakeLLM:
    return FakeLLM([(match_any(), respond_with(b"250"))])


# --- Requirement: The crossing call trips the budget --------------------------


async def test_the_crossing_call_fails_fast() -> None:
    # Scenario: The crossing call fails fast. Three 250-token responses against
    # a 600-token budget: the third crosses, raises after its response is
    # decoded and charged, and never returns a `FacadeResult` to the agent.
    budget = TokenBudget(600)
    facade, staging = _facade(_provider(), budget=budget)

    await _complete(facade, "a")
    await _complete(facade, "b")
    with pytest.raises(BudgetExceeded) as excinfo:
        await _complete(facade, "c")

    assert excinfo.value.consumed == 750
    assert excinfo.value.limit == 600
    # Billed usage still accumulated for the crossing call: the provider was
    # reached and the tokens were spent, whatever the runtime does next.
    assert [usage.total_tokens for usage in staging.usages] == [250, 250, 250]


async def test_the_crossing_call_stages_its_llm_call_trace_with_the_error_type() -> None:
    # Design D3: the trip stages the call's own LLM_CALL event first, exactly as
    # `OutputSchemaError` does, with the usage attributes present. On the normal
    # fail-fast path it is discarded with everything else; it becomes visible
    # only when the agent swallows the trip and completes -- and then it is the
    # one record showing precisely where the budget died.
    facade, staging = _facade(_provider(), budget=TokenBudget(100))

    with pytest.raises(BudgetExceeded):
        await _complete(facade, "a")

    (event,) = staging.trace_events
    assert event.event_type == TraceEvent.LLM_CALL
    assert event.attributes["error.type"] == "BudgetExceeded"
    assert event.attributes["gen_ai.usage.input_tokens"] == "125"
    assert event.attributes["gen_ai.usage.output_tokens"] == "125"


async def test_a_cache_hit_charges_the_same_as_the_miss_that_stored_it() -> None:
    # Scenario: A cache hit charges the same as the miss that stored it. The
    # budget meters consumption, so a hit charges its stored response's decoded
    # total; billed accounting meters spend, so the hit contributes none. The
    # two are allowed to disagree -- that disagreement IS the replay cache.
    provider = _provider()
    budget = TokenBudget(10_000)
    facade, staging = _facade(provider, budget=budget)

    await _complete(facade, "same")
    await _complete(facade, "same")

    assert provider.call_count == 1
    assert budget.consumed == 500
    assert [usage.total_tokens for usage in staging.usages] == [250]
    cache_hits = [e.attributes["beam_agents.cache_hit"] for e in staging.trace_events]
    assert cache_hits == ["false", "true"]


async def test_a_cache_hit_can_be_the_crossing_call() -> None:
    # The corollary of charging hits: a replayed walk trips at exactly the same
    # point the original did, without reaching the provider at all.
    provider = _provider()
    budget = TokenBudget(400)
    facade, _ = _facade(provider, budget=budget)

    await _complete(facade, "same")
    with pytest.raises(BudgetExceeded):
        await _complete(facade, "same")

    assert provider.call_count == 1
    assert budget.consumed == 500


async def test_a_swallowed_trip_cannot_spend_again() -> None:
    # Scenario: A swallowed trip cannot spend again. The post-trip call raises
    # at entry -- before the cache lookup, before the breaker, before the
    # provider -- so a spent budget serves nothing, not even a free hit.
    provider = _provider()
    breaker = _CountingBreaker(endpoint="test", threshold=1_000, cooldown_ms=1_000)
    cache = _CountingCache(now_ms=_NOW_MS)
    facade, staging = _facade(
        provider, budget=TokenBudget(100), breaker=breaker, replay_cache=cache
    )

    with pytest.raises(BudgetExceeded):
        await _complete(facade, "a")
    calls_before = provider.call_count
    consults_before = breaker.consulted
    traces_before = len(staging.trace_events)
    reads_before = cache.reads

    # ...including one the cache could serve: the first call's response was
    # stored before the trip, so this repeat is a live hit.
    with pytest.raises(BudgetExceeded):
        await _complete(facade, "a")

    assert provider.call_count == calls_before
    assert breaker.consulted == consults_before
    assert cache.reads == reads_before
    # No trace either: the entry check is upstream of everything the call would
    # otherwise stage.
    assert len(staging.trace_events) == traces_before


async def test_a_tripped_budget_is_never_retried_as_a_transport_failure() -> None:
    # Scenario: A tripped budget is never retried as a transport failure. The
    # retry loop classifies by class; `BudgetExceeded` is deliberately not a
    # `ProviderError`, so no backoff sleep and no second attempt happen.
    provider = _provider()
    sleep = RecordingSleep()
    facade, _ = _facade(provider, budget=TokenBudget(100), sleep=sleep)

    with pytest.raises(BudgetExceeded):
        await _complete(facade, "a")

    assert provider.call_count == 1
    assert sleep.calls == []


async def test_a_transport_failure_still_retries_with_a_budget_configured() -> None:
    # The converse: configuring a budget must not disturb the retry loop for
    # the errors it does classify as retryable.
    provider = FakeLLM([(match_any(), raise_error(ProviderError("boom")))])
    sleep = RecordingSleep()
    facade, _ = _facade(provider, budget=TokenBudget(10_000), sleep=sleep)

    with pytest.raises(ProviderError):
        await _complete(facade, "a")

    assert provider.call_count == 3
    assert len(sleep.calls) == 2


async def test_the_entry_check_precedes_the_breaker() -> None:
    # Budget state, like cache state, must not depend on endpoint health: a
    # spent budget raises `BudgetExceeded`, not `CircuitOpenError`, even with
    # the breaker already open.
    breaker = CircuitBreaker(endpoint="test", threshold=1, cooldown_ms=1_000)
    breaker.record_failure(_NOW_MS)
    budget = TokenBudget(100)
    with pytest.raises(BudgetExceeded):
        budget.charge(101)
    facade, _ = _facade(_provider(), budget=budget, breaker=breaker)

    with pytest.raises(BudgetExceeded):
        await _complete(facade, "a")

    # ...and without the budget, the same facade would have raised the breaker's
    # error instead -- so the ordering above is a real precedence, not a
    # coincidence of an unconsulted breaker.
    unbudgeted, _ = _facade(_provider(), budget=None, breaker=breaker)
    with pytest.raises(CircuitOpenError):
        await _complete(unbudgeted, "a")


async def test_an_unbudgeted_facade_is_unchanged() -> None:
    # Scenario: Unset means unlimited. `budget=None` is the default and every
    # pre-existing facade behavior must be byte-identical under it.
    provider = _provider()
    facade, staging = _facade(provider, budget=None)

    first = await _complete(facade, "a")
    second = await _complete(facade, "a")

    assert first.response.response == b"250"  # type: ignore[attr-defined]
    assert second.cache_hit is True  # type: ignore[attr-defined]
    assert provider.call_count == 1
    assert [usage.total_tokens for usage in staging.usages] == [250]


class _CountingBreaker(CircuitBreaker):
    """Breaker that records how many times `before_call` was consulted."""

    def __init__(self, *, endpoint: str, threshold: int, cooldown_ms: int) -> None:
        super().__init__(endpoint=endpoint, threshold=threshold, cooldown_ms=cooldown_ms)
        self.consulted = 0

    def before_call(self, now_ms: int) -> None:
        self.consulted += 1
        super().before_call(now_ms)


class _CountingCache(ReplayCache):
    """Replay cache that records how many lookups it served.

    "No cache read" is the claim; `ReplayCache` exposes no counter of its own,
    and inferring it from the serialized blob would confuse "was not read" with
    "was read and unchanged".
    """

    def __init__(self, *, now_ms: int) -> None:
        super().__init__(now_ms=now_ms)
        self.reads = 0

    def get(self, cache_key: str) -> ReplayEntry | None:
        self.reads += 1
        return super().get(cache_key)
