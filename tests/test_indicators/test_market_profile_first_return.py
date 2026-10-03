from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest

from indicators.market_profile.runtime.first_return import FirstReturnState
from indicators.market_profile.runtime.signals import build_signal_outputs
from indicators.registry import get_indicator_manifest
from indicators.manifest import serialize_indicator_manifest
from tests.test_indicators.test_market_profile_signal_state import _state


def bar(i, close=100, previous="inside_value", profile="a", **kw):
    value = _state(hour=i, profile_key=profile, previous_location=previous,
                   location="above_value" if close > 101 else "below_value" if close < 99 else "inside_value",
                   open=100, high=max(102, close), low=min(98, close), close=close, **kw)
    return replace(value, known_at=value.bar_time+timedelta(hours=1))


def test_strict_first_return_keeps_original_range_profile_and_prior_atr():
    machine = FirstReturnState(atr_period=2, lifetime_bars=6)
    machine.step(bar(0)); machine.step(bar(1))
    origin = bar(2, 103)
    assert not machine.step(origin)[0]
    assert not machine.step(bar(3, 101, previous="above_value"))[0]  # equality is not return
    events, context = machine.step(bar(4, 100.5, profile="changed", vah=150, val=130))
    event = events[0]
    assert event["metadata"]["reference"] == build_signal_outputs(origin)["balance_breakout"].value["events"][0]["metadata"]["reference"]
    assert event["known_at"] == bar(4).known_at.timestamp()
    assert event["metadata"]["prior_atr"] == pytest.approx(4.75)
    assert context["fields"]["origins"][0]["status"] == "returned"
    assert not machine.step(bar(5))[0]


def test_repeated_origins_are_not_overwritten_and_prefix_is_immutable():
    machine = FirstReturnState(atr_period=1, lifetime_bars=3)
    machine.step(bar(0))
    saved = machine.step(bar(1, 103))[1]
    before = deepcopy(saved)
    machine.step(bar(2, 104))
    events, ctx = machine.step(bar(3, 100))
    assert len(events) == 2
    assert {e["metadata"]["breakout_time"] for e in events} == {bar(1).bar_time.timestamp(), bar(2).bar_time.timestamp()}
    assert saved == before
    machine.step(bar(4)); _, ctx = machine.step(bar(5))
    assert all(o["age_bars"] <= 4 for o in ctx["fields"]["origins"])


def test_gap_censors_pending_origin_and_rewarms_prior_atr():
    machine = FirstReturnState(atr_period=2, lifetime_bars=6)
    machine.step(bar(0)); machine.step(bar(1, 103))
    machine.interrupt("candle_gap")
    events, ctx = machine.step(bar(2, 100))
    assert not events and not ctx["fields"]["prior_atr_ready"]
    assert ctx["fields"]["origins"][0]["reason"] == "candle_gap"


def test_current_bar_volatility_cannot_change_prior_atr():
    a, b = (FirstReturnState(atr_period=1, lifetime_bars=6) for _ in range(2))
    for m in (a,b): m.step(bar(0)); m.step(bar(1, 103))
    normal = a.step(bar(2,100))[1]
    extreme = b.step(replace(bar(2,100), high=10000))[1]
    assert normal["fields"]["prior_atr"] == extreme["fields"]["prior_atr"]


def test_short_return_and_invalid_range_and_unready_atr_remain_explicit():
    machine = FirstReturnState(atr_period=14, lifetime_bars=6)
    machine.step(bar(0,97))
    events, ctx = machine.step(bar(1,99.5))
    assert events[0]["key"] == "first_value_return_short"
    assert events[0]["metadata"]["prior_atr"] is None
    bad = FirstReturnState(atr_period=1, lifetime_bars=6)
    bad.step(bar(0,103,val=102,vah=101))
    assert bad.step(bar(1))[1]["fields"]["origins"][0]["status"] == "censored"


def test_v1_manifest_is_default_and_v2_is_explicit():
    old = get_indicator_manifest("market_profile")
    assert old == get_indicator_manifest("market_profile", "v1")
    new = get_indicator_manifest("market_profile", "v2")
    assert new.outputs[:-2] == old.outputs
    assert "first_value_return" not in {o.name for o in old.outputs}
    assert serialize_indicator_manifest(new)["version"] == "v2"
    with pytest.raises(ValueError, match="version_unsupported"):
        get_indicator_manifest("market_profile", "guessed")


def test_v2_engine_snapshot_preserves_all_legacy_outputs():
    from indicators.market_profile.runtime.typed_indicator import TypedMarketProfileIndicator
    from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
    from engines.bot_runtime.core.domain import Candle
    from tests.test_indicator_engine_overlays import _market_profile
    old = _market_profile()
    new = TypedMarketProfileIndicator(indicator_id="profile-1", version="v2",
        params={"bin_size":1,"price_precision":2}, source_facts={
            "symbol":"TEST","profile_params":{"use_merged_value_areas":False,"extend_value_area_to_chart_end":True},
            "profiles":deepcopy(old._profiles_payload)})
    engines = [IndicatorExecutionEngine([old]), IndicatorExecutionEngine([new])]
    for i,price in enumerate([100,110,100,110,111,112,99]):
        state=bar(i)
        candle=Candle(time=state.bar_time, end=state.known_at, open=price, high=price+1, low=price-1, close=price, volume=10)
        frames=[e.step(bar=candle,bar_time=candle.time,include_overlays=False) for e in engines]
        legacy,newer=frames[0].outputs,frames[1].outputs
        assert all(newer[key] == value for key,value in legacy.items())
        assert len(newer)==len(legacy)+2
        if i==2:
            assert newer["profile-1.first_value_return"].value["events"][0]["key"]=="first_value_return_long"
