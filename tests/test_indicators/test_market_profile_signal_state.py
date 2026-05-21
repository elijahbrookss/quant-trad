from __future__ import annotations

from datetime import datetime, timezone

from indicators.market_profile.runtime.models import MarketProfileBarState
from indicators.market_profile.runtime.signal_state import BreakoutRetestStateMachine


def _state(
    *,
    hour: int,
    profile_key: str = "profile-a",
    previous_location: str | None,
    location: str,
    open: float,
    high: float,
    low: float,
    close: float,
    vah: float = 101.0,
    val: float = 99.0,
) -> MarketProfileBarState:
    return MarketProfileBarState(
        bar_time=datetime(2026, 1, 1, hour, 0, tzinfo=timezone.utc),
        active_profile_key=profile_key,
        previous_location=previous_location,
        location=location,
        balance_state="balanced" if location == "inside_value" else "imbalanced",
        open=open,
        high=high,
        low=low,
        close=close,
        val=val,
        vah=vah,
        poc=100.0,
        precision=2,
    )


def _machine(**overrides) -> BreakoutRetestStateMachine:
    params = {
        "breakout_confirm_bars": 2,
        "reclaim_max_bars": 4,
        "retest_min_acceptance_bars": 2,
        "retest_min_excursion_atr": 1.0,
        "retest_max_bars": 4,
        "retest_atr_period": 3,
        "retest_touch_tolerance_atr": 0.25,
        "retest_max_penetration_atr": 0.35,
        "retest_hold_confirm_bars": 1,
    }
    params.update(overrides)
    return BreakoutRetestStateMachine(**params)


def test_breakout_state_machine_emits_long_confirm_then_reclaim() -> None:
    machine = _machine()

    states = [
        _state(hour=0, previous_location=None, location="inside_value", open=100.0, high=100.8, low=99.5, close=100.0),
        _state(hour=1, previous_location="inside_value", location="above_value", open=100.2, high=104.6, low=100.0, close=104.0),
        _state(hour=2, previous_location="above_value", location="above_value", open=104.0, high=104.4, low=102.8, close=103.5),
        _state(hour=3, previous_location="above_value", location="inside_value", open=103.2, high=103.4, low=100.3, close=100.5),
        _state(hour=4, previous_location="inside_value", location="above_value", open=100.6, high=103.2, low=100.4, close=103.0),
    ]

    emitted = [machine.step(state) for state in states]

    assert emitted[1]["confirmed_balance_breakout"] == []
    confirmed = emitted[2]["confirmed_balance_breakout"][0]
    reclaim = emitted[4]["balance_reclaim"][0]

    assert confirmed["key"] == "confirmed_balance_breakout_long"
    assert confirmed["direction"] == "long"
    assert confirmed["metadata"]["reference"]["name"] == "VAH"
    assert confirmed["metadata"]["breakout_time"] == int(states[1].bar_time.timestamp())
    assert reclaim["key"] == "balance_reclaim_long"
    assert reclaim["pattern_id"] == confirmed["pattern_id"]
    assert reclaim["metadata"]["reclaim_touch_time"] == int(states[3].bar_time.timestamp())
    assert reclaim["metadata"]["reference"]["price"] == 101.0


def test_breakout_state_machine_emits_short_confirm_then_reclaim() -> None:
    machine = _machine()

    states = [
        _state(hour=0, previous_location=None, location="inside_value", open=100.0, high=100.5, low=99.3, close=100.0),
        _state(hour=1, previous_location="inside_value", location="below_value", open=99.8, high=100.0, low=95.4, close=96.0),
        _state(hour=2, previous_location="below_value", location="below_value", open=96.2, high=97.1, low=95.9, close=97.0),
        _state(hour=3, previous_location="below_value", location="inside_value", open=97.2, high=99.7, low=96.8, close=99.5),
        _state(hour=4, previous_location="inside_value", location="below_value", open=99.2, high=99.4, low=97.1, close=97.5),
    ]

    emitted = [machine.step(state) for state in states]

    confirmed = emitted[2]["confirmed_balance_breakout"][0]
    reclaim = emitted[4]["balance_reclaim"][0]

    assert confirmed["key"] == "confirmed_balance_breakout_short"
    assert confirmed["direction"] == "short"
    assert confirmed["metadata"]["reference"]["name"] == "VAL"
    assert reclaim["key"] == "balance_reclaim_short"
    assert reclaim["pattern_id"] == confirmed["pattern_id"]
    assert reclaim["metadata"]["reference"]["price"] == 99.0


def test_breakout_state_machine_emits_structural_long_retest_after_acceptance() -> None:
    machine = _machine()

    states = [
        _state(hour=0, previous_location=None, location="inside_value", open=100.0, high=100.8, low=99.8, close=100.1),
        _state(hour=1, previous_location="inside_value", location="above_value", open=100.3, high=103.8, low=100.1, close=103.2),
        _state(hour=2, previous_location="above_value", location="above_value", open=103.1, high=104.0, low=102.6, close=103.5),
        _state(hour=3, previous_location="above_value", location="above_value", open=103.7, high=106.5, low=103.2, close=105.8),
        _state(hour=4, previous_location="above_value", location="above_value", open=105.7, high=107.0, low=104.9, close=106.1),
        _state(hour=5, previous_location="above_value", location="above_value", open=106.0, high=106.4, low=100.7, close=101.6),
    ]

    emitted = [machine.step(state) for state in states]

    confirmed = emitted[2]["confirmed_balance_breakout"][0]
    retest = emitted[5]["balance_retest"][0]

    assert emitted[5]["balance_reclaim"] == []
    assert retest["key"] == "balance_retest_long"
    assert retest["pattern_id"] == confirmed["pattern_id"]
    assert retest["metadata"]["acceptance_time"] == int(states[4].bar_time.timestamp())
    assert retest["metadata"]["retest_touch_time"] == int(states[5].bar_time.timestamp())
    assert retest["metadata"]["hold_confirm_price"] == states[5].close
    assert retest["metadata"]["max_excursion_from_reference"] > 1.0
    assert retest["metadata"]["retest_touch_penetration"] > 0.0


def test_breakout_state_machine_rejects_deep_pullback_as_structural_retest() -> None:
    machine = _machine()

    states = [
        _state(hour=0, previous_location=None, location="inside_value", open=100.0, high=100.8, low=99.8, close=100.1),
        _state(hour=1, previous_location="inside_value", location="above_value", open=100.3, high=103.8, low=100.1, close=103.2),
        _state(hour=2, previous_location="above_value", location="above_value", open=103.1, high=104.0, low=102.6, close=103.5),
        _state(hour=3, previous_location="above_value", location="above_value", open=103.7, high=106.5, low=103.2, close=105.8),
        _state(hour=4, previous_location="above_value", location="above_value", open=105.7, high=107.0, low=104.9, close=106.1),
        _state(hour=5, previous_location="above_value", location="inside_value", open=106.0, high=106.2, low=99.0, close=100.4),
    ]

    emitted = [machine.step(state) for state in states]

    assert emitted[5]["balance_retest"] == []
    assert emitted[5]["balance_reclaim"] == []


def test_breakout_state_machine_times_out_reclaim_and_allows_new_sequence() -> None:
    machine = _machine(reclaim_max_bars=1)

    states = [
        _state(hour=0, previous_location=None, location="inside_value", open=100.0, high=100.8, low=99.8, close=100.0),
        _state(hour=1, previous_location="inside_value", location="above_value", open=100.2, high=104.6, low=100.0, close=104.0),
        _state(hour=2, previous_location="above_value", location="above_value", open=104.0, high=104.4, low=102.8, close=103.5),
        _state(hour=3, previous_location="above_value", location="above_value", open=103.3, high=103.8, low=102.7, close=103.0),
        _state(hour=4, previous_location="above_value", location="inside_value", open=102.8, high=103.1, low=100.4, close=100.5),
        _state(hour=5, previous_location="inside_value", location="above_value", open=100.6, high=103.7, low=100.5, close=103.5),
        _state(hour=6, previous_location="above_value", location="above_value", open=103.6, high=104.2, low=103.1, close=104.0),
    ]

    emitted = [machine.step(state) for state in states]

    first_confirm = emitted[2]["confirmed_balance_breakout"][0]
    second_confirm = emitted[6]["confirmed_balance_breakout"][0]

    assert emitted[5]["balance_reclaim"] == []
    assert first_confirm["pattern_id"] != second_confirm["pattern_id"]
    assert second_confirm["metadata"]["breakout_time"] == int(states[5].bar_time.timestamp())


def test_breakout_state_machine_resets_on_profile_change() -> None:
    machine = _machine()

    states = [
        _state(hour=0, profile_key="profile-a", previous_location=None, location="inside_value", open=100.0, high=100.8, low=99.8, close=100.0),
        _state(hour=1, profile_key="profile-a", previous_location="inside_value", location="above_value", open=100.2, high=104.6, low=100.0, close=104.0),
        _state(hour=2, profile_key="profile-a", previous_location="above_value", location="above_value", open=104.0, high=104.4, low=102.8, close=103.5),
        _state(hour=3, profile_key="profile-b", previous_location=None, location="inside_value", open=100.3, high=100.8, low=99.8, close=100.5),
        _state(hour=4, profile_key="profile-b", previous_location="inside_value", location="above_value", open=100.7, high=103.4, low=100.4, close=103.0),
        _state(hour=5, profile_key="profile-b", previous_location="above_value", location="above_value", open=103.1, high=103.9, low=102.7, close=103.5),
    ]

    emitted = [machine.step(state) for state in states]

    first_confirm = emitted[2]["confirmed_balance_breakout"][0]
    second_confirm = emitted[5]["confirmed_balance_breakout"][0]

    assert emitted[4]["balance_reclaim"] == []
    assert emitted[4]["balance_retest"] == []
    assert first_confirm["pattern_id"] != second_confirm["pattern_id"]
    assert second_confirm["metadata"]["reference"]["key"] == "profile-b"
