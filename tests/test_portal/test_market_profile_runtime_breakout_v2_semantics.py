from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state.plugins import market_profile as mp_plugin


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 8, 15, hour, minute, tzinfo=timezone.utc)


def _profile(*, start: datetime, end: datetime, vah: float, val: float, poc: float) -> Dict[str, Any]:
    return {
        "start": int(start.timestamp()),
        "end": int(end.timestamp()),
        "known_at": int(end.timestamp()),
        "VAH": vah,
        "VAL": val,
        "POC": poc,
        "session_count": 1,
        "precision": 2,
    }


def _payload(
    *,
    profiles: List[Dict[str, Any]],
    runtime_scope: str = "repro|scope|30m",
    source_timeframe: str = "30m",
    chart_timeframe: str = "1h",
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "_indicator_id": "ind-1",
        "_runtime_scope": runtime_scope,
        "symbol": "BIP-20DEC30-CDE",
        "profiles": profiles,
        "profile_params": {
            "use_merged_value_areas": False,
            "extend_value_area_to_chart_end": True,
            "market_profile_breakout_v2_confirm_bars": 2,
            "market_profile_breakout_v2_lockout_bars": 2,
            "market_profile_retest_v2_min_bars": 1,
            "market_profile_retest_v2_max_lookback": 20,
            **(params or {}),
        },
        "source_timeframe": source_timeframe,
        "chart_timeframe": chart_timeframe,
        "source_timeframe_seconds": 1800,
        "chart_timeframe_seconds": 3600,
    }


def _candle(*, ts: datetime, open_: float, close: float) -> Candle:
    high = max(open_, close) + 0.5
    low = min(open_, close) - 0.5
    return Candle(time=ts, open=open_, high=high, low=low, close=close, volume=1.0)


def _run_sequence(payload: Dict[str, Any], candles: List[Candle]) -> List[Dict[str, Any]]:
    previous = None
    outputs: List[Dict[str, Any]] = []
    for candle in candles:
        out = mp_plugin.market_profile_rule_payload(
            snapshot_payload=payload,
            candle=candle,
            previous_candle=previous,
        )
        outputs.append(out)
        previous = candle
    return outputs


def _runtime_key(payload: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(payload.get("_indicator_id") or ""),
        str(payload.get("symbol") or ""),
        str(payload.get("_runtime_scope") or ""),
    )


def setup_function() -> None:
    mp_plugin._PROFILE_RESOLUTION_CACHE.clear()
    mp_plugin._PROFILE_KNOWN_EPOCHS_CACHE.clear()
    mp_plugin._PROFILE_RUNTIME_VIEW_CACHE.clear()
    mp_plugin._MP_RUNTIME_STATE.clear()


def test_breakout_requires_full_body_streak_and_not_cross_transition() -> None:
    profiles = [_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, poc=95.0)]
    payload = _payload(profiles=profiles, params={"market_profile_breakout_v2_confirm_bars": 2, "market_profile_breakout_v2_lockout_bars": 0})
    candles = [
        _candle(ts=_dt(1, 30), open_=100.2, close=100.3),  # first full body above VAH, wick touches boundary
        _candle(ts=_dt(2, 0), open_=100.25, close=100.35), # second full body above VAH => emit
        _candle(ts=_dt(2, 30), open_=99.8, close=99.7),    # reset below VAH
        _candle(ts=_dt(3, 0), open_=100.2, close=100.25),  # first full body above again
        _candle(ts=_dt(3, 30), open_=100.3, close=100.4),  # second full body above => emit
    ]
    outputs = _run_sequence(payload, candles)

    assert len(outputs[0]["signals"]) == 0
    assert len(outputs[1]["signals"]) == 1
    assert len(outputs[2]["signals"]) == 0
    assert len(outputs[3]["signals"]) == 0
    assert len(outputs[4]["signals"]) == 1
    signal = outputs[1]["signals"][0]
    assert signal["breakout_direction"] == "above"
    assert signal["streak_count"] == 2
    assert signal["run_length"] == 2


def test_deterministic_single_profile_selected_per_bar() -> None:
    profiles = [
        _profile(start=_dt(0), end=_dt(1), vah=105.0, val=95.0, poc=109.0),
        _profile(start=_dt(0), end=_dt(1), vah=108.0, val=98.0, poc=150.0),
    ]
    payload = _payload(profiles=profiles, params={"market_profile_breakout_v2_confirm_bars": 1, "market_profile_breakout_v2_lockout_bars": 0})
    candles = [
        _candle(ts=_dt(1, 30), open_=105.2, close=105.3),
    ]
    outputs = _run_sequence(payload, candles)
    assert len(outputs[0]["signals"]) == 1
    signal = outputs[0]["signals"][0]
    assert signal["candidate_count"] == 2
    assert signal["chosen_profile_key"] == signal["value_area_id"]
    assert signal["selection_reason"] in {"in_or_cross", "nearby_threshold"}


def test_time_lockout_survives_state_reload() -> None:
    profiles = [_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, poc=95.0)]
    payload = _payload(profiles=profiles, params={"market_profile_breakout_v2_confirm_bars": 1, "market_profile_breakout_v2_lockout_bars": 2})
    runtime_key = _runtime_key(payload)

    c1 = _candle(ts=_dt(1, 30), open_=100.2, close=100.3)  # emit
    out1 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c1, previous_candle=None)
    assert len(out1["signals"]) == 1

    # Simulate process restart with state reload.
    saved_state = dict(mp_plugin._MP_RUNTIME_STATE[runtime_key])
    mp_plugin._MP_RUNTIME_STATE.clear()
    mp_plugin._MP_RUNTIME_STATE[runtime_key] = saved_state

    c2 = _candle(ts=_dt(2, 0), open_=99.8, close=99.7)     # reset
    c3 = _candle(ts=_dt(2, 30), open_=100.25, close=100.35)  # +60m from first emit (locked out; chart timeframe=1h)
    c4 = _candle(ts=_dt(3, 0), open_=99.8, close=99.75)    # reset
    c5 = _candle(ts=_dt(3, 30), open_=100.2, close=100.3)  # +120m from first emit (still locked, strict inequality)
    c6 = _candle(ts=_dt(4, 30), open_=100.2, close=100.3)  # +180m from first emit (allowed)

    out2 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c2, previous_candle=c1)
    out3 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c3, previous_candle=c2)
    out4 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c4, previous_candle=c3)
    out5 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c5, previous_candle=c4)
    out6 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c6, previous_candle=c5)

    assert len(out2["signals"]) == 0
    assert len(out3["signals"]) == 0
    assert len(out4["signals"]) == 0
    assert len(out5["signals"]) == 0
    assert len(out6["signals"]) == 1


def test_timeframe_mismatch_diagnostic_is_emitted() -> None:
    profiles = [_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, poc=95.0)]
    payload = _payload(
        profiles=profiles,
        source_timeframe="30m",
        chart_timeframe="1h",
        params={"market_profile_breakout_v2_confirm_bars": 3},
    )
    candle = _candle(ts=_dt(1, 30), open_=95.0, close=96.0)
    out = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=candle, previous_candle=None)
    diagnostics = out.get("diagnostics") or {}
    assert int(diagnostics.get("source_timeframe_seconds") or 0) == 3600
    assert int(diagnostics.get("lockout_timeframe_seconds") or 0) == 3600
    assert int(diagnostics.get("timeframe_mismatch_warning") or 0) == 0


def test_inside_value_area_body_does_not_emit_breakout() -> None:
    profiles = [_profile(start=_dt(0), end=_dt(1), vah=110.0, val=100.0, poc=105.0)]
    payload = _payload(
        profiles=profiles,
        params={"market_profile_breakout_v2_confirm_bars": 1, "market_profile_breakout_v2_lockout_bars": 0},
    )
    c1 = _candle(ts=_dt(1, 30), open_=102.0, close=108.0)  # fully inside VA
    out = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c1, previous_candle=None)
    assert len(out["signals"]) == 0


def test_default_lockout_bars_is_one() -> None:
    profiles = [_profile(start=_dt(0), end=_dt(1), vah=110.0, val=100.0, poc=105.0)]
    payload = _payload(
        profiles=profiles,
        params={"market_profile_breakout_v2_confirm_bars": 1},
    )
    c1 = _candle(ts=_dt(1, 30), open_=110.2, close=110.3)  # emit above
    out1 = mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=c1, previous_candle=None)
    assert len(out1["signals"]) == 1
    signal = out1["signals"][0]
    assert int(signal["lockout_bars"]) == 1


def test_candidate_selection_prefers_nearest_boundary_over_poc() -> None:
    runtime_views = [
        {
            "profile": object(),
            "vah": 117095.0,
            "val": 112150.0,
            "poc": 114469.28571428571,
            "profile_key": "near-boundary",
            "known_epoch": int(_dt(1).timestamp()),
            "end_epoch": int(_dt(1).timestamp()),
        },
        {
            "profile": object(),
            "vah": 114170.0,
            "val": 107610.0,
            "poc": 110523.07692307692,
            "profile_key": "near-poc",
            "known_epoch": int(_dt(1).timestamp()),
            "end_epoch": int(_dt(1).timestamp()),
        },
    ]
    candidates = mp_plugin._select_nearby_profiles(
        runtime_views=runtime_views,
        bar_epoch=int(_dt(20).timestamp()),
        extend_to_end=True,
        close_price=112460.0,
        previous_close=112525.0,
        candle_low=112085.0,
        candle_high=112660.0,
        nearby_threshold_pct=0.35,
    )
    assert len(candidates) == 2
    chosen = mp_plugin._select_single_candidate_profile(candidates)
    assert chosen is not None
    assert str(chosen["profile_key"]) == "near-boundary"
