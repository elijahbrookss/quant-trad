from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state.plugins import market_profile as mp_plugin


def _dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 8, 15, hour, minute, tzinfo=timezone.utc)


def _profile(
    *,
    start: datetime,
    end: datetime,
    vah: float,
    val: float,
    poc: float = 0.0,
    session_count: int = 1,
) -> Dict[str, Any]:
    return {
        "start": int(start.timestamp()),
        "end": int(end.timestamp()),
        "known_at": int(end.timestamp()),
        "VAH": vah,
        "VAL": val,
        "POC": poc,
        "session_count": session_count,
        "precision": 2,
    }


def _payload(
    *,
    profiles: List[Dict[str, Any]],
    runtime_scope: str = "repro|scope|30m",
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "_indicator_id": "ind-v3",
        "_runtime_scope": runtime_scope,
        "symbol": "BIP-20DEC30-CDE",
        "profiles": profiles,
        "profile_params": {
            "use_merged_value_areas": False,
            "extend_value_area_to_chart_end": True,
            "market_profile_breakout_v3_confirm_bars": 1,
            "market_profile_breakout_v3_lockout_bars": 0,
            **(params or {}),
        },
        "source_timeframe": "30m",
        "chart_timeframe": "1h",
        "source_timeframe_seconds": 1800,
        "chart_timeframe_seconds": 3600,
    }


def _candle(*, ts: datetime, open_: float, close: float) -> Candle:
    high = max(open_, close) + 0.5
    low = min(open_, close) - 0.5
    return Candle(time=ts, open=open_, high=high, low=low, close=close, volume=1.0)


def _eval(payload: Dict[str, Any], candle: Candle, previous: Candle | None) -> Dict[str, Any]:
    return mp_plugin.market_profile_rule_payload(snapshot_payload=payload, candle=candle, previous_candle=previous)


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


@pytest.fixture(autouse=True)
def _stub_runtime_profiles_module() -> None:
    class _Profile:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self.start = datetime.fromtimestamp(int(payload["start"]), tz=timezone.utc)
            self.end = datetime.fromtimestamp(int(payload["end"]), tz=timezone.utc)
            self.vah = float(payload["VAH"])
            self.val = float(payload["VAL"])
            self.poc = float(payload.get("POC") or 0.0)
            self.session_count = int(payload.get("session_count") or 1)

    def _resolve_effective_profiles(*, profiles_payload: List[Dict[str, Any]], current_epoch: int, **_: Any):
        out = []
        for payload in profiles_payload:
            known_at = int(payload.get("known_at") or payload.get("end") or 0)
            if known_at <= int(current_epoch):
                out.append(_Profile(payload))
        return out, {"count": len(out)}

    module = types.SimpleNamespace(resolve_effective_profiles=_resolve_effective_profiles)
    sys.modules["indicators.market_profile._internal.runtime_profiles"] = module


@pytest.mark.parametrize(
    ("variant", "prev_candle", "candidate_candle", "confirm_candle"),
    [
        (
            "breakout_up",
            _candle(ts=_dt(1, 0), open_=95.0, close=99.0),
            _candle(ts=_dt(1, 30), open_=99.0, close=100.4),
            _candle(ts=_dt(2, 0), open_=100.6, close=101.0),
        ),
        (
            "breakout_down",
            _candle(ts=_dt(1, 0), open_=95.5, close=94.5),
            _candle(ts=_dt(1, 30), open_=90.6, close=89.8),
            _candle(ts=_dt(2, 0), open_=89.7, close=89.3),
        ),
        (
            "breakin_from_above",
            _candle(ts=_dt(1, 0), open_=101.8, close=101.2),
            _candle(ts=_dt(1, 30), open_=101.0, close=99.6),
            _candle(ts=_dt(2, 0), open_=99.3, close=99.8),
        ),
        (
            "breakin_from_below",
            _candle(ts=_dt(1, 0), open_=88.8, close=89.2),
            _candle(ts=_dt(1, 30), open_=89.1, close=90.6),
            _candle(ts=_dt(2, 0), open_=94.8, close=95.2),
        ),
    ],
)
def test_breakout_v3_variants_accept_straddle_candidates_only_after_confirm(
    variant: str,
    prev_candle: Candle,
    candidate_candle: Candle,
    confirm_candle: Candle,
) -> None:
    payload = _payload(profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, poc=95.0)])

    out_candidate = _eval(payload, candidate_candle, prev_candle)
    assert out_candidate["signals"] == []

    out_confirm = _eval(payload, confirm_candle, candidate_candle)
    assert len(out_confirm["signals"]) == 1
    signal = out_confirm["signals"][0]
    assert signal["rule_id"] == "market_profile_breakout"
    assert signal["pattern_id"] == "value_area_breakout"
    assert signal["variant"] == variant
    assert signal["confirm_streak_at_emit"] == 1
    assert signal["direction"] in {"long", "short"}
    assert signal["bias"] in {"bullish", "bearish"}
    assert int(signal["signal_time"]) == int(confirm_candle.time.timestamp())
    known_at = signal["known_at"]
    known_epoch = int(known_at.timestamp()) if hasattr(known_at, "timestamp") else int(known_at)
    assert known_epoch <= int(signal["signal_time"])
    assert "entry_time" not in signal
    assert "action_time" not in signal
    assert "fill_time" not in signal
    meta = signal.get("metadata") or {}
    assert meta.get("direction") == signal["direction"]
    assert meta.get("bias") == signal["bias"]


def test_breakout_v3_retest_emits_bias_and_direction() -> None:
    payload = _payload(
        profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0)],
        params={"market_profile_retest_min_bars": 1, "market_profile_retest_max_bars": 20},
    )
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    candidate = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)
    confirm = _candle(ts=_dt(2, 0), open_=100.7, close=101.0)
    retest = _candle(ts=_dt(2, 30), open_=99.9, close=100.2)

    assert _eval(payload, candidate, prev)["signals"] == []
    breakout_out = _eval(payload, confirm, candidate)
    assert any(str(sig.get("rule_id") or "") == "market_profile_breakout" for sig in breakout_out["signals"])

    out = _eval(payload, retest, confirm)
    retests = [sig for sig in out["signals"] if str(sig.get("rule_id") or "") == "market_profile_retest"]
    assert len(retests) == 1
    signal = retests[0]
    assert signal["direction"] in {"long", "short"}
    assert signal["bias"] in {"bullish", "bearish"}
    meta = signal.get("metadata") or {}
    assert meta.get("direction") == signal["direction"]
    assert meta.get("bias") == signal["bias"]


def test_breakout_v3_confirmation_rejects_straddled_body() -> None:
    payload = _payload(profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, poc=95.0)])
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    candidate = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)  # breakout_up candidate
    invalid_confirm = _candle(ts=_dt(2, 0), open_=99.8, close=100.2)  # straddles VAH, not fully above
    after = _candle(ts=_dt(2, 30), open_=100.7, close=101.1)

    out_candidate = _eval(payload, candidate, prev)
    out_invalid = _eval(payload, invalid_confirm, candidate)
    out_after = _eval(payload, after, invalid_confirm)

    assert out_candidate["signals"] == []
    assert out_invalid["signals"] == []
    assert out_after["signals"] == []


def test_breakout_v3_candidate_tiebreak_prefers_merged_profile() -> None:
    profiles = [
        _profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, session_count=1),
        _profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0, session_count=2),
    ]
    payload = _payload(profiles=profiles)
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    candidate = _candle(ts=_dt(1, 30), open_=99.0, close=100.7)
    confirm = _candle(ts=_dt(2, 0), open_=100.8, close=101.0)

    _eval(payload, candidate, prev)
    out_confirm = _eval(payload, confirm, candidate)
    signal = out_confirm["signals"][0]
    assert signal["profile_key"].endswith(":2")


def test_breakout_v3_lockout_blocks_restart_and_reemit() -> None:
    payload = _payload(
        profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0)],
        params={"market_profile_breakout_v3_lockout_bars": 2},
    )
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    c1 = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)   # candidate
    c2 = _candle(ts=_dt(2, 0), open_=100.7, close=101.0)   # emit
    c3 = _candle(ts=_dt(2, 30), open_=99.2, close=99.6)    # reset
    c4 = _candle(ts=_dt(3, 0), open_=99.0, close=100.7)    # candidate blocked by lockout
    c5 = _candle(ts=_dt(3, 30), open_=100.7, close=101.0)  # no emit
    c6 = _candle(ts=_dt(4, 30), open_=99.2, close=99.7)    # reset inside
    c7 = _candle(ts=_dt(5, 0), open_=99.0, close=100.8)    # candidate allowed
    c8 = _candle(ts=_dt(5, 30), open_=100.8, close=101.1)  # emit

    def _breakouts(out: dict) -> list[dict]:
        return [s for s in out.get("signals", []) if str(s.get("rule_id") or "") == "market_profile_breakout"]

    assert len(_breakouts(_eval(payload, c1, prev))) == 0
    assert len(_breakouts(_eval(payload, c2, c1))) == 1
    assert len(_breakouts(_eval(payload, c3, c2))) == 0
    assert len(_breakouts(_eval(payload, c4, c3))) == 0
    assert len(_breakouts(_eval(payload, c5, c4))) == 0
    assert len(_breakouts(_eval(payload, c6, c5))) == 0
    assert len(_breakouts(_eval(payload, c7, c6))) == 0
    assert len(_breakouts(_eval(payload, c8, c7))) == 1


def test_breakout_v3_rewind_resets_pending_and_dedupe() -> None:
    payload = _payload(profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0)])
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    c1 = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)
    c2 = _candle(ts=_dt(2, 0), open_=100.7, close=101.0)

    assert len(_eval(payload, c1, prev)["signals"]) == 0
    assert len(_eval(payload, c2, c1)["signals"]) == 1

    runtime_key = _runtime_key(payload)
    state = mp_plugin._MP_RUNTIME_STATE[runtime_key]
    state["bar_index"] = 1
    state["v3_last_processed_bar_index"] = 2

    assert len(_eval(payload, c1, prev)["signals"]) == 0
    assert len(_eval(payload, c2, c1)["signals"]) == 1


def test_breakout_v3_known_at_filter_respected() -> None:
    future_profile = {
        "start": int(_dt(2).timestamp()),
        "end": int(_dt(3).timestamp()),
        "known_at": int(_dt(3).timestamp()),
        "VAH": 100.0,
        "VAL": 90.0,
        "POC": 95.0,
        "session_count": 1,
        "precision": 2,
    }
    payload = _payload(profiles=[future_profile])
    candle = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)

    out = _eval(payload, candle, None)
    assert out["signals"] == []
    diagnostics = out.get("diagnostics") or {}
    assert int(diagnostics.get("num_effective_profiles") or 0) == 0
    assert int(diagnostics.get("candidate_count") or 0) == 0


def test_breakout_v3_fail_loud_on_missing_timeframe_seconds() -> None:
    payload = _payload(profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0)])
    payload.pop("chart_timeframe_seconds", None)
    payload["chart_timeframe"] = ""
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    curr = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)

    with pytest.raises(RuntimeError, match="timeframe_seconds"):
        _eval(payload, curr, prev)


def test_breakout_v3_fail_loud_on_missing_previous_bar() -> None:
    payload = _payload(profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0)])
    curr = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)

    with pytest.raises(RuntimeError, match="missing_previous_bar"):
        _eval(payload, curr, None)


def test_breakout_v3_emits_without_enable_or_version_switch() -> None:
    payload = _payload(
        profiles=[_profile(start=_dt(0), end=_dt(1), vah=100.0, val=90.0)],
        params={},
    )
    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    c1 = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)
    c2 = _candle(ts=_dt(2, 0), open_=100.7, close=101.0)

    assert _eval(payload, c1, prev)["signals"] == []
    out = _eval(payload, c2, c1)
    assert len(out["signals"]) >= 1
    assert any(str(sig.get("rule_id") or "") == "market_profile_breakout" for sig in out["signals"])
