from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Any, Dict, List

import pytest

from engines.bot_runtime.core.domain import Candle

_EMITTER_PATH = Path(__file__).resolve().parents[2] / "src/indicators/market_profile/signals/emitter.py"
_EMITTER_SPEC = spec_from_file_location("market_profile_emitter_test_module", _EMITTER_PATH)
assert _EMITTER_SPEC is not None and _EMITTER_SPEC.loader is not None
mp_emitter = module_from_spec(_EMITTER_SPEC)
_EMITTER_SPEC.loader.exec_module(mp_emitter)


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
    runtime_scope: str = "runtime-scope-1m",
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "_indicator_id": "ind-v3-bounds",
        "_runtime_scope": runtime_scope,
        "symbol": "BTC-USD",
        "profiles": profiles,
        "profile_params": {
            "use_merged_value_areas": False,
            "extend_value_area_to_chart_end": True,
            "market_profile_breakout_v3_confirm_bars": 1,
            "market_profile_breakout_v3_lockout_bars": 0,
            **(params or {}),
        },
        "source_timeframe": "1m",
        "chart_timeframe": "1m",
        "source_timeframe_seconds": 60,
        "chart_timeframe_seconds": 60,
    }


def _candle(*, ts: datetime, open_: float, close: float) -> Candle:
    high = max(open_, close) + 0.5
    low = min(open_, close) - 0.5
    return Candle(time=ts, open=open_, high=high, low=low, close=close, volume=1.0)


def _profile_key(start: datetime, end: datetime, session_count: int = 1) -> str:
    return f"{start.isoformat()}:{end.isoformat()}:{int(session_count)}"


def _runtime_key(payload: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(payload.get("_indicator_id") or ""),
        str(payload.get("symbol") or ""),
        str(payload.get("_runtime_scope") or ""),
    )


@pytest.fixture(autouse=True)
def _stub_runtime_profiles_module(monkeypatch: pytest.MonkeyPatch) -> None:
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
        for item in profiles_payload:
            known_at = int(item.get("known_at") or item.get("end") or 0)
            if known_at <= int(current_epoch):
                out.append(_Profile(item))
        return out, {"count": len(out)}

    module = types.SimpleNamespace(resolve_effective_profiles=_resolve_effective_profiles)
    monkeypatch.setitem(sys.modules, "indicators.market_profile.compute.internal.runtime_profiles", module)


def setup_function() -> None:
    mp_emitter._PROFILE_RESOLUTION_CACHE.clear()
    mp_emitter._PROFILE_KNOWN_EPOCHS_CACHE.clear()
    mp_emitter._PROFILE_RUNTIME_VIEW_CACHE.clear()


def test_v3_runtime_state_prunes_stale_and_caps_sizes() -> None:
    start = _dt(0, 0)
    end = _dt(1, 0)
    profile = _profile(start=start, end=end, vah=100.0, val=90.0, poc=95.0)
    profile_key = _profile_key(start, end, 1)
    payload = _payload(
        profiles=[profile],
        params={
            "market_profile_breakout_v3_state_ttl_bars": 2,
            "market_profile_breakout_v3_pending_max_entries": 3,
            "market_profile_breakout_v3_active_breakouts_max_entries": 4,
            "market_profile_breakout_v3_history_max_entries": 5,
        },
    )
    current = _candle(ts=_dt(2, 0), open_=95.0, close=95.0)
    previous = _candle(ts=_dt(1, 59), open_=95.0, close=95.0)
    current_epoch = int(current.time.timestamp())
    min_epoch = int(current_epoch - (2 * 60))
    runtime_key = _runtime_key(payload)
    temporal_key = f"{runtime_key[0]}|{runtime_key[1]}|{runtime_key[2]}"

    stale_epoch = min_epoch - 120
    fresh_base_epoch = min_epoch + 1
    pending = {
        f"pending-{idx}": {
            "profile_key": profile_key,
            "variant": "breakout_up",
            "started_bar_index": idx,
            "started_epoch": stale_epoch if idx < 6 else fresh_base_epoch + idx,
        }
        for idx in range(10)
    }
    last_emit_epoch = {
        f"emit-{idx}": stale_epoch if idx < 6 else fresh_base_epoch + idx
        for idx in range(10)
    }
    emitted_signatures = {
        f"sig-{idx}": stale_epoch if idx < 6 else fresh_base_epoch + idx
        for idx in range(10)
    }
    retest_signatures = {
        f"retest-{idx}": stale_epoch if idx < 6 else fresh_base_epoch + idx
        for idx in range(10)
    }
    active_breakouts = [
        {
            "event_signature": f"breakout-{idx}",
            "profile_key": profile_key,
            "value_area_id": profile_key,
            "variant": "breakout_up",
            "boundary_type": "VAH",
            "boundary_price": 100.0,
            "direction": "long",
            "breakout_bar_index": idx,
            "breakout_epoch": stale_epoch if idx < 6 else fresh_base_epoch + idx,
        }
        for idx in range(10)
    ]

    payload["_runtime_state_storage"] = {
        temporal_key: {
            "bar_index": 120,
            "last_epoch": current_epoch - 60,
            "v3_last_processed_bar_index": 119,
            "v3_pending": pending,
            "v3_last_emit_epoch": last_emit_epoch,
            "v3_emitted_signatures": emitted_signatures,
            "v3_active_breakouts": active_breakouts,
            "v3_retest_signatures": retest_signatures,
        }
    }

    out = mp_emitter.market_profile_rule_payload(
        snapshot_payload=payload,
        candle=current,
        previous_candle=previous,
    )
    diagnostics = out.get("diagnostics") or {}
    state = payload["_runtime_state_storage"][temporal_key]

    assert len(state["v3_pending"]) <= 3
    assert len(state["v3_active_breakouts"]) <= 4
    assert len(state["v3_last_emit_epoch"]) <= 5
    assert len(state["v3_emitted_signatures"]) <= 5
    assert len(state["v3_retest_signatures"]) <= 5
    assert all(int(v) >= min_epoch for v in state["v3_last_emit_epoch"].values())
    assert all(int(v) >= min_epoch for v in state["v3_emitted_signatures"].values())
    assert all(int(v) >= min_epoch for v in state["v3_retest_signatures"].values())
    assert all(
        int((entry or {}).get("started_epoch") or 0) >= min_epoch
        for entry in state["v3_pending"].values()
    )
    assert all(
        int((entry or {}).get("breakout_epoch") or 0) >= min_epoch
        for entry in state["v3_active_breakouts"]
    )
    assert int(diagnostics.get("state_pruned_pending") or 0) >= 1
    assert int(diagnostics.get("state_capped_pending") or 0) >= 1
    assert int(diagnostics.get("state_pruned_emitted_signatures") or 0) >= 1


def test_v3_runtime_state_caps_history_after_new_breakout_emit() -> None:
    start = _dt(0, 0)
    end = _dt(1, 0)
    profile = _profile(start=start, end=end, vah=100.0, val=90.0, poc=95.0)
    profile_key = _profile_key(start, end, 1)
    payload = _payload(
        profiles=[profile],
        params={
            "market_profile_breakout_v3_confirm_bars": 1,
            "market_profile_breakout_v3_lockout_bars": 0,
            "market_profile_breakout_v3_state_ttl_bars": 100,
            "market_profile_breakout_v3_history_max_entries": 1,
            "market_profile_breakout_v3_pending_max_entries": 8,
            "market_profile_breakout_v3_active_breakouts_max_entries": 8,
        },
    )
    runtime_key = _runtime_key(payload)
    temporal_key = f"{runtime_key[0]}|{runtime_key[1]}|{runtime_key[2]}"
    payload["_runtime_state_storage"] = {
        temporal_key: {
            "bar_index": 0,
            "last_epoch": None,
            "v3_last_processed_bar_index": -1,
            "v3_pending": {},
            "v3_last_emit_epoch": {f"{profile_key}|breakout_up": int(_dt(1, 5).timestamp())},
            "v3_emitted_signatures": {"existing-breakout-signature": int(_dt(1, 5).timestamp())},
            "v3_active_breakouts": [],
            "v3_retest_signatures": {},
        }
    }

    prev = _candle(ts=_dt(1, 0), open_=95.0, close=99.0)
    candidate = _candle(ts=_dt(1, 30), open_=99.0, close=100.6)
    confirm = _candle(ts=_dt(2, 0), open_=100.7, close=101.0)

    first = mp_emitter.market_profile_rule_payload(
        snapshot_payload=payload,
        candle=candidate,
        previous_candle=prev,
    )
    assert first["signals"] == []

    second = mp_emitter.market_profile_rule_payload(
        snapshot_payload=payload,
        candle=confirm,
        previous_candle=candidate,
    )
    assert any(str(item.get("rule_id") or "") == "market_profile_breakout" for item in second["signals"])

    state = payload["_runtime_state_storage"][temporal_key]
    assert len(state["v3_last_emit_epoch"]) <= 1
    assert len(state["v3_emitted_signatures"]) <= 1
    assert max(state["v3_last_emit_epoch"].values()) == int(confirm.time.timestamp())
    assert max(state["v3_emitted_signatures"].values()) == int(confirm.time.timestamp())
