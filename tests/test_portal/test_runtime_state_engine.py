from __future__ import annotations

from datetime import datetime, timezone

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state.contracts import IndicatorStateSnapshot, OverlayProjectionInput
from engines.bot_runtime.core.indicator_state.market_profile_engine import MarketProfileStateEngine
from engines.bot_runtime.core.indicator_state.plugins.market_profile import market_profile_overlay_entries


def _candle(ts: str, close: float) -> Candle:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return Candle(time=dt, open=close - 1, high=close + 1, low=close - 2, close=close, volume=10.0)


def test_market_profile_state_engine_rolls_session_and_updates_revision() -> None:
    engine = MarketProfileStateEngine()
    state = engine.initialize({"symbol": "BTC/USD"})

    first = engine.apply_bar(state, _candle("2024-01-01T00:00:00Z", 100.0))
    assert first.changed is True
    assert first.revision == 1

    second = engine.apply_bar(state, _candle("2024-01-02T00:00:00Z", 101.0))
    assert second.changed is True
    assert second.revision == 2

    snapshot = engine.snapshot(state)
    profiles = snapshot.payload["profiles"]
    assert len(profiles) == 2
    assert profiles[0]["session"] == "2024-01-01"
    assert profiles[0]["status"] == "completed"
    assert profiles[1]["session"] == "2024-01-02"
    assert profiles[1]["status"] == "active"
    assert profiles[0]["start"].isoformat().startswith("2024-01-01T00:00:00")
    assert profiles[0]["end"].isoformat().startswith("2024-01-01T23:59:59")
    assert snapshot.revision == 2


def test_market_profile_overlay_projection_emits_profiles_and_params() -> None:
    snapshot = IndicatorStateSnapshot(
        revision=2,
        known_at=datetime(2024, 1, 3, tzinfo=timezone.utc),
        formed_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        source_timeframe="30m",
        payload={
            "profile_params": {
                "use_merged_value_areas": True,
                "merge_threshold": 0.6,
                "min_merge_sessions": 5,
                "extend_value_area_to_chart_end": True,
            },
            "overlay_color": "#22d3ee",
            "profiles": [
                {
                    "session": "2024-01-02",
                    "start": "2024-01-02T00:00:00+00:00",
                    "end": "2024-01-02T23:59:59+00:00",
                    "known_at": "2024-01-02T23:59:59+00:00",
                    "VAH": 105.0,
                    "VAL": 95.0,
                    "POC": 100.0,
                }
            ]
        },
    )
    entries = market_profile_overlay_entries(
        OverlayProjectionInput(snapshot=snapshot, previous_projection_state={"seq": 1, "entries": {}})
    )
    assert len(entries) == 1
    entry = next(iter(entries.values()))
    assert entry["type"] == "market_profile"
    assert entry["color"] == "#22d3ee"
    payload = entry["payload"]
    assert payload["profiles"][0]["VAL"] == 95.0
    assert payload["profiles"][0]["VAH"] == 105.0
    assert payload["profile_params"]["use_merged_value_areas"] is True
    assert payload["profile_params"]["merge_threshold"] == 0.6
