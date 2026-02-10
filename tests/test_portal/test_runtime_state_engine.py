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
    assert first.changed is False
    assert first.revision == 0

    second = engine.apply_bar(state, _candle("2024-01-02T00:00:00Z", 101.0))
    assert second.changed is True
    assert second.revision == 1

    snapshot = engine.snapshot(state)
    profiles = snapshot.payload["profiles"]
    assert len(profiles) == 1
    assert profiles[0]["session"] == "2024-01-01"
    assert snapshot.revision == 1


def test_market_profile_overlay_projection_emits_overlay_payload_boxes() -> None:
    snapshot = IndicatorStateSnapshot(
        revision=2,
        known_at=datetime(2024, 1, 3, tzinfo=timezone.utc),
        formed_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        source_timeframe="30m",
        payload={
            "profiles": [
                {"session": "2024-01-02", "VAH": 105.0, "VAL": 95.0, "POC": 100.0}
            ]
        },
    )
    entries = market_profile_overlay_entries(
        OverlayProjectionInput(snapshot=snapshot, previous_projection_state={"seq": 1, "entries": {}})
    )
    assert len(entries) == 1
    entry = next(iter(entries.values()))
    assert entry["type"] == "market_profile"
    payload = entry["payload"]
    assert payload["boxes"][0]["y1"] == 95.0
    assert payload["boxes"][0]["y2"] == 105.0
    assert payload["boxes"][0]["x1"].startswith("2024-01-02T00:00:00")
