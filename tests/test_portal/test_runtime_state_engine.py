from __future__ import annotations

from datetime import datetime, timezone

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state.market_profile_engine import MarketProfileStateEngine


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
