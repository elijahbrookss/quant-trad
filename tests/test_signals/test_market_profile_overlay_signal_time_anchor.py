from __future__ import annotations

from datetime import datetime, timezone

import pytest

pd = pytest.importorskip("pandas")

from indicators.market_profile.overlays import market_profile_overlay_adapter
from signals.base import BaseSignal


def _df() -> pd.DataFrame:
    idx = pd.to_datetime(
        [
            datetime(2026, 1, 1, 10, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 11, tzinfo=timezone.utc),
        ],
        utc=True,
    )
    return pd.DataFrame(
        {
            "open": [99.0, 100.0],
            "high": [101.0, 102.0],
            "low": [98.0, 99.0],
            "close": [100.0, 101.0],
        },
        index=idx,
    )


def test_overlay_anchors_on_exact_signal_time_close() -> None:
    signal = BaseSignal(
        type="breakout",
        symbol="BTC-USD",
        time=datetime(2026, 1, 1, 11, tzinfo=timezone.utc),
        confidence=1.0,
        metadata={
            "source": "MarketProfile",
            "signal_time": int(datetime(2026, 1, 1, 11, tzinfo=timezone.utc).timestamp()),
            "boundary_type": "VAH",
            "boundary_price": 100.5,
            "breakout_direction": "above",
            "rule_id": "market_profile_breakout_v3_confirmed",
        },
    )
    overlays = market_profile_overlay_adapter([signal], _df())
    assert len(overlays) == 1
    bubbles = overlays[0]["payload"]["bubbles"]
    assert len(bubbles) == 1
    assert bubbles[0]["time"] == int(datetime(2026, 1, 1, 11, tzinfo=timezone.utc).timestamp())
    # Y anchor should be the candle close (101.0), not the profile boundary price (100.5).
    assert bubbles[0]["price"] == pytest.approx(101.0)


def test_overlay_skips_when_signal_time_has_no_exact_close_match() -> None:
    signal = BaseSignal(
        type="breakout",
        symbol="BTC-USD",
        time=datetime(2026, 1, 1, 10, 30, tzinfo=timezone.utc),
        confidence=1.0,
        metadata={
            "source": "MarketProfile",
            "signal_time": int(datetime(2026, 1, 1, 10, 30, tzinfo=timezone.utc).timestamp()),
            "boundary_type": "VAH",
            "boundary_price": 100.5,
            "breakout_direction": "above",
            "rule_id": "market_profile_breakout_v3_confirmed",
        },
    )
    overlays = market_profile_overlay_adapter([signal], _df())
    assert overlays == []
