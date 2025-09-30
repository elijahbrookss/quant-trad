import pytest

pd = pytest.importorskip("pandas")

from datetime import datetime

from signals.base import BaseSignal
from signals.engine.signal_generator import build_signal_overlays
from signals.engine import market_profile_generator  # noqa: F401 ensure adapter registration


def _make_df():
    index = pd.date_range("2024-06-01", periods=10, freq="30T", tz="UTC")
    data = {
        "open": [4300.0 + i for i in range(10)],
        "high": [4301.0 + i for i in range(10)],
        "low": [4299.0 + i for i in range(10)],
        "close": [4300.5 + i for i in range(10)],
    }
    return pd.DataFrame(data, index=index)


def _breakout_signal(time: datetime) -> BaseSignal:
    metadata = {
        "source": "MarketProfile",
        "level_type": "VAH",
        "level_price": 4310.0,
        "breakout_direction": "above",
        "trigger_close": 4311.5,
        "trigger_high": 4312.0,
        "trigger_low": 4310.5,
        "confidence": 0.72,
        "value_area_id": "session-123",
    }
    return BaseSignal(
        type="breakout",
        symbol="ES",
        time=time,
        confidence=metadata["confidence"],
        metadata=metadata,
    )


def _retest_signal(time: datetime) -> BaseSignal:
    metadata = {
        "source": "MarketProfile",
        "level_type": "VAL",
        "VAL": 4295.0,
        "breakout_direction": "below",
        "direction": "down",
        "retest_role": "resistance",
        "retest_close": 4294.5,
        "bars_since_breakout": 3,
        "confidence": 0.58,
    }
    return BaseSignal(
        type="retest",
        symbol="ES",
        time=time,
        confidence=metadata["confidence"],
        metadata=metadata,
    )


def test_market_profile_signals_render_as_bubbles():
    df = _make_df()
    breakout = _breakout_signal(df.index[5].to_pydatetime())
    retest = _retest_signal(df.index[7].to_pydatetime())

    overlays = build_signal_overlays("market_profile", [breakout, retest], df)

    assert len(overlays) == 1
    payload = overlays[0]["payload"]

    assert payload["price_lines"] == []
    assert payload["markers"] == []

    bubbles = payload["bubbles"]
    assert len(bubbles) == 2

    labels = {bubble["label"] for bubble in bubbles}
    assert "VAH breakout" in labels
    assert "VAL retest" in labels

    breakout_bubble = next(b for b in bubbles if b["label"] == "VAH breakout")
    retest_bubble = next(b for b in bubbles if b["label"] == "VAL retest")

    assert breakout_bubble["direction"] == "above"
    assert breakout_bubble["accentColor"] == "#16a34a"
    assert breakout_bubble["subtype"] == "bubble"
    assert breakout_bubble["price"] > 4311.0

    assert retest_bubble["direction"] == "down"
    assert retest_bubble["accentColor"] == "#f97316"
    assert retest_bubble["detail"].startswith("Retest after 3 bars")
