import pytest

pd = pytest.importorskip("pandas")

from datetime import datetime, timezone

from signals.base import BaseSignal
from signals.engine.signal_generator import build_signal_overlays
from signals.rules.pivot import pivot_signals_to_overlays
from signals.engine import pivot_level_generator  # noqa: F401


def _make_df():
    index = pd.date_range("2024-01-01", periods=5, freq="h", tz="UTC")
    data = {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}
    return pd.DataFrame(data, index=index)


def _make_signal(direction: str, level_price: float) -> BaseSignal:
    ts = datetime(2024, 1, 1, 4, tzinfo=timezone.utc)
    metadata = {
        "level_price": level_price,
        "breakout_direction": direction,
        "level_kind": "resistance" if direction == "above" else "support",
        "breakout_start": datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
    }
    return BaseSignal(
        type="breakout",
        symbol="ES",
        time=ts,
        confidence=1.0,
        metadata=metadata,
    )


def test_pivot_signals_to_overlays_builds_colored_payload():
    df = _make_df()
    signals = [_make_signal("above", 105.0), _make_signal("below", 95.0)]

    overlays = pivot_signals_to_overlays(signals, df)

    assert len(overlays) == 1
    payload = overlays[0]["payload"]

    markers = payload["markers"]
    price_lines = payload["price_lines"]

    assert {m["shape"] for m in markers} == {"triangleUp", "triangleDown"}
    colors = {m["color"] for m in markers}
    assert colors == {"#16a34a", "#dc2626"}
    assert {pl["color"] for pl in price_lines} == colors
    assert all(pl.get("extend") == "none" for pl in price_lines)
    assert all(pl.get("originTime") <= pl.get("endTime") for pl in price_lines)


def test_build_signal_overlays_uses_pivot_adapter():
    pivot_level_generator.ensure_registration(force=True)
    df = _make_df()
    signal = _make_signal("above", 110.0)

    overlays = build_signal_overlays("pivot_level", [signal], df)

    assert len(overlays) == 1
    overlay = overlays[0]
    assert overlay["type"] == "pivot_level"

    payload = overlay["payload"]
    assert isinstance(payload.get("markers"), list)
    assert isinstance(payload.get("price_lines"), list)
    assert payload["markers"][0]["color"] == "#16a34a"
