import sys
import types
from datetime import datetime, timedelta, timezone

from portal.backend.service.bot_runtime.domain import Candle
from portal.backend.service.bot_runtime.series_builder import SeriesBuilder
from portal.backend.service.bot_runtime.reporting import instrument_key


def _candle_batch(start: datetime, count: int = 3) -> list[Candle]:
    candles = []
    for i in range(count):
        open_price = 1.0 + (i * 0.1)
        start_time = start + timedelta(minutes=i)
        end_time = start_time + timedelta(minutes=1)
        candles.append(
            Candle(
                time=start_time,
                open=open_price,
                high=open_price + 0.1,
                low=open_price - 0.1,
                close=open_price + 0.05,
                end=end_time,
                atr=None,
                volume=None,
                range=0.2,
                lookback_15={},
            )
        )
    return candles


class _DummyIndex(list):
    @property
    def is_monotonic_increasing(self) -> bool:
        return True


class _DummyFrame:
    def __init__(self, marker: str):
        super().__init__()
        self.marker = marker
        self.index = _DummyIndex([1])
        self.empty = False

    def copy(self):
        return self


def test_indicator_overlays_cached(monkeypatch):
    overlay_calls = {"count": 0}

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    initial_candles = _candle_batch(start)
    appended_candles = _candle_batch(start + timedelta(minutes=5))

    fetch_calls = {"count": 0}

    def fake_fetch(*_args, **_kwargs):
        fetch_calls["count"] += 1
        marker = "initial" if fetch_calls["count"] == 1 else "append"
        return _DummyFrame(marker)

    def fake_generate_strategy_signals(**_kwargs):
        return {"chart_markers": {"buy": [{"time": start.timestamp()}]}}

    def fake_overlays_for_instance(*_args, **_kwargs):
        overlay_calls["count"] += 1
        return {"points": [1, 2, 3]}

    monkeypatch.setitem(
        sys.modules,
        "portal.backend.service.strategy_service",
        types.SimpleNamespace(generate_strategy_signals=fake_generate_strategy_signals),
    )
    monkeypatch.setitem(
        sys.modules,
        "portal.backend.service.indicator_service",
        types.SimpleNamespace(overlays_for_instance=fake_overlays_for_instance),
    )
    monkeypatch.setitem(
        sys.modules,
        "portal.backend.service.candle_service",
        types.SimpleNamespace(fetch_ohlcv=fake_fetch),
    )
    monkeypatch.setattr(
        SeriesBuilder,
        "_build_candles",
        staticmethod(
            lambda df, timeframe=None: list(initial_candles)
            if getattr(df, "marker", None) == "initial"
            else list(appended_candles)
        ),
    )

    instrument = {"tick_size": 1, "tick_value": 1, "contract_size": 1}
    config = {
        "instrument_index": {instrument_key(None, None, "BTC"): instrument},
        "backtest_start": "2024-01-01T00:00:00Z",
        "backtest_end": "2024-01-02T00:00:00Z",
    }
    builder = SeriesBuilder("bot-1", config, "backtest")
    strategy = {"id": "strat-1", "symbol": "BTC", "timeframe": "1m", "indicator_links": [{"indicator_id": "ind-1"}]}

    first_series = builder.build_series([strategy])[0]
    assert first_series.overlays and first_series.overlays[0]["payload"] == {"points": [1, 2, 3]}
    assert overlay_calls["count"] == 1

    second_series = builder.build_series([strategy])[0]
    assert second_series.overlays and second_series.overlays[0]["payload"] == {"points": [1, 2, 3]}
    assert overlay_calls["count"] == 1  # cached overlay reused


def test_append_series_updates_refreshes_signals(monkeypatch):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    initial_candles = _candle_batch(start)
    new_candles = _candle_batch(start + timedelta(minutes=len(initial_candles)), count=2)

    fetch_calls = {"count": 0}

    def fake_fetch(*_args, **_kwargs):
        fetch_calls["count"] += 1
        marker = "initial" if fetch_calls["count"] == 1 else "append"
        return _DummyFrame(marker)

    def fake_generate_strategy_signals(**_kwargs):
        return {"chart_markers": {"buy": [{"time": new_candles[-1].time.timestamp()}]}}

    monkeypatch.setitem(
        sys.modules,
        "portal.backend.service.strategy_service",
        types.SimpleNamespace(
            evaluate=fake_generate_strategy_signals,
            generate_strategy_signals=fake_generate_strategy_signals,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "portal.backend.service.indicator_service",
        types.SimpleNamespace(overlays_for_instance=lambda *_args, **_kwargs: {}),
    )
    monkeypatch.setitem(
        sys.modules,
        "portal.backend.service.candle_service",
        types.SimpleNamespace(fetch_ohlcv=fake_fetch),
    )
    monkeypatch.setattr(
        SeriesBuilder,
        "_build_candles",
        staticmethod(
            lambda df, timeframe=None: list(initial_candles)
            if getattr(df, "marker", None) == "initial"
            else list(new_candles)
        ),
    )

    builder = SeriesBuilder("bot-2", {}, "backtest")
    strategy = {"id": "strat-2", "symbol": "ETH", "timeframe": "1m"}
    series = builder.build_series([strategy])[0]
    last_time = series.candles[-1].time
    start_iso = last_time.isoformat()
    end_iso = (last_time + timedelta(minutes=1)).isoformat()

    updated = builder.append_series_updates(series, start_iso, end_iso)

    assert updated is True
    assert series.signals
    assert series.window_end == end_iso
