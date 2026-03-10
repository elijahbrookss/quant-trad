from __future__ import annotations

from datetime import datetime, timedelta, timezone

from portal.backend.service.bots.bot_runtime.strategy.series_builder import SeriesBuilder


def test_indicator_overlay_entries_reuses_source_bucket_cache(monkeypatch):
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={"include_indicator_overlays": True},
        run_type="backtest",
    )
    strategy_meta = {
        "id": "strat-1",
        "indicator_links": [{"indicator_id": "ind-1"}],
    }
    calls = {"overlays": 0}

    from portal.backend.service.indicators import indicator_service

    def _meta(indicator_id, ctx=None):  # noqa: ANN001
        _ = indicator_id, ctx
        return {
            "id": "ind-1",
            "type": "market_profile",
            "color": "#fff",
            "params": {
                "symbol": "BTC/USDT",
                "interval": "1m",
            },
            "datasource": "demo",
            "exchange": "demo",
        }

    def _plan(indicator_id, strategy_interval, start, end, ctx=None):  # noqa: ANN001
        _ = indicator_id, strategy_interval, start, ctx
        return {
            "source_timeframe": "30m",
            "start": "2026-01-01T21:00:00+00:00",
            "end": end,
        }

    def _overlay(*args, **kwargs):  # noqa: ANN002, ANN003
        _ = args, kwargs
        calls["overlays"] += 1
        return {"type": "market_profile", "payload": {"boxes": [{"x1": 1, "x2": 2, "y1": 1.0, "y2": 2.0}]}}

    monkeypatch.setattr(indicator_service, "get_instance_meta", _meta)
    monkeypatch.setattr(indicator_service, "runtime_input_plan_for_instance", _plan)
    monkeypatch.setattr(indicator_service, "overlays_for_instance", _overlay)

    first = builder._indicator_overlay_entries(
        strategy_meta,
        "2026-01-01T22:00:00+00:00",
        "2026-01-01T22:05:00+00:00",
        "1m",
        "BTC/USDT",
        "demo",
        "demo",
    )
    second = builder._indicator_overlay_entries(
        strategy_meta,
        "2026-01-01T22:00:00+00:00",
        "2026-01-01T22:19:00+00:00",
        "1m",
        "BTC/USDT",
        "demo",
        "demo",
    )

    assert len(first) == 1
    assert len(second) == 1
    assert calls["overlays"] == 1


def test_regime_rows_for_window_reuses_cached_snapshot():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={},
        run_type="backtest",
    )
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=10)
    calls = {"count": 0}

    def _fetch(**kwargs):  # noqa: ANN003
        calls["count"] += 1
        _ = kwargs
        return {
            start: {"state": "a"},
            end: {"state": "b"},
        }

    builder._fetch_regime_rows = _fetch  # type: ignore[method-assign]

    rows_a = builder._regime_rows_for_window(
        instrument_id="inst-1",
        timeframe="1m",
        timeframe_seconds=60,
        start_dt=start,
        end_dt=end,
        strategy_id="s1",
        symbol="BTC/USDT",
    )
    rows_b = builder._regime_rows_for_window(
        instrument_id="inst-1",
        timeframe="1m",
        timeframe_seconds=60,
        start_dt=start,
        end_dt=end,
        strategy_id="s1",
        symbol="BTC/USDT",
    )

    assert calls["count"] == 1
    assert rows_a == rows_b


def test_regime_rows_for_window_handles_mixed_timezone_keys():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={},
        run_type="backtest",
    )
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    # One naive and one aware timestamp to simulate mixed upstream payloads.
    aware_mid = start + timedelta(minutes=30)
    naive_mid = aware_mid.replace(tzinfo=None)

    def _fetch(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            naive_mid: {"state": "naive"},
            aware_mid: {"state": "aware"},
        }

    builder._fetch_regime_rows = _fetch  # type: ignore[method-assign]
    rows = builder._regime_rows_for_window(
        instrument_id="inst-1",
        timeframe="1m",
        timeframe_seconds=60,
        start_dt=start,
        end_dt=end,
        strategy_id="s1",
        symbol="BTC/USDT",
    )

    assert len(rows) == 1
