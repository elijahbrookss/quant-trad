from __future__ import annotations

from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.strategy.series_builder import SeriesBuilder


def _deps(*, calls: dict[str, int]) -> BotRuntimeDeps:
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

    return BotRuntimeDeps(
        fetch_strategy=lambda _strategy_id: None,
        fetch_ohlcv=lambda *args, **kwargs: None,
        resolve_instrument=lambda _datasource, _exchange, _symbol: None,
        strategy_evaluate=lambda *args, **kwargs: {},
        strategy_generate_signals=lambda *args, **kwargs: {},
        indicator_get_instance_meta=_meta,
        indicator_runtime_input_plan_for_instance=_plan,
        indicator_overlays_for_instance=_overlay,
        build_indicator_context=lambda _bot_id, _overlay_cache: None,
        build_runtime_series_derived_state=lambda *args, **kwargs: None,
        record_bot_runtime_event=lambda _payload: None,
        record_bot_runtime_events_batch=lambda _payloads: 0,
        record_bot_trade=lambda _payload: None,
        record_bot_trade_event=lambda _payload: None,
        record_bot_run_steps_batch=lambda _payloads: 0,
        update_bot_run_artifact=lambda _run_id, _payload: None,
        record_run_report=lambda *args, **kwargs: None,
    )


def test_indicator_overlay_entries_reuses_source_bucket_cache():
    calls = {"overlays": 0}
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={"include_indicator_overlays": True},
        run_type="backtest",
        deps=_deps(calls=calls),
    )
    strategy_meta = {
        "id": "strat-1",
        "indicator_links": [{"indicator_id": "ind-1"}],
    }

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
