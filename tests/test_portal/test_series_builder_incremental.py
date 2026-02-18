from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pandas")

from engines.bot_runtime.core.domain import Candle, isoformat
from portal.backend.service.bots.bot_runtime.strategy.series_builder import SeriesBuilder, StrategySeries


def _candle_at(ts: datetime, value: float = 100.0) -> Candle:
    return Candle(
        time=ts,
        open=value,
        high=value + 1.0,
        low=value - 1.0,
        close=value,
        volume=10.0,
    )


def test_incremental_eval_emits_only_current_epoch_and_newer_than_cursor():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={"incremental_signal_lookback_bars": 10},
        run_type="backtest",
    )
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    series = StrategySeries(
        strategy_id="s1",
        name="S1",
        symbol="BTC/USDT",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        candles=[_candle_at(now)],
        instrument={"id": "inst-1"},
        window_start=isoformat(now - timedelta(hours=1)),
    )

    def _fake_evaluate(*args, **kwargs):
        _ = args, kwargs
        return {
            "chart_markers": {
                "buy": [
                    {"time": isoformat(now - timedelta(minutes=1))},
                    {"time": isoformat(now)},
                ],
                "sell": [],
            },
            "perf": {"indicator_eval_ms": 3.5, "rule_eval_ms": 2.0},
        }

    builder._evaluate_strategy = _fake_evaluate  # type: ignore[assignment]
    builder._indicator_overlay_entries = lambda *a, **k: []  # type: ignore[assignment]
    builder._build_regime_overlays = lambda **k: []  # type: ignore[assignment]

    signals, overlays, metrics = builder.evaluate_incremental_for_bar(
        series=series,
        candle=_candle_at(now),
        visible_candles=[_candle_at(now)],
        last_evaluated_epoch=int((now - timedelta(minutes=1)).timestamp()),
    )

    assert len(signals) == 1
    only = list(signals)[0]
    assert only.epoch == int(now.timestamp())
    assert only.direction == "long"
    assert overlays == []
    assert metrics["epochs_evaluated_this_tick"] == 1.0
    assert metrics["signals_emitted_count"] == 1.0
    assert metrics["indicator_eval_ms"] == 3.5
    assert metrics["rule_eval_ms"] == 2.0


def test_incremental_eval_uses_bounded_lookback_window():
    builder = SeriesBuilder(
        bot_id="bot-1",
        config={"incremental_signal_lookback_bars": 5},
        run_type="backtest",
    )
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    series = StrategySeries(
        strategy_id="s1",
        name="S1",
        symbol="BTC/USDT",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        candles=[_candle_at(now)],
        instrument={"id": "inst-1"},
        window_start=isoformat(now - timedelta(hours=2)),
    )
    observed: dict[str, str] = {}

    def _fake_evaluate(*, start_iso, end_iso, timeframe, instrument_id, strategy, include_walk_forward_markers=False):
        observed["start_iso"] = start_iso
        observed["end_iso"] = end_iso
        _ = timeframe, instrument_id, strategy, include_walk_forward_markers
        return {"chart_markers": {"buy": [], "sell": []}, "perf": {}}

    builder._evaluate_strategy = _fake_evaluate  # type: ignore[assignment]
    builder._indicator_overlay_entries = lambda *a, **k: []  # type: ignore[assignment]
    builder._build_regime_overlays = lambda **k: []  # type: ignore[assignment]

    builder.evaluate_incremental_for_bar(
        series=series,
        candle=_candle_at(now),
        visible_candles=[_candle_at(now)],
        last_evaluated_epoch=0,
    )

    expected_start = now - timedelta(minutes=5)
    assert observed["start_iso"] == isoformat(expected_start)
    assert observed["end_iso"] == isoformat(now)


def test_build_signals_from_markers_preserves_signal_time_without_shift() -> None:
    ts = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    markers = {
        "buy": [
            {
                "time": int(ts.timestamp()),
                "known_at": int((ts - timedelta(minutes=1)).timestamp()),
            }
        ],
        "sell": [],
    }
    out = SeriesBuilder._build_signals_from_markers(markers)
    assert len(out) == 1
    assert out[0].epoch == int(ts.timestamp())


def test_build_signals_from_markers_raises_when_known_at_after_signal_time() -> None:
    ts = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    markers = {
        "buy": [
            {
                "time": int(ts.timestamp()),
                "known_at": int((ts + timedelta(minutes=1)).timestamp()),
            }
        ],
        "sell": [],
    }
    with pytest.raises(RuntimeError, match="known_at"):
        SeriesBuilder._build_signals_from_markers(markers)
