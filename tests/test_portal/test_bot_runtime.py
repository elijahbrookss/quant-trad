import math
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

if "pandas" not in sys.modules:  # pragma: no cover - testing convenience
    sys.modules["pandas"] = SimpleNamespace(
        DataFrame=object,
        to_datetime=lambda *args, **kwargs: None,
    )

from portal.backend.service.bot_runtime import BotRuntime, Candle, LadderRiskEngine
from portal.backend.service import bot_service


def make_runtime(**overrides):
    runtime_kwargs = {}
    if "state_callback" in overrides:
        runtime_kwargs["state_callback"] = overrides.pop("state_callback")
    config = {
        "runtime_mode": "backtest",
        "mode": "walk-forward",
        "fetch_seconds": 5,
        "symbol": "ES",
        "timeframe": "15m",
        "strategies_meta": [
            {
                "id": "strategy-1",
                "name": "test-strategy",
                "symbols": ["ES"],
                "datasource": "timescale",
            }
        ],
    }
    config.update(overrides)
    return BotRuntime("bot-test", config, **runtime_kwargs)


@pytest.mark.unit
def test_bot_runtime_snapshot_exposes_timer_fields():
    runtime = make_runtime()
    future = datetime.utcnow() + timedelta(seconds=3)
    runtime._next_bar_at = future  # emulate scheduled bar

    snapshot = runtime.snapshot()

    assert snapshot["paused"] is False
    assert isinstance(snapshot.get("next_bar_at"), str)
    assert math.isfinite(snapshot["next_bar_in_seconds"])
    assert 0 <= snapshot["next_bar_in_seconds"] <= 3


@pytest.mark.unit
def test_bot_runtime_pause_and_resume_flip_state():
    runtime = make_runtime()

    runtime.pause()
    paused_snapshot = runtime.snapshot()
    assert paused_snapshot["paused"] is True
    assert paused_snapshot["status"] == "paused"

    runtime.resume()
    resumed_snapshot = runtime.snapshot()
    assert resumed_snapshot["paused"] is False
    assert resumed_snapshot["status"] == "running"


@pytest.mark.unit
def test_bot_runtime_reset_if_finished_resets_state():
    runtime = make_runtime()
    runtime.state["status"] = "completed"
    runtime._total_bars = 5
    runtime._bar_index = 5
    runtime._chart_overlays = [{"foo": "bar"}]
    runtime._logs.append({"id": "1"})

    runtime.reset_if_finished()

    assert runtime.state["status"] == "idle"
    assert runtime._bar_index == 0
    assert len(runtime._logs) == 0
    assert runtime._chart_overlays == []


@pytest.mark.unit
def test_bot_runtime_state_callback_receives_updates():
    captured = []
    runtime = make_runtime(state_callback=lambda payload: captured.append(payload))
    runtime._last_stats = {"wins": 2}

    runtime._persist_runtime_state("completed")

    assert captured
    assert captured[0]["status"] == "completed"
    assert captured[0]["last_stats"] == {"wins": 2}
    assert "last_run_at" in captured[0]


@pytest.mark.unit
def test_bot_runtime_market_profile_overlays_keep_extension(monkeypatch):
    captured = {}

    def fake_overlays(*args, **kwargs):
        captured["kwargs"] = kwargs
        return {"boxes": [{"id": "va"}]}

    monkeypatch.setattr(
        "portal.backend.service.bot_runtime.indicator_service.overlays_for_instance",
        fake_overlays,
    )

    strategy = {
        "id": "strategy-1",
        "indicator_links": [
            {
                "indicator_id": "mpf-1",
                "indicator_snapshot": {
                    "params": {"symbol": "ES", "interval": "1h"},
                    "type": "market_profile",
                },
            }
        ],
    }

    runtime = make_runtime(strategies_meta=[strategy])

    overlays = runtime._indicator_overlay_entries(
        strategy,
        "2024-01-01T00:00:00Z",
        "2024-01-02T00:00:00Z",
        "1h",
        "ES",
        "timescale",
        "cme",
    )

    assert overlays and overlays[0]["ind_id"] == "mpf-1"
    assert "overlay_options" not in captured["kwargs"] or captured["kwargs"].get("overlay_options") is None


@pytest.mark.unit
def test_visible_overlays_hide_future_profiles_and_markers():
    runtime = make_runtime()
    runtime.state["status"] = "running"
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    candles = [
        Candle(
            time=base + timedelta(hours=idx),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
        )
        for idx in range(0, 48)
    ]
    runtime._primary_series = SimpleNamespace(candles=candles)
    runtime._bar_index = 24

    first_start = int(base.timestamp())
    first_end = int((base + timedelta(hours=23)).timestamp())
    future_start = int((base + timedelta(days=3)).timestamp())
    future_end = int((base + timedelta(days=4)).timestamp())

    runtime._chart_overlays = [
        {
            "type": "market_profile",
            "payload": {
                "boxes": [
                    {
                        "x1": first_start,
                        "x2": first_end + 10_000,
                        "start": first_start,
                        "end": first_end,
                        "extend": True,
                    },
                    {
                        "x1": future_start,
                        "x2": future_end + 10_000,
                        "start": future_start,
                        "end": future_end,
                        "extend": True,
                    },
                ],
                "markers": [
                    {"time": first_start, "subtype": "touch", "price": 100.0},
                    {"time": future_start, "subtype": "touch", "price": 101.0},
                ],
                "touchPoints": [
                    {"time": first_start, "price": 100.0},
                    {"time": future_start, "price": 101.0},
                ],
            },
        }
    ]

    visible = runtime._visible_overlays()

    assert visible, "expected at least one overlay"
    payload = visible[0]["payload"]
    assert len(payload.get("boxes", [])) == 1
    assert payload["boxes"][0]["start"] == first_start
    assert all(entry.get("time") == first_start for entry in payload.get("markers", []))
    assert all(entry.get("time") == first_start for entry in payload.get("touchPoints", []))


@pytest.mark.unit
def test_performance_meta_merges_indicator_and_atm_data(monkeypatch):
    stored_strategy = {
        "id": "strategy-1",
        "name": "Breakout",
        "symbols": ["ES"],
        "timeframe": "1h",
        "indicator_links": [
            {
                "indicator_id": "ind-1",
                "indicator_snapshot": {
                    "name": "Market profile",
                    "type": "market_profile",
                    "params": {"symbol": "ES", "interval": "1h"},
                },
            }
        ],
        "atm_template": {
            "contracts": 2,
            "take_profit_orders": [
                {"ticks": 10, "contracts": 1},
                {"ticks": 30, "contracts": 1},
            ],
        },
    }

    monkeypatch.setattr(
        bot_service,
        "load_strategies",
        lambda: [stored_strategy],
    )

    bot = {
        "id": "bot-1",
        "name": "Test bot",
        "strategy_ids": ["strategy-1"],
        "strategies_meta": [
            {
                "id": "strategy-1",
                "symbols": ["ES"],
                "datasource": "TIMESCALE",
            }
        ],
    }

    meta = bot_service._performance_meta(bot)

    assert meta["strategies"], "Expected at least one strategy entry"
    entry = meta["strategies"][0]
    assert entry["indicators"][0]["id"] == "ind-1"
    assert entry["atm_template"]["take_profit_orders"][0]["ticks"] == 10


@pytest.mark.unit
def test_ladder_risk_engine_uses_strategy_template():
    template = {
        "contracts": 4,
        "stop_ticks": 12,
        "take_profit_orders": [
            {"ticks": 10, "contracts": 1, "label": "Scout"},
            {"ticks": 25, "contracts": 3, "label": "Runner"},
        ],
        "breakeven": {"target_index": 0},
    }
    instrument = {"tick_size": 0.25, "quote_currency": "USD"}
    engine = LadderRiskEngine(template, instrument=instrument)
    candle = Candle(
        time=datetime.utcnow(),
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.0,
    )

    trade = engine.maybe_enter(candle, "long")

    assert trade is not None
    assert len(trade.legs) == 2
    assert trade.legs[0].name == "Scout"
    assert trade.legs[0].contracts == 1
    assert trade.legs[1].contracts == 3
    expected_stop = candle.close - template["stop_ticks"] * instrument["tick_size"]
    assert trade.stop_price == pytest.approx(expected_stop)


@pytest.mark.unit
def test_ladder_risk_engine_stats_count_trade_outcomes():
    template = {
        "contracts": 3,
        "stop_ticks": 2,
        "take_profit_orders": [
            {"ticks": 1, "contracts": 1},
            {"ticks": 2, "contracts": 2},
        ],
        "breakeven": {"target_index": 0},
    }
    instrument = {"tick_size": 1.0, "quote_currency": "USD"}
    engine = LadderRiskEngine(template, instrument=instrument)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)

    first_entry = Candle(time=start, open=100.0, high=100.0, low=100.0, close=100.0)
    engine.maybe_enter(first_entry, "long")

    # Hit the first target, then stop out remaining contracts at breakeven
    engine.step(
        Candle(
            time=start + timedelta(minutes=1),
            open=100.5,
            high=101.2,
            low=99.8,
            close=101.0,
        )
    )
    engine.step(
        Candle(
            time=start + timedelta(minutes=2),
            open=100.2,
            high=100.3,
            low=99.5,
            close=99.8,
        )
    )

    second_entry = Candle(
        time=start + timedelta(minutes=10),
        open=102.0,
        high=102.0,
        low=102.0,
        close=102.0,
    )
    engine.maybe_enter(second_entry, "long")
    engine.step(
        Candle(
            time=start + timedelta(minutes=11),
            open=102.0,
            high=102.5,
            low=99.0,
            close=100.0,
        )
    )

    stats = engine.stats()

    assert stats["total_trades"] == 2
    assert stats["wins"] == 1
    assert stats["losses"] == 1
    assert stats["breakeven_trades"] == 0
    assert stats["win_rate"] == pytest.approx(0.5)
