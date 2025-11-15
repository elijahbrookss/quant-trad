import math
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from portal.backend.service.bot_runtime import BotRuntime, Candle


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
