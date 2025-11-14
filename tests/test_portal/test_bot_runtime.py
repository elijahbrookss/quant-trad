import math
from datetime import datetime, timedelta

import pytest

from portal.backend.service.bot_runtime import BotRuntime


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
