import math
from datetime import datetime, timedelta

import pytest

from portal.backend.service.bot_runtime import BotRuntime


def make_runtime(**overrides):
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
    return BotRuntime("bot-test", config)


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
