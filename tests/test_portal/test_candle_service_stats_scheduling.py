from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from indicators.config import DataContext
from portal.backend.service.market import candle_service


def _frame():
    return pd.DataFrame(
        {"open": [1.0, 2.0], "high": [1.0, 2.0], "low": [1.0, 2.0], "close": [1.0, 2.0]},
        index=pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"], utc=True),
    )


def test_schedule_stats_for_context_skips_enqueue_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued: list[dict] = []
    monkeypatch.setattr(candle_service, "enqueue_stats_job", lambda **kwargs: enqueued.append(dict(kwargs)))

    ctx = DataContext(
        symbol="BTC",
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T00:02:00Z",
        interval="1m",
        instrument_id="inst-1",
        schedule_stats=False,
    )

    candle_service._schedule_stats_for_context(_frame(), ctx)

    assert enqueued == []


def test_schedule_stats_for_context_enqueues_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued: list[dict] = []
    monkeypatch.setattr(candle_service, "enqueue_stats_job", lambda **kwargs: enqueued.append(dict(kwargs)))

    ctx = DataContext(
        symbol="BTC",
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T00:02:00Z",
        interval="1m",
        instrument_id="inst-1",
        schedule_stats=True,
    )

    candle_service._schedule_stats_for_context(_frame(), ctx)

    assert len(enqueued) == 1
    assert enqueued[0]["instrument_id"] == "inst-1"
    assert enqueued[0]["timeframe_seconds"] == 60
