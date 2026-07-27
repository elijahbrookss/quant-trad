from __future__ import annotations

from datetime import datetime, timezone

import pytest

from portal.backend.service.reports.summary_metrics import compute_summary


def test_compute_summary_drawdown_preserves_intraday_trade_path() -> None:
    trades = [
        {
            "id": "trade-1",
            "entry_time": "2026-01-02T09:00:00Z",
            "exit_time": "2026-01-02T10:00:00Z",
            "net_pnl": 100.0,
            "fees_paid": 0.0,
        },
        {
            "id": "trade-2",
            "entry_time": "2026-01-02T10:00:00Z",
            "exit_time": "2026-01-02T11:00:00Z",
            "net_pnl": -300.0,
            "fees_paid": 0.0,
        },
        {
            "id": "trade-3",
            "entry_time": "2026-01-02T11:00:00Z",
            "exit_time": "2026-01-02T12:00:00Z",
            "net_pnl": 100.0,
            "fees_paid": 0.0,
        },
    ]

    summary = compute_summary(
        trades,
        {"wallet_start": {"balances": {"USD": 1_000.0}}},
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )

    assert summary["max_drawdown"] == pytest.approx(300.0)
    assert summary["max_drawdown_pct"] == pytest.approx(300.0 / 1_100.0)
    assert summary["net_pnl"] == pytest.approx(-100.0)


def test_compute_summary_drawdown_is_independent_of_input_trade_order() -> None:
    trades = [
        {
            "id": "trade-late",
            "entry_time": "2026-01-02T10:00:00Z",
            "exit_time": "2026-01-02T11:00:00Z",
            "net_pnl": -50.0,
            "fees_paid": 0.0,
        },
        {
            "id": "trade-early",
            "entry_time": "2026-01-02T09:00:00Z",
            "exit_time": "2026-01-02T10:00:00Z",
            "net_pnl": 25.0,
            "fees_paid": 0.0,
        },
    ]

    kwargs = {
        "run_config": {"wallet_start": {"balances": {"USD": 1_000.0}}},
        "start_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2026, 1, 3, tzinfo=timezone.utc),
    }

    forward = compute_summary(trades, **kwargs)
    reverse = compute_summary(list(reversed(trades)), **kwargs)

    assert forward["max_drawdown"] == pytest.approx(50.0)
    assert forward["max_drawdown_pct"] == pytest.approx(50.0 / 1_025.0)
    assert forward["max_drawdown"] == reverse["max_drawdown"]
    assert forward["max_drawdown_pct"] == reverse["max_drawdown_pct"]
