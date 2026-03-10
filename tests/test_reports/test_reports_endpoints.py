import uuid

import pytest

pytest.importorskip("fastapi")
pytestmark = pytest.mark.db
from fastapi.testclient import TestClient

from portal.backend.main import app
from portal.backend.service.storage import storage
from tests.helpers.builders.report_storage_builder import (
    build_run_payload,
    build_trade_payload,
    ensure_report_bot,
)


def _iso(ts: str) -> str:
    return ts


def test_reports_list_and_fetch():
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    bot_id = f"bot-{uuid.uuid4().hex[:6]}"
    summary = {
        "net_pnl": 25.0,
        "total_return": 0.025,
        "max_drawdown_pct": 0.05,
        "sharpe": 1.1,
        "total_trades": 2,
    }
    ensure_report_bot(bot_id, name="Test Bot", strategy_id="strategy-1")
    storage.upsert_bot_run(
        build_run_payload(
            run_id=run_id,
            bot_id=bot_id,
            bot_name="Test Bot",
            strategy_id="strategy-1",
            strategy_name="Momentum",
            symbol="BTCUSD",
            timeframe="1h",
            backtest_start=_iso("2024-01-01T00:00:00Z"),
            backtest_end=_iso("2024-01-31T00:00:00Z"),
            summary=summary,
        )
    )
    storage.record_bot_trade(
        build_trade_payload(
            trade_id=f"trade-{uuid.uuid4().hex[:8]}",
            run_id=run_id,
            bot_id=bot_id,
            symbol="BTCUSD",
            direction="long",
            entry_time="2024-01-05T00:00:00Z",
            exit_time="2024-01-06T00:00:00Z",
            gross_pnl=15.0,
            fees_paid=1.0,
            net_pnl=14.0,
        )
    )
    storage.record_bot_trade(
        build_trade_payload(
            trade_id=f"trade-{uuid.uuid4().hex[:8]}",
            run_id=run_id,
            bot_id=bot_id,
            symbol="BTCUSD",
            direction="short",
            entry_time="2024-01-10T00:00:00Z",
            exit_time="2024-01-11T00:00:00Z",
            gross_pnl=12.0,
            fees_paid=1.0,
            net_pnl=11.0,
        )
    )

    client = TestClient(app)
    response = client.get("/api/reports?type=backtest&status=completed")
    assert response.status_code == 200
    body = response.json()
    run_ids = [item["run_id"] for item in body["items"]]
    assert run_id in run_ids

    response = client.get(f"/api/reports/{run_id}")
    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == run_id
    assert payload["summary"]["net_pnl"] == pytest.approx(25.0)
    assert payload["charts"]["equity_curve"]
