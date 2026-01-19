import csv
import io
import uuid
import zipfile

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from portal.backend.db.session import db
from portal.backend.main import app
from portal.backend.service.storage import storage


def _ensure_export_tables(dsn: str) -> None:
    engine = create_engine(dsn, future=True)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS market_candles_raw (
                    instrument_id TEXT NOT NULL,
                    timeframe_seconds INTEGER NOT NULL,
                    candle_time TIMESTAMPTZ NOT NULL,
                    close_time TIMESTAMPTZ,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    trade_count BIGINT,
                    is_closed BOOLEAN,
                    source_time TIMESTAMPTZ,
                    inserted_at TIMESTAMPTZ
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS derivatives_market_state (
                    instrument_id TEXT NOT NULL,
                    observed_at TIMESTAMPTZ NOT NULL,
                    source_time TIMESTAMPTZ,
                    open_interest DOUBLE,
                    open_interest_value DOUBLE,
                    funding_rate DOUBLE,
                    funding_time TIMESTAMPTZ,
                    mark_price DOUBLE,
                    index_price DOUBLE,
                    premium_rate DOUBLE,
                    premium_index DOUBLE,
                    next_funding_time TIMESTAMPTZ,
                    inserted_at TIMESTAMPTZ
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS candle_stats (
                    instrument_id TEXT NOT NULL,
                    timeframe_seconds INTEGER NOT NULL,
                    candle_time TIMESTAMPTZ NOT NULL,
                    stats_version TEXT NOT NULL,
                    computed_at TIMESTAMPTZ,
                    stats JSON
                )
                """
            )
        )


def _load_csv(payload: bytes, name: str):
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        with archive.open(name) as handle:
            text_stream = io.TextIOWrapper(handle, newline="")
            return list(csv.DictReader(text_stream))


def test_report_export_contains_trades_and_events(monkeypatch):
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    bot_id = f"bot-{uuid.uuid4().hex[:6]}"
    trade_id = f"trade-{uuid.uuid4().hex[:8]}"

    monkeypatch.setenv("PG_DSN", db.dsn)
    _ensure_export_tables(db.dsn)

    storage.upsert_instrument(
        {
            "symbol": "BTCUSD",
            "datasource": "local",
            "exchange": "test",
        }
    )
    storage.upsert_bot_run(
        {
            "run_id": run_id,
            "bot_id": bot_id,
            "bot_name": "Export Bot",
            "strategy_id": "strategy-1",
            "strategy_name": "Momentum",
            "run_type": "backtest",
            "status": "completed",
            "timeframe": "1h",
            "datasource": "local",
            "exchange": "test",
            "symbols": ["BTCUSD"],
            "backtest_start": "2024-01-01T00:00:00Z",
            "backtest_end": "2024-01-02T00:00:00Z",
            "started_at": "2024-01-01T00:00:00Z",
            "ended_at": "2024-01-02T00:00:00Z",
            "summary": {"net_pnl": 10.0},
            "config_snapshot": {
                "wallet_start": {"balances": {"USDC": 1000}},
                "date_range": {"start": "2024-01-01T00:00:00Z", "end": "2024-01-02T00:00:00Z"},
                "symbols": ["BTCUSD"],
                "timeframe": "1h",
                "strategies": [],
            },
        }
    )
    storage.record_bot_trade(
        {
            "trade_id": trade_id,
            "run_id": run_id,
            "bot_id": bot_id,
            "symbol": "BTCUSD",
            "direction": "long",
            "entry_time": "2024-01-01T02:00:00Z",
            "exit_time": "2024-01-01T03:00:00Z",
            "gross_pnl": 12.0,
            "fees_paid": 1.0,
            "net_pnl": 11.0,
            "status": "closed",
        }
    )
    storage.record_bot_trade_event(
        {
            "trade_id": trade_id,
            "bot_id": bot_id,
            "symbol": "BTCUSD",
            "event_type": "exit",
            "price": 41000,
            "pnl": 11.0,
            "event_time": "2024-01-01T03:00:00Z",
        }
    )

    client = TestClient(app)
    response = client.post(f"/api/reports/{run_id}/export", json={})
    assert response.status_code == 200

    trades = _load_csv(response.content, "trades.csv")
    events = _load_csv(response.content, "trade_events.csv")
    trade_ids = {row["trade_id"] for row in trades}
    event_trade_ids = {row["trade_id"] for row in events}

    assert trade_id in trade_ids
    assert trade_id in event_trade_ids
