import csv
import io
import json
import uuid
import zipfile
from typing import Dict

import pytest

pytest.importorskip("fastapi")
pytestmark = pytest.mark.db
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from portal.backend.db.session import db
from portal.backend.main import app
from engines.bot_runtime.runtime.event_types import runtime_event_type
from portal.backend.service.market.stats_contract import REGIME_VERSION, STATS_VERSION
from portal.backend.service.storage import storage
from tests.helpers.builders.report_storage_builder import (
    build_run_payload,
    build_trade_payload,
    ensure_report_bot,
    ensure_report_instrument,
)


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
                    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    stats JSONB NOT NULL,
                    PRIMARY KEY (instrument_id, timeframe_seconds, candle_time, stats_version)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS regime_stats (
                    instrument_id TEXT NOT NULL,
                    timeframe_seconds INTEGER NOT NULL,
                    candle_time TIMESTAMPTZ NOT NULL,
                    regime_version TEXT NOT NULL,
                    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    regime JSONB NOT NULL,
                    PRIMARY KEY (instrument_id, timeframe_seconds, candle_time, regime_version)
                )
                """
            )
        )
        conn.execute(
            text("SELECT 1")
        )


def _load_csv(payload: bytes, name: str):
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        with archive.open(name) as handle:
            text_stream = io.TextIOWrapper(handle, newline="")
            return list(csv.DictReader(text_stream))


def _upsert_export_candle(
    conn,
    *,
    instrument_id: str,
    timeframe_seconds: int,
    candle_time: str,
    close_time: str,
    close: float,
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO market_candles_raw (
                instrument_id,
                timeframe_seconds,
                candle_time,
                close_time,
                open,
                high,
                low,
                close,
                volume,
                trade_count,
                is_closed,
                source_time
            ) VALUES (
                :instrument_id,
                :timeframe_seconds,
                :candle_time,
                :close_time,
                :open,
                :high,
                :low,
                :close,
                :volume,
                :trade_count,
                :is_closed,
                :source_time
            )
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "instrument_id": instrument_id,
            "timeframe_seconds": timeframe_seconds,
            "candle_time": candle_time,
            "close_time": close_time,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 0.0,
            "trade_count": 0,
            "is_closed": True,
            "source_time": close_time,
        },
    )


def _upsert_export_candle_stats(
    conn,
    *,
    instrument_id: str,
    timeframe_seconds: int,
    bar_time: str,
    stats: Dict[str, object],
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO candle_stats (
                instrument_id,
                timeframe_seconds,
                candle_time,
                stats_version,
                computed_at,
                stats
            ) VALUES (
                :instrument_id,
                :timeframe_seconds,
                :candle_time,
                :stats_version,
                :computed_at,
                CAST(:stats AS JSONB)
            )
            ON CONFLICT (instrument_id, timeframe_seconds, candle_time, stats_version)
            DO UPDATE SET
                computed_at = EXCLUDED.computed_at,
                stats = EXCLUDED.stats
            """
        ),
        {
            "instrument_id": instrument_id,
            "timeframe_seconds": timeframe_seconds,
            "candle_time": bar_time,
            "stats_version": STATS_VERSION,
            "computed_at": bar_time,
            "stats": json.dumps(dict(stats)),
        },
    )


def _upsert_export_regime_stats(
    conn,
    *,
    instrument_id: str,
    timeframe_seconds: int,
    bar_time: str,
    regime: Dict[str, object],
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO regime_stats (
                instrument_id,
                timeframe_seconds,
                candle_time,
                regime_version,
                computed_at,
                regime
            ) VALUES (
                :instrument_id,
                :timeframe_seconds,
                :candle_time,
                :regime_version,
                :computed_at,
                CAST(:regime AS JSONB)
            )
            ON CONFLICT (instrument_id, timeframe_seconds, candle_time, regime_version)
            DO UPDATE SET
                computed_at = EXCLUDED.computed_at,
                regime = EXCLUDED.regime
            """
        ),
        {
            "instrument_id": instrument_id,
            "timeframe_seconds": timeframe_seconds,
            "candle_time": bar_time,
            "regime_version": REGIME_VERSION,
            "computed_at": bar_time,
            "regime": json.dumps(dict(regime)),
        },
    )


def _record_runtime_signal_event(
    *,
    run_id: str,
    bot_id: str,
    event_id: str,
    seq: int,
    strategy_id: str,
    symbol: str,
    timeframe: str,
    event_ts: str,
) -> None:
    storage.record_bot_runtime_event(
        {
            "event_id": event_id,
            "bot_id": bot_id,
            "run_id": run_id,
            "seq": seq,
            "event_type": runtime_event_type("SIGNAL_EMITTED"),
            "schema_version": 1,
            "event_time": event_ts,
            "known_at": event_ts,
            "payload": {
                "event_id": event_id,
                "event_ts": event_ts,
                "run_id": run_id,
                "bot_id": bot_id,
                "strategy_id": strategy_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "event_name": "SIGNAL_EMITTED",
                "category": "SIGNAL",
                "root_id": event_id,
                "parent_id": None,
                "correlation_id": f"{run_id}:{symbol}:{timeframe}:{event_ts}",
                "reason_code": "SIGNAL_STRATEGY_SIGNAL",
                "payload": {
                    "direction": "long",
                    "qty": 1,
                    "price": 40000.0,
                    "context": {"source": "test"},
                },
            },
        }
    )


def test_report_export_contains_trades_and_events(monkeypatch):
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    bot_id = f"bot-{uuid.uuid4().hex[:6]}"
    trade_id = f"trade-{uuid.uuid4().hex[:8]}"

    monkeypatch.setenv("PG_DSN", db.dsn)
    _ensure_export_tables(db.dsn)

    ensure_report_bot(bot_id, name="Export Bot", strategy_id="strategy-1")
    instrument = ensure_report_instrument("BTCUSD", datasource="local", exchange="test")
    instrument_id = instrument.get("id")
    storage.upsert_bot_run(
        build_run_payload(
            run_id=run_id,
            bot_id=bot_id,
            bot_name="Export Bot",
            strategy_id="strategy-1",
            strategy_name="Momentum",
            symbol="BTCUSD",
            timeframe="1h",
            backtest_start="2024-01-01T00:00:00Z",
            backtest_end="2024-01-02T00:00:00Z",
            datasource="local",
            exchange="test",
            summary={"net_pnl": 10.0},
        )
    )
    engine = create_engine(db.dsn, future=True)
    stats_payload = {
        "tr_pct": 0.02,
        "atr_ratio": 1.25,
        "atr_slope": 0.3,
        "atr_zscore": 0.7,
        "directional_efficiency": 0.9,
        "overlap_pct": 0.15,
        "range_position": 0.55,
        "slope_stability_warmup": True,
    }
    regime_payload = {
        "volatility": {"state": "low"},
        "structure": {"state": "balanced"},
        "expansion": {"state": "expanding"},
        "liquidity": {"state": "deep"},
        "confidence": 0.82,
    }
    with engine.begin() as conn:
        _upsert_export_candle(
            conn,
            instrument_id=instrument_id,
            timeframe_seconds=3600,
            candle_time="2024-01-01T02:00:00Z",
            close_time="2024-01-01T03:00:00Z",
            close=40000.0,
        )
        _upsert_export_candle_stats(
            conn,
            instrument_id=instrument_id,
            timeframe_seconds=3600,
            bar_time="2024-01-01T02:00:00Z",
            stats=stats_payload,
        )
        _upsert_export_regime_stats(
            conn,
            instrument_id=instrument_id,
            timeframe_seconds=3600,
            bar_time="2024-01-01T02:00:00Z",
            regime=regime_payload,
        )
    _record_runtime_signal_event(
        run_id=run_id,
        bot_id=bot_id,
        event_id=f"{run_id}:runtime:2",
        seq=2,
        strategy_id="strategy-1",
        symbol="BTCUSD",
        timeframe="1h",
        event_ts="2024-01-01T02:00:00Z",
    )

    entry_time = "2024-01-01T02:00:00Z"
    storage.record_bot_trade(
        build_trade_payload(
            trade_id=trade_id,
            run_id=run_id,
            bot_id=bot_id,
            symbol="BTCUSD",
            direction="long",
            entry_time=entry_time,
            exit_time="2024-01-01T03:00:00Z",
            gross_pnl=12.0,
            fees_paid=1.0,
            net_pnl=11.0,
            extra={
                "timeframe": "1h",
                "timeframe_seconds": 3600,
                "instrument_id": instrument_id,
                "metrics": {},
            },
        )
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
    response = client.post(f"/api/reports/{run_id}/export", json={"stats_versions": [STATS_VERSION]})
    assert response.status_code == 200

    trades = _load_csv(response.content, "trades.csv")
    events = _load_csv(response.content, "trade_events.csv")
    candle_stats = _load_csv(response.content, "candle_stats_flat.csv")
    regime_stats = _load_csv(response.content, "regime_stats_flat.csv")
    decision_ledger = _load_csv(response.content, "decision_ledger.csv")
    trade_ids = {row["trade_id"] for row in trades}
    event_trade_ids = {row["trade_id"] for row in events}

    assert trade_id in trade_ids
    assert trade_id in event_trade_ids
    assert len(candle_stats) == 1
    assert candle_stats[0]["instrument_id"] == instrument_id
    assert len(regime_stats) == 1
    assert regime_stats[0]["instrument_id"] == instrument_id
    assert len(decision_ledger) == 1
    assert decision_ledger[0]["decision_type"] == "signal"
    assert decision_ledger[0]["action"] == "strategy_signal"

    metrics = json.loads(trades[0]["metrics_json"])
    assert metrics["entry_atr_ratio"] == 1.25
    assert metrics["entry_tr_pct"] == 0.02
    assert metrics["entry_overlap_pct"] == 0.15
    assert metrics["entry_range_position"] == 0.55
    assert metrics["entry_directional_efficiency"] == 0.9
    assert metrics["entry_stats_warmup"] is True
    assert metrics["entry_regime_missing"] is False
    assert metrics["entry_fallback_used"] is False
    assert metrics["entry_volatility_state"] == "low"
    assert metrics["entry_structure_state"] == "balanced"
    assert metrics["entry_expansion_state"] == "expanding"
    assert metrics["entry_liquidity_state"] == "deep"
    assert metrics["entry_regime_confidence"] == 0.82


def test_report_export_entry_metrics_falls_back_to_regime(monkeypatch):
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    bot_id = f"bot-{uuid.uuid4().hex[:6]}"
    trade_id = f"trade-{uuid.uuid4().hex[:8]}"

    monkeypatch.setenv("PG_DSN", db.dsn)
    _ensure_export_tables(db.dsn)

    ensure_report_bot(bot_id, name="Regime Fallback", strategy_id="strategy-2")
    instrument = ensure_report_instrument("ETHUSD", datasource="local", exchange="test")
    instrument_id = instrument.get("id")
    storage.upsert_bot_run(
        build_run_payload(
            run_id=run_id,
            bot_id=bot_id,
            bot_name="Regime Fallback",
            strategy_id="strategy-2",
            strategy_name="RegimeOnly",
            symbol="ETHUSD",
            timeframe="1h",
            backtest_start="2024-01-01T00:00:00Z",
            backtest_end="2024-01-02T00:00:00Z",
            datasource="local",
            exchange="test",
            summary={"net_pnl": 15.0},
            config_snapshot={
                "wallet_start": {"balances": {"USDC": 2000}},
                "date_range": {
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-02T00:00:00Z",
                },
                "symbols": ["ETHUSD"],
                "timeframe": "1h",
                "strategies": [],
            },
        )
    )
    regime_payload = {
        "volatility": {"state": "high", "atr_zscore": 1.8, "tr_pct": 0.045, "atr_ratio": 1.35},
        "structure": {
            "state": "trend",
            "directional_efficiency": 0.75,
            "slope_stability": 0.1,
            "range_position": 0.6,
        },
        "expansion": {"state": "expanding", "atr_slope": 0.41, "overlap_pct": 0.25},
        "liquidity": {"state": "normal"},
        "confidence": 0.91,
    }
    engine = create_engine(db.dsn, future=True)
    with engine.begin() as conn:
        _upsert_export_candle(
            conn,
            instrument_id=instrument_id,
            timeframe_seconds=3600,
            candle_time="2024-01-01T02:00:00Z",
            close_time="2024-01-01T03:00:00Z",
            close=2750.0,
        )
        _upsert_export_candle_stats(
            conn,
            instrument_id=instrument_id,
            timeframe_seconds=3600,
            bar_time="2024-01-01T02:00:00Z",
            stats={},
        )
        _upsert_export_regime_stats(
            conn,
            instrument_id=instrument_id,
            timeframe_seconds=3600,
            bar_time="2024-01-01T02:00:00Z",
            regime=regime_payload,
        )

    entry_time = "2024-01-01T02:00:00Z"
    storage.record_bot_trade(
        build_trade_payload(
            trade_id=trade_id,
            run_id=run_id,
            bot_id=bot_id,
            symbol="ETHUSD",
            direction="long",
            entry_time=entry_time,
            exit_time="2024-01-01T03:00:00Z",
            gross_pnl=14.0,
            fees_paid=0.9,
            net_pnl=13.1,
            extra={
                "timeframe": "1h",
                "timeframe_seconds": 3600,
                "instrument_id": instrument_id,
                "metrics": {},
            },
        )
    )
    storage.record_bot_trade_event(
        {
            "trade_id": trade_id,
            "bot_id": bot_id,
            "symbol": "ETHUSD",
            "event_type": "exit",
            "price": 2800,
            "pnl": 13.1,
            "event_time": "2024-01-01T03:00:00Z",
        }
    )

    client = TestClient(app)
    response = client.post(f"/api/reports/{run_id}/export", json={"stats_versions": [STATS_VERSION]})
    assert response.status_code == 200
    trades = _load_csv(response.content, "trades.csv")
    metrics = json.loads(trades[0]["metrics_json"])
    assert metrics["entry_tr_pct"] == 0.045
    assert metrics["entry_atr_ratio"] == 1.35
    assert metrics["entry_atr_slope"] == 0.41
    assert metrics["entry_atr_zscore"] == 1.8
    assert metrics["entry_overlap_pct"] == 0.25
    assert metrics["entry_directional_efficiency"] == 0.75
    assert metrics["entry_range_position"] == 0.6
    assert metrics["entry_volatility_state"] == "high"
    assert metrics["entry_structure_state"] == "trend"
    assert metrics["entry_expansion_state"] == "expanding"
    assert metrics["entry_liquidity_state"] == "normal"
    assert metrics["entry_regime_confidence"] == 0.91
