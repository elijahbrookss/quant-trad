from __future__ import annotations

from typing import Any, Dict, Optional

from portal.backend.service.storage import storage


def ensure_report_bot(
    bot_id: str,
    *,
    name: Optional[str] = None,
    strategy_id: Optional[str] = None,
    status: str = "idle",
) -> Dict[str, Any]:
    payload = {
        "id": bot_id,
        "name": name or bot_id,
        "strategy_id": strategy_id,
        "run_type": "backtest",
        "status": status,
    }
    storage.upsert_bot(payload)
    bot = storage.get_bot(bot_id)
    if not bot or bot.get("id") != bot_id:
        raise RuntimeError(f"report_test_builder_failed: bot_id={bot_id}")
    return bot


def ensure_report_instrument(
    symbol: str,
    *,
    datasource: str = "local",
    exchange: str = "test",
) -> Dict[str, Any]:
    instrument = storage.upsert_instrument(
        {
            "symbol": symbol,
            "datasource": datasource,
            "exchange": exchange,
        }
    )
    instrument_id = instrument.get("id")
    if not instrument_id:
        raise RuntimeError(f"report_test_builder_failed: instrument_symbol={symbol}")
    return instrument


def build_run_payload(
    *,
    run_id: str,
    bot_id: str,
    bot_name: str,
    strategy_id: str,
    strategy_name: str,
    symbol: str,
    timeframe: str = "1h",
    backtest_start: str = "2024-01-01T00:00:00Z",
    backtest_end: str = "2024-01-31T00:00:00Z",
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
    config_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "bot_id": bot_id,
        "bot_name": bot_name,
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "run_type": "backtest",
        "status": "completed",
        "timeframe": timeframe,
        "datasource": datasource,
        "exchange": exchange,
        "symbols": [symbol],
        "backtest_start": backtest_start,
        "backtest_end": backtest_end,
        "started_at": started_at or backtest_start,
        "ended_at": ended_at or backtest_end,
        "summary": dict(summary or {}),
        "config_snapshot": dict(
            config_snapshot
            or {
                "wallet_start": {"balances": {"USDC": 1000}},
                "date_range": {
                    "start": backtest_start,
                    "end": backtest_end,
                },
                "symbols": [symbol],
                "timeframe": timeframe,
                "strategies": [],
            }
        ),
    }


def build_trade_payload(
    *,
    trade_id: str,
    run_id: str,
    bot_id: str,
    symbol: str,
    direction: str,
    entry_time: str,
    exit_time: str,
    gross_pnl: float,
    fees_paid: float,
    net_pnl: float,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "trade_id": trade_id,
        "run_id": run_id,
        "bot_id": bot_id,
        "symbol": symbol,
        "direction": direction,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "gross_pnl": gross_pnl,
        "fees_paid": fees_paid,
        "net_pnl": net_pnl,
        "status": "closed",
    }
    if extra:
        payload.update(extra)
    return payload
