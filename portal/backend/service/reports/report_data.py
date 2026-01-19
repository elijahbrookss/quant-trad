"""Thin data-access layer for report-related storage queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ..storage import storage


def list_runs(
    *,
    run_type: str,
    status: str,
    bot_id: Optional[str] = None,
    timeframe: Optional[str] = None,
    started_after: Optional[str] = None,
    started_before: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return storage.list_bot_runs(
        run_type=run_type,
        status=status,
        bot_id=bot_id,
        timeframe=timeframe,
        started_after=started_after,
        started_before=started_before,
    )


def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    return storage.get_bot_run(run_id)


def list_trades_for_run(run_id: str) -> List[Dict[str, Any]]:
    return storage.list_bot_trades_for_run(run_id)


def list_trade_events_for_trades(trade_ids: Sequence[str]) -> List[Dict[str, Any]]:
    return storage.list_bot_trade_events_for_trades(trade_ids)


def find_instrument(
    datasource: Optional[str],
    exchange: Optional[str],
    symbol: str,
) -> Optional[Dict[str, Any]]:
    return storage.find_instrument(datasource, exchange, symbol)


__all__ = [
    "find_instrument",
    "get_run",
    "list_runs",
    "list_trade_events_for_trades",
    "list_trades_for_run",
]
