from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Dict, Optional


CHART_RETRIEVAL_SCHEMA_VERSION = 3


def _chart_candle_contract(candle: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "time": candle.get("time"),
        "open": candle.get("open"),
        "high": candle.get("high"),
        "low": candle.get("low"),
        "close": candle.get("close"),
        "end": candle.get("end"),
        "atr": candle.get("atr"),
        "volume": candle.get("volume"),
        "range": candle.get("range"),
    }


_CHART_TRADE_FIELDS = (
    "event_id",
    "event_name",
    "event_ts",
    "trade_id",
    "symbol_key",
    "instrument_id",
    "symbol",
    "timeframe",
    "trade_state",
    "status",
    "side",
    "direction",
    "qty",
    "quantity",
    "entry_time",
    "opened_at",
    "entry_price",
    "stop_price",
    "exit_time",
    "closed_at",
    "exit_price",
    "realized_pnl",
    "event_impact_pnl",
    "trade_net_pnl",
    "gross_pnl",
    "fees_paid",
    "net_pnl",
    "reason_code",
    "close_reason",
    "position_commit_seq",
    "position_commit_seq_status",
    "updated_at",
)


def _chart_trade_contract(trade: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        **{key: trade.get(key) for key in _CHART_TRADE_FIELDS},
        "legs": [dict(entry) for entry in trade.get("legs", ()) if isinstance(entry, Mapping)],
        "metrics": dict(trade.get("metrics") or {}) if isinstance(trade.get("metrics"), Mapping) else {},
    }


def chart_history_response_contract(
    *,
    run_id: str,
    symbol_key: str,
    start_time: Optional[str],
    end_time: Optional[str],
    limit: int,
    candles: Iterable[Mapping[str, Any]],
    trades: Iterable[Mapping[str, Any]],
    trade_evidence: Optional[Mapping[str, Any]],
    overlay_evidence: Optional[Mapping[str, Any]],
    has_more_before: bool,
    has_more_after: bool,
    evidence_source: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    candle_list = [_chart_candle_contract(entry) for entry in candles]
    trade_list = [_chart_trade_contract(entry) for entry in trades]
    returned_start = candle_list[0]["time"] if candle_list else None
    returned_end = candle_list[-1]["time"] if candle_list else None
    return {
        "schema_version": CHART_RETRIEVAL_SCHEMA_VERSION,
        "contract": "botlens_chart_history",
        "run_id": str(run_id),
        "symbol_key": str(symbol_key),
        "order": "asc",
        "range": {
            "start_time": start_time,
            "end_time": end_time,
            "returned_start_time": returned_start,
            "returned_end_time": returned_end,
            "limit": int(limit),
            "has_more_before": bool(has_more_before),
            "has_more_after": bool(has_more_after),
        },
        "candles": candle_list,
        "trades": trade_list,
        "trade_evidence": dict(trade_evidence or {}),
        "overlay_evidence": dict(overlay_evidence or {}),
        "evidence_source": dict(evidence_source or {}),
    }


__all__ = ["CHART_RETRIEVAL_SCHEMA_VERSION", "chart_history_response_contract"]
