from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Dict, Optional


CHART_RETRIEVAL_SCHEMA_VERSION = 1


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


def chart_history_response_contract(
    *,
    run_id: str,
    symbol_key: str,
    start_time: Optional[str],
    end_time: Optional[str],
    limit: int,
    candles: Iterable[Mapping[str, Any]],
    has_more_before: bool,
    has_more_after: bool,
) -> Dict[str, Any]:
    candle_list = [_chart_candle_contract(entry) for entry in candles]
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
    }


__all__ = ["CHART_RETRIEVAL_SCHEMA_VERSION", "chart_history_response_contract"]
