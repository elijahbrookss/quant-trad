"""Database access helpers for report exports.

Centralizes SQL used by exporter so schema changes stay in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


def _isoformat(value: Any) -> Any:
    # Local copy to avoid circular imports; exporter uses the same semantics.
    from datetime import timezone

    if not value:
        return None
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, sort_keys=True, ensure_ascii=True)


@dataclass(frozen=True)
class ExportTables:
    candles_raw: str
    derivatives_state: str
    candle_stats: str
    regime_stats: str


class ReportExportRepository:
    def __init__(self, engine: Engine, tables: ExportTables) -> None:
        self.engine = engine
        self.tables = tables

    # Candles -----------------------------------------------------------------
    def fetch_candles(
        self,
        instrument_id: str,
        timeframe_seconds: int,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        query = text(
            f"""
            SELECT instrument_id, timeframe_seconds, candle_time, close_time, open, high, low, close, volume, trade_count,
                   is_closed, source_time, inserted_at
            FROM {self.tables.candles_raw}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND candle_time BETWEEN :start AND :end
            ORDER BY candle_time
            """
        )
        rows: List[Dict[str, Any]] = []
        with self.engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "start": start,
                    "end": end,
                },
            )
            for row in result.mappings():
                rows.append(
                    {
                        "instrument_id": row["instrument_id"],
                        "timeframe_seconds": row["timeframe_seconds"],
                        "candle_time": _isoformat(row["candle_time"]),
                        "close_time": _isoformat(row["close_time"]),
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                        "trade_count": row["trade_count"],
                        "is_closed": row["is_closed"],
                        "source_time": _isoformat(row["source_time"]),
                        "inserted_at": _isoformat(row["inserted_at"]),
                    }
                )
        return rows

    # Derivatives -------------------------------------------------------------
    def fetch_derivatives_state(
        self,
        instrument_id: str,
        start: datetime,
        end: datetime,
    ) -> List[Dict[str, Any]]:
        query = text(
            f"""
            SELECT instrument_id, observed_at, source_time, open_interest, open_interest_value, funding_rate,
                   funding_time, mark_price, index_price, premium_rate, premium_index, next_funding_time, inserted_at
            FROM {self.tables.derivatives_state}
            WHERE instrument_id = :instrument_id
              AND observed_at BETWEEN :start AND :end
            ORDER BY observed_at
            """
        )
        rows: List[Dict[str, Any]] = []
        with self.engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_id": instrument_id,
                    "start": start,
                    "end": end,
                },
            )
            for row in result.mappings():
                rows.append(
                    {
                        "instrument_id": row["instrument_id"],
                        "observed_at": _isoformat(row["observed_at"]),
                        "source_time": _isoformat(row["source_time"]),
                        "open_interest": row["open_interest"],
                        "open_interest_value": row["open_interest_value"],
                        "funding_rate": row["funding_rate"],
                        "funding_time": _isoformat(row["funding_time"]),
                        "mark_price": row["mark_price"],
                        "index_price": row["index_price"],
                        "premium_rate": row["premium_rate"],
                        "premium_index": row["premium_index"],
                        "next_funding_time": _isoformat(row["next_funding_time"]),
                        "inserted_at": _isoformat(row["inserted_at"]),
                    }
                )
        return rows

    # Candle stats ------------------------------------------------------------
    def fetch_candle_stats(
        self,
        instrument_id: str,
        timeframe_seconds: int,
        start: datetime,
        end: datetime,
        stats_versions: Sequence[str],
        stats_key_limit: int,
    ) -> List[Dict[str, Any]]:
        if not stats_versions:
            return []
        query = text(
            f"""
            SELECT instrument_id, timeframe_seconds, candle_time, stats_version, computed_at, stats
            FROM {self.tables.candle_stats}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND candle_time BETWEEN :start AND :end
              AND stats_version IN :stats_versions
            ORDER BY candle_time
            """
        ).bindparams(bindparam("stats_versions", expanding=True))
        rows: List[Dict[str, Any]] = []
        with self.engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "start": start,
                    "end": end,
                    "stats_versions": list(stats_versions),
                },
            )
            for row in result.mappings():
                stats = dict(row["stats"] or {})
                payload = {
                    "instrument_id": row["instrument_id"],
                    "timeframe_seconds": row["timeframe_seconds"],
                    "candle_time": _isoformat(row["candle_time"]),
                    "stats_version": row["stats_version"],
                    "computed_at": _isoformat(row["computed_at"]),
                    "stats_json": _json_dumps(stats),
                    **stats,
                }
                rows.append(payload)
        return rows

    # Regime stats ------------------------------------------------------------
    def fetch_regime_stats(
        self,
        instrument_id: str,
        timeframe_seconds: int,
        start: datetime,
        end: datetime,
        regime_versions: Sequence[str],
    ) -> List[Dict[str, Any]]:
        if not regime_versions:
            return []
        query = text(
            f"""
            SELECT instrument_id, timeframe_seconds, candle_time, regime_version, computed_at, regime
            FROM {self.tables.regime_stats}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND candle_time BETWEEN :start AND :end
              AND regime_version IN :regime_versions
            ORDER BY candle_time
            """
        ).bindparams(bindparam("regime_versions", expanding=True))
        rows: List[Dict[str, Any]] = []
        with self.engine.begin() as conn:
            result = conn.execute(
                query,
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "start": start,
                    "end": end,
                    "regime_versions": list(regime_versions),
                },
            )
            for row in result.mappings():
                regime = dict(row["regime"] or {})
                volatility = regime.get("volatility") or {}
                structure = regime.get("structure") or {}
                expansion = regime.get("expansion") or {}
                liquidity = regime.get("liquidity") or {}
                payload = {
                    "instrument_id": row["instrument_id"],
                    "timeframe_seconds": row["timeframe_seconds"],
                    "candle_time": _isoformat(row["candle_time"]),
                    "regime_version": row["regime_version"],
                    "computed_at": _isoformat(row["computed_at"]),
                    "regime_json": _json_dumps(regime),
                    "volatility_state": volatility.get("state"),
                    "structure_state": structure.get("state"),
                    "expansion_state": expansion.get("state"),
                    "liquidity_state": liquidity.get("state"),
                    "confidence": regime.get("confidence"),
                    **{f"regime_{k}": v for k, v in regime.items() if k not in {"volatility", "structure", "expansion", "liquidity", "confidence"}},
                }
                rows.append(payload)
        return rows
