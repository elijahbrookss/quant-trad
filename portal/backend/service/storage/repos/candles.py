"""Candle storage read helpers for reporting contracts."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from data_providers.utils.ohlcv import interval_to_timedelta
from sqlalchemy import text

from ._shared import _parse_optional_timestamp, db


def _seconds_to_timeframe(seconds: int) -> str:
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def get_candle_storage_summary(
    *,
    instrument_id: str,
    timeframe: str,
    start: Any,
    end: Any,
) -> Optional[Dict[str, Any]]:
    """Return persisted candle availability and continuity for a series window."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    if not instrument or not interval:
        return None
    start_at = _parse_optional_timestamp(start)
    end_at = _parse_optional_timestamp(end)
    if start_at is None or end_at is None or end_at <= start_at:
        return None

    timeframe_seconds = int(interval_to_timedelta(interval).total_seconds())
    if timeframe_seconds <= 0:
        return None

    with db.session() as session:
        stats = session.execute(
            text(
                """
                WITH ordered AS (
                    SELECT
                        candle_time,
                        open,
                        high,
                        low,
                        close,
                        lag(candle_time) OVER (ORDER BY candle_time) AS previous_time
                    FROM market_candles_raw
                    WHERE instrument_id = :instrument_id
                      AND timeframe_seconds = :timeframe_seconds
                      AND candle_time >= :start_at
                      AND candle_time <= :end_at
                ),
                base AS (
                    SELECT
                        count(*) AS candle_count,
                        min(candle_time) AS first_candle,
                        max(candle_time) AS last_candle,
                        count(*) FILTER (WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL) AS missing_ohlc_count,
                        count(*) - count(DISTINCT candle_time) AS duplicate_count
                    FROM ordered
                ),
                gaps AS (
                    SELECT
                        count(*) FILTER (
                            WHERE previous_time IS NOT NULL
                              AND extract(epoch FROM (candle_time - previous_time)) > :timeframe_seconds
                        ) AS gap_count,
                        coalesce(
                            sum(
                                greatest(
                                    floor(extract(epoch FROM (candle_time - previous_time)) / :timeframe_seconds)::int - 1,
                                    0
                                )
                            ) FILTER (WHERE previous_time IS NOT NULL),
                            0
                        ) AS missing_count
                    FROM ordered
                )
                SELECT
                    base.candle_count,
                    base.first_candle,
                    base.last_candle,
                    base.missing_ohlc_count,
                    base.duplicate_count,
                    gaps.gap_count,
                    gaps.missing_count
                FROM base CROSS JOIN gaps
                """
            ),
            {
                "instrument_id": instrument,
                "timeframe_seconds": timeframe_seconds,
                "start_at": start_at,
                "end_at": end_at,
            },
        ).mappings().first()
        available = session.execute(
            text(
                """
                SELECT DISTINCT timeframe_seconds
                FROM market_candles_raw
                WHERE instrument_id = :instrument_id
                ORDER BY timeframe_seconds
                """
            ),
            {"instrument_id": instrument},
        ).scalars().all()
    if not stats:
        return None

    return {
        "instrument_id": instrument,
        "timeframe": interval,
        "timeframe_seconds": timeframe_seconds,
        "candle_count": int(stats.get("candle_count") or 0),
        "first_candle": stats.get("first_candle"),
        "last_candle": stats.get("last_candle"),
        "missing_ohlc_count": int(stats.get("missing_ohlc_count") or 0),
        "duplicate_count": int(stats.get("duplicate_count") or 0),
        "gap_count": int(stats.get("gap_count") or 0),
        "missing_count": int(stats.get("missing_count") or 0),
        "available_resolutions": [_seconds_to_timeframe(int(value)) for value in available if value],
    }


def list_candle_closure_evidence(
    *,
    instrument_id: str,
    timeframe: str,
    start: Any,
    end: Any,
) -> List[Dict[str, Any]]:
    """Return provider closure evidence ranges for a series window."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    if not instrument or not interval:
        return []
    start_at = _parse_optional_timestamp(start)
    end_at = _parse_optional_timestamp(end)
    if start_at is None or end_at is None or end_at <= start_at:
        return []
    timeframe_seconds = int(interval_to_timedelta(interval).total_seconds())
    if timeframe_seconds <= 0:
        return []

    with db.session() as session:
        rows = session.execute(
            text(
                """
                SELECT start_ts, end_ts, metadata
                FROM portal_candle_closures
                WHERE instrument_id = :instrument_id
                  AND timeframe_seconds = :timeframe_seconds
                  AND end_ts >= :start_at
                  AND start_ts <= :end_at
                ORDER BY start_ts
                """
            ),
            {
                "instrument_id": instrument,
                "timeframe_seconds": timeframe_seconds,
                "start_at": start_at,
                "end_at": end_at,
            },
        ).mappings().all()

    evidence: List[Dict[str, Any]] = []
    for row in rows:
        metadata = row.get("metadata")
        evidence.append(
            {
                "instrument_id": instrument,
                "timeframe": interval,
                "timeframe_seconds": timeframe_seconds,
                "start": row.get("start_ts"),
                "end": row.get("end_ts"),
                "metadata": dict(metadata) if isinstance(metadata, dict) else {},
            }
        )
    return evidence


def list_candles_for_series(
    *,
    instrument_id: str,
    timeframe: str,
    start: Any,
    end: Any,
    limit: int,
    prefer_latest: bool = False,
) -> List[Dict[str, Any]]:
    """Return persisted source candles for a bounded chart/debug window."""

    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    if not instrument or not interval:
        return []
    start_at = _parse_optional_timestamp(start)
    end_at = _parse_optional_timestamp(end)
    if start_at is None and end_at is None:
        return []
    if start_at is not None and end_at is not None and end_at <= start_at:
        return []
    timeframe_seconds = int(interval_to_timedelta(interval).total_seconds())
    if timeframe_seconds <= 0:
        return []
    normalized_limit = max(1, min(int(limit or 320), 2000))
    order_sql = "DESC" if prefer_latest else "ASC"
    predicates = [
        "instrument_id = :instrument_id",
        "timeframe_seconds = :timeframe_seconds",
    ]
    params: Dict[str, Any] = {
        "instrument_id": instrument,
        "timeframe_seconds": timeframe_seconds,
        "limit": normalized_limit,
    }
    if start_at is not None:
        predicates.append("candle_time >= :start_at")
        params["start_at"] = start_at
    if end_at is not None:
        predicates.append("candle_time < :end_at")
        params["end_at"] = end_at
    with db.session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT candle_time, open, high, low, close, volume
                FROM market_candles_raw
                WHERE {' AND '.join(predicates)}
                ORDER BY candle_time {order_sql}
                LIMIT :limit
                """
            ),
            params,
        ).mappings().all()
    normalized = [
        {
            "time": int(row["candle_time"].timestamp()),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]) if row.get("volume") is not None else None,
        }
        for row in rows
        if row.get("candle_time") is not None
    ]
    return sorted(normalized, key=lambda row: int(row["time"]))


__all__ = ["get_candle_storage_summary", "list_candle_closure_evidence", "list_candles_for_series"]
